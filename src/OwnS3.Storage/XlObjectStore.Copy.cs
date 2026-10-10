using Microsoft.Extensions.Logging;

namespace OwnS3.Storage;

// CopyObject (хардлинк + fallback на побайтовое копирование, спека §4.3/Q2)
// и GetObjectAttributes (conditional по GET-семантике, P10).
public sealed partial class XlObjectStore
{
    private const long MaxCopySize = 5L * 1024 * 1024 * 1024; // 5 ГБ (канон 02 §1)

    // Тестовый хук fallback: задан и вернул false — принудительное побайтовое копирование.
    internal Func<string, string, bool>? HardLinkProbe;

    public async Task<PutResult> CopyObjectAsync(CopyRequest request, CancellationToken ct)
    {
        // 1. Источник (bkp-фолбэк tolerated — warning в ReadObjectMeta не подходит:
        // EnsureBucket отдельно, ключ другой)
        EnsureBucket(request.SourceBucket);
        XlMetaRecord src;
        try
        {
            src = XlMetaFile.Read(ObjectDir(request.SourceBucket, request.SourceKey), out var fromBackup);
            if (fromBackup)
                _logger.LogWarning("xl.meta источника {Bucket}/{Key} прочитан из страховочной копии",
                    request.SourceBucket, request.SourceKey);
        }
        catch (FileNotFoundException)
        {
            throw new ObjectStoreException(ObjectStoreErrorCode.NoSuchKey);
        }
        // 2. Conditional источника: copy — не GET/HEAD (If-Modified-Since не
        // применяется); NotModified ИЛИ PreconditionFailed → 412
        var outcome = ConditionalEvaluator.Evaluate(request.SourceConditions, src.ETag, src.ModTime,
            ifModifiedSinceApplies: false);
        if (outcome != ConditionalOutcome.Proceed)
            throw new ObjectStoreException(ObjectStoreErrorCode.PreconditionFailed);
        // 3. Лимит копии (App не валидирует copy по длине)
        if (src.Size > MaxCopySize)
            throw new ObjectStoreException(ObjectStoreErrorCode.EntityTooLarge);
        // 4. Приёмник + staging: новый part.1 — жёсткая ссылка на источник (Q2);
        //    при отказе link — побайтовое копирование (warning)
        EnsureBucket(request.DestBucket);
        var versionId = Guid.NewGuid();
        var staging = Path.Combine(volume.TmpDir, Guid.NewGuid().ToString("N"));
        var dataDir = Path.Combine(staging, versionId.ToString("N"));
        Directory.CreateDirectory(dataDir);
        try
        {
            // 4. Пофайловый перенос частей источника (простой PUT — ровно part.1;
            //    multipart — все, М8): хардлинк-точка t37 на каждую часть;
            //    etag/sha256/Size наследуются из записи источника
            foreach (var (_, partPath, _) in EnumerateDataParts(
                         ObjectDir(request.SourceBucket, request.SourceKey), src.DataDirName))
                CopyPart(partPath, Path.Combine(dataDir, Path.GetFileName(partPath)));
            // 5. xl.meta: метаданные по директиве; etag/sha256/Size наследованы;
            //    versionId/modTime новые
            var (contentType, userMetadata) = request.ReplaceMetadata && request.NewMetadata is not null
                ? (request.NewMetadata.ContentType, request.NewMetadata.UserMetadata)
                : (src.ContentType, src.UserMetadata);
            var record = new XlMetaRecord(versionId, src.Size, timeProvider.GetUtcNow(), src.ETag,
                contentType, userMetadata, EmptyHeaders, src.ContentSha256);
            XlMetaFile.Write(staging, record);
            return await CommitStagedObjectAsync(request.DestBucket, request.DestKey, record, staging, ct);
        }
        finally
        {
            if (Directory.Exists(staging))
                Directory.Delete(staging, recursive: true);
        }
    }

    // part.1 копии: хардлинк — мгновенная копия (Q2); на net10.0 BCL-API хардлинка
    // нет (File.CreateHardLink появился в net11) — fallback-копирование всегда.
    // HardLinkProbe (тест-хук) полностью заменяет попытку линка: false —
    // принудительный fallback, true — имитация успешного линка (фиксация
    // линк-ветвления для net11).
    private void CopyPart(string srcPart, string destPart)
    {
        var linked = HardLinkProbe is null ? TryCreateHardLink(srcPart, destPart)
                                           : HardLinkProbe(srcPart, destPart);
        if (linked)
            return;
        _logger.LogWarning(
            "хардлинк недоступен (нет BCL-API в net10.0) — побайтовое копирование {Src} → {Dst}",
            srcPart, destPart);
        using (var src = new FileStream(srcPart, FileMode.Open, FileAccess.Read, FileShare.Read))
        using (var dst = new FileStream(destPart, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            src.CopyTo(dst);
            dst.Flush(flushToDisk: true);
        }
    }

    // Точка включения хардлинка: на net10.0 стандартного API нет — всегда false;
    // при переходе на net11 включить File.CreateHardLink(source, dest) одной
    // правкой здесь (Q2: копирование станет мгновенным).
    private static bool TryCreateHardLink(string source, string dest) => false;

    // GetObjectAttributes (P10): conditional по таблице канона 02 §1 как
    // GET-семантика; unversioned-объект — одна синтетическая часть.
    public Task<ObjectAttributesResult> GetObjectAttributesAsync(string bucket, string key,
        IReadOnlyList<ObjectAttributeName> attributes, int? maxParts, int? partNumberMarker,
        ObjectConditions? conditions, CancellationToken ct)
    {
        // 1. Запись
        EnsureBucket(bucket);
        XlMetaRecord meta;
        try
        {
            meta = XlMetaFile.Read(ObjectDir(bucket, key), out var fromBackup);
            if (fromBackup)
                _logger.LogWarning("xl.meta объекта {Bucket}/{Key} прочитан из страховочной копии", bucket, key);
        }
        catch (FileNotFoundException)
        {
            throw new ObjectStoreException(ObjectStoreErrorCode.NoSuchKey);
        }
        // 2. Conditional (GET-семантика): 412 / 304-обёртка с метаданными (философия §5.3)
        var outcome = ConditionalEvaluator.Evaluate(conditions, meta.ETag, meta.ModTime,
            ifModifiedSinceApplies: true);
        if (outcome == ConditionalOutcome.PreconditionFailed)
            throw new ObjectStoreException(ObjectStoreErrorCode.PreconditionFailed);
        if (outcome == ConditionalOutcome.NotModified)
            return Task.FromResult(new ObjectAttributesResult(null, ToMetadata(key, meta)));
        // 3. Атрибуты: ETag В КАВЫЧКАХ (P8 — хендлер ставит значение в HTTP-заголовок);
        //    LastModified — modTime записи (заголовок Last-Modified ответа);
        //    фильтрацию по запрошенным делает App. Части — реальные файлы dataDir
        //    (канон 02 §5 GetObjectAttributes; простой PUT — part.1, М8)
        var dataParts = EnumerateDataParts(ObjectDir(bucket, key), meta.DataDirName);
        var marker = partNumberMarker ?? 0;
        var limit = maxParts ?? 1000;
        // max-parts=0: пустой список частей без усечения (конвенция ListWalker t37)
        if (limit == 0)
            return Task.FromResult(new ObjectAttributesResult(new ObjectAttributes(
                '"' + meta.ETag + '"', meta.Size, "STANDARD",
                new ObjectPartsAttributes(dataParts.Count, marker, NextPartNumberMarker: null,
                    MaxParts: 0, IsTruncated: false, Parts: []),
                meta.ModTime), null));
        var selected = dataParts.Where(p => p.Number > marker)
            .Take(limit)
            .Select(p => (p.Number, p.Size))
            .ToList();
        var truncated = dataParts.Count(p => p.Number > marker) > selected.Count;
        var parts = new ObjectPartsAttributes(dataParts.Count, marker,
            truncated && selected.Count > 0 ? selected[^1].Number : null, limit, truncated, selected);
        var result = new ObjectAttributes('"' + meta.ETag + '"', meta.Size, "STANDARD", parts, meta.ModTime);
        return Task.FromResult(new ObjectAttributesResult(result, null));
    }
}
