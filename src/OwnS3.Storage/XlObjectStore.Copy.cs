namespace OwnS3.Storage;

// CopyObject (перенос частей драйвом — хардлинк/побайтовое, Q2) и
// GetObjectAttributes (conditional по GET-семантике, P10).
public sealed partial class XlObjectStore
{
    private const long MaxCopySize = 5L * 1024 * 1024 * 1024; // 5 ГБ (канон 02 §1)

    public async Task<PutResult> CopyObjectAsync(CopyRequest request, CancellationToken ct)
    {
        // 1. Источник (bkp-фолбэк tolerated — warning внутри драйва)
        EnsureBucket(request.SourceBucket);
        var src = Drive.TryReadMeta(request.SourceBucket, request.SourceKey)
            ?? throw new ObjectStoreException(ObjectStoreErrorCode.NoSuchKey);
        // 2. Conditional источника: copy — не GET/HEAD (If-Modified-Since не
        // применяется); NotModified ИЛИ PreconditionFailed → 412
        var outcome = ConditionalEvaluator.Evaluate(request.SourceConditions, src.ETag, src.ModTime,
            ifModifiedSinceApplies: false);
        if (outcome != ConditionalOutcome.Proceed)
            throw new ObjectStoreException(ObjectStoreErrorCode.PreconditionFailed);
        // 3. Лимит копии (App не валидирует copy по длине)
        if (src.Size > MaxCopySize)
            throw new ObjectStoreException(ObjectStoreErrorCode.EntityTooLarge);
        // 4. Приёмник + staging: перенос частей источника драйвом (простой PUT —
        //    ровно part.1; multipart — все, М8); etag/sha256/Size наследуются
        EnsureBucket(request.DestBucket);
        using var staging = Drive.CreateStaging(request.DestBucket, request.DestKey);
        try
        {
            Drive.CopyPartsToStaging(staging, request.SourceBucket, request.SourceKey, src);
            // 5. xl.meta: метаданные по директиве; etag/sha256/Size наследованы;
            //    versionId/modTime новые (versionId нормализует драйв)
            var (contentType, userMetadata) = request.ReplaceMetadata && request.NewMetadata is not null
                ? (request.NewMetadata.ContentType, request.NewMetadata.UserMetadata)
                : (src.ContentType, src.UserMetadata);
            var record = new XlMetaRecord(Guid.Empty, src.Size, timeProvider.GetUtcNow(), src.ETag,
                contentType, userMetadata, EmptyHeaders, src.ContentSha256);
            Drive.WriteMeta(staging, record);
            await Task.Yield(); // async-контракт при синхронном файловом IO
            return Drive.Commit(request.DestBucket, request.DestKey, record, staging);
        }
        finally
        {
            staging.Dispose();
        }
    }

    // GetObjectAttributes (P10): conditional по таблице канона 02 §1 как
    // GET-семантика; unversioned-объект — одна синтетическая часть.
    public Task<ObjectAttributesResult> GetObjectAttributesAsync(string bucket, string key,
        IReadOnlyList<ObjectAttributeName> attributes, int? maxParts, int? partNumberMarker,
        ObjectConditions? conditions, CancellationToken ct)
    {
        // 1. Запись
        EnsureBucket(bucket);
        var meta = Drive.TryReadMeta(bucket, key)
            ?? throw new ObjectStoreException(ObjectStoreErrorCode.NoSuchKey);
        // 2. Conditional (GET-семантика): 412 / 304-обёртка с метаданными (философия §5.3)
        var outcome = ConditionalEvaluator.Evaluate(conditions, meta.ETag, meta.ModTime,
            ifModifiedSinceApplies: true);
        if (outcome == ConditionalOutcome.PreconditionFailed)
            throw new ObjectStoreException(ObjectStoreErrorCode.PreconditionFailed);
        if (outcome == ConditionalOutcome.NotModified)
            return Task.FromResult(new ObjectAttributesResult(null, ToMetadata(key, meta)));
        // 3. Атрибуты: ETag В КАВЫЧКАХ (P8 — хендлер ставит значение в HTTP-заголовок);
        //    LastModified — modTime записи; части — исходные размеры из драйва
        //    (канон 02 §5 GetObjectAttributes; простой PUT — part.1, М8)
        var dataParts = Drive.EnumerateParts(bucket, key, meta);
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
            .ToList();
        var truncated = dataParts.Count(p => p.Number > marker) > selected.Count;
        var parts = new ObjectPartsAttributes(dataParts.Count, marker,
            truncated && selected.Count > 0 ? selected[^1].Number : null, limit, truncated, selected);
        var result = new ObjectAttributes('"' + meta.ETag + '"', meta.Size, "STANDARD", parts, meta.ModTime);
        return Task.FromResult(new ObjectAttributesResult(result, null));
    }
}
