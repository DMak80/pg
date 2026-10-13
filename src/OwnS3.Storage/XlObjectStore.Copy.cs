namespace OwnS3.Storage;

// CopyObject (перенос частей драйвом — хардлинк/побайтовое, Q2) и
// GetObjectAttributes (conditional по GET-семантике, P10).
public sealed partial class XlObjectStore
{
    private const long MaxCopySize = 5L * 1024 * 1024 * 1024; // 5 ГБ (канон 02 §1)

    public async Task<Result<PutResult>> CopyObjectAsync(CopyRequest request, CancellationToken ct)
    {
        // 1. Источник (bkp-фолбэк tolerated — warning внутри драйва)
        var ensuredSrc = EnsureBucket(request.SourceBucket);
        if (!ensuredSrc.IsSuccess)
            return Result<PutResult>.Failed(ensuredSrc.Error!);
        try
        {
            var srcLookup = Drive.TryReadMeta(request.SourceBucket, request.SourceKey);
            if (!srcLookup.IsSuccess)
                return Result<PutResult>.Failed(srcLookup.Error!);
            if (srcLookup.Value is not XlMetaLookup.Found found)
                return Result<PutResult>.Failed(new ObjectStoreException(ObjectStoreErrorCode.NoSuchKey));
            var src = found.Record;
            // 2. Conditional источника: copy — не GET/HEAD (If-Modified-Since не
            // применяется); NotModified ИЛИ PreconditionFailed → 412
            var outcome = ConditionalEvaluator.Evaluate(request.SourceConditions, src.ETag, src.ModTime,
                ifModifiedSinceApplies: false);
            if (outcome != ConditionalOutcome.Proceed)
                return Result<PutResult>.Failed(new ObjectStoreException(ObjectStoreErrorCode.PreconditionFailed));
            // 3. Лимит копии (App не валидирует copy по длине)
            if (src.Size > MaxCopySize)
                return Result<PutResult>.Failed(new ObjectStoreException(ObjectStoreErrorCode.EntityTooLarge));
            // 4. Приёмник + staging: перенос частей источника драйвом (простой PUT —
            //    ровно part.1; multipart — все, М8); etag/sha256/Size наследуются
            var ensuredDest = EnsureBucket(request.DestBucket);
            if (!ensuredDest.IsSuccess)
                return Result<PutResult>.Failed(ensuredDest.Error!);
            // Экранированные броски драйва — конвертация на границе слоя операции:
            // ключ не представим xl-путём (сегмент >255 байт, канон 04 §1) — доменный 400;
            // кворумный отказ staging-пути EC (сет приёмника без онлайн-дисков,
            // multi-set) — доменный 503
            using var staging = Drive.CreateStaging(request.DestBucket, request.DestKey);
            var copied = Drive.CopyPartsToStaging(staging, request.SourceBucket, request.SourceKey, src);
            if (!copied.IsSuccess)
                return Result<PutResult>.Failed(copied.Error!);
            // 5. xl.meta: метаданные по директиве; etag/sha256/Size наследованы;
            //    versionId/modTime новые (versionId нормализует драйв)
            var (contentType, userMetadata) = request.ReplaceMetadata && request.NewMetadata is not null
                ? (request.NewMetadata.ContentType, request.NewMetadata.UserMetadata)
                : (src.ContentType, src.UserMetadata);
            var record = new XlMetaRecord(Guid.Empty, src.Size, timeProvider.GetUtcNow(), src.ETag,
                contentType, userMetadata, EmptyHeaders, src.ContentSha256);
            var metaResult = Drive.WriteMeta(staging, record);
            if (!metaResult.IsSuccess)
                return Result<PutResult>.Failed(metaResult.Error!);
            await Task.Yield(); // async-контракт при синхронном файловом IO
            return Drive.Commit(request.DestBucket, request.DestKey, record, staging);
        }
        catch (Exception ex) when (ex is ObjectStoreException or XlInvalidArgumentException)
        {
            return Result<PutResult>.Failed(ex);
        }
    }

    // GetObjectAttributes (P10): conditional по таблице канона 02 §1 как
    // GET-семантика; unversioned-объект — одна синтетическая часть.
    // NotModified|Full — терминальные Success-варианты GetAttributesOutcome.
    public async Task<Result<GetAttributesOutcome>> GetObjectAttributesAsync(string bucket, string key,
        IReadOnlyList<ObjectAttributeName> attributes, int? maxParts, int? partNumberMarker,
        ObjectConditions? conditions, CancellationToken ct)
    {
        // 1. Запись
        var ensured = EnsureBucket(bucket);
        if (!ensured.IsSuccess)
            return Result<GetAttributesOutcome>.Failed(ensured.Error!);
        var lookup = Drive.TryReadMeta(bucket, key);
        if (!lookup.IsSuccess)
            return Result<GetAttributesOutcome>.Failed(lookup.Error!);
        if (lookup.Value is not XlMetaLookup.Found found)
            return Result<GetAttributesOutcome>.Failed(
                new ObjectStoreException(ObjectStoreErrorCode.NoSuchKey));
        var meta = found.Record;
        // 2. Conditional (GET-семантика): 412 / 304-обёртка с метаданными (философия §5.3)
        var outcome = ConditionalEvaluator.Evaluate(conditions, meta.ETag, meta.ModTime,
            ifModifiedSinceApplies: true);
        if (outcome == ConditionalOutcome.PreconditionFailed)
            return Result<GetAttributesOutcome>.Failed(
                new ObjectStoreException(ObjectStoreErrorCode.PreconditionFailed));
        if (outcome == ConditionalOutcome.NotModified)
            return Result<GetAttributesOutcome>.Success(
                new GetAttributesOutcome.NotModified(ToMetadata(key, meta)));
        // 3. Атрибуты: ETag В КАВЫЧКАХ (P8 — хендлер ставит значение в HTTP-заголовок);
        //    LastModified — modTime записи; части — исходные размеры из драйва
        //    (канон 02 §5 GetObjectAttributes; простой PUT — part.1, М8)
        var dataPartsResult = Drive.EnumerateParts(bucket, key, meta);
        if (!dataPartsResult.IsSuccess)
            return Result<GetAttributesOutcome>.Failed(dataPartsResult.Error!);
        var dataParts = dataPartsResult.Value;
        var marker = partNumberMarker ?? 0;
        var limit = maxParts ?? 1000;
        // max-parts=0: пустой список частей без усечения (конвенция ListWalker t37)
        if (limit == 0)
            return Result<GetAttributesOutcome>.Success(new GetAttributesOutcome.Full(new ObjectAttributes(
                '"' + meta.ETag + '"', meta.Size, "STANDARD",
                new ObjectPartsAttributes(dataParts.Count, marker, NextPartNumberMarker: null,
                    MaxParts: 0, IsTruncated: false, Parts: []),
                meta.ModTime)));
        var selected = dataParts.Where(p => p.Number > marker)
            .Take(limit)
            .ToList();
        var truncated = dataParts.Count(p => p.Number > marker) > selected.Count;
        var parts = new ObjectPartsAttributes(dataParts.Count, marker,
            truncated && selected.Count > 0 ? selected[^1].Number : null, limit, truncated, selected);
        var result = new ObjectAttributes('"' + meta.ETag + '"', meta.Size, "STANDARD", parts, meta.ModTime);
        return Result<GetAttributesOutcome>.Success(new GetAttributesOutcome.Full(result));
    }
}
