using Microsoft.Extensions.Logging;

namespace OwnS3.Storage;

// Объектные операции XlObjectStore: Put/Delete/DeleteObjects — коммит-цикл
// канона 04 §4 поверх IXlDrive; чтение — conditional-таблица, Range, полная
// SHA-256 сверка ДО передачи любых байтов клиенту.
public sealed partial class XlObjectStore
{
    public async Task<Result<PutResult>> PutObjectAsync(string bucket, string key, Stream body, long contentLength,
        ObjectUploadMetadata metadata, CancellationToken ct)
    {
        // 1. Бакет
        var ensured = EnsureBucket(bucket);
        if (!ensured.IsSuccess)
            return Result<PutResult>.Failed(ensured.Error!);
        try
        {
            // 2. Staging драйва (versionId/dataDir — за драйвом, нормализация в WriteMeta)
            // 3–6. Запись/коммит; ключ не представим xl-путём (сегмент >255 байт,
            // канон 04 §1) — доменный 400 (чистая функция кодирования бросает
            // из staging/коммит-пути драйва — конвертация на границе слоя операции)
            using var staging = Drive.CreateStaging(bucket, key);
            // 3. Тело → part.1 (MD5+SHA-256 исходника, fsync — внутри драйва)
            var digestResult = await Drive.WritePartAsync(staging, 1, body, contentLength, ct);
            if (!digestResult.IsSuccess)
                return Result<PutResult>.Failed(digestResult.Error!);
            var digest = digestResult.Value;
            var record = new XlMetaRecord(Guid.Empty, contentLength, timeProvider.GetUtcNow(),
                digest.ETagHex, metadata.ContentType, metadata.UserMetadata, EmptyHeaders, digest.Sha256Hex);
            var metaResult = Drive.WriteMeta(staging, record);
            if (!metaResult.IsSuccess)
                return Result<PutResult>.Failed(metaResult.Error!);
            // 4–6. Коммит + возврат (ETag HTTP-значение — в кавычках, P8)
            await Task.Yield(); // async-контракт при синхронном файловом IO (P4)
            return Drive.Commit(bucket, key, record, staging);
        }
        catch (XlInvalidArgumentException ex)
        {
            return Result<PutResult>.Failed(ex);
        }
    }

    // Delete канона 04 §4 п.5 через drive; идемпотентность: повторный Delete
    // не находит запись — успех (гварды покрывают краш-остатки чтением bkp).
    public async Task<Result> DeleteObjectAsync(string bucket, string key, CancellationToken ct)
    {
        var ensured = EnsureBucket(bucket);
        if (!ensured.IsSuccess)
            return ensured;
        var lookup = Drive.TryReadMeta(bucket, key);
        if (!lookup.IsSuccess)
            return lookup; // кворумный отказ — Failed
        if (lookup.Value is not XlMetaLookup.Found found)
            return Result.Success(); // идемпотентность (канон 02 §1)
        return Drive.Delete(bucket, key, found.Record);
    }

    // Per-key исходы; quiet фильтрует App (Storage игнорирует).
    public async Task<Result<IReadOnlyList<DeletedKeyResult>>> DeleteObjectsAsync(string bucket,
        IReadOnlyList<string> keys, bool quiet, CancellationToken ct)
    {
        var ensured = EnsureBucket(bucket);
        if (!ensured.IsSuccess)
            return Result<IReadOnlyList<DeletedKeyResult>>.Failed(ensured.Error!);
        var results = new List<DeletedKeyResult>(keys.Count);
        foreach (var key in keys)
        {
            var delete = await DeleteObjectAsync(bucket, key, ct);
            if (delete.IsSuccess)
            {
                results.Add(new DeletedKeyResult(key, Deleted: true, null, null));
            }
            else if (delete.Error is ObjectStoreException ex)
            {
                results.Add(new DeletedKeyResult(key, Deleted: false, ex.Code.ToString(), ex.Message));
            }
            else
            {
                // Отказ всей операции (не per-key доменный) — Failed
                return Result<IReadOnlyList<DeletedKeyResult>>.Failed(delete.Error!);
            }
        }

        return Result<IReadOnlyList<DeletedKeyResult>>.Success(results);
    }

    // ── чтение (спека §4.4) ──

    // Чтение записи объекта; отсутствие → NoSuchKey (преобразование
    // Missing→NoSuchKey на уровне S3-операции, принцип 6 spec).
    private Result<XlMetaRecord> ReadObjectMeta(string bucket, string key)
    {
        var ensured = EnsureBucket(bucket);
        if (!ensured.IsSuccess)
            return Result<XlMetaRecord>.Failed(ensured.Error!);
        var lookup = Drive.TryReadMeta(bucket, key);
        if (!lookup.IsSuccess)
            return Result<XlMetaRecord>.Failed(lookup.Error!);
        return lookup.Value is XlMetaLookup.Found found
            ? Result<XlMetaRecord>.Success(found.Record)
            : Result<XlMetaRecord>.Failed(new ObjectStoreException(ObjectStoreErrorCode.NoSuchKey));
    }

    // Доменное представление записи: ETag HTTP-значение — в кавычках (P8).
    private static ObjectMetadata ToMetadata(string key, XlMetaRecord record) =>
        new(key, '"' + record.ETag + '"', record.Size, record.ModTime, record.ContentType, record.UserMetadata);

    public async Task<Result<ObjectContent>> GetObjectAsync(string bucket, string key, ObjectReadOptions options,
        CancellationToken ct)
    {
        // 1. Запись (с фолбэком bkp — drive)
        var metaResult = ReadObjectMeta(bucket, key);
        if (!metaResult.IsSuccess)
            return Result<ObjectContent>.Failed(metaResult.Error!);
        var meta = metaResult.Value;
        // 2. Conditional (GET-семантика: If-Modified-Since применяется)
        var outcome = ConditionalEvaluator.Evaluate(options.Conditions, meta.ETag, meta.ModTime,
            ifModifiedSinceApplies: true);
        if (outcome == ConditionalOutcome.PreconditionFailed)
            return Result<ObjectContent>.Failed(
                new ObjectStoreException(ObjectStoreErrorCode.PreconditionFailed));
        if (outcome == ConditionalOutcome.NotModified)
            return Result<ObjectContent>.Success(
                new ObjectContent(ToMetadata(key, meta), Stream.Null, NotModified: true, Range: null));
        // 3. Range (заголовок условного диапазона не поддерживается — канон 02 §1);
        // 416 InvalidRange приходит как Failed (трансляция в HTTP — App)
        var resolved = RangeResolver.Resolve(options.Range, meta.Size);
        if (!resolved.IsSuccess)
            return Result<ObjectContent>.Failed(resolved.Error!);
        AppliedByteRange? range = resolved.Value;
        // 4. Полная SHA-256 сверка ДО отдачи: Range не влияет на хэш (Q1)
        var verified = Drive.VerifyFullChecksum(bucket, key, meta);
        if (!verified.IsSuccess)
            return Result<ObjectContent>.Failed(verified.Error!);
        // 5. Тело: составной поток драйва (полный объект или срез Range)
        var opened = range is null
            ? Drive.OpenData(bucket, key, meta, 0, meta.Size)
            : Drive.OpenData(bucket, key, meta, range.Start, range.End - range.Start + 1);
        if (!opened.IsSuccess)
            return Result<ObjectContent>.Failed(opened.Error!);
        return Result<ObjectContent>.Success(
            new ObjectContent(ToMetadata(key, meta), opened.Value, NotModified: false, Range: range));
    }

    public async Task<Result<ObjectContent>> HeadObjectAsync(string bucket, string key, ObjectReadOptions? options,
        CancellationToken ct)
    {
        // Шаги 1–3 те же; данных не читает — сверка не выполняется (канон 04 §2)
        var metaResult = ReadObjectMeta(bucket, key);
        if (!metaResult.IsSuccess)
            return Result<ObjectContent>.Failed(metaResult.Error!);
        var meta = metaResult.Value;
        var outcome = ConditionalEvaluator.Evaluate(options?.Conditions, meta.ETag, meta.ModTime,
            ifModifiedSinceApplies: true);
        if (outcome == ConditionalOutcome.PreconditionFailed)
            return Result<ObjectContent>.Failed(
                new ObjectStoreException(ObjectStoreErrorCode.PreconditionFailed));
        if (outcome == ConditionalOutcome.NotModified)
            return Result<ObjectContent>.Success(
                new ObjectContent(ToMetadata(key, meta), Stream.Null, NotModified: true, Range: null));
        var resolved = RangeResolver.Resolve(options?.Range, meta.Size);
        if (!resolved.IsSuccess)
            return Result<ObjectContent>.Failed(resolved.Error!);
        return Result<ObjectContent>.Success(
            new ObjectContent(ToMetadata(key, meta), Stream.Null, NotModified: false, Range: resolved.Value));
    }
}
