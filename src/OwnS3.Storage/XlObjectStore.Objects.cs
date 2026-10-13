using Microsoft.Extensions.Logging;

namespace OwnS3.Storage;

// Объектные операции XlObjectStore: Put/Delete/DeleteObjects — коммит-цикл
// канона 04 §4 поверх IXlDrive; чтение — conditional-таблица, Range, полная
// SHA-256 сверка ДО передачи любых байтов клиенту.
public sealed partial class XlObjectStore
{
    public async Task<PutResult> PutObjectAsync(string bucket, string key, Stream body, long contentLength,
        ObjectUploadMetadata metadata, CancellationToken ct)
    {
        // 1. Бакет
        EnsureBucket(bucket);
        // 2. Staging драйва (versionId/dataDir — за драйвом, нормализация в WriteMeta)
        using var staging = Drive.CreateStaging(bucket, key);
        try
        {
            // 3. Тело → part.1 (MD5+SHA-256 исходника, fsync — внутри драйва)
            var digest = (await Drive.WritePartAsync(staging, 1, body, contentLength, ct)).Throw().Value; // мост t38
            var record = new XlMetaRecord(Guid.Empty, contentLength, timeProvider.GetUtcNow(),
                digest.ETagHex, metadata.ContentType, metadata.UserMetadata, EmptyHeaders, digest.Sha256Hex);
            Drive.WriteMeta(staging, record).Throw(); // мост t38
            // 4–6. Коммит + возврат (ETag HTTP-значение — в кавычках, P8)
            await Task.Yield(); // async-контракт при синхронном файловом IO (P4)
            return Drive.Commit(bucket, key, record, staging).Throw().Value; // мост t38
        }
        finally
        {
            // Staging удаляется при любом отказе — не ждёт чисток (спека §4.3 п.4);
            // после успешного Commit каталога уже нет — Dispose безвреден
            staging.Dispose();
        }
    }

    // Delete канона 04 §4 п.5 через drive; идемпотентность: повторный Delete
    // не находит запись — успех (гварды покрывают краш-остатки чтением bkp).
    public Task DeleteObjectAsync(string bucket, string key, CancellationToken ct)
    {
        EnsureBucket(bucket);
        var meta = UnwrapMeta(Drive.TryReadMeta(bucket, key)); // мост t38: демонтаж в задаче перевода IObjectStore/IReplicaStore
        if (meta is null)
            return Task.CompletedTask; // идемпотентность (канон 02 §1)
        Drive.Delete(bucket, key, meta).Throw(); // мост t38
        return Task.CompletedTask;
    }

    // Per-key исходы; quiet фильтрует App (Storage игнорирует).
    public async Task<IReadOnlyList<DeletedKeyResult>> DeleteObjectsAsync(string bucket,
        IReadOnlyList<string> keys, bool quiet, CancellationToken ct)
    {
        EnsureBucket(bucket);
        var results = new List<DeletedKeyResult>(keys.Count);
        foreach (var key in keys)
        {
            try
            {
                await DeleteObjectAsync(bucket, key, ct);
                results.Add(new DeletedKeyResult(key, Deleted: true, null, null));
            }
            catch (ObjectStoreException ex)
            {
                results.Add(new DeletedKeyResult(key, Deleted: false, ex.Code.ToString(), ex.Message));
            }
        }

        return results;
    }

    // ── чтение (спека §4.4) ──

    // Чтение записи объекта; отсутствие → NoSuchKey.
    private XlMetaRecord ReadObjectMeta(string bucket, string key)
    {
        EnsureBucket(bucket);
        return UnwrapMeta(Drive.TryReadMeta(bucket, key)) // мост t38: демонтаж в задаче перевода IObjectStore/IReplicaStore
            ?? throw new ObjectStoreException(ObjectStoreErrorCode.NoSuchKey);
    }

    // Доменное представление записи: ETag HTTP-значение — в кавычках (P8).
    private static ObjectMetadata ToMetadata(string key, XlMetaRecord record) =>
        new(key, '"' + record.ETag + '"', record.Size, record.ModTime, record.ContentType, record.UserMetadata);

    public Task<ObjectContent> GetObjectAsync(string bucket, string key, ObjectReadOptions options,
        CancellationToken ct)
    {
        // 1. Запись (с фолбэком bkp — drive)
        var meta = ReadObjectMeta(bucket, key);
        // 2. Conditional (GET-семантика: If-Modified-Since применяется)
        var outcome = ConditionalEvaluator.Evaluate(options.Conditions, meta.ETag, meta.ModTime,
            ifModifiedSinceApplies: true);
        if (outcome == ConditionalOutcome.PreconditionFailed)
            throw new ObjectStoreException(ObjectStoreErrorCode.PreconditionFailed);
        if (outcome == ConditionalOutcome.NotModified)
            return Task.FromResult(new ObjectContent(ToMetadata(key, meta), Stream.Null, NotModified: true, Range: null));
        // 3. Range (заголовок условного диапазона не поддерживается — канон 02 §1)
        var range = RangeResolver.Resolve(options.Range, meta.Size);
        // 4. Полная SHA-256 сверка ДО отдачи: Range не влияет на хэш (Q1)
        Drive.VerifyFullChecksum(bucket, key, meta).Throw(); // мост t38: демонтаж в задаче перевода IObjectStore/IReplicaStore
        // 5. Тело: составной поток драйва (полный объект или срез Range)
        Stream body = range is null
            ? Drive.OpenData(bucket, key, meta, 0, meta.Size).Throw().Value // мост t38
            : Drive.OpenData(bucket, key, meta, range.Start, range.End - range.Start + 1).Throw().Value; // мост t38
        return Task.FromResult(new ObjectContent(ToMetadata(key, meta), body, NotModified: false, Range: range));
    }

    public Task<ObjectContent> HeadObjectAsync(string bucket, string key, ObjectReadOptions? options,
        CancellationToken ct)
    {
        // Шаги 1–3 те же; данных не читает — сверка не выполняется (канон 04 §2)
        var meta = ReadObjectMeta(bucket, key);
        var outcome = ConditionalEvaluator.Evaluate(options?.Conditions, meta.ETag, meta.ModTime,
            ifModifiedSinceApplies: true);
        if (outcome == ConditionalOutcome.PreconditionFailed)
            throw new ObjectStoreException(ObjectStoreErrorCode.PreconditionFailed);
        if (outcome == ConditionalOutcome.NotModified)
            return Task.FromResult(new ObjectContent(ToMetadata(key, meta), Stream.Null, NotModified: true, Range: null));
        var range = RangeResolver.Resolve(options?.Range, meta.Size);
        return Task.FromResult(new ObjectContent(ToMetadata(key, meta), Stream.Null, NotModified: false, Range: range));
    }
}
