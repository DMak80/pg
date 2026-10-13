namespace OwnS3.Storage;

// Реализация IReplicaStore поверх IXlDrive (t35, глава 07 §11):
// применение реплики — штатный xl-коммит (staging→WritePart→WriteMeta→Commit,
// глава 04 §4) с ИСХОДНЫМИ метаданными источника; sha-сверка тела ДО коммита;
// репликационные чтения — состояние ключа (LWW), листинг (Utf8ByteOrder),
// fetch-отдача (без VerifyFullChecksum — сверка на приёмнике, глава 07 §6).
// Исходы — Result-конвенция главы 01 §4.
public sealed class ReplicaStore(IXlDrive drive, TimeProvider time) : IReplicaStore
{
    private static readonly IReadOnlyDictionary<string, string> EmptyHeaders =
        new Dictionary<string, string>();

    public async Task<Result> ApplyObjectAsync(string bucket, string key, Stream body, long contentLength,
        ReplicaObjectMetadata metadata, string expectedSha256Hex, CancellationToken ct)
    {
        var ensured = EnsureBucket(bucket);
        if (!ensured.IsSuccess)
            return ensured;
        IXlStaging staging;
        try
        {
            staging = drive.CreateStaging(bucket, key);
        }
        catch (Exception ex) when (ex is ObjectStoreException or XlInvalidArgumentException)
        {
            // Экранированные броски драйва — конвертация на границе слоя (фетчер
            // получает Error-исход → повтор по бэкоффу): ключ не представим xl-путём
            // (сегмент >255 байт, канон 04 §1) — доменный 400; кворумный отказ
            // staging-пути EC (сет ключа без онлайн-дисков, multi-set) — доменный 503
            return Result.Failed(ex);
        }

        await Task.Yield(); // async-контракт при синхронном файловом IO (P4 t37)
        try
        {
            var digestResult = await drive.WritePartAsync(staging, 1, body, contentLength, ct);
            if (!digestResult.IsSuccess)
                return Result.Failed(digestResult.Error!);
            var digest = digestResult.Value;
            if (!string.Equals(digest.Sha256Hex, expectedSha256Hex, StringComparison.OrdinalIgnoreCase))
                return Result.Failed(new ReplicationChecksumMismatchException(
                    $"sha тела реплики не совпал с ожидаемым: bucket={bucket}, key={key}"));
            // xl-запись с ИСХОДНЫМИ метаданными источника (modTime/etag/sha/user-meta)
            var record = new XlMetaRecord(Guid.Empty, contentLength, metadata.ModTime,
                metadata.ETagHex, metadata.ContentType, metadata.UserMetadata, EmptyHeaders,
                metadata.ContentSha256Hex);
            var metaResult = drive.WriteMeta(staging, record);
            if (!metaResult.IsSuccess)
                return Result.Failed(metaResult.Error!);
            return drive.Commit(bucket, key, record, staging);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw; // graceful stop — не исход операции
        }
        catch (Exception ex)
        {
            // Технический отказ чтения тела/записи (обрыв сети фетчера, IO диска) —
            // Failed по конвенции 01 §4: ошибки стора не летят через границу интерфейса
            return Result.Failed(ex);
        }
        finally
        {
            // Сбой/мисматч: staging сносится; после успешного Commit — безвреден
            staging.Dispose();
        }
    }

    public Result DeleteObject(string bucket, string key)
    {
        // Идемпотентно: ключа нет — 204-семантика Delete, ничего не делаем
        var lookup = drive.TryReadMeta(bucket, key);
        if (!lookup.IsSuccess)
            return lookup; // кворумный отказ — Failed
        if (lookup.Value is not XlMetaLookup.Found found)
            return Result.Success(); // ключа нет — идемпотентный успех
        return drive.Delete(bucket, key, found.Record);
    }

    public Result<ReplicaKeyLookup> ReadKeyState(string bucket, string key)
    {
        var lookup = drive.TryReadMeta(bucket, key);
        if (!lookup.IsSuccess)
            return Result<ReplicaKeyLookup>.Failed(lookup.Error!); // кворумный отказ — Failed
        return Result<ReplicaKeyLookup>.Success(lookup.Value is XlMetaLookup.Found found
            ? new ReplicaKeyLookup.Found(
                new ReplicaKeyState(found.Record.ModTime, found.Record.ContentSha256, found.Record.Size))
            : new ReplicaKeyLookup.Missing());
    }

    public Result<ReplicaListingPage> ListReplicationEntries(string bucket, string? marker, int maxKeys)
    {
        var entries = new List<ReplicationListEntry>();
        string? nextMarker = null;
        var exists = drive.BucketExists(bucket);
        if (!exists.IsSuccess)
            return Result<ReplicaListingPage>.Failed(exists.Error!); // кворумный отказ — Failed («503»)
        if (!exists.Value)
            return Result<ReplicaListingPage>.Success(new ReplicaListingPage(entries, null));
        var enumerated = drive.EnumerateObjects(bucket);
        if (!enumerated.IsSuccess)
            return Result<ReplicaListingPage>.Failed(enumerated.Error!);
        var cursor = enumerated.Value;
        while (entries.Count < maxKeys && cursor.PeekKey is not null)
        {
            var key = cursor.PeekKey;
            if (marker is not null && string.CompareOrdinal(key, marker) <= 0)
            {
                cursor.MoveNext();
                continue;
            }
            var meta = cursor.ReadCurrentMeta();
            entries.Add(new ReplicationListEntry(key, meta.ModTime, meta.ContentSha256, meta.Size));
            nextMarker = key;
            cursor.MoveNext();
        }
        // Исчерпали обход раньше лимита — маркер конца (пагинация грани, глава 07 §6)
        if (cursor.PeekKey is null)
            nextMarker = null;
        return Result<ReplicaListingPage>.Success(new ReplicaListingPage(entries, nextMarker));
    }

    public Result<(ObjectMetadata Metadata, Stream Body)> OpenObjectForReplication(string bucket, string key)
    {
        var lookup = drive.TryReadMeta(bucket, key);
        if (!lookup.IsSuccess)
            return Result<(ObjectMetadata, Stream)>.Failed(lookup.Error!);
        if (lookup.Value is not XlMetaLookup.Found found)
            return Result<(ObjectMetadata, Stream)>.Failed(
                new ObjectStoreException(ObjectStoreErrorCode.NoSuchKey));
        var meta = found.Record;
        var metadata = new ObjectMetadata(key, '"' + meta.ETag + '"', meta.Size, meta.ModTime,
            meta.ContentType, meta.UserMetadata);
        var opened = drive.OpenData(bucket, key, meta, 0, meta.Size);
        if (!opened.IsSuccess)
            return Result<(ObjectMetadata, Stream)>.Failed(opened.Error!);
        return Result<(ObjectMetadata, Stream)>.Success((metadata, opened.Value));
    }

    public Result ApplyBucketCreate(string bucket, DateTimeOffset creationDate)
    {
        // Идемпотентно: есть → skip (первая CreationDate сохраняется)
        var exists = drive.BucketExists(bucket);
        if (!exists.IsSuccess)
            return exists; // кворумный отказ — Failed
        if (exists.Value)
            return Result.Success();
        return drive.CreateBucket(bucket, creationDate);
    }

    public Result ApplyBucketDelete(string bucket)
    {
        // Идемпотентно: нет бакета — уже удалён; непустой — Failed(BucketNotEmpty)
        var exists = drive.BucketExists(bucket);
        if (!exists.IsSuccess)
            return exists; // кворумный отказ — Failed
        if (!exists.Value)
            return Result.Success();
        return drive.DeleteBucket(bucket);
    }

    public Result<IReadOnlyList<BucketEntry>> ListBuckets() => drive.ListBuckets();

    // Реплика объекта может прийти раньше события bucket-create (дыра/порядок
    // доставки): бакет создаётся по месту с CreationDate = modTime объекта.
    private Result EnsureBucket(string bucket)
    {
        var exists = drive.BucketExists(bucket);
        if (!exists.IsSuccess)
            return exists; // кворумный отказ — Failed
        if (exists.Value)
            return Result.Success();
        return drive.CreateBucket(bucket, time.GetUtcNow());
    }
}
