namespace OwnS3.Storage;

// Реализация IReplicaStore поверх IXlDrive (t35, глава 07 §11):
// применение реплики — штатный xl-коммит (staging→WritePart→WriteMeta→Commit,
// глава 04 §4) с ИСХОДНЫМИ метаданными источника; sha-сверка тела ДО коммита;
// репликационные чтения — состояние ключа (LWW), листинг (Utf8ByteOrder),
// fetch-отдача (без VerifyFullChecksum — сверка на приёмнике, глава 07 §6).
public sealed class ReplicaStore(IXlDrive drive, TimeProvider time) : IReplicaStore
{
    private static readonly IReadOnlyDictionary<string, string> EmptyHeaders =
        new Dictionary<string, string>();

    // мост t38: демонтаж в задаче перевода IObjectStore/IReplicaStore
    private static XlMetaRecord? UnwrapMeta(Result<XlMetaLookup> lookup)
        => lookup.IsSuccess
            ? lookup.Value is XlMetaLookup.Found f ? f.Record : null
            : throw lookup.Error!;

    public async Task ApplyObjectAsync(string bucket, string key, Stream body, long contentLength,
        ReplicaObjectMetadata metadata, string expectedSha256Hex, CancellationToken ct)
    {
        EnsureBucket(bucket);
        var staging = drive.CreateStaging(bucket, key);
        await Task.Yield(); // async-контракт при синхронном файловом IO (P4 t37)
        try
        {
            var digest = (await drive.WritePartAsync(staging, 1, body, contentLength, ct)).Throw().Value; // мост t38
            if (!string.Equals(digest.Sha256Hex, expectedSha256Hex, StringComparison.OrdinalIgnoreCase))
                throw new ReplicationChecksumMismatchException(
                    $"sha тела реплики не совпал с ожидаемым: bucket={bucket}, key={key}");
            // xl-запись с ИСХОДНЫМИ метаданными источника (modTime/etag/sha/user-meta)
            var record = new XlMetaRecord(Guid.Empty, contentLength, metadata.ModTime,
                metadata.ETagHex, metadata.ContentType, metadata.UserMetadata, EmptyHeaders,
                metadata.ContentSha256Hex);
            drive.WriteMeta(staging, record).Throw(); // мост t38
            drive.Commit(bucket, key, record, staging).Throw(); // мост t38
        }
        finally
        {
            // Сбой/мисматч: staging сносится; после успешного Commit — безвреден
            staging.Dispose();
        }
    }

    public void DeleteObject(string bucket, string key)
    {
        // Идемпотентно: ключа нет — 204-семантика Delete, ничего не делаем
        var meta = UnwrapMeta(drive.TryReadMeta(bucket, key)); // мост t38: демонтаж в задаче перевода IObjectStore/IReplicaStore
        if (meta is null)
            return;
        drive.Delete(bucket, key, meta).Throw(); // мост t38
    }

    public ReplicaKeyState? ReadKeyState(string bucket, string key)
    {
        var meta = UnwrapMeta(drive.TryReadMeta(bucket, key)); // мост t38: демонтаж в задаче перевода IObjectStore/IReplicaStore
        return meta is null ? null : new ReplicaKeyState(meta.ModTime, meta.ContentSha256, meta.Size);
    }

    public IReadOnlyList<ReplicationListEntry> ListReplicationEntries(string bucket, string? marker,
        int maxKeys, out string? nextMarker)
    {
        var entries = new List<ReplicationListEntry>();
        nextMarker = null;
        if (!drive.BucketExists(bucket).Throw().Value) // мост t38: демонтаж в задаче перевода IObjectStore/IReplicaStore
            return entries;
        var cursor = drive.EnumerateObjects(bucket).Throw().Value; // мост t38
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
        return entries;
    }

    public (ObjectMetadata Metadata, Stream Body) OpenObjectForReplication(string bucket, string key)
    {
        var meta = UnwrapMeta(drive.TryReadMeta(bucket, key)) // мост t38: демонтаж в задаче перевода IObjectStore/IReplicaStore
            ?? throw new ObjectStoreException(ObjectStoreErrorCode.NoSuchKey);
        var metadata = new ObjectMetadata(key, '"' + meta.ETag + '"', meta.Size, meta.ModTime,
            meta.ContentType, meta.UserMetadata);
        return (metadata, drive.OpenData(bucket, key, meta, 0, meta.Size).Throw().Value); // мост t38
    }

    public void ApplyBucketCreate(string bucket, DateTimeOffset creationDate)
    {
        // Идемпотентно: есть → skip (первая CreationDate сохраняется)
        if (drive.BucketExists(bucket).Throw().Value) // мост t38: демонтаж в задаче перевода IObjectStore/IReplicaStore
            return;
        drive.CreateBucket(bucket, creationDate).Throw(); // мост t38
    }

    public void ApplyBucketDelete(string bucket)
    {
        // Идемпотентно: нет бакета — уже удалён; непустой — DeleteBucket кинет BucketNotEmpty
        if (!drive.BucketExists(bucket).Throw().Value) // мост t38: демонтаж в задаче перевода IObjectStore/IReplicaStore
            return;
        drive.DeleteBucket(bucket).Throw(); // мост t38
    }

    public IReadOnlyList<BucketEntry> ListBuckets() =>
        drive.ListBuckets().Throw().Value; // мост t38: демонтаж в задаче перевода IObjectStore/IReplicaStore

    // Реплика объекта может прийти раньше события bucket-create (дыра/порядок
    // доставки): бакет создаётся по месту с CreationDate = modTime объекта.
    private void EnsureBucket(string bucket)
    {
        // мост t38: демонтаж в задаче перевода IObjectStore/IReplicaStore
        if (!drive.BucketExists(bucket).Throw().Value)
            drive.CreateBucket(bucket, time.GetUtcNow()).Throw();
    }
}
