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

    public async Task ApplyObjectAsync(string bucket, string key, Stream body, long contentLength,
        ReplicaObjectMetadata metadata, string expectedSha256Hex, CancellationToken ct)
    {
        EnsureBucket(bucket);
        var staging = drive.CreateStaging(bucket, key);
        await Task.Yield(); // async-контракт при синхронном файловом IO (P4 t37)
        try
        {
            var digest = await drive.WritePartAsync(staging, 1, body, contentLength, ct);
            if (!string.Equals(digest.Sha256Hex, expectedSha256Hex, StringComparison.OrdinalIgnoreCase))
                throw new ReplicationChecksumMismatchException(
                    $"sha тела реплики не совпал с ожидаемым: bucket={bucket}, key={key}");
            // xl-запись с ИСХОДНЫМИ метаданными источника (modTime/etag/sha/user-meta)
            var record = new XlMetaRecord(Guid.Empty, contentLength, metadata.ModTime,
                metadata.ETagHex, metadata.ContentType, metadata.UserMetadata, EmptyHeaders,
                metadata.ContentSha256Hex);
            drive.WriteMeta(staging, record);
            drive.Commit(bucket, key, record, staging);
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
        var meta = drive.TryReadMeta(bucket, key);
        if (meta is null)
            return;
        drive.Delete(bucket, key, meta);
    }

    public ReplicaKeyState? ReadKeyState(string bucket, string key)
    {
        var meta = drive.TryReadMeta(bucket, key);
        return meta is null ? null : new ReplicaKeyState(meta.ModTime, meta.ContentSha256, meta.Size);
    }

    public IReadOnlyList<ReplicationListEntry> ListReplicationEntries(string bucket, string? marker,
        int maxKeys, out string? nextMarker)
    {
        var entries = new List<ReplicationListEntry>();
        nextMarker = null;
        if (!drive.BucketExists(bucket))
            return entries;
        var cursor = drive.EnumerateObjects(bucket);
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
        var meta = drive.TryReadMeta(bucket, key)
            ?? throw new ObjectStoreException(ObjectStoreErrorCode.NoSuchKey);
        var metadata = new ObjectMetadata(key, '"' + meta.ETag + '"', meta.Size, meta.ModTime,
            meta.ContentType, meta.UserMetadata);
        return (metadata, drive.OpenData(bucket, key, meta, 0, meta.Size));
    }

    public void ApplyBucketCreate(string bucket, DateTimeOffset creationDate)
    {
        // Идемпотентно: есть → skip (первая CreationDate сохраняется)
        if (drive.BucketExists(bucket))
            return;
        drive.CreateBucket(bucket, creationDate);
    }

    public void ApplyBucketDelete(string bucket)
    {
        // Идемпотентно: нет бакета — уже удалён; непустой — DeleteBucket кинет BucketNotEmpty
        if (!drive.BucketExists(bucket))
            return;
        drive.DeleteBucket(bucket);
    }

    // Реплика объекта может прийти раньше события bucket-create (дыра/порядок
    // доставки): бакет создаётся по месту с CreationDate = modTime объекта.
    private void EnsureBucket(string bucket)
    {
        if (!drive.BucketExists(bucket))
            drive.CreateBucket(bucket, time.GetUtcNow());
    }
}
