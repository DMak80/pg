// Применение реплики и репликационные чтения (t35, глава 07 §11/§6/§7).
// Реализация — ReplicaStore поверх IXlDrive: штатный xl-коммит (глава 04 §4)
// с ИСХОДНЫМИ метаданными источника — LWW-сходимость mesh.
namespace OwnS3.Storage;

// Метаданные объекта источника для применения реплики (modTime/sha256 — ключи LWW).
public sealed record ReplicaObjectMetadata(DateTimeOffset ModTime, string ETagHex, string ContentSha256Hex,
    string ContentType, IReadOnlyDictionary<string, string> UserMetadata);

// Состояние ключа для LWW-сравнения приёмника (null — ключа нет).
public sealed record ReplicaKeyState(DateTimeOffset ModTime, string ContentSha256Hex, long Size);

// Элемент листинга replication-грани (reconcile/листинг грани, глава 07 §6/§9).
public sealed record ReplicationListEntry(string Key, DateTimeOffset ModTime, string ContentSha256Hex, long Size);

// sha тела ≠ ожидаемого (заголовок x-owns3-repl-sha256) — до Commit (глава 07 §6/§7).
public sealed class ReplicationChecksumMismatchException(string message) : Exception(message);

public interface IReplicaStore
{
    // Штатный xl-коммит (staging→WritePart→WriteMeta→Commit) с ИСХОДНЫМИ
    // modTime/etag/sha/метаданными; expectedSha256Hex сверяется по дайджесту
    // тела ДО Commit (мисматч → Failed(ReplicationChecksumMismatchException),
    // staging сносится).
    Task<Result> ApplyObjectAsync(string bucket, string key, Stream body, long contentLength,
        ReplicaObjectMetadata metadata, string expectedSha256Hex, CancellationToken ct);

    Result DeleteObject(string bucket, string key);            // идемпотентно (204-семантика Delete)

    Result<ReplicaKeyLookup> ReadKeyState(string bucket, string key); // Found | Missing

    // Utf8ByteOrder от маркера (reconcile/листинг грани); NextMarker — последний
    // отданный ключ в составе значения (null — конец).
    Result<ReplicaListingPage> ListReplicationEntries(string bucket, string? marker, int maxKeys);

    // fetch-грань: без VerifyFullChecksum; NoSuchKey → Failed(ObjectStoreException(NoSuchKey))
    Result<(ObjectMetadata Metadata, Stream Body)> OpenObjectForReplication(string bucket, string key);

    Result ApplyBucketCreate(string bucket, DateTimeOffset creationDate); // идемпотентно: есть → skip
    Result ApplyBucketDelete(string bucket);                    // пустой → удалить; непустой → Failed(ObjectStoreException(BucketNotEmpty))

    // Состав бакетов (листинг replication-грани без bucket — backfill reconcile, глава 07 §6).
    Result<IReadOnlyList<BucketEntry>> ListBuckets();
}
