// Применение реплики и репликационные чтения (t35, глава 07 §11/§6/§7).
// Реализация — ReplicaStore поверх IXlDrive: штатный xl-коммит (глава 04 §4)
// с ИСХОДНЫМИ метаданными источника — LWW-сходимость mesh.
namespace OwnS3.Storage;

// Метаданные объекта источника для применения реплики (modTime/sha256 — ключи LWW).
public sealed record ReplicaObjectMetadata(DateTimeOffset ModTime, string ETagHex, string ContentSha256Hex,
    string ContentType, IReadOnlyDictionary<string, string> UserMetadata);

// Состояние ключа для LWW-сравнения приёмника (null — ключа нет).
public sealed record ReplicaKeyState(DateTimeOffset ModTime, string ContentSha256Hex, long Size);

// Элемент листинга replication-грани (reconcile/backfill, глава 07 §6/§9).
public sealed record ReplicationListEntry(string Key, DateTimeOffset ModTime, string ContentSha256Hex, long Size);

// sha тела ≠ ожидаемого (заголовок x-owns3-repl-sha256) — до Commit (глава 07 §6/§7).
public sealed class ReplicationChecksumMismatchException(string message) : Exception(message);

public interface IReplicaStore
{
    // Штатный xl-коммит (staging→WritePart→WriteMeta→Commit) с ИСХОДНЫМИ
    // modTime/etag/sha/метаданными; expectedSha256Hex сверяется по дайджесту
    // тела ДО Commit (мисматч → ReplicationChecksumMismatchException, staging сносится).
    Task ApplyObjectAsync(string bucket, string key, Stream body, long contentLength,
        ReplicaObjectMetadata metadata, string expectedSha256Hex, CancellationToken ct);

    void DeleteObject(string bucket, string key);            // идемпотентно (204-семантика Delete)

    ReplicaKeyState? ReadKeyState(string bucket, string key); // null — ключа нет

    // Utf8ByteOrder от маркера (reconcile/листинг грани); nextMarker — последний отданный ключ (null — конец).
    IReadOnlyList<ReplicationListEntry> ListReplicationEntries(string bucket, string? marker,
        int maxKeys, out string? nextMarker);

    (ObjectMetadata Metadata, Stream Body) OpenObjectForReplication(string bucket, string key); // fetch-грань: без VerifyFullChecksum

    void ApplyBucketCreate(string bucket, DateTimeOffset creationDate); // идемпотентно: есть → skip
    void ApplyBucketDelete(string bucket);                    // пустой → удалить; непустой → ObjectStoreException(BucketNotEmpty)
}
