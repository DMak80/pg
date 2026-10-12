using OwnS3.Replication.Lww;
using OwnS3.Replication.Outbox;

namespace OwnS3.Replication;

// Точки вставки событий — интерфейс хендлеров App (глава 07 §3): 21 операция
// ставит события ПОСЛЕ успешного коммит-поинта, до ответа клиенту.
public interface IReplicationEventSink
{
    void ObjectPut(string bucket, string key, long modTimeUnixMs, string etag, long size, string contentSha256);
    void ObjectDeleted(string bucket, string key, long deleteTimeUnixMs);
    void BucketCreated(string bucket, long creationUnixMs);
    void BucketDeleted(string bucket, long deleteUnixMs);

    // Клиентский DELETE: событие + локальная tombstone-запись (fetch-ответы «ключ удалён»).
    void ClientObjectDeleted(string bucket, string key, long deleteTimeUnixMs);
}

// Синхронный sink пути записи: НЕТ соседей → no-op (ноль IO, Existing-регресс);
// есть → Append+fsync (мс) в outbox (глава 07 §3/§4). Приём реплики события НЕ ставит
// (анти-петля: inbox работает через ReplicaApplier, минуя sink). originHostId —
// OWNS3_HOST_ID (дефолт — имя контейнера, глава 05 §4).
public sealed class ReplicationEventSink(OutboxJournal journal, ReplicationTargets targets,
    TombstoneStore tombstones, string originHostId) : IReplicationEventSink
{
    public void ObjectPut(string bucket, string key, long modTimeUnixMs, string etag, long size,
        string contentSha256)
    {
        if (!targets.Active)
            return;
        journal.Append(ReplicationEventType.ObjectPut, bucket, key, modTimeUnixMs, etag, size, contentSha256,
            originHostId);
    }

    public void ObjectDeleted(string bucket, string key, long deleteTimeUnixMs)
    {
        if (!targets.Active)
            return;
        journal.Append(ReplicationEventType.ObjectDelete, bucket, key, deleteTimeUnixMs, "", 0, "", originHostId);
    }

    public void BucketCreated(string bucket, long creationUnixMs)
    {
        if (!targets.Active)
            return;
        journal.Append(ReplicationEventType.BucketCreate, bucket, "", creationUnixMs, "", 0, "", originHostId);
    }

    public void BucketDeleted(string bucket, long deleteUnixMs)
    {
        if (!targets.Active)
            return;
        journal.Append(ReplicationEventType.BucketDelete, bucket, "", deleteUnixMs, "", 0, "", originHostId);
    }

    public void ClientObjectDeleted(string bucket, string key, long deleteTimeUnixMs)
    {
        // Tombstone и событие — только при активной репликации (глава 07 §7/§12:
        // без соседей каталог replication/ не создаётся; добавленный позже сосед
        // захватывает состояние стартовым reconcile §9 — отсутствие tombstone
        // безопасно)
        if (!targets.Active)
            return;
        tombstones.Put(bucket, key, deleteTimeUnixMs);
        ObjectDeleted(bucket, key, deleteTimeUnixMs);
    }
}

// Неактивная репликация (соседей нет): ноль IO — поведение байт-в-байт как single.
public sealed class NullReplicationEventSink : IReplicationEventSink
{
    public static readonly NullReplicationEventSink Instance = new();

    public void ObjectPut(string bucket, string key, long modTimeUnixMs, string etag, long size, string contentSha256) { }
    public void ObjectDeleted(string bucket, string key, long deleteTimeUnixMs) { }
    public void BucketCreated(string bucket, long creationUnixMs) { }
    public void BucketDeleted(string bucket, long deleteUnixMs) { }
    public void ClientObjectDeleted(string bucket, string key, long deleteTimeUnixMs) { }
}
