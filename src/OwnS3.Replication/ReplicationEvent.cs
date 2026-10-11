// Модель события репликации (t35, глава 07 §3): единая форма записи журнала
// (outbox §4) и уведомления (грань §6). Поля — состав таблицы событий главы 07 §3.
namespace OwnS3.Replication;

// type записи outbox v1 (§4): object-put=1, object-delete=2, bucket-create=3, bucket-delete=4.
public enum ReplicationEventType : byte
{
    ObjectPut = 1,
    ObjectDelete = 2,
    BucketCreate = 3,
    BucketDelete = 4,
}

// Единая модель события журнала/уведомления; для delete/бакетов ETag/ContentSha256 = "".
// SeqNo — сквозной монотонный номер журнала источника; ModTimeUnixMs — время фиксации
// (unix-мс); OriginHostId — диагностика конфликтов (НЕ ключ идентификации — глава 07 §2).
public sealed record ReplicationEvent(
    long SeqNo,
    ReplicationEventType Type,
    string Bucket,
    string Key,
    long ModTimeUnixMs,
    string ETag,
    long Size,
    string ContentSha256,
    string OriginHostId);
