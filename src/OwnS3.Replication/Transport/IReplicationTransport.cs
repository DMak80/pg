namespace OwnS3.Replication.Transport;

// Транспорт mesh (t35, глава 07 §6): подписанные запросы к replication-грани соседа.
// Реализация — ReplicationHttpClient (SigV4Signer + HttpClient); тесты подменяют
// HttpMessageHandler либо фейком интерфейса.

// Исход ping-пробы линка (глава 07 §2/§6): reachable — строго по 200 ping-грани;
// rttMs — при любом полученном HTTP-ответе (null при таймауте/обрыве транспорта).
public sealed record PingResult(bool Reachable, long? RttMs);

public interface IReplicationTransport
{
    // POST /replication/notifications → 200 {acceptedToSeqNo} | 429 (→ null,
    // вызывающий бэкоффит).
    Task<long?> SendNotificationsAsync(TargetRecord target, NotificationBatchRequest batch, CancellationToken ct);

    // GET /replication/object/{bucket}/{key+}: 200-объект (Body + заголовки
    // modtime/sha/etag/origin/contentType/x-amz-meta-*) | 200-tombstone (TombstoneUnixMs)
    // | 404 (ключ неизвестен — состояние приёмника не меняется).
    Task<FetchResult> FetchObjectAsync(TargetRecord target, string bucket, string key, CancellationToken ct);

    // GET /replication/listing?bucket=&marker=&max-keys= — постраничный листинг ключей.
    Task<ReplicationListingResponse> ListAsync(TargetRecord target, string bucket, string? marker,
        int maxKeys, CancellationToken ct);

    // GET /replication/listing без bucket — состав бакетов соседа (backfill reconcile, §9).
    Task<IReadOnlyList<(string Name, long CreationUnixMs)>> ListingBucketsAsync(TargetRecord target,
        CancellationToken ct);

    // GET /replication/ping → 200 без тела: лёгкая проба линка (глава 07 §2/§6).
    // timeout — per-target бюджет probe (глава 07 §11); внешняя отмена ct — проброс.
    Task<PingResult> PingAsync(TargetRecord target, TimeSpan timeout, CancellationToken ct);
}
