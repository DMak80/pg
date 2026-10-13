namespace OwnS3.Replication.Transport;

// Транспорт mesh (t35, глава 07 §6): подписанные запросы к replication-грани соседа.
// Реализация — ReplicationHttpClient (SigV4Signer + HttpClient); тесты подменяют
// HttpMessageHandler либо фейком интерфейса. Исходы — Result-конвенция главы 01 §4:
// 429/не-200/обрыв/таймаут — Failed(PeerRejectedException); полученный ответ —
// Success (терминальное знание, включая tombstone/404/ping-reachable=false).

// Исход ping-пробы линка (глава 07 §2/§6): reachable — строго по 200 ping-грани;
// RttMs — при любом полученном HTTP-ответе (нет ответа — нет Success, конвенция 01 §4).
public sealed record PingResult(bool Reachable, long RttMs);

public interface IReplicationTransport
{
    // POST /replication/notifications → 200 {acceptedToSeqNo} → Success;
    // 429/HTTP-отказ/обрыв → Failed(PeerRejectedException), вызывающий бэкоффит.
    Task<Result<long>> SendNotificationsAsync(TargetRecord target, NotificationBatchRequest batch, CancellationToken ct);

    // GET /replication/object/{bucket}/{key+}: 200-объект (Body + заголовки
    // modtime/sha/etag/origin/contentType/x-amz-meta-*) | 200-tombstone | 404 —
    // терминальные Success-варианты FetchOutcome; транспортный отказ → Failed.
    Task<Result<FetchOutcome>> FetchObjectAsync(TargetRecord target, string bucket, string key, CancellationToken ct);

    // GET /replication/listing?bucket=&marker=&max-keys= — постраничный листинг ключей.
    Task<Result<ReplicationListingResponse>> ListAsync(TargetRecord target, string bucket, string? marker,
        int maxKeys, CancellationToken ct);

    // GET /replication/listing без bucket — состав бакетов соседа (backfill reconcile, §9).
    Task<Result<IReadOnlyList<(string Name, long CreationUnixMs)>>> ListingBucketsAsync(TargetRecord target,
        CancellationToken ct);

    // GET /replication/ping → 200 без тела: лёгкая проба линка (глава 07 §2/§6).
    // timeout — per-target бюджет probe (глава 07 §11); любой полученный HTTP-ответ —
    // Success(PingResult(reachable, rttMs)), обрыв/per-target-таймаут → Failed;
    // внешняя отмена ct — проброс.
    Task<Result<PingResult>> PingAsync(TargetRecord target, TimeSpan timeout, CancellationToken ct);
}
