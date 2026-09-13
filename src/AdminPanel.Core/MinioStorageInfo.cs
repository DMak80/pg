namespace AdminPanel.Core;

// Live-инвентарь MinIO-грани «Хранилище бэкапов» (t08, arch/adminpanel/02 §2.5).
// Заполняет MinioInventoryLoop (Probes/S3) через IMinioInventoryStore; снапшот
// вносит refresher. Все метрики — факты, без вердиктов (вердикты — воркер).

/// <summary>Drives-поля тела /minio/health/cluster (свежие MinIO); null-поля —
/// в теле нет; сам Drives null — тела/полей не было вовсе (только статус-код).</summary>
public sealed record MinioDrives(
    long? HealthyDrives, long? OfflineDrives, long? HealingDrives, long? TotalDrives);

/// <summary>Health MinIO: ApiOk — ListBuckets прошёл (API жив + креды валидны),
/// LiveOk — /minio/health/live 200, ClusterOk — /minio/health/cluster 200
/// (null — эндпоинт не отвечает/не поддерживается).</summary>
public sealed record MinioHealth(
    bool ApiOk, string? ApiError, bool LiveOk, bool? ClusterOk, MinioDrives? Drives);

/// <summary>Один полный бэкап в S3-дереве: префикс full/&lt;id&gt;/.</summary>
public sealed record MinioFullNode(
    string Id, long SizeBytes, long ObjectCount, long LastModifiedUnix);

/// <summary>WAL-часть шарда в S3: сегменты wal/&lt;segment&gt; и истории wal/&lt;TLI&gt;.history.</summary>
public sealed record MinioWalNode(
    long SegmentCount, long HistoryCount, long SizeBytes, long LastModifiedUnix);

public sealed record MinioShardNode(
    string Cluster, string Shard, long SizeBytes,
    IReadOnlyList<MinioFullNode> Fulls, MinioWalNode? Wal);

public sealed record MinioClusterNode(
    string Cluster, long SizeBytes, IReadOnlyList<MinioShardNode> Shards);

/// <summary>Агрегат одного прогона list-v2 всего bucket (MinioInventory.Build).</summary>
public sealed partial record MinioInventory(
    IReadOnlyList<MinioClusterNode> Clusters, long UsedBytes, long ObjectCount,
    IReadOnlyList<string> Buckets, IReadOnlyList<string> ForeignPrefixes);

/// <summary>Состояние грани в снапшоте: null — AdminPanel:Backups:S3 не настроен
/// (configured=false); Configured=true + UpdatedAtUnix=0 — первый тик ещё шёл.</summary>
public sealed record MinioStorageInfo(
    bool Configured, string Endpoint, string Bucket,
    MinioHealth? Health, IReadOnlyList<string> Buckets,
    long UsedBytes, long ObjectCount,
    IReadOnlyList<MinioClusterNode> Clusters, IReadOnlyList<string> ForeignPrefixes,
    long UpdatedAtUnix, int ConsecutiveFailures, string? LastError);

/// <summary>Стор инвентаря: loop пишет, SnapshotRefresher вносит готовым в
/// снапшот (паттерн IWorkerHealthStore — KV-тик не блокируется).</summary>
public interface IMinioInventoryStore
{
    MinioStorageInfo? Current { get; }

    void Replace(MinioStorageInfo state);
}
