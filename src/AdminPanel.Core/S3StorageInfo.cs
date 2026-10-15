namespace AdminPanel.Core;

// Live-инвентарь S3-грани «Хранилище бэкапов» (t08, arch/adminpanel/02 §2.5).
// Заполняет S3InventoryLoop (Probes/S3) через IS3InventoryStore; снапшот
// вносит refresher. Все метрики — факты, без вердиктов (вердикты — воркер).

/// <summary>Drives-поля тела /s3/health/cluster (свежие MinIO); null-поля —
/// в теле нет; сам Drives null — тела/полей не было вовсе (только статус-код).</summary>
public sealed record S3Drives(
    long? HealthyDrives, long? OfflineDrives, long? HealingDrives, long? TotalDrives);

/// <summary>Health ownS3: ApiOk — ListBuckets прошёл (API жив + креды валидны),
/// LiveOk — /s3/health/live 200, ClusterOk — /s3/health/cluster 200
/// (null — эндпоинт не отвечает/не поддерживается).</summary>
public sealed record S3Health(
    bool ApiOk, string? ApiError, bool LiveOk, bool? ClusterOk, S3Drives? Drives);

/// <summary>Один полный бэкап в S3-дереве: префикс full/&lt;id&gt;/.</summary>
public sealed record S3FullNode(
    string Id, long SizeBytes, long ObjectCount, long LastModifiedUnix);

/// <summary>WAL-часть шарда в S3: сегменты wal/&lt;segment&gt; и истории wal/&lt;TLI&gt;.history;
/// LastSegment — старейший→новейший максимум имени по Ordinal среди СЕГМЕНТОВ
/// (истории в «последний» не идут; 24-hex имена при равном TLI лексикографически
/// упорядочены, cross-TLI — факт без вердикта); null — сегментов не было.</summary>
public sealed record S3WalNode(
    long SegmentCount, long HistoryCount, long SizeBytes, long LastModifiedUnix,
    string? LastSegment = null);

public sealed record S3ShardNode(
    string Cluster, string Shard, long SizeBytes,
    IReadOnlyList<S3FullNode> Fulls, S3WalNode? Wal);

public sealed record S3ClusterNode(
    string Cluster, long SizeBytes, IReadOnlyList<S3ShardNode> Shards);

/// <summary>Агрегат одного прогона list-v2 всего bucket (S3Inventory.Build).</summary>
public sealed partial record S3Inventory(
    IReadOnlyList<S3ClusterNode> Clusters, long UsedBytes, long ObjectCount,
    IReadOnlyList<string> Buckets, IReadOnlyList<string> ForeignPrefixes);

/// <summary>Состояние грани в снапшоте: null — AdminPanel:Backups:S3 не настроен
/// (configured=false); Configured=true + UpdatedAtUnix=0 — первый тик ещё шёл.
/// t36 (adminpanel/02 §2.5): Endpoints — цели инвентаря списком (env Endpoints;
/// одиночный Endpoint — валидный частный случай, один элемент); ключей нет.</summary>
public sealed record S3StorageInfo(
    bool Configured, IReadOnlyList<string> Endpoints, string Bucket,
    S3Health? Health, IReadOnlyList<string> Buckets,
    long UsedBytes, long ObjectCount,
    IReadOnlyList<S3ClusterNode> Clusters, IReadOnlyList<string> ForeignPrefixes,
    long UpdatedAtUnix, int ConsecutiveFailures, string? LastError);

/// <summary>Стор инвентаря: loop пишет, SnapshotRefresher вносит готовым в
/// снапшот (паттерн IWorkerHealthStore — KV-тик не блокируется).</summary>
public interface IS3InventoryStore
{
    S3StorageInfo? Current { get; }

    void Replace(S3StorageInfo state);
}
