using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using AdminPanel.Etcd;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;
using AdminPanel.Probes.S3;
using Microsoft.Extensions.Options;

namespace AdminPanel.Api.Inspection;

// Запросы грани «Хранилище бэкапов» (t08, arch/03 §1): сводка из снапшота +
// сверка MinioReconciler; детали шарда (джойн etcd×S3); on-demand objects —
// единственный прямой выход в MinIO на запрос. Креды S3 в DTO не отдаются
// никогда (AC8): только endpoint/bucket.

public sealed record BackupStorageQuery : IQuery<BackupStorageDto>;

public sealed record BackupShardStorageQuery(string Cluster, string Shard) : IQuery<BackupShardStorageDto>;

public sealed record BackupObjectsQuery(string? Prefix, int? MaxKeys, string? ContinuationToken)
    : IQuery<BackupObjectsPageDto>
{
    public const int DefaultMaxKeys = 200;
    public const int MaxKeysLimit = 1000; // потолок list-v2 (spec §3.14)

    // Ступень 1 гварда prefix (форма): пусто/null — весь bucket (валидно);
    // иначе первый сегмент (до первого '/') матчит кластерный паттерн
    // ^[a-z][a-z0-9_]{0,62}$ — быстрый 400 на мусор (Решение 11).
    public static bool IsValidPrefixForm(string? prefix)
    {
        if (string.IsNullOrEmpty(prefix))
            return true;
        var head = prefix.Split('/')[0];
        return ClusterNameRegex.IsMatch(head);
    }

    // Ступень 2 гварда prefix (принадлежность, в handler'е по снапшоту):
    // первый сегмент — имя кластера снапшота ИЛИ кластер S3-дерева инвентаря
    // (сироты просматриваемы), иначе 400 «префикс вне грани» (AC7).
    public static bool IsPrefixKnown(string? prefix, EtcdSnapshot? snapshot)
    {
        if (string.IsNullOrEmpty(prefix) || snapshot is null)
            return true;
        var head = prefix.Split('/')[0];
        if (snapshot.Clusters.Any(c => c.Name == head))
            return true;
        var tree = snapshot.MinioStorage?.Clusters;
        return tree is not null && tree.Any(c => c.Cluster == head);
    }

    // maxKeys: null → дефолт 200; вне 1..1000 — 400.
    public static bool IsValidMaxKeys(int? maxKeys)
        => maxKeys is null or (>= 1 and <= MaxKeysLimit);

    public static int EffectiveMaxKeys(int? maxKeys) => maxKeys ?? DefaultMaxKeys;

    // Кластерные имена etcd (arch/02 §9.3) — тот же паттерн для корня префикса.
    public static readonly System.Text.RegularExpressions.Regex ClusterNameRegex = new(
        "^[a-z][a-z0-9_]{0,62}$",
        System.Text.RegularExpressions.RegexOptions.Compiled);
}

// ——— DTO (camelCase; S3-ключи AccessKey/SecretKey НЕ отдаются — AC8) ———

public sealed record MinioHealthDto(
    bool ApiOk, string? ApiError, bool LiveOk, bool? ClusterOk,
    long? HealthyDrives, long? OfflineDrives, long? HealingDrives, long? TotalDrives);

public sealed record BackupStorageEtcdDto(
    long UsedBytes, long? QuotaBytes, double? UsedPercent, string State, long UpdatedUnix);

public sealed record BackupStorageDto(
    bool Configured, string? NotConfiguredReason, string? Endpoint, string? Bucket,
    MinioHealthDto? Health,
    BackupStorageEtcdDto? Etcd, // ключ /pgworker/backups/storage (вердикт воркера)
    long? LiveUsedBytes,        // live-инвентарь (снапшот MinioStorage)
    IReadOnlyList<BackupClusterStorageDto> Clusters,
    IReadOnlyList<BackupOrphanDto> Orphans, // реестр воркера + сверка панели, слитые
    long InventoryUpdatedUnix, string? InventoryError);

public sealed record BackupClusterStorageDto(
    string Cluster, long SizeBytes, IReadOnlyList<BackupShardSummaryDto> Shards);

public sealed record BackupShardSummaryDto(
    string Cluster, string Shard, long SizeBytes,
    int FullsCount, long WalSegmentCount, bool HasS3Only, bool Orphan); // пометки сверки

public sealed record BackupOrphanDto(
    string Prefix, string Kind, long SizeBytes,
    bool InWorkerRegistry, string? RegistryState, long? FirstSeenUnix, long? TtlLeftSec);

public sealed record BackupShardStorageDto(
    string Cluster, string Shard,
    IReadOnlyList<BackupFullDto> Fulls,
    BackupWalDto? Wal,
    BackupRestoreBadgeDto? ActiveRestore, // state/phase/error (без кнопок — arch/19 §3.5)
    string? ReconcileNote);               // сводная пометка (напр. «объекты без ключа: 2»)

public sealed record BackupFullDto(
    string Id, long? SizeBytes, long? ObjectCount, long? LastModifiedUnix,
    string? EtcdState, string? VerifyState, long? EtcdSizeBytes, string Reconcile);

public sealed record BackupWalDto(
    string? EtcdState, string? EtcdLastSegment, long? EtcdLastUnix,
    long S3SegmentCount, long S3HistoryCount, long S3SizeBytes,
    long S3LastModifiedUnix, string? S3LastObject);

public sealed record BackupRestoreBadgeDto(string State, string? Phase, string? Error);

public sealed record BackupObjectsPageDto(
    IReadOnlyList<BackupObjectDto> Items, string? NextContinuationToken);

public sealed record BackupObjectDto(string Key, long SizeBytes, long LastModifiedUnix);

// ——— отказы (маппинг в ProblemDetails — InspectionModule) ———

// Шард отсутствует и в etcd-Backups, и в S3-дереве инвентаря — 404.
public sealed class BackupShardNotFound(string cluster, string shard)
    : Exception($"шард {cluster}/{shard} отсутствует и в etcd, и в S3-дереве инвентаря");

// Гвард ступени 2: префикс вне грани (не кластер снапшота и не объект дерева) — 400.
public sealed class InvalidBackupPrefixException(string prefix)
    : Exception($"префикс вне грани (не кластер и не объект инвентаря): {prefix}");

// Гвард maxKeys вне 1..1000 — 400.
public sealed class InvalidBackupMaxKeysException(int? maxKeys)
    : Exception($"maxKeys вне диапазона 1..{BackupObjectsQuery.MaxKeysLimit}: {maxKeys?.ToString() ?? "null"}");

// objects при незаданном Endpoint — 503 (грань выключена).
public sealed class BackupsStorageNotConfiguredException()
    : Exception("AdminPanel:Backups:S3:Endpoint не задан — грань «Хранилище бэкапов» выключена");

// Транспортный сбой обращения к MinIO — 502.
public sealed class BackupsS3UnavailableException(string message)
    : Exception(message);

// ——— мапперы (чистые функции снапшот → DTO) ———

public static class BackupStorageMappers
{
    public static BackupStorageDto MapStorage(EtcdSnapshot snapshot, long orphanTtlSec, long nowUnix)
    {
        var minio = snapshot.MinioStorage;
        if (minio is null)
        {
            // AC1: грань не настроена — только configured + причина.
            return new BackupStorageDto(
                Configured: false,
                NotConfiguredReason: "AdminPanel:Backups:S3:Endpoint не задан",
                Endpoint: null, Bucket: null, Health: null, Etcd: null,
                LiveUsedBytes: null, Clusters: [], Orphans: [],
                InventoryUpdatedUnix: 0, InventoryError: null);
        }

        var reconcile = MinioReconciler.Reconcile(
            minio, snapshot.Backups, snapshot.Clusters, snapshot.BackupOrphans);

        return new BackupStorageDto(
            Configured: true,
            NotConfiguredReason: null,
            Endpoint: minio.Endpoint,
            Bucket: minio.Bucket,
            Health: MapHealth(minio.Health),
            Etcd: MapEtcdStorage(snapshot.BackupStorage),
            LiveUsedBytes: minio.UsedBytes,
            Clusters: MapClusters(minio.Clusters, reconcile),
            Orphans: MergeOrphans(reconcile.OrphanPrefixes, snapshot.BackupOrphans, orphanTtlSec, nowUnix),
            InventoryUpdatedUnix: minio.UpdatedAtUnix,
            InventoryError: minio.LastError);
    }

    public static MinioHealthDto? MapHealth(MinioHealth? health)
        => health is null ? null : new MinioHealthDto(
            health.ApiOk, health.ApiError, health.LiveOk, health.ClusterOk,
            health.Drives?.HealthyDrives, health.Drives?.OfflineDrives,
            health.Drives?.HealingDrives, health.Drives?.TotalDrives);

    private static BackupStorageEtcdDto? MapEtcdStorage(BackupStorageInfo? storage)
        => storage is null ? null : new BackupStorageEtcdDto(
            storage.UsedBytes, storage.QuotaBytes, storage.UsedPercent,
            storage.State.ToString().ToUpperInvariant(), storage.UpdatedUnix);

    private static IReadOnlyList<BackupClusterStorageDto> MapClusters(
        IReadOnlyList<MinioClusterNode> clusters, BackupReconcileInfo reconcile)
    {
        var s3OnlyByShard = reconcile.Fulls
            .Where(f => f.Status == BackupFullReconcileStatus.S3Only)
            .Select(f => (f.Cluster, f.Shard))
            .ToHashSet();
        var orphanPrefixes = reconcile.OrphanPrefixes
            .Where(o => o.Kind == "shard")
            .Select(o => o.Prefix)
            .ToHashSet();

        return [.. clusters.Select(c => new BackupClusterStorageDto(
            c.Cluster,
            c.SizeBytes,
            [.. c.Shards.Select(s => new BackupShardSummaryDto(
                s.Cluster,
                s.Shard,
                s.SizeBytes,
                s.Fulls.Count,
                s.Wal?.SegmentCount ?? 0,
                HasS3Only: s3OnlyByShard.Contains((s.Cluster, s.Shard)),
                Orphan: orphanPrefixes.Contains($"{s.Cluster}/{s.Shard}")))]))];
    }

    // Сироты: панельная сверка (с джойном на реестр) ∪ записи реестра без
    // S3-факта (воркер уже удалил — TTL-строка оператору), слияние по Prefix.
    private static IReadOnlyList<BackupOrphanDto> MergeOrphans(
        IReadOnlyList<BackupOrphanPrefix> panelOrphans,
        BackupOrphansInfo? registry,
        long orphanTtlSec,
        long nowUnix)
    {
        var byRegistry = registry?.Orphans.ToDictionary(o => o.Prefix, StringComparer.Ordinal)
            ?? new Dictionary<string, BackupOrphanInfo>(StringComparer.Ordinal);

        var merged = panelOrphans
            .OrderBy(o => o.Prefix, StringComparer.Ordinal)
            .Select(o =>
            {
                byRegistry.TryGetValue(o.Prefix, out var entry);
                return new BackupOrphanDto(
                    o.Prefix, o.Kind, entry?.SizeBytes ?? o.SizeBytes,
                    o.InWorkerRegistry, o.RegistryState, o.FirstSeenUnix,
                    TtlLeftSec(o.FirstSeenUnix, orphanTtlSec, nowUnix));
            })
            .ToList();

        merged.AddRange(byRegistry
            .Where(kv => merged.All(m => m.Prefix != kv.Key))
            .OrderBy(kv => kv.Key, StringComparer.Ordinal)
            .Select(kv => new BackupOrphanDto(
                kv.Value.Prefix, kv.Value.Kind, kv.Value.SizeBytes,
                InWorkerRegistry: true, kv.Value.State, kv.Value.FirstSeenUnix,
                TtlLeftSec(kv.Value.FirstSeenUnix, orphanTtlSec, nowUnix))));
        return merged;
    }

    private static long? TtlLeftSec(long? firstSeenUnix, long ttlSec, long nowUnix)
        => firstSeenUnix is { } seen && ttlSec > 0 ? seen + ttlSec - nowUnix : null;

    // Детали шарда: джойн сверки per-full + WAL + активный restore.
    public static BackupShardStorageDto MapShard(
        string cluster, string shard,
        MinioShardNode? s3Shard,
        ClusterBackupsInfo? etcdCluster,
        BackupReconcileInfo reconcile)
    {
        var fulls = reconcile.Fulls
            .Where(f => f.Cluster == cluster && f.Shard == shard)
            .Select(f => new BackupFullDto(
                f.Id, f.SizeBytes, f.ObjectCount, f.LastModifiedUnix,
                f.EtcdState, f.VerifyState, f.EtcdSizeBytes,
                f.Status.ToString()))
            .ToList();

        var walEntry = reconcile.Wal.FirstOrDefault(w => w.Cluster == cluster && w.Shard == shard);
        var etcdWal = etcdCluster?.Shards?.TryGetValue(shard, out var info) == true ? info : null;
        var wal = walEntry is null && etcdWal is null ? null : new BackupWalDto(
            EtcdState: etcdWal?.State.ToString().ToUpperInvariant(),
            EtcdLastSegment: walEntry?.EtcdLastSegment,
            EtcdLastUnix: walEntry?.EtcdLastUnix,
            S3SegmentCount: s3Shard?.Wal?.SegmentCount ?? 0,
            S3HistoryCount: s3Shard?.Wal?.HistoryCount ?? 0,
            S3SizeBytes: s3Shard?.Wal?.SizeBytes ?? 0,
            S3LastModifiedUnix: s3Shard?.Wal?.LastModifiedUnix ?? 0,
            S3LastObject: walEntry?.S3LastObject);

        var restore = etcdCluster?.ShardsRestores?.TryGetValue(shard, out var restores) == true
            ? restores.FirstOrDefault(r => r.State is not ("COMPLETED" or "FAILED"))
            : null;

        // Сводная пометка сверки (для шапки деталей шарда).
        var shardFulls = reconcile.Fulls
            .Where(f => f.Cluster == cluster && f.Shard == shard).ToList();
        var notes = new List<string>();
        var s3Only = shardFulls.Count(f => f.Status == BackupFullReconcileStatus.S3Only);
        var etcdOnly = shardFulls.Count(f => f.Status == BackupFullReconcileStatus.EtcdOnly);
        var deleting = shardFulls.Count(f => f.Status == BackupFullReconcileStatus.Deleting);
        if (s3Only > 0)
            notes.Add($"объекты без ключа: {s3Only}");
        if (etcdOnly > 0)
            notes.Add($"ключи без объектов: {etcdOnly}");
        if (deleting > 0)
            notes.Add($"идёт удаление: {deleting}");

        return new BackupShardStorageDto(
            cluster, shard, fulls, wal,
            restore is null
                ? null
                : new BackupRestoreBadgeDto(restore.State, restore.Phase, restore.Error),
            notes.Count > 0 ? string.Join("; ", notes) : null);
    }
}

// ——— handlers ([InjectAsScoped], образец KafkaQuery) ———

// Сводка грани: снапшот + MinioReconciler (чистая функция, без IO на запрос).
[InjectAsScoped]
public sealed class BackupStorageQueryHandler(ISnapshotReader reader, IOptions<AlertsOptions> alertsOptions)
    : IQueryHandler<BackupStorageQuery, BackupStorageDto>
{
    public ValueTask<Result<BackupStorageDto>> Handle(BackupStorageQuery query, CancellationToken ct)
    {
        var snapshot = reader.Current;
        if (snapshot is null)
            return ValueTask.FromResult(Result<BackupStorageDto>.Failed(
                new InspectionModule.SnapshotNotReadyException()));

        return ValueTask.FromResult(Result<BackupStorageDto>.Success(
            BackupStorageMappers.MapStorage(
                snapshot,
                alertsOptions.Value.Backups.OrphanTtlSec,
                DateTimeOffset.UtcNow.ToUnixTimeSeconds())));
    }
}

// Детали шарда: 404 — нет ни в etcd-Backups, ни в S3-дереве.
[InjectAsScoped]
public sealed class BackupShardStorageQueryHandler(ISnapshotReader reader)
    : IQueryHandler<BackupShardStorageQuery, BackupShardStorageDto>
{
    public ValueTask<Result<BackupShardStorageDto>> Handle(
        BackupShardStorageQuery query, CancellationToken ct)
    {
        var snapshot = reader.Current;
        if (snapshot is null)
            return ValueTask.FromResult(Result<BackupShardStorageDto>.Failed(
                new InspectionModule.SnapshotNotReadyException()));

        var minio = snapshot.MinioStorage;
        var etcdCluster = snapshot.Backups.FirstOrDefault(c => c.Cluster == query.Cluster);
        var s3Shard = minio?.Clusters.FirstOrDefault(c => c.Cluster == query.Cluster)
            ?.Shards.FirstOrDefault(s => s.Shard == query.Shard);
        if (etcdCluster is null && s3Shard is null)
            return ValueTask.FromResult(Result<BackupShardStorageDto>.Failed(
                new BackupShardNotFound(query.Cluster, query.Shard)));

        var reconcile = minio is null
            ? new BackupReconcileInfo([], [], [])
            : MinioReconciler.Reconcile(
                minio, snapshot.Backups, snapshot.Clusters, snapshot.BackupOrphans);

        return ValueTask.FromResult(Result<BackupShardStorageDto>.Success(
            BackupStorageMappers.MapShard(query.Cluster, query.Shard, s3Shard, etcdCluster, reconcile)));
    }
}

// On-demand list-v2: единственный прямой выход API в MinIO на запрос.
[InjectAsScoped]
public sealed class BackupObjectsQueryHandler(
    IMinioS3 client,
    IOptions<MinioOptions> options,
    ISnapshotReader reader) : IQueryHandler<BackupObjectsQuery, BackupObjectsPageDto>
{
    public async ValueTask<Result<BackupObjectsPageDto>> Handle(
        BackupObjectsQuery query, CancellationToken ct)
    {
        if (!options.Value.IsConfigured)
            return Result<BackupObjectsPageDto>.Failed(new BackupsStorageNotConfiguredException());

        var snapshot = reader.Current;
        if (snapshot is null)
            return Result<BackupObjectsPageDto>.Failed(
                new InspectionModule.SnapshotNotReadyException());

        // Ступень 2 гварда prefix (форма проверена в эндпоинте — 400 до query).
        if (!BackupObjectsQuery.IsPrefixKnown(query.Prefix, snapshot))
            return Result<BackupObjectsPageDto>.Failed(
                new InvalidBackupPrefixException(query.Prefix ?? ""));

        if (!BackupObjectsQuery.IsValidMaxKeys(query.MaxKeys))
            return Result<BackupObjectsPageDto>.Failed(
                new InvalidBackupMaxKeysException(query.MaxKeys));

        var page = await client.ListPageAsync(
            query.Prefix, query.ContinuationToken,
            BackupObjectsQuery.EffectiveMaxKeys(query.MaxKeys), ct);
        if (!page.IsSuccess)
            return Result<BackupObjectsPageDto>.Failed(
                new BackupsS3UnavailableException(page.Error!.Message));

        return Result<BackupObjectsPageDto>.Success(new BackupObjectsPageDto(
            [.. page.Value.Items.Select(i => new BackupObjectDto(i.Key, i.SizeBytes, i.LastModifiedUnix))],
            page.Value.NextContinuationToken));
    }
}
