using AdminPanel.Core;
using AdminPanel.Core.Alerting.Rules;
using AdminPanel.Etcd;
using Shared.Core.CQRS;
using Shared.Core.DI;
using Microsoft.Extensions.Options;

namespace AdminPanel.Api.Inspection;

// Запрос сводки дашборда (arch/03 §1 GET /api/overview).
public sealed record OverviewQuery : IQuery<OverviewDto>;

// Ответ GET /api/overview: etcd-часть реальна, кластерная часть — (spec §3.15).
public sealed record OverviewDto(
    int AlertsCritical,
    int AlertsWarning,
    OverviewEtcdDto Etcd,
    IReadOnlyList<OverviewClusterDto> Clusters,
    IReadOnlyList<OverviewMoveDto> ActiveMoves,
    OverviewKafkaDto? Kafka,
    OverviewValkeyDto? Valkey,
    OverviewS3Dto? S3,
    long SnapshotAgeMs,
    bool Stale,
    OverviewHaDto? Ha = null,               // u06: HA-сводка с сервера
    OverviewBackupsDto? Backups = null,     // u06: бэкапы с сервера
    OverviewReliabilityDto? Reliability = null, // u06: надёжность с сервера
    IReadOnlyList<OverviewOperationDto>? Operations = null, // u07: активные операции
    OverviewFreshnessDto? Freshness = null, // u08: свежесть per-домен
    BackupsDisabledDto? BackupsDisabled = null); // u10: null = бэкапы работают

// Глобальный стоп бэкапов (u10): значение /pgworker/backups-disabled.
public sealed record BackupsDisabledDto(long UpdatedUnix, string? UpdatedBy);

// HA-сводка (u06): без лидера считаются только matched-скопы (как TS HaCard).
public sealed record OverviewHaDto(int ScopesTotal, int WithoutLeader, int Unmatched);

// Бэкапы (u06): health S3 + место + etcd-снапшоты; Health null — фактов нет.
public sealed record OverviewBackupsDto(
    bool Configured, OverviewBackupsHealthDto? Health, string? StorageState,
    long UsedBytes, long? QuotaBytes, OverviewEtcdSnapshotsDto? EtcdSnapshots);

public sealed record OverviewBackupsHealthDto(bool ApiOk, bool LiveOk, bool? ClusterOk);

public sealed record OverviewEtcdSnapshotsDto(bool Enabled, string? State);

// Надёжность (u06): перенос TS-агрегации ReliabilityCard (D6) — worst RPO по
// Active-кластерам, mode=full/off, самый недавний закрытый failover.
public sealed record OverviewReliabilityDto(
    long? WorstRpoPotentialSec, int FullOnlyCount, int OffCount, OverviewLastFailoverDto? LastFailover);

public sealed record OverviewLastFailoverDto(long DurationSec, long AtUnix);

// Активная операция домена (u07): имена kind — фиксированный словарь сервера
// (UI их не парсит); entity — бакет/топик/нода или null.
public sealed record OverviewOperationDto(
    string Domain, string Kind, string Cluster, string? Entity, string Detail, long? UpdatedUnix);

// Свежесть (u08): BuiltAtUtc per снапшот в unix-секундах; etcd — lastRefresh;
// null-домен — снапшот ещё не тикал.
public sealed record OverviewFreshnessDto(
    OverviewFreshnessItemDto? Etcd, OverviewFreshnessItemDto? Pg,
    OverviewFreshnessItemDto? Kafka, OverviewFreshnessItemDto? Valkey, OverviewFreshnessItemDto? S3);

public sealed record OverviewFreshnessItemDto(long BuiltAtUnix);

// Сводка kafka-домена (arch/03 §7.1): из KafkaSnapshot + kafka-алертов.
public sealed record OverviewKafkaDto(
    int ClustersTotal,
    int ClustersCritical);

// Сводка valkey-домена (arch/03 §8.1): из ValkeySnapshot + valkey-алертов.
public sealed record OverviewValkeyDto(
    int ClustersTotal,
    int ClustersCritical);

// Сводка s3-домена (arch/03 §12.1): из S3Snapshot + s3-алертов.
public sealed record OverviewS3Dto(
    int ClustersTotal,
    int ClustersCritical);

public sealed record OverviewEtcdDto(bool Reachable, int EndpointsOk, int EndpointsTotal);

// Заглушки контракта (arch/03 §2): поля полные, значения — всегда пусто в.
// Наполнены из снапшота в (spec §6.2).
public sealed record OverviewClusterDto(
    string Name, int Shards, int Buckets, int ActiveMoves, int MasterlessShards, bool NotInitialized);

public sealed record OverviewMoveDto(
    string Cluster, int Bucket, string State, string? Owner, string? Target, long? UpdatedUnix);

// Снапшот → сводку: чистая функция (spec §6.2).
public static class OverviewMapper
{
    public static OverviewDto Map(
        EtcdSnapshot snapshot, DateTimeOffset nowUtc, double refreshIntervalSeconds,
        Core.Kafka.KafkaSnapshot? kafka = null,
        Core.Valkey.ValkeySnapshot? valkey = null,
        Core.S3.S3Snapshot? s3 = null)
    {
        var age = nowUtc - snapshot.BuiltAtUtc;
        return new OverviewDto(
            snapshot.Alerts.Count(a => a.Severity == AlertSeverity.Critical),
            snapshot.Alerts.Count(a => a.Severity == AlertSeverity.Warning),
            new OverviewEtcdDto(
                snapshot.Etcd.Reachable,
                snapshot.Etcd.Endpoints.Count(e => e.Reachable),
                snapshot.Etcd.Endpoints.Count),
            [.. snapshot.Clusters.Select(c => new OverviewClusterDto(
                c.Name,
                c.Shards.Count,
                c.BucketsCount,
                // NOT_INITIALIZED — не переезд: только реальные состояния перемещения (spec §3.6)
                c.Buckets.Count(b => b.State is BucketState.Syncing or BucketState.Frozen or BucketState.Aborting),
                c.State == ClusterState.NotInitialized
                    ? 0 // без мастера у не поднятого кластера — норма (arch/03 §2)
                    : c.Shards.Count(s => s.MasterAddress is null),
                c.State == ClusterState.NotInitialized))],
            [.. snapshot.Clusters
                .SelectMany(c => c.Buckets
                    .Where(b => b.State is BucketState.Syncing or BucketState.Frozen or BucketState.Aborting)
                    .OrderBy(b => b.Id) // внутри кластера — по Id (spec §3.6): модель порядка Buckets не гарантирует
                    .Select(b => new OverviewMoveDto(
                        c.Name, b.Id, BucketStates.Name(b.State),
                        b.Move?.Owner, b.Move?.Target, b.Move?.UpdatedUnix)))],
            kafka is null ? null : MapKafkaSummary(kafka), // kafka-сводка (B5)
            MapValkey(valkey), // valkey-сводка
            MapS3(s3), // s3-сводка
            Math.Max(0L, (long)Math.Round(age.TotalMilliseconds)),
            age > TimeSpan.FromSeconds(SnapshotStaleRule.Multiplier * refreshIntervalSeconds),
            MapHa(snapshot),
            MapBackups(snapshot),
            MapReliability(snapshot, nowUtc),
            MapOperations(snapshot, kafka, valkey, s3),
            MapFreshness(snapshot, kafka, valkey, s3),
            snapshot.BackupsDisabled is { } disabled
                ? new BackupsDisabledDto(disabled.UpdatedUnix, disabled.UpdatedBy)
                : null);
    }

    // HA-сводка (u06): тот же расчёт, что TS HaCard — scopesTotal, без лидера
    // только matched, unmatched отдельно.
    public static OverviewHaDto MapHa(EtcdSnapshot snapshot)
        => new(
            snapshot.HaScopes.Count,
            snapshot.HaScopes.Count(s => s.Matched && s.LeaderName is null),
            snapshot.HaScopes.Count(s => !s.Matched));

    // Бэкапы (u06): S3Storage (live-инвентарь/health) + BackupStorage (etcd-ключ
    // места) + EtcdSnapshots (выгрузка); Health null — фактов нет; usedBytes из
    // etcd-ключа (как TS BackupsCard), storageState — вердикт воркера.
    public static OverviewBackupsDto MapBackups(EtcdSnapshot snapshot)
    {
        var s3 = snapshot.S3Storage;
        var storage = snapshot.BackupStorage;
        var export = snapshot.EtcdSnapshots;
        var health = s3?.Health;
        return new OverviewBackupsDto(
            s3 is not null,
            health is null
                ? null
                : new OverviewBackupsHealthDto(health.ApiOk, health.LiveOk, health.ClusterOk),
            storage?.State.ToString().ToUpperInvariant(),
            storage?.UsedBytes ?? 0,
            storage?.QuotaBytes,
            export is null ? null : new OverviewEtcdSnapshotsDto(export.Enabled, export.State));
    }

    // Надёжность (u06): перенос TS ReliabilityCard 1:1 (D6) — только Active-
    // кластерам (не Active: notInitialized/TO_REMOVE не завышают счётчики и не
    // задают worst); worst = max rpoPotentialSec по rpo.mode != off;
    // lastFailover — самый НЕдавний закрытый факт (max resolvedUnix ??
    // detectedUnix), только с durationSec.
    public static OverviewReliabilityDto MapReliability(EtcdSnapshot snapshot, DateTimeOffset nowUtc)
    {
        var reliability = ReliabilityCalculator.Calculate(snapshot, nowUtc);
        var active = snapshot.Clusters
            .Where(c => c.State == ClusterState.Active)
            .Select(c => c.Name)
            .ToHashSet(StringComparer.Ordinal);
        var shards = reliability
            .Where(c => active.Contains(c.Cluster))
            .SelectMany(c => c.Shards)
            .ToList();

        long? worst = null;
        long? lastFailoverAt = null;
        OverviewLastFailoverDto? lastFailover = null;
        foreach (var shard in shards)
        {
            if (shard.Rpo is { } rpo)
            {
                if (rpo.Mode != RpoMode.Off && rpo.RpoPotentialSec is { } potential
                    && (worst is null || potential > worst))
                    worst = potential;
            }

            var failover = shard.Rto?.LastFailover;
            if (failover?.DurationSec is not { } duration)
                continue;
            var at = failover.ResolvedUnix ?? failover.DetectedUnix;
            if (lastFailoverAt is null || at > lastFailoverAt)
            {
                lastFailoverAt = at;
                lastFailover = new OverviewLastFailoverDto(duration, at);
            }
        }

        return new OverviewReliabilityDto(
            worst,
            shards.Count(s => s.Rpo?.Mode == RpoMode.Full),
            shards.Count(s => s.Rpo?.Mode == RpoMode.Off),
            lastFailover);
    }

    // Активные операции всех доменов (u07): pg — переезды/заявки/ротации/
    // restore/drill/work; kafka — ротации/ребаланс/reassignment/regens/lifecycle;
    // valkey/s3 — ротации. Имена kind — словарь этого метода (UI не парсит).
    public static IReadOnlyList<OverviewOperationDto> MapOperations(
        EtcdSnapshot pg,
        Core.Kafka.KafkaSnapshot? kafka,
        Core.Valkey.ValkeySnapshot? valkey,
        Core.S3.S3Snapshot? s3)
    {
        var operations = new List<OverviewOperationDto>();

        // pg: не-ACTIVE бакеты (SYNCING/FROZEN/ABORTING — как activeMoves).
        foreach (var cluster in pg.Clusters)
        {
            foreach (var bucket in cluster.Buckets
                .Where(b => b.State is BucketState.Syncing or BucketState.Frozen or BucketState.Aborting)
                .OrderBy(b => b.Id))
            {
                operations.Add(new OverviewOperationDto(
                    "pg", "move", cluster.Name, bucket.Id.ToString(),
                    $"{bucket.Move?.Owner} → {bucket.Move?.Target}", bucket.Move?.UpdatedUnix));
            }
        }

        // pg: очередь заявок переездов.
        foreach (var ticket in pg.MoveTickets.OrderBy(t => t.RequestedUnix))
            operations.Add(new OverviewOperationDto(
                "pg", "move-ticket", ticket.Cluster, ticket.Bucket, ticket.Op, ticket.RequestedUnix));

        // pg: живые заявки ротаций per-cluster секретов.
        foreach (var rotation in pg.PgRotations ?? [])
            operations.Add(new OverviewOperationDto(
                "pg", "rotation", rotation.Cluster, null, rotation.RequestedBy ?? "", rotation.RequestedUnix));

        // pg: активные restore (RUNNING|REJOINING) и RUNNING-дриллы.
        foreach (var backups in pg.Backups)
        {
            foreach (var (shard, restores) in backups.ShardsRestores
                ?? (IReadOnlyDictionary<string, IReadOnlyList<RestoreOperationInfo>>)new Dictionary<string, IReadOnlyList<RestoreOperationInfo>>())
            {
                foreach (var restore in restores.Where(r => r.State is "RUNNING" or "REJOINING"))
                    operations.Add(new OverviewOperationDto(
                        "pg", "restore", backups.Cluster, shard,
                        restore.Phase ?? restore.State.ToLowerInvariant(), restore.RequestedUnix));
            }

            foreach (var (shard, drill) in backups.ShardsDrills
                ?? (IReadOnlyDictionary<string, DrillInfo>)new Dictionary<string, DrillInfo>())
            {
                if (drill.State == "RUNNING")
                    operations.Add(new OverviewOperationDto(
                        "pg", "drill", backups.Cluster, shard,
                        drill.Phase ?? "running", drill.StartedUnix));
            }
        }

        // pg: переходные фазы work-журнала (op воркера в исполнении; не done).
        foreach (var work in pg.PgWorkerWork)
        {
            if (work.Op is "supervise" || work.Phase is "done" or "expired")
                continue; // стационарная запись надзора/терминальные фазы — не операции
            operations.Add(new OverviewOperationDto(
                "pg", "work", work.Cluster, null, $"{work.Op}/{work.Phase}", work.UpdatedUnix));
        }

        // kafka: ротации app/admin (kind rotation), CA, ребаланс, прогресс.
        if (kafka is not null)
        {
            foreach (var ticket in kafka.Rotations)
                operations.Add(new OverviewOperationDto(
                    "kafka", "rotation", ticket.Cluster, null,
                    ticket.RequestedBy ?? "", ticket.RequestedUnix));
            foreach (var ticket in kafka.AdminRotations ?? [])
                operations.Add(new OverviewOperationDto(
                    "kafka", "rotation", ticket.Cluster, null,
                    ticket.RequestedBy ?? "", ticket.RequestedUnix));
            foreach (var ticket in kafka.CaRotations ?? [])
                operations.Add(new OverviewOperationDto(
                    "kafka", "ca-rotation", ticket.Cluster, null,
                    ticket.RequestedBy ?? "", ticket.RequestedUnix));
            foreach (var ticket in kafka.Rebalances)
                operations.Add(new OverviewOperationDto(
                    "kafka", "rebalance", ticket.Cluster, null,
                    ticket.RequestedBy ?? "", ticket.RequestedUnix));
            foreach (var progress in kafka.Reassignments)
                operations.Add(new OverviewOperationDto(
                    "kafka", "reassignment", progress.Cluster, progress.DrainBroker,
                    $"{progress.PartitionsRemaining}/{progress.PartitionsTotal}", progress.UpdatedUnix));
            foreach (var regen in kafka.Regens)
                operations.Add(new OverviewOperationDto(
                    "kafka", "regen", regen.Cluster, regen.CurrentBroker,
                    $"{regen.BrokersRemaining}/{regen.BrokersTotal}", regen.UpdatedUnix));
            foreach (var cluster in kafka.Clusters)
            {
                foreach (var ticket in cluster.LifecycleTickets ?? [])
                    operations.Add(new OverviewOperationDto(
                        "kafka", "lifecycle", cluster.Name, ticket.Topic,
                        ticket.Op, ticket.RequestedUnix));
            }
        }

        // valkey: ротация пароля (role в деталях) и CA.
        if (valkey is not null)
        {
            foreach (var ticket in valkey.Rotations)
                operations.Add(new OverviewOperationDto(
                    "valkey", "rotation", ticket.Cluster, null,
                    ticket.Role, ticket.RequestedUnix));
            foreach (var ticket in valkey.CaRotations ?? [])
                operations.Add(new OverviewOperationDto(
                    "valkey", "ca-rotation", ticket.Cluster, null,
                    ticket.RequestedBy ?? "", ticket.RequestedUnix));
        }

        // s3: живая заявка ротации клиентских кредов.
        if (s3 is not null)
        {
            foreach (var ticket in s3.Rotations)
                operations.Add(new OverviewOperationDto(
                    "s3", "rotation", ticket.Cluster, null,
                    ticket.RequestedBy ?? "", ticket.RequestedUnix));
        }

        return operations;
    }

    // Свежесть (u08): BuiltAtUtc per снапшот (unix-секунды); etcd — lastRefresh;
    // null-домен — снапшот ещё не тикал (UI рисует серое «—»).
    public static OverviewFreshnessDto MapFreshness(
        EtcdSnapshot pg,
        Core.Kafka.KafkaSnapshot? kafka,
        Core.Valkey.ValkeySnapshot? valkey,
        Core.S3.S3Snapshot? s3)
        => new(
            new OverviewFreshnessItemDto(pg.Etcd.LastRefreshUtc.ToUnixTimeSeconds()),
            new OverviewFreshnessItemDto(pg.BuiltAtUtc.ToUnixTimeSeconds()),
            kafka is null ? null : new OverviewFreshnessItemDto(kafka.BuiltAtUtc.ToUnixTimeSeconds()),
            valkey is null ? null : new OverviewFreshnessItemDto(valkey.BuiltAtUtc.ToUnixTimeSeconds()),
            s3 is null ? null : new OverviewFreshnessItemDto(s3.BuiltAtUtc.ToUnixTimeSeconds()));

    // kafka-сводка (arch/03 §7.1): кластеры + ВСЕ critical kafka-алерты
    // (MapKafka хендлера — public-семантика прежняя, дубликат осознан).
    public static OverviewKafkaDto? MapKafkaSummary(Core.Kafka.KafkaSnapshot? kafka)
        => kafka is null
            ? null
            : new OverviewKafkaDto(
                kafka.Clusters.Count,
                kafka.Alerts.Count(a => a.Severity == Core.AlertSeverity.Critical));

    // valkey-сводка: кластеры + critical-алерты ТОЛЬКО кластерных kinds
    // valkey-node-not-running/valkey-endpoints-missing (arch/03 §8.1 — НЕ
    // worker-api-unreachable: сознательное отличие от MapKafka, считающего
    // все critical — фиксируется юнит-тестом в Task 7); null до первого тика
    // valkey-refresher'а. Public static — юнит-тесты (как OverviewMapper.Map).
    public static OverviewValkeyDto? MapValkey(Core.Valkey.ValkeySnapshot? valkey)
        => valkey is null
            ? null
            : new OverviewValkeyDto(
                valkey.Clusters.Count,
                valkey.Alerts.Count(a => a.Severity == Core.AlertSeverity.Critical
                    && a.Kind is "valkey-node-not-running" or "valkey-endpoints-missing"));

    // s3-сводка (arch/03 §12.1): кластеры + critical-алерты кластерных
    // kinds s3-node-not-running/s3-endpoints-missing/s3-connectivity-down/
    // s3-sync-diverged (симметрия MapValkey — worker-api-unreachable не считаем);
    // null до первого тика s3-refresher'а. Public static — юнит-тесты.
    public static OverviewS3Dto? MapS3(Core.S3.S3Snapshot? s3)
        => s3 is null
            ? null
            : new OverviewS3Dto(
                s3.Clusters.Count,
                s3.Alerts.Count(a => a.Severity == Core.AlertSeverity.Critical
                    && a.Kind is "s3-node-not-running" or "s3-endpoints-missing"
                        or "s3-connectivity-down" or "s3-sync-diverged"));
}

// Хендлер: store → отказ «снапшота нет» или маппер (spec §3.12).
[InjectAsScoped]
public sealed class OverviewQueryHandler(
    ISnapshotStore store,
    IKafkaSnapshotReader kafkaStore,
    IValkeySnapshotReader valkeyStore,
    IS3SnapshotReader s3Store,
    TimeProvider time,
    IOptions<EtcdOptions> etcdOptions) : IQueryHandler<OverviewQuery, OverviewDto>
{
    public ValueTask<Result<OverviewDto>> Handle(OverviewQuery query, CancellationToken ct)
    {
        var snapshot = store.Current;
        if (snapshot is null)
            return ValueTask.FromResult(
                Result<OverviewDto>.Failed(new InspectionModule.SnapshotNotReadyException()));

        var overview = OverviewMapper.Map(
            snapshot, time.GetUtcNow(), EffectiveInterval(etcdOptions),
            kafkaStore.Current, valkeyStore.Current, s3Store.Current);
        return ValueTask.FromResult(Result<OverviewDto>.Success(overview));
    }

    // kafka-сводка: кластеры + critical-алерты kafka-* (arch/03 §7.1); null до
    // первого тика kafka-refresher'а.
    private static OverviewKafkaDto? MapKafka(Core.Kafka.KafkaSnapshot? kafka)
        => kafka is null
            ? null
            : new OverviewKafkaDto(
                kafka.Clusters.Count,
                kafka.Alerts.Count(a => a.Severity == Core.AlertSeverity.Critical));

    // Эффективный интервал: fallback 3 c при опечатке конфига — как в refresher (§3.3).
    private static double EffectiveInterval(IOptions<EtcdOptions> options)
        => options.Value.RefreshIntervalSeconds > 0
            ? options.Value.RefreshIntervalSeconds
            : 3;
}
