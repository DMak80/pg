using Microsoft.Extensions.Logging;
using PgWorker.Backups.Job;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Etcd.Backups;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Processes;
using Shared.Core;
using Shared.Etcd.Client;
using Shared.Metrics.Worker;

namespace PgWorker.Backups.Supervisor;

/// <summary>Фоновая чистка мусора префикса wal/ (arch/19 §4 «wal-sweeper»):
/// ТОЛЬКО глобальный лидер /pgworker/leader; per-shard независимо (отказ/пауза
/// шарда не останавливает остальные); листинг — соло, первая доступная нода
/// (инвентарная семантика §5, повторные проходы досматривают лаг mesh).
/// Удаляет: финальные сегменты ниже cutoff (cutoff = wal_start новейшего
/// verify-OK COMPLETED из оставляемых GFS-отбором — «по факту ретенционного
/// отбора», повтор RetentionPlanner; нет OK-полного ⇒ срез ниже cutoff не
/// выполняется) и .partial-гигиену (SelectPartialForDeletion: поглощённые
/// финалом, устаревшие версии, ниже cutoff). Список на удаление — ОДИН
/// batch-delete на шард (идемпотентно). Backups:Enabled=false ⇒ no-op.
/// Наблюдаемость: worker_loop_*{loop="wal-sweeper"} (цикл) + per-shard серия
/// WalSweepShardUnix (arch/18 §2.2).</summary>
public sealed class WalSweeperProcess(
    IEtcdGateway etcd,
    string[] endpoints,
    BackupS3Factory s3Factory,
    ClaimStore claims,
    WorkJournal journal,
    Func<BackupsRuntimeOptions?> runtime,
    TimeProvider time,
    ILogger? logger = null,
    WorkerMetricsInstrumentation? metrics = null)
{
    private const string Op = "backup-wal-sweeper";

    public async Task<Result> SweepAsync(CancellationToken ct)
    {
        // Гварды: только глобальный лидер; выключено — no-op (как ретенция).
        if (!claims.IsLeader)
            return Result.Success();
        if (runtime() is not { } options)
            return Result.Success();

        var range = await RangeAsync("/clusters/", ct);
        if (!range.IsSuccess)
            return Result.Failed(range.Error!);
        var parsed = ClusterSnapshotParser.ParseClusters(range.Value, out _);
        if (!parsed.IsSuccess)
            return Result.Failed(parsed.Error!);

        var backupsByCluster = new Dictionary<string, ClusterBackups?>();
        foreach (var snap in parsed.Value)
            backupsByCluster[snap.Config.Cluster] = await ReadBackupsAsync(snap.Config.Cluster, ct);

        return await SweepShardsAsync(parsed.Value, backupsByCluster,
            time.GetUtcNow().ToUnixTimeSeconds(), ct);
    }

    /// <summary>Ядро прохода (юнит-точка входа): снапшоты кластеров + их
    /// бэкап-статусы; per-shard cutoff/листинг/batch-delete; завершение —
    /// снятие per-shard серий исчезнувших шардов.</summary>
    internal async Task<Result> SweepShardsAsync(
        IReadOnlyList<ClusterSnapshot> snaps,
        IReadOnlyDictionary<string, ClusterBackups?> backupsByCluster,
        long nowUnix, CancellationToken ct)
    {
        var options = runtime();
        if (options is null)
            return Result.Success();

        var live = new HashSet<(string Cluster, string Shard)>();
        foreach (var snap in snaps)
        {
            if (snap.Config.State == ClusterState.ToRemove)
                continue;
            var poolResult = await s3Factory.ResolveAsync(snap.Config.Cluster, ct);
            if (!poolResult.IsSuccess || poolResult.Value is null)
            {
                // Пул не выбран/Failed — шард skip (ретенция молчит симметрично).
                logger?.LogWarning("{Op} {Cluster}: pool not resolved — shards skipped",
                    Op, snap.Config.Cluster);
                continue;
            }
            var backups = backupsByCluster.GetValueOrDefault(snap.Config.Cluster);
            var policy = backups?.Policy ?? new BackupPolicy(
                options.PolicyRetentionDays, options.PolicyRetentionWeeks,
                options.PolicyRetentionMonths, options.FullMaxAgeSec, options.VerifyOnCreate);

            foreach (var shard in snap.Shards)
            {
                if (shard.ToRemove)
                    continue;
                live.Add((snap.Config.Cluster, shard.Name));
                try
                {
                    await SweepShardAsync(snap.Config.Cluster, shard.Name,
                        backups?.Shards.GetValueOrDefault(shard.Name), policy, options,
                        poolResult.Value, nowUnix, ct);
                    metrics?.WalSweepShardUnix(snap.Config.Cluster, shard.Name, nowUnix);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    // Ошибка шарда не валит остальные (образец WalStreamProcess).
                    logger?.LogError(ex, "{Op} {Cluster}/{Shard}: {Message}",
                        Op, snap.Config.Cluster, shard.Name, ex.Message);
                    await journal.WritePhaseAsync(snap.Config.Cluster, Op,
                        $"shard-error/{shard.Name}", claims.InstanceId, ex.Message, ct);
                }
            }
        }

        metrics?.WalSweepShardRetain(live);
        return Result.Success();
    }

    // Один шард: list wal/ → cutoff (GFS-отбор оставляемых ретенции) →
    // doomed = финалы ниже cutoff + .partial-гигиена → ОДИН batch-delete.
    private async Task SweepShardAsync(
        string cluster, string shard, ShardBackups? shardBackups, BackupPolicy policy,
        BackupsRuntimeOptions options, BackupPoolContext pool, long nowUnix, CancellationToken ct)
    {
        var s3 = s3Factory.Create(pool);
        var listed = await s3.ListWalAsync(cluster, shard, ct: ct);
        if (!listed.IsSuccess)
            throw new ApplicationException($"list wal: {listed.Error!.Message}");
        var names = listed.Value.Select(o => o.Name).ToList();

        // Cutoff = wal_start новейшего verify-OK COMPLETED из ОСТАВЛЯЕМЫХ
        // (SelectKeep — тот же отбор, что у ретенции; кандидат Delete[0] не
        // якорит — arch/19 §4 п.5). Нет OK-полного ⇒ cutoff null.
        WalFileName? cutoff = null;
        if (shardBackups?.Full is { Count: > 0 } fulls)
        {
            var selection = RetentionPlanner.SelectKeep(fulls, policy, nowUnix);
            var remaining = fulls.Where(f =>
                f.State == FullBackupStatus.Completed
                && !(selection.Delete.Count > 0 && f.Id == selection.Delete[0])).ToList();
            cutoff = RetentionPlanner.LatestVerifiedWalStart(remaining);
        }

        // t15 (spec §4.1(д)/§4.6, решение №20): вход кусочной гигиены — текущий
        // номер жизни из счётчика. Гвард активного restore: пока заявка шарда не
        // терминальна, ветка чужих номеров отключена — куски её жизни читает
        // fetch-wal наката. Отказ чтения счётчика — консервативно null.
        uint? chainLife = null;
        var activeRestore = shardBackups?.Restores.Any(r => r.State
            is RestoreStatus.Planned or RestoreStatus.Running or RestoreStatus.Rejoining) == true;
        if (!activeRestore)
        {
            var life = await new WalLifeStore(etcd, endpoints).ReadValueAsync(cluster, shard, ct);
            if (life.IsSuccess)
                chainLife = life.Value;
        }

        // Гейт ветки (д): куски жизни последнего полного шарда не выметаются —
        // это RPO-хвост повторного restore/drill выбранного полного в окне до
        // пересъёма (arch/backups/processes/d-wal-sweeper.md). Полных нет/старые
        // ключи без chain_life — гейта нет.
        var lastFullLife = RetentionPlanner.LastFullLife(shardBackups?.Full ?? []);

        var doomed = (cutoff is { } limit
                ? RetentionPlanner.SelectWalForDeletion(names, limit) : [])
            .Concat(RetentionPlanner.SelectPartialForDeletion(names, cutoff, chainLife, lastFullLife))
            .ToList();
        if (doomed.Count == 0)
            return;
        var keys = doomed.Select(n => $"{cluster}/{shard}/wal/{n}").ToList();
        var deleted = await s3.DeleteKeysAsync(keys, ct);
        if (!deleted.IsSuccess)
            throw new ApplicationException($"delete wal garbage: {deleted.Error!.Message}");
        await journal.WritePhaseAsync(cluster, Op, $"wal-swept/{shard}/{doomed.Count}",
            claims.InstanceId, null, ct);
    }

    // Снапшот бэкапов кластера: Range /pgworker/backups/<C>/ → BackupsParser;
    // отказ — null (transient: проход без cutoff-ветки, только .partial-гигиена).
    private async Task<ClusterBackups?> ReadBackupsAsync(string cluster, CancellationToken ct)
    {
        var range = await RangeAsync($"/pgworker/backups/{cluster}/", ct);
        if (!range.IsSuccess)
            return null;
        var parsed = BackupsParser.Parse(range.Value, out _);
        return parsed.IsSuccess
            ? parsed.Value.FirstOrDefault(b => b.Cluster == cluster)
            : null;
    }

    // Failover-Range (образец BackupOrphanSweeper.RangeClustersAsync).
    private async Task<Result<IReadOnlyList<Kv>>> RangeAsync(string prefix, CancellationToken ct)
    {
        Result<IReadOnlyList<Kv>> last = default;
        foreach (var endpoint in endpoints)
        {
            var range = await etcd.RangeAsync(endpoint, prefix, ct);
            if (range.IsSuccess)
                return range;
            last = range;
        }
        return Result<IReadOnlyList<Kv>>.Failed(last.Error
            ?? new EtcdUnreachableException("etcd endpoints not configured"));
    }
}
