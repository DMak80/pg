using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using PgWorker.Backups.Job;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Processes;

namespace PgWorker.Backups;

/// <summary>Ретенционный проход (t06, arch/19 §4): под клэймом &lt;C&gt; по
/// расписанию Retention:IntervalSec для каждого шарда — доводка DELETING,
/// GFS-отбор (один кандидат-полный в DELETING за проход), чистка WAL ниже
/// cutoff оставляемых, гигиена FAILED; после шардов — монитор занятости
/// /pgworker/backups/storage. Enabled=false → no-op. Ошибка шарда не роняет
/// остальные (образец WalStreamProcess). Единственный удаляющий S3-объекты
/// воркера (R4).</summary>
public sealed class RetentionProcess(
    IEtcdGateway etcd,
    string[] endpoints,
    IBackupS3 s3,
    ClaimStore claims,
    WorkJournal journal,
    BackupsRuntimeOptions options,
    TimeProvider time,
    ILogger<RetentionProcess> logger)
{
    private const string Op = "backups-retention";

    // Расписание per-cluster (ключ — имя кластера). Монитор занятости —
    // ОТДЕЛЬНОЕ поле: имя кластера "storage" валидно по regex
    // ^[a-z][a-z0-9_]{0,62}$ — общий словарь коллидировал бы, и расписание
    // кластера с монитором взаимно отодвигали бы друг друга (ревью Ф4-2 №2).
    private readonly ConcurrentDictionary<string, long> _lastPassUnix = [];

    private long _lastStoragePassUnix;

    public async Task<Result<ProcessOutcome>> TickAsync(
        ClusterSnapshot snap, ClusterBackups? backups, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;

        // Мутации /pgworker/backups/* — только держатель клэйма (arch/19 §4).
        if (!claims.IsMine(cluster))
            return Result<ProcessOutcome>.Failed(new ApplicationException(
                $"{Op} {cluster}: клэйм не наш (или потерян) — мутации запрещены"));

        // G0: подсистема выключена — no-op (AC8).
        if (!options.Enabled)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);

        // Только Active-кластер (spec §3.3 G0).
        if (snap.Config.State != ClusterState.Active)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);

        var nowUnix = time.GetUtcNow().ToUnixTimeSeconds();

        // S: расписание per-cluster; не время — только storage-монитор.
        if (nowUnix - _lastPassUnix.GetOrAdd(cluster, 0L) >= options.RetentionIntervalSec)
        {
            _lastPassUnix[cluster] = nowUnix;

            // Дефолт-политика: policy-ключ кластера ?? конфиг (spec §3.3 п.2).
            var policy = backups?.Policy ?? new BackupPolicy(
                options.PolicyRetentionDays, options.PolicyRetentionWeeks,
                options.PolicyRetentionMonths, options.FullMaxAgeSec, options.VerifyOnCreate);
            var shards = backups?.Shards
                         ?? (IReadOnlyDictionary<string, ShardBackups>)new Dictionary<string, ShardBackups>();

            foreach (var shard in snap.Shards)
            {
                if (!shards.TryGetValue(shard.Name, out var shardBackups))
                    continue; // шард без ключей бэкапов → skip (сироты — t07)

                try
                {
                    await TickShardAsync(cluster, shard.Name, shardBackups, policy, nowUnix, ct);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    // ошибка шарда не роняет остальные (образец WalStreamProcess)
                    logger.LogError(ex, "{Op} {Cluster}/{Shard}: {Message}", Op, cluster, shard.Name, ex.Message);
                    await journal.WritePhaseAsync(cluster, Op, "shard-error", claims.InstanceId,
                        $"{shard.Name}: {ex.Message}", ct);
                }
            }
        }

        // Монитор занятости (spec §3.4) — per-instance расписание.
        var storageResult = await MonitorStorageAsync(nowUnix, ct);
        if (!storageResult.IsSuccess)
            return Result<ProcessOutcome>.Failed(storageResult.Error!);

        return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
    }

    private async Task TickShardAsync(
        string cluster, string shard, ShardBackups shardBackups, BackupPolicy policy,
        long nowUnix, CancellationToken ct)
    {
        var fulls = shardBackups.Full;

        // (1) DELETING-доводка: list full/<id>/ → delete → list пуст → del ключа.
        foreach (var deleting in fulls.Where(f => f.State == FullBackupStatus.Deleting))
            await FinishDeletionAsync(cluster, shard, deleting, ct);

        // (2) GFS-отбор COMPLETED-полных.
        var selection = RetentionPlanner.SelectKeep(fulls, policy, nowUnix);

        // (3) Один кандидат за проход (spec §3.3 п.3): journal-before-manipulations.
        if (selection.Delete.Count > 0)
        {
            var candidate = fulls.First(f => f.Id == selection.Delete[0]);
            var marked = candidate with { State = FullBackupStatus.Deleting };
            var put = await PutAsync(BackupNames.FullKey(cluster, shard, candidate.Id),
                BackupStatusJson.Serialize(marked), ct);
            if (!put.IsSuccess)
                throw new ApplicationException($"put DELETING: {put.Error!.Message}");
            await FinishDeletionAsync(cluster, shard, marked, ct);
        }

        // (4) Чистка WAL: cutoff = min(wal_start оставляемых COMPLETED — Keep ∪
        // неподавленные шагом 3, т.е. все текущие COMPLETED минус удалённый).
        var remaining = fulls.Where(f =>
            f.State == FullBackupStatus.Completed
            && !(selection.Delete.Count > 0 && f.Id == selection.Delete[0]));
        var cutoffs = remaining
            .Select(f => WalFileName.TryParse(f.WalStartSegment ?? ""))
            .Where(w => w is not null)
            .Select(w => w!.Value)
            .OrderBy(w => w.Name, StringComparer.Ordinal)
            .Cast<WalFileName?>()
            .FirstOrDefault();
        if (cutoffs is { } cutoff)
        {
            var listed = await s3.ListPrefixAsync($"{cluster}/{shard}/wal/", ct: ct);
            if (!listed.IsSuccess)
                throw new ApplicationException($"list wal: {listed.Error!.Message}");
            var doomed = RetentionPlanner.SelectWalForDeletion(
                listed.Value.Select(o => o.Key.Split('/')[^1]).ToList(), cutoff);
            if (doomed.Count > 0)
            {
                var keys = doomed.Select(n => $"{cluster}/{shard}/wal/{n}").ToList();
                var deleted = await s3.DeleteKeysAsync(keys, ct);
                if (!deleted.IsSuccess)
                    throw new ApplicationException($"delete wal: {deleted.Error!.Message}");
                await journal.WritePhaseAsync(cluster, Op, $"wal-trimmed/{shard}/{doomed.Count}",
                    claims.InstanceId, null, ct);
            }
        }

        // (5) Гигиена FAILED: держать последние Retention:KeepFailed.
        var prune = RetentionPlanner.SelectFailedForPrune(fulls, options.RetentionKeepFailed);
        foreach (var id in prune)
        {
            var deleted = await DeleteAsync(BackupNames.FullKey(cluster, shard, id), ct);
            if (!deleted.IsSuccess)
                throw new ApplicationException($"del FAILED {id}: {deleted.Error!.Message}");
        }

        if (prune.Count > 0)
            await journal.WritePhaseAsync(cluster, Op, $"failed-pruned/{shard}/{prune.Count}",
                claims.InstanceId, null, ct);
    }

    // Доводка DELETING (идемпотентна): list префикса → есть объекты →
    // batch-delete → list пуст → del etcd-ключа. transient-отказ S3 —
    // исключение наверх (пер-шардовый catch): статус остаётся DELETING,
    // следующий проход повторит (spec §3.3 п.1, AC3).
    private async Task FinishDeletionAsync(
        string cluster, string shard, FullBackupState deleting, CancellationToken ct)
    {
        var prefix = $"{cluster}/{shard}/full/{deleting.Id}/";
        var listed = await s3.ListPrefixAsync(prefix, ct: ct);
        if (!listed.IsSuccess)
            throw new ApplicationException($"list {prefix}: {listed.Error!.Message}");
        if (listed.Value.Count > 0)
        {
            var keys = listed.Value.Select(o => o.Key).ToList();
            var deleted = await s3.DeleteKeysAsync(keys, ct);
            if (!deleted.IsSuccess)
                throw new ApplicationException($"delete {prefix}: {deleted.Error!.Message}");
            var recheck = await s3.ListPrefixAsync(prefix, ct: ct);
            if (!recheck.IsSuccess || recheck.Value.Count > 0)
                throw new ApplicationException($"удаление {prefix} не завершилось — повторит следующий проход");
        }

        var del = await DeleteAsync(BackupNames.FullKey(cluster, shard, deleting.Id), ct);
        if (!del.IsSuccess)
            throw new ApplicationException($"del ключа {deleting.Id}: {del.Error!.Message}");
        await journal.WritePhaseAsync(cluster, Op, $"deleted-full/{shard}/{deleting.Id}",
            claims.InstanceId, null, ct);
    }

    // Монитор занятости (spec §3.4): list ВЕСЬ bucket (вкл. чужие/осиротевшие
    // префиксы) → used → EvaluateStorage → ключ при изменении (строковое
    // сравнение — образец WalStatusWriter.WriteIfChangedAsync). Отдельное
    // поле-расписание, не словарь кластеров (коллизия "storage" — см. поле);
    // параллельные тики разных кластеров могут гоняться за поле — двойной list
    // безвреден (put при изменении идемпотентен; тики одного кластера
    // последовательны — ReconcileLoop).
    private async Task<Result> MonitorStorageAsync(long nowUnix, CancellationToken ct)
    {
        if (nowUnix - _lastStoragePassUnix < options.RetentionIntervalSec)
            return Result.Success();
        _lastStoragePassUnix = nowUnix;

        var listed = await s3.ListPrefixAsync("", ct: ct);
        if (!listed.IsSuccess)
            return Result.Failed(listed.Error!); // transient: повтор тика

        var used = listed.Value.Sum(o => o.SizeBytes);
        var verdict = RetentionPlanner.EvaluateStorage(
            used, options.QuotaBytes, options.QuotaWarnPercent, options.QuotaCritPercent);
        var payload = StorageStatusJson.Serialize(new StorageStatus(
            verdict.UsedBytes, verdict.QuotaBytes, verdict.UsedPercent, verdict.State, nowUnix));

        foreach (var endpoint in endpoints)
        {
            var current = await etcd.GetAsync(endpoint, "/pgworker/backups/storage", ct);
            if (!current.IsSuccess)
                continue; // failover
            if (current.Value is { } kv && kv.Value == payload)
                return Result.Success(); // без изменений — не пишем
            var put = await etcd.PutAsync(endpoint, "/pgworker/backups/storage", payload, null, ct);
            if (put.IsSuccess)
                return Result.Success();
        }

        return Result.Failed(new ApplicationException("запись /pgworker/backups/storage не удалась"));
    }

    // Failover-обёртки (образец BackupProcess.PutAsync / WalStreamProcess.GetAsync).
    private async Task<Result> PutAsync(string key, string value, CancellationToken ct)
    {
        Result? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.PutAsync(endpoint, key, value, null, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }

    private async Task<Result> DeleteAsync(string key, CancellationToken ct)
    {
        Result? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.DeleteAsync(endpoint, key, prefix: false, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }
}
