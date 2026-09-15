using Microsoft.Extensions.Logging;
using PgWorker.Core;
using PgWorker.Core.Model;
using Shared.Etcd.Client;
using PgWorker.Etcd.Coordination;
using PgWorker.Etcd.Parsing;

namespace PgWorker.Backups.Supervisor;

/// <summary>Глобальный проход сверки bucket↔etcd (t07, arch/19 §4): list ВСЕГО
/// bucket → префиксы «&lt;C&gt;/&lt;X&gt;/» без владельца (кластер ToRemove/исчез,
/// шард удалён) — сироты: реестр /pgworker/backups/orphans (merge first_seen,
/// гвард воскресения) + TTL-удаление по Supervisor:OrphanTtlSec (ОДИН префикс
/// за проход: DELETING → batch-delete → list-подтверждение → del записи).
/// Выполняется ТОЛЬКО глобальным лидером /pgworker/leader (двойного писателя
/// нет); runtime() == null (Enabled=false) → no-op. Все шаги идемпотентны,
/// transient-отказы (S3/etcd) — Result.Failed без мутаций, следующий проход
/// повторит (arch/17).</summary>
public sealed class BackupOrphanSweeper(
    IEtcdGateway etcd,
    string[] endpoints,
    IBackupS3 s3,
    ClaimStore claims,
    WorkJournal journal,
    Func<BackupsRuntimeOptions?> runtime,
    TimeProvider time,
    ILogger? logger = null)
{
    private const string Op = "backup-orphan-sweeper";

    /// <summary>Один проход (лидерство/расписание — в BackupOrphanSweeperLoop).</summary>
    public async Task<Result> SweepAsync(CancellationToken ct)
    {
        // Гвард 1: только глобальный лидер (арх/19 §4 — единственный писатель).
        if (!claims.IsLeader)
            return Result.Success(); // no-op — не наша роль

        // Гвард 2: подсистема выключена — реестр не пишется, удаления нет.
        var options = runtime();
        if (options is null)
            return Result.Success();

        var nowUnix = time.GetUtcNow().ToUnixTimeSeconds();

        // (1) list ВЕСЬ bucket (образец storage-монитора t06) → группировка.
        var listed = await s3.ListPrefixAsync("", ct: ct);
        if (!listed.IsSuccess)
            return Result.Failed(listed.Error!);
        var observed = OrphanRegistry.GroupShardPrefixes(listed.Value);

        // (2) Владельцы: /clusters/ → живые кластеры (State ≠ ToRemove) и их
        // шарды (!ToRemove).
        var range = await RangeClustersAsync(ct);
        if (!range.IsSuccess)
            return Result.Failed(range.Error!);
        var parsed = ClusterSnapshotParser.ParseClusters(range.Value, out _);
        if (!parsed.IsSuccess)
            return Result.Failed(parsed.Error!);
        var liveClusters = new HashSet<string>(StringComparer.Ordinal);
        var liveShards = new HashSet<(string Cluster, string Shard)>();
        foreach (var snap in parsed.Value)
        {
            if (snap.Config.State == ClusterState.ToRemove)
                continue;
            liveClusters.Add(snap.Config.Cluster);
            foreach (var shard in snap.Shards)
                if (!shard.ToRemove)
                    liveShards.Add((snap.Config.Cluster, shard.Name));
        }

        // (3) Merge реестра: новые сироты OBSERVED; воскресшие гаснут. Put —
        // только при изменении записей (безделье не пишет; updated_unix —
        // свежесть прохода, в сравнение не входит).
        var current = await ReadRegistryAsync(ct);
        var merged = OrphanRegistry.Merge(current, observed, liveShards, liveClusters, nowUnix);
        var changed = current is null || !OrphanRegistry.SameOrphans(current, merged);
        if (changed && await PutRegistryAsync(merged, ct) is { IsSuccess: false } putMerged)
            return putMerged; // transient — реестр не записан, удаление не начинаем

        // (4) TTL: один префикс за проход (тик короткий, образец ретенции).
        var candidate = OrphanRegistry.SelectTtlCandidate(
            merged, options.SupervisorOrphanTtlSec, nowUnix);
        if (candidate is null)
            return Result.Success();
        var entry = merged.Orphans.First(e => e.Prefix == candidate);

        // journal-before-manipulations: DELETING пишется ДО удаления объектов.
        var deleting = merged with
        {
            Orphans = merged.Orphans
                .Select(e => e.Prefix == candidate
                    ? e with { State = OrphanState.Deleting } : e)
                .ToList(),
        };
        if (await PutRegistryAsync(deleting, ct) is { IsSuccess: false } putDeleting)
            return putDeleting; // запись остаётся OBSERVED — следующий проход повторит
        await journal.WritePhaseAsync(candidate.Split('/')[0], Op,
            $"orphan-deleting/{candidate}", claims.InstanceId, null, ct);

        // batch-delete префикса + list-подтверждение пустоты.
        var prefix = $"{candidate}/";
        var objects = await s3.ListPrefixAsync(prefix, ct: ct);
        if (!objects.IsSuccess)
            return Result.Failed(objects.Error!); // запись остаётся DELETING — доведёт
        if (objects.Value.Count > 0)
        {
            var deleted = await s3.DeleteKeysAsync(
                objects.Value.Select(o => o.Key).ToList(), ct);
            if (!deleted.IsSuccess)
                return Result.Failed(deleted.Error!);
            var recheck = await s3.ListPrefixAsync(prefix, ct: ct);
            if (!recheck.IsSuccess || recheck.Value.Count > 0)
                return Result.Failed(new ApplicationException(
                    $"удаление {prefix} не завершилось — повторит следующий проход"));
        }

        // Объекты удалены — del записи из реестра + журнал-факт.
        var done = deleting with
        {
            Orphans = deleting.Orphans.Where(e => e.Prefix != candidate).ToList(),
        };
        if (await PutRegistryAsync(done, ct) is { IsSuccess: false } putDone)
            return putDone;
        await journal.WritePhaseAsync(candidate.Split('/')[0], Op,
            $"orphan-deleted/{candidate}", claims.InstanceId, null, ct);
        logger?.LogInformation("{Op}: сирота {Prefix} удалена по TTL", Op, candidate);

        return Result.Success();
    }

    // Чтение /clusters/ (failover по endpoints) — владельцы префиксов.
    private async Task<Result<IReadOnlyList<Kv>>> RangeClustersAsync(CancellationToken ct)
    {
        Result<IReadOnlyList<Kv>>? last = null;
        foreach (var endpoint in endpoints)
        {
            var range = await etcd.RangeAsync(endpoint, "/clusters/", ct);
            if (range.IsSuccess)
                return range;
            last = range;
        }

        return Result<IReadOnlyList<Kv>>.Failed(last?.Error
            ?? new ApplicationException("нет живых endpoints etcd"));
    }

    // Чтение реестра (failover по endpoints); ключа нет → null; битый → null.
    private async Task<OrphanRegistry.Registry?> ReadRegistryAsync(CancellationToken ct)
    {
        Result? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.GetAsync(endpoint, OrphanRegistry.Key, ct);
            if (!result.IsSuccess)
            {
                last = result;
                continue;
            }

            return result.Value is { } kv ? OrphanRegistry.Parse(kv.Value) : null;
        }

        if (last is not null)
            throw new ApplicationException($"get {OrphanRegistry.Key}: {last.Error!.Message}");
        return null;
    }

    // Failover-put реестра (образец WalStreamProcess.PutAsync).
    private async Task<Result> PutRegistryAsync(OrphanRegistry.Registry registry, CancellationToken ct)
    {
        Result? last = null;
        foreach (var endpoint in endpoints)
        {
            var put = await etcd.PutAsync(
                endpoint, OrphanRegistry.Key, OrphanRegistry.ToJson(registry), null, ct);
            if (put.IsSuccess)
                return Result.Success();
            last = put;
        }

        return Result.Failed(last?.Error ?? new ApplicationException(
            $"put {OrphanRegistry.Key}: нет живых endpoints"));
    }
}
