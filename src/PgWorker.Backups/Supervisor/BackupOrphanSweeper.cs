using Microsoft.Extensions.Logging;
using PgWorker.Core;
using PgWorker.Core.Model;
using Shared.Etcd.Client;
using PgWorker.Etcd.Parsing;

namespace PgWorker.Backups.Supervisor;

/// <summary>Глобальный проход сверки bucket↔etcd (t07, arch/19 §4): list ВСЕГО
/// bucket → префиксы «&lt;C&gt;/&lt;X&gt;/» без владельца (кластер ToRemove/исчез,
/// шард удалён) — сироты: реестр /pgworker/backups/orphans (merge first_seen и
/// has_valid_full, гвард воскресения) + отбор кандидата с приоритетом (t04,
/// arch/19 §4): доводка первого DELETING → первая заявка /orphan-deletes/ (t04,
/// обходит hold и автоправило) → TTL-кандидат без валидного полного и без
/// hold-ключа (DR-hold t04). Санитар прохода (t04): hold/заявки с префиксом вне
/// merged-реестра (воскрес/исчез/уже удалён) гасятся. ОДИН префикс за проход:
/// DELETING → batch-delete → list-подтверждение → del записи (+ del заявки).
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
        var grouped = OrphanRegistry.GroupPrefixes(listed.Value);
        var observed = grouped.Sizes;

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
        var merged = OrphanRegistry.Merge(
            current, observed, grouped.FullPrefixes, liveShards, liveClusters, nowUnix);
        var changed = current is null || !OrphanRegistry.SameOrphans(current, merged);
        if (changed && await PutRegistryAsync(merged, ct) is { IsSuccess: false } putMerged)
            return putMerged; // transient — реестр не записан, удаление не начинаем

        // (3.5) Санитар: hold/заявки, чей префикс вне merged-реестра (воскрес/
        // исчез/уже удалён), гасятся — «висящих» флагов не копится (spec §3.3).
        var holdsRange = await RangeKeysAsync(OrphanRegistry.HoldsPrefix, ct);
        if (!holdsRange.IsSuccess)
            return holdsRange;
        var deletesRange = await RangeKeysAsync(OrphanRegistry.DeletesPrefix, ct);
        if (!deletesRange.IsSuccess)
            return deletesRange;
        var registryPrefixes = merged.Orphans
            .Select(e => e.Prefix).ToHashSet(StringComparer.Ordinal);
        var heldPrefixes = new HashSet<string>(StringComparer.Ordinal);
        foreach (var kv in holdsRange.Value)
        {
            var prefix = PrefixOfKey(kv.Key); // «<C>/<X>» — 2 последних сегмента ключа
            if (registryPrefixes.Contains(prefix))
                heldPrefixes.Add(prefix);
            else if (await DeleteKeyAsync(kv.Key, ct) is { IsSuccess: false } delHold)
                return delHold; // transient — следующий проход повторит
            else
                await journal.WritePhaseAsync(prefix.Split('/')[0], Op,
                    $"orphan-key-cleaned/{prefix}", claims.InstanceId, null, ct);
        }
        string? requested = null; // префикс первой заявки (по ordinal ключа)
        foreach (var kv in deletesRange.Value.OrderBy(k => k.Key, StringComparer.Ordinal))
        {
            var prefix = PrefixOfKey(kv.Key);
            if (registryPrefixes.Contains(prefix))
            {
                requested ??= prefix;
                continue;
            }
            if (await DeleteKeyAsync(kv.Key, ct) is { IsSuccess: false } delReq)
                return delReq;
            await journal.WritePhaseAsync(prefix.Split('/')[0], Op,
                $"orphan-key-cleaned/{prefix}", claims.InstanceId, null, ct);
        }

        // (4) Отбор (спека §3.1): (1) доводка первого DELETING безусловно →
        // (2) первая заявка orphan-deletes (обходит hold и автоправило — осознанная
        // команда) → (3) TTL-кандидат без полных и без hold. Один префикс за проход.
        var candidate = merged.Orphans
            .FirstOrDefault(e => e.State == OrphanState.Deleting)?.Prefix;
        var byRequest = false;
        if (candidate is null && requested is not null)
        {
            candidate = requested;
            byRequest = true;
        }
        if (candidate is null)
            candidate = OrphanRegistry.SelectTtlCandidate(
                merged, options.SupervisorOrphanTtlSec, nowUnix, heldPrefixes);
        if (candidate is null)
            return Result.Success();
        var entry = merged.Orphans.First(e => e.Prefix == candidate);

        // journal-before-manipulations: DELETING пишется ДО удаления объектов.
        // По заявке — журнал заявки до перехода в DELETING (AC4).
        if (byRequest)
            await journal.WritePhaseAsync(candidate.Split('/')[0], Op,
                $"orphan-delete-requested/{candidate}", claims.InstanceId, null, ct);
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
        var delPrefix = $"{candidate}/";
        var objects = await s3.ListPrefixAsync(delPrefix, ct: ct);
        if (!objects.IsSuccess)
            return Result.Failed(objects.Error!); // запись остаётся DELETING — доведёт
        if (objects.Value.Count > 0)
        {
            var deleted = await s3.DeleteKeysAsync(
                objects.Value.Select(o => o.Key).ToList(), ct);
            if (!deleted.IsSuccess)
                return Result.Failed(deleted.Error!);
            var recheck = await s3.ListPrefixAsync(delPrefix, ct: ct);
            if (!recheck.IsSuccess || recheck.Value.Count > 0)
                return Result.Failed(new ApplicationException(
                    $"удаление {delPrefix} не завершилось — повторит следующий проход"));
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
        logger?.LogInformation("{Op}: сирота {Prefix} удалена {Reason}", Op, candidate,
            byRequest ? "по заявке оператора" : "по TTL");

        // Заявка исполнена — гасим её ключ (санитар следующего прохода тоже бы
        // гасил, но чистим сразу: заявка и запись гасятся вместе — AC4).
        if (byRequest && await DeleteKeyAsync(OrphanRegistry.DeleteKey(candidate), ct)
            is { IsSuccess: false } delDone)
            return delDone;

        return Result.Success();
    }

    // Range hold/заявок сирот (failover по endpoints — образец RangeClustersAsync).
    private async Task<Result<IReadOnlyList<Kv>>> RangeKeysAsync(string prefix, CancellationToken ct)
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
            ?? new ApplicationException($"range {prefix}: нет живых endpoints etcd"));
    }

    // Одиночный del ключа hold/заявки (failover по endpoints, образец
    // PutRegistryAsync; prefix: false — точный ключ, не префикс).
    private async Task<Result> DeleteKeyAsync(string key, CancellationToken ct)
    {
        Result last = default;
        foreach (var endpoint in endpoints)
        {
            var del = await etcd.DeleteAsync(endpoint, key, prefix: false, ct);
            if (del.IsSuccess)
                return Result.Success();
            last = del;
        }

        return Result.Failed(last.Error ?? new ApplicationException(
            $"del {key}: нет живых endpoints"));
    }

    // «<C>/<X>» — 2 последних сегмента ключа hold/заявки (формат arch/19 §4).
    private static string PrefixOfKey(string key)
        => string.Join('/', key.Split('/')[^2..]);

    // Чтение /clusters/ (failover по endpoints) — владельцы префиксов.
    private async Task<Result<IReadOnlyList<Kv>>> RangeClustersAsync(CancellationToken ct)
    {
        Result<IReadOnlyList<Kv>> last = default;
        foreach (var endpoint in endpoints)
        {
            var range = await etcd.RangeAsync(endpoint, "/clusters/", ct);
            if (range.IsSuccess)
                return range;
            last = range;
        }

        return Result<IReadOnlyList<Kv>>.Failed(last.Error
            ?? new EtcdUnreachableException("etcd endpoints не заданы"));
    }

    // Чтение реестра (failover по endpoints); ключа нет → null; битый → null.
    private async Task<OrphanRegistry.Registry?> ReadRegistryAsync(CancellationToken ct)
    {
        Result last = default;
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

        if (!last.IsSuccess)
            throw new ApplicationException($"get {OrphanRegistry.Key}: {last.Error!.Message}");
        return null;
    }

    // Failover-put реестра (образец WalStreamProcess.PutAsync).
    private async Task<Result> PutRegistryAsync(OrphanRegistry.Registry registry, CancellationToken ct)
    {
        Result last = default;
        foreach (var endpoint in endpoints)
        {
            var put = await etcd.PutAsync(
                endpoint, OrphanRegistry.Key, OrphanRegistry.ToJson(registry), null, ct);
            if (put.IsSuccess)
                return Result.Success();
            last = put;
        }

        return Result.Failed(last.Error ?? new ApplicationException(
            $"put {OrphanRegistry.Key}: нет живых endpoints"));
    }
}
