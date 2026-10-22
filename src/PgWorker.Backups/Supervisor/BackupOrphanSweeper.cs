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
/// Перед удалением — перепроверка (arch/19 §4): лидерство, владельцы /clusters/
/// и hold-ключ кандидата заново (снимок /clusters/ мог устареть) — воскресший
/// владелец отменяет и DELETING-доводку: гасятся запись/hold/заявка, объекты
/// живого владельца не трогаются. Выполняется ТОЛЬКО глобальным лидером
/// /pgworker/leader (двойного писателя
/// нет); runtime() == null (Enabled=false) → no-op. Все шаги идемпотентны,
/// transient-отказы (S3/etcd) — Result.Failed без мутаций, следующий проход
/// повторит (arch/17).</summary>
public sealed class BackupOrphanSweeper(
    IEtcdGateway etcd,
    string[] endpoints,
    BackupS3Factory s3Factory,
    PgWorker.Etcd.S3.IS3PoolStore pools,
    ClaimStore claims,
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

        // (1) t36 (arch/19 §5): union по ВСЕМ существующим пулам /s3/clusters/
        // (первый доступный endpoint каждого; дедуп по (пул, ключ)) → группировка.
        var poolNames = await pools.ListPoolNamesAsync(ct);
        if (!poolNames.IsSuccess)
            return Result.Failed(poolNames.Error!);
        var observedKeys = new HashSet<(string Pool, string Key)>();
        var observedObjects = new List<S3ObjectInfo>();
        foreach (var poolName in poolNames.Value)
        {
            var created = await s3Factory.CreateForPoolAsync(poolName, ct);
            if (!created.IsSuccess)
                continue; // пул недоступен/без кредов — union по остальным
            await using var poolS3 = created.Value.Client;
            var listed = await poolS3.ListPrefixAsync("", ct: ct);
            if (!listed.IsSuccess)
                continue; // transient этого пула — повтор прохода
            foreach (var obj in listed.Value)
                if (observedKeys.Add((poolName, obj.Key)))
                    observedObjects.Add(obj);
        }

        var grouped = OrphanRegistry.GroupPrefixes(observedObjects);
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
        if (!current.IsSuccess)
            return Result.Failed(current.Error!); // transient — повтор прохода
        var merged = OrphanRegistry.Merge(
            current.Value, observed, grouped.FullPrefixes, liveShards, liveClusters, nowUnix);
        var changed = current.Value is null || !OrphanRegistry.SameOrphans(current.Value, merged);
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
                await WriteJournalAsync($"orphan-key-cleaned/{prefix}", ct);
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
            await WriteJournalAsync($"orphan-key-cleaned/{prefix}", ct);
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

        // (4.5) Перепроверка перед удалением (arch/19 §4): снимок /clusters/ (2)
        // мог устареть — воскресение владельца между снимком и удалением отменяет
        // и DELETING-доводку: вместо объектов гасятся запись реестра, hold-ключ и
        // заявка (объекты живого владельца не трогаются). Потеря лидерства — тихий
        // no-op (удаление повторит проход нового лидера). Hold, появившийся на
        // TTL-кандидата после отбора, откладывает удаление до следующего прохода
        // (DELETING-доводка и заявка hold не проверяют — канон: «DELETING-записи
        // hold не защищают», заявка минует hold).
        if (!claims.IsLeader)
            return Result.Success();
        var recheckClusters = await RangeClustersAsync(ct);
        if (!recheckClusters.IsSuccess)
            return Result.Failed(recheckClusters.Error!);
        var reparsed = ClusterSnapshotParser.ParseClusters(recheckClusters.Value, out _);
        if (!reparsed.IsSuccess)
            return Result.Failed(reparsed.Error!);
        var (candCluster, candShard) = SplitPrefix(candidate);
        var ownerLive = candCluster is not null && reparsed.Value.Any(snap =>
            snap.Config.Cluster == candCluster
            && snap.Config.State != ClusterState.ToRemove
            && snap.Shards.Any(sh => sh.Name == candShard && !sh.ToRemove));
        if (ownerLive)
        {
            var cancelled = merged with
            {
                Orphans = merged.Orphans.Where(e => e.Prefix != candidate).ToList(),
            };
            if (await PutRegistryAsync(cancelled, ct) is { IsSuccess: false } putCancelled)
                return putCancelled; // запись остаётся — следующий проход повторит отмену
            if (await DeleteKeyAsync(OrphanRegistry.HoldKey(candidate), ct)
                    is { IsSuccess: false } delHoldCancelled)
                return delHoldCancelled;
            if (await DeleteKeyAsync(OrphanRegistry.DeleteKey(candidate), ct)
                    is { IsSuccess: false } delReqCancelled)
                return delReqCancelled;
            await WriteJournalAsync($"orphan-cancelled/{candidate}", ct);
            logger?.LogInformation(
                "{Op}: orphan {Prefix} cancelled — owner resurrected", Op, candidate);
            return Result.Success();
        }
        if (!byRequest && entry.State == OrphanState.Observed)
        {
            var holdRecheck = await RangeKeysAsync(OrphanRegistry.HoldKey(candidate), ct);
            if (!holdRecheck.IsSuccess)
                return holdRecheck;
            if (holdRecheck.Value.Any(kv => kv.Key == OrphanRegistry.HoldKey(candidate)))
                return Result.Success(); // hold после отбора — удалит следующий проход
        }

        // journal-before-manipulations: DELETING пишется ДО удаления объектов.
        // По заявке — журнал заявки до перехода в DELETING (AC4).
        if (byRequest)
            await WriteJournalAsync($"orphan-delete-requested/{candidate}", ct);
        var deleting = merged with
        {
            Orphans = merged.Orphans
                .Select(e => e.Prefix == candidate
                    ? e with { State = OrphanState.Deleting } : e)
                .ToList(),
        };
        if (await PutRegistryAsync(deleting, ct) is { IsSuccess: false } putDeleting)
            return putDeleting; // запись остаётся OBSERVED — следующий проход повторит
        await WriteJournalAsync($"orphan-deleting/{candidate}", ct);

        // batch-delete префикса + list-подтверждение пустоты — через КАЖДЫЙ
        // доступный пул (t36: префикс <C>/<X> может лежать в любом пуле;
        // удаление идемпотентно, mesh разносит tombstone — arch/19 §5).
        var delPrefix = $"{candidate}/";
        foreach (var poolName in poolNames.Value)
        {
            var created = await s3Factory.CreateForPoolAsync(poolName, ct);
            if (!created.IsSuccess)
                continue; // пул недоступен — доводку делает оставшийся проход
            await using var poolS3 = created.Value.Client;
            var objects = await poolS3.ListPrefixAsync(delPrefix, ct: ct);
            if (!objects.IsSuccess)
                return Result.Failed(objects.Error!); // запись остаётся DELETING — доведёт
            if (objects.Value.Count > 0)
            {
                var deleted = await poolS3.DeleteKeysAsync(
                    objects.Value.Select(o => o.Key).ToList(), ct);
                if (!deleted.IsSuccess)
                    return Result.Failed(deleted.Error!);
                var recheck = await poolS3.ListPrefixAsync(delPrefix, ct: ct);
                if (!recheck.IsSuccess || recheck.Value.Count > 0)
                    return Result.Failed(new ApplicationException(
                        $"deletion of {delPrefix} did not complete — next pass will retry"));
            }
        }

        // Объекты удалены — del записи из реестра + журнал-факт.
        var done = deleting with
        {
            Orphans = deleting.Orphans.Where(e => e.Prefix != candidate).ToList(),
        };
        if (await PutRegistryAsync(done, ct) is { IsSuccess: false } putDone)
            return putDone;
        await WriteJournalAsync($"orphan-deleted/{candidate}", ct);
        logger?.LogInformation("{Op}: orphan {Prefix} deleted {Reason}", Op, candidate,
            byRequest ? "by operator request" : "by TTL");

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
            ?? new ApplicationException($"range {prefix}: no live etcd endpoints"));
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
            $"del {key}: no live endpoints"));
    }

    // «<C>/<X>» — 2 последних сегмента ключа hold/заявки (формат arch/19 §4).
    private static string PrefixOfKey(string key)
        => string.Join('/', key.Split('/')[^2..]);

    // «<C>/<X>» → компоненты (записи реестра всегда нашей формы — фильтр
    // SplitPrefix merge'а); не-нашей-формы → (null, null): владельческая
    // перепроверка мимо, удаление как раньше.
    private static (string? Cluster, string? Shard) SplitPrefix(string prefix)
    {
        var parts = prefix.Split('/');
        return parts.Length == 2 ? (parts[0], parts[1]) : (null, null);
    }

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
            ?? new EtcdUnreachableException("etcd endpoints not configured"));
    }

    // Чтение реестра (failover по endpoints): ключа нет или битый → Success(null)
    // (следующий проход пересоберёт реестр — Parse), все endpoints недоступны →
    // Failed (transient — повтор прохода): контракт как у соседних методов, без
    // throw мимо различения шага.
    private async Task<Result<OrphanRegistry.Registry?>> ReadRegistryAsync(CancellationToken ct)
    {
        Result<OrphanRegistry.Registry?> last = default;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.GetAsync(endpoint, OrphanRegistry.Key, ct);
            if (!result.IsSuccess)
            {
                last = Result<OrphanRegistry.Registry?>.Failed(result.Error!);
                continue;
            }

            return result.Value is { } kv ? OrphanRegistry.Parse(kv.Value) : null;
        }

        return Result<OrphanRegistry.Registry?>.Failed(last.Error
            ?? new EtcdUnreachableException("etcd endpoints not configured"));
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
            $"put {OrphanRegistry.Key}: no live endpoints"));
    }

    // Фаза свипера — глобальный ключ /pgworker/backups/orphan-journal
    // (arch/19 §4): failover-put по endpoints (образец PutRegistryAsync),
    // plain put без read-modify-write. Журнал — наблюдаемость: неудача
    // записи НЕ роняет проход (все endpoints упали — предупреждение).
    private async Task WriteJournalAsync(string phase, CancellationToken ct)
    {
        foreach (var endpoint in endpoints)
        {
            var put = await etcd.PutAsync(
                endpoint, OrphanRegistry.JournalKey,
                OrphanRegistry.JournalPhaseToJson(
                    phase, claims.InstanceId, time.GetUtcNow().ToUnixTimeSeconds(), null),
                null, ct);
            if (put.IsSuccess)
                return;
        }

        logger?.LogWarning("{Op}: orphan journal write failed for phase {Phase}", Op, phase);
    }
}
