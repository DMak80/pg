using System.Text.Json;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Planning;
using PgWorker.Core.Templates;
using PgWorker.Docker.Drivers;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;
using PgWorker.Provisioning.Probes;
using PgWorker.Provisioning.Sql;

namespace PgWorker.Provisioning.Processes;

/// <summary>
/// BucketEvacuator — аварийная эвакуация полностью мёртвого шарда (задача 22;
/// spec §6.4 E, arch/14 §5 D, решение Д6): перевод владения бакетами на живые
/// шарды ПУСТЫМИ схемами (источник недоступен — копировать нечего; данные
/// шарда остаются на его дисках и вернутся вместе с ним). Guard'ы:
/// незавершённый переезд блокирует эвакуацию; живых нет — ждём. Журнал
/// /pgworker/evacuations/&lt;C&gt;/&lt;X&gt; — ДО манипуляций (P7); снапшоты до/после
/// (P12). Возврат шарда (journal DONE + живой REST) — карантин: docker stop
/// БЕЗ удаления (данные целы), state=QUARANTINED, returned_unix.
/// </summary>
public sealed class BucketEvacuator(
    IEtcdGateway etcd,
    string[] endpoints,
    IClusterDriver driver,
    ISqlExecutor db,
    ShardProbe probe,
    ClaimStore claims,
    WorkJournal journal,
    InstallSecrets secrets,
    Func<CancellationToken, Task<Result>>? snapshot = null)
{
    private const string Reason = "shard-dead";

    public async Task<Result<ProcessOutcome>> TickAsync(
        ClusterSnapshot snap, string deadShard, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;

        // Мутации — только держателем живого клэйма (инвариант spec §4.3).
        if (!claims.IsMine(cluster))
            return Result<ProcessOutcome>.Failed(new ApplicationException(
                $"evacuate {cluster}/{deadShard}: клэйм не наш (или потерян) — мутации запрещены"));

        var existing = await journal.ReadEvacuationAsync(cluster, deadShard, ct);
        if (!existing.IsSuccess)
            return Result<ProcessOutcome>.Failed(existing.Error!);

        // Уже эвакуирован: обработка возврата шарда (E3-карантин) или hold.
        if (existing.Value is { } done)
            return await HandleReturnedShardAsync(snap, deadShard, done, ct);

        // Guard: незавершённый переезд любого бакета кластера — блокируем
        // эвакуацию (alert в work-журнале, разбор оператором; arch/14 §5 D).
        var moving = snap.Routing.FirstOrDefault(r => r.Status is BucketMoveState.Syncing
            or BucketMoveState.Frozen or BucketMoveState.Aborting);
        if (moving is not null)
        {
            await journal.WritePhaseAsync(cluster, "evacuate", "blocked-moving", claims.InstanceId,
                $"бакет {moving.Id} в статусе {moving.Status} — эвакуация заблокирована", ct);
            return Result<ProcessOutcome>.Failed(new InvalidOperationException(
                $"бакет {moving.Id} в статусе {moving.Status} — незавершённый переезд"));
        }

        // Guard: эвакуировать некуда — ждём живых (при N живых=0 — ждать, E0).
        var aliveShards = await AliveShardsAsync(snap, deadShard, ct);
        if (aliveShards.Count == 0)
        {
            await journal.WritePhaseAsync(cluster, "evacuate", "waiting-alive", claims.InstanceId, null, ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.InProgress);
        }

        // Снапшот «до» (P12) — до любых манипуляций контрол-плейном.
        if (snapshot is not null)
        {
            var before = await snapshot(ct);
            if (!before.IsSuccess)
                return Result<ProcessOutcome>.Failed(before.Error!);
        }

        // План: живые шарды получают бакеты сбалансированно (round-robin).
        var plan = EvacuationPlanner.Plan(snap.Routing, deadShard, aliveShards);
        if (!plan.IsSuccess)
            return Result<ProcessOutcome>.Failed(plan.Error!);
        if (plan.Value.Count == 0)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done); // бакетов у шарда нет

        // E0: journal-before-manipulations — план в /pgworker/evacuations/<C>/<X>.
        var evacuation = new EvacuationJournal(
            plan.Value.ToDictionary(a => a.BucketId, a => a.ToShard),
            Reason, DateTimeOffset.UtcNow.ToUnixTimeSeconds(), "PLANNED", null);
        var written = await journal.WriteEvacuationAsync(cluster, deadShard, evacuation, ct);
        if (!written.IsSuccess)
            return Result<ProcessOutcome>.Failed(written.Error!);

        // E1: пустые схемы эвакуированных бакетов на целевых шардах.
        var addresses = await ReadPortAllocAsync(cluster, ct);
        if (!addresses.IsSuccess)
            return Result<ProcessOutcome>.Failed(addresses.Error!);

        foreach (var target in plan.Value.GroupBy(a => a.ToShard))
        {
            var shardSpec = snap.Shards.Single(s => s.Name == target.Key);
            var master = await ResolveMasterAsync(shardSpec, addresses.Value, ct);
            if (master is null)
                return await FailWaiting(cluster, deadShard, $"нет мастера целевого шарда {target.Key}", ct);

            var dsn = DatabaseProvisioner.BuildAdminDsn(master.Host, master.Ports.Pg, snap.Config.DbName, secrets);
            var schemas = await db.ExecuteAsync(
                dsn,
                DatabaseProvisioner.BuildSchemasSql(snap.Config.DbName, target.Select(a => a.BucketId)),
                ct);
            if (!schemas.IsSuccess)
                return Result<ProcessOutcome>.Failed(schemas.Error!);
        }

        // E2: по каждому бакету txn (compare routing=<dead>) put routing=<to>.
        foreach (var assignment in plan.Value)
        {
            var key = $"/clusters/{cluster}/buckets/routing/bucket_{assignment.BucketId}";
            var txn = await TxnAsync(
                TxnRequest.Of(
                    [TxnCompare.ValueEqual(key, assignment.FromShard)],
                    [new TxnOp.Put(key, assignment.ToShard, null)]),
                ct);
            if (!txn.IsSuccess)
                return Result<ProcessOutcome>.Failed(txn.Error!);

            if (txn.Value.Succeeded)
                continue;

            // Compare не сошёлся: перечитать — уже переведён нами (повтор) или конфликт.
            var current = await GetAsync(key, ct);
            if (!current.IsSuccess)
                return Result<ProcessOutcome>.Failed(current.Error!);
            if (current.Value?.Value == assignment.ToShard)
                continue; // идемпотентность повторного тика

            var conflict = evacuation with { State = "CONFLICT" };
            await journal.WriteEvacuationAsync(cluster, deadShard, conflict, ct);
            return Result<ProcessOutcome>.Failed(new InvalidOperationException(
                $"routing bucket_{assignment.BucketId} = '{current.Value?.Value}' (ожидался '{assignment.FromShard}') — конкурентное изменение, эвакуация остановлена"));
        }

        // E3: ноды мёртвого шарда — QUARANTINED; контейнеры НЕ удаляются (данные
        // на месте); остановка — при возврате REST-живости (HandleReturnedShard).
        foreach (var node in snap.Shards.Single(s => s.Name == deadShard).Nodes)
        {
            if (node.State == NodeState.Quarantined)
                continue;
            var quarantined = await PutAsync(
                $"/clusters/{cluster}/shards/{deadShard}/nodes/{node.Name}/state", "QUARANTINED", ct);
            if (!quarantined.IsSuccess)
                return Result<ProcessOutcome>.Failed(quarantined.Error!);
        }

        // E4: journal DONE + снапшот «после» (P12).
        var finished = evacuation with { State = "DONE" };
        var closed = await journal.WriteEvacuationAsync(cluster, deadShard, finished, ct);
        if (!closed.IsSuccess)
            return Result<ProcessOutcome>.Failed(closed.Error!);

        if (snapshot is not null)
        {
            var after = await snapshot(ct);
            if (!after.IsSuccess)
                return Result<ProcessOutcome>.Failed(after.Error!);
        }

        return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
    }

    // Возврат/состояние после эвакуации: DONE + живой REST → остановить ноды
    // (P1-логика «призраков»: не пишут в осиротевшие схемы); QUARANTINED —
    // держим (идемпотентно повторяем stop при повторном оживании).
    private async Task<Result<ProcessOutcome>> HandleReturnedShardAsync(
        ClusterSnapshot snap, string deadShard, EvacuationJournal journalState, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;
        if (journalState.State is "PLANNED")
        {
            // Прерванный тик: план был записан, но манипуляции не дошли —
            // безопасный перезапуск с чистого плана (идемпотентность E2).
            await DeleteEvacuationAsync(cluster, deadShard, ct);
            return await TickAsync(snap, deadShard, ct);
        }

        var shard = snap.Shards.SingleOrDefault(s => s.Name == deadShard);
        if (shard is null)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);

        var alive = false;
        foreach (var node in shard.Nodes)
        {
            var addr = await NodeAddressOf(cluster, deadShard, node.Name, ct);
            if (addr is not null && await probe.IsAliveAsync(addr, ct))
            {
                alive = true;
                break;
            }
        }

        if (!alive)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done); // ещё мёртв — держим карантин

        // Шард «ожил» после эвакуации: docker stop без удаления (данные целы).
        foreach (var node in shard.Nodes)
        {
            var stopped = await driver.StopNodeAsync(cluster, deadShard, node.Name, ct);
            if (!stopped.IsSuccess)
                return Result<ProcessOutcome>.Failed(stopped.Error!);

            if (node.State != NodeState.Quarantined)
            {
                var marked = await PutAsync(
                    $"/clusters/{cluster}/shards/{deadShard}/nodes/{node.Name}/state", "QUARANTINED", ct);
                if (!marked.IsSuccess)
                    return Result<ProcessOutcome>.Failed(marked.Error!);
            }
        }

        if (journalState.State != "QUARANTINED" || journalState.ReturnedUnix is null)
        {
            var returned = journalState with { State = "QUARANTINED", ReturnedUnix = DateTimeOffset.UtcNow.ToUnixTimeSeconds() };
            var written = await journal.WriteEvacuationAsync(cluster, deadShard, returned, ct);
            if (!written.IsSuccess)
                return Result<ProcessOutcome>.Failed(written.Error!);
        }

        return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
    }

    // Живые шарды кластера (REST любой ноды отвечает), кроме мёртвого.
    private async Task<List<string>> AliveShardsAsync(ClusterSnapshot snap, string deadShard, CancellationToken ct)
    {
        var alive = new List<string>();
        foreach (var shard in snap.Shards.Where(s => s.Name != deadShard))
        {
            foreach (var node in shard.Nodes)
            {
                var addr = await NodeAddressOf(snap.Config.Cluster, shard.Name, node.Name, ct);
                if (addr is not null && await probe.IsAliveAsync(addr, ct))
                {
                    alive.Add(shard.Name);
                    break;
                }
            }
        }

        return alive;
    }

    private async Task<NodeAddress?> NodeAddressOf(string cluster, string shard, string node, CancellationToken ct)
    {
        var addresses = await ReadPortAllocAsync(cluster, ct);
        if (!addresses.IsSuccess)
            return null;
        return addresses.Value.TryGetValue($"{shard}/{node}", out var addr) ? addr : null;
    }

    // Мастер шарда для SQL: master-ключ → host/имя ноды → portalloc; поиск
    // ТОЛЬКО среди нод этого шарда (host неуникален — на нём ноды разных шардов).
    private async Task<NodeAddress?> ResolveMasterAsync(
        ShardSpec shard, IReadOnlyDictionary<string, NodeAddress> addresses, CancellationToken ct)
    {
        var shardNodes = addresses
            .Where(p => p.Key.StartsWith($"{shard.Name}/", StringComparison.Ordinal))
            .ToDictionary(p => p.Key.Split('/')[1], p => p.Value);

        if (!string.IsNullOrWhiteSpace(shard.Master))
        {
            var left = shard.Master.Split(':')[0];
            var byName = shardNodes.FirstOrDefault(p => p.Key == left);
            if (byName.Value is not null)
                return byName.Value;
            var byHost = shardNodes.FirstOrDefault(p => p.Value.Host == left);
            if (byHost.Value is not null)
                return byHost.Value;
        }

        foreach (var node in shardNodes)
        {
            var members = await probe.GetClusterAsync(node.Value, ct);
            if (!members.IsSuccess)
                continue;
            // Patroni 3.x в /cluster называет мастера "leader" (legacy: "master").
            var master = members.Value.FirstOrDefault(m =>
                m.Role is "master" or "leader" or "primary" && m.State == "running");
            if (master is not null && shardNodes.TryGetValue(master.Name, out var addr))
                return addr;
        }

        return null;
    }

    private async Task<Result<ProcessOutcome>> FailWaiting(
        string cluster, string shard, string message, CancellationToken ct)
    {
        await journal.WritePhaseAsync(cluster, "evacuate", "waiting-master", claims.InstanceId, message, ct);
        return Result<ProcessOutcome>.Failed(new ApplicationException(message));
    }

    private async Task<Result<IReadOnlyDictionary<string, NodeAddress>>> ReadPortAllocAsync(
        string cluster, CancellationToken ct)
    {
        var result = await GetAsync($"/pgworker/portalloc/{cluster}", ct);
        if (!result.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(result.Error!);
        if (result.Value is not { } kv)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(
                (IReadOnlyDictionary<string, NodeAddress>)new Dictionary<string, NodeAddress>());

        return Portalloc.Parse(cluster, kv.Value);
    }

    // Failover-обёртки: первый успешный endpoint выигрывает.
    private async Task<Result<Kv?>> GetAsync(string key, CancellationToken ct)
    {
        Result<Kv?>? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.GetAsync(endpoint, key, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }

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

    private async Task<Result> DeleteEvacuationAsync(string cluster, string shard, CancellationToken ct)
    {
        Result? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.DeleteAsync(endpoint, $"/pgworker/evacuations/{cluster}/{shard}", prefix: false, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }

    private async Task<Result<TxnResult>> TxnAsync(TxnRequest req, CancellationToken ct)
    {
        Result<TxnResult>? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.TxnAsync(endpoint, req, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }
}
