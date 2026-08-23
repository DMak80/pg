using System.Text.Json;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Docker.Drivers;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Probes;

namespace PgWorker.Provisioning.Processes;

/// <summary>
/// NodeSupervisor — штатный надзор инициализированных кластеров (задача 21;
/// spec §6.4 C, arch/14 §5 C; эталон rebuild-node.sh): сверка декларации
/// (снесённый контейнер пересоздаётся), Patroni-пробы, rebuild мёртвой
/// не-лидерской ноды при живом кворуме, детект полностью мёртвого шарда
/// (DeadShards → BucketEvacuator задачи 22/23) и P11-сверка мастер-ключей.
/// Пороговые времена — в /pgworker/work/&lt;C&gt; поле unreachable (план №4).
/// </summary>
public sealed class NodeSupervisor(
    IEtcdGateway etcd,
    string[] endpoints,
    IClusterDriver driver,
    ShardProbe probe,
    ClaimStore claims,
    WorkJournal journal,
    ThresholdsOptions thresholds,
    TimeProvider clock,
    InstallSecrets secrets,
    MasterKeyReconciler? masterKeys = null) : IClusterProcess
{
    /// <summary>Полностью мёртвые шарды последнего тика (эвакуация — цикл задачи 23).</summary>
    public IReadOnlyList<string> DeadShards { get; private set; } = [];

    public async Task<Result<ProcessOutcome>> TickAsync(ClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;

        // Мутации — только держателем живого клэйма (инвариант spec §4.3).
        if (!claims.IsMine(cluster))
            return Result<ProcessOutcome>.Failed(new ApplicationException(
                $"supervise {cluster}: клэйм не наш (или потерян) — мутации запрещены"));

        var addresses = await ReadPortAllocAsync(cluster, ct);
        if (!addresses.IsSuccess)
            return Result<ProcessOutcome>.Failed(addresses.Error!);

        // 1) Сверка декларации: каждой плановой ноде — контейнер/сервис по имени;
        //    снесённый руками пересоздаётся (декларативное самовосстановление).
        var declared = await EnsureDeclaredNodesAsync(cluster, snap, addresses.Value, ct);
        if (!declared.IsSuccess)
            return Result<ProcessOutcome>.Failed(declared.Error!);

        // 2) Пробы + сценарии недоступности (трек в work-журнале, план №4).
        var unreachable = await journal.ReadUnreachableAsync(cluster, ct);
        if (!unreachable.IsSuccess)
            return Result<ProcessOutcome>.Failed(unreachable.Error!);
        var track = new Dictionary<string, long>(unreachable.Value);
        var deadShards = new List<string>();

        foreach (var shard in snap.Shards)
        {
            var shardTrack = await SuperviseShardAsync(cluster, snap, shard, addresses.Value, track, ct);
            if (!shardTrack.IsSuccess)
                return Result<ProcessOutcome>.Failed(shardTrack.Error!);

            // Весь шард недоступен (все ноды молчат) + master-ключ протух дольше
            // ShardDeadSec → кандидат на эвакуацию (spec §6.4 D).
            var allDead = shard.Nodes is { Count: > 0 }
                && shard.Nodes.All(n => track.ContainsKey($"{shard.Name}/{n.Name}"));
            if (allDead && string.IsNullOrWhiteSpace(shard.Master))
            {
                var oldest = shard.Nodes
                    .Select(n => track[$"{shard.Name}/{n.Name}"])
                    .Min();
                if (Now() - oldest > thresholds.ShardDeadSec)
                    deadShards.Add(shard.Name);
            }
        }

        DeadShards = deadShards;
        await journal.WriteSupervisionAsync(cluster, claims.InstanceId, track, ct);

        // 3) P11-сверка мастер-ключей (только при рассинхроне — отдельный контур).
        if (masterKeys is not null)
        {
            var keys = await masterKeys.ReconcileAsync(snap, addresses.Value, ct);
            if (!keys.IsSuccess)
                return Result<ProcessOutcome>.Failed(keys.Error!);
        }

        // Надзор не имеет терминальной фазы: успешный тик = Done (цикл повторит).
        return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
    }

    // Сверка декларации: EnsureNode плановых нод без docker-объекта.
    private async Task<Result> EnsureDeclaredNodesAsync(
        string cluster, ClusterSnapshot snap,
        IReadOnlyDictionary<string, NodeAddress> addresses, CancellationToken ct)
    {
        var objects = await driver.ListNodeObjectsAsync(cluster, ct);
        if (!objects.IsSuccess)
            return objects;
        var existing = objects.Value.ToHashSet();

        foreach (var shard in snap.Shards)
        {
            var topology = TopologyOf(cluster, snap, shard.Name, addresses);
            if (topology.Nodes.Count == 0)
                continue; // нет закреплённых адресов (внешний кластер) — не наш объект

            foreach (var node in shard.Nodes)
            {
                if (node.State is NodeState.Quarantined or NodeState.Removing)
                    continue; // карантин/удаление — не пересоздаём (E3/задача 22)
                if (!topology.Nodes.ContainsKey(node.Name))
                    continue;
                if (existing.Contains($"pgw-{cluster}-{shard.Name}-{node.Name}"))
                    continue; // контейнер на месте

                if (node.State != NodeState.Provisioning)
                {
                    var marked = await PutAsync(
                        $"/clusters/{cluster}/shards/{shard.Name}/nodes/{node.Name}/state",
                        "PROVISIONING", ct);
                    if (!marked.IsSuccess)
                        return marked;
                }

                var ensured = await driver.EnsureNodeAsync(
                    topology, node.Name, topology.Nodes[node.Name], secrets,
                    new EtcdEndpoints(endpoints), ct);
                if (!ensured.IsSuccess)
                    return ensured;
            }
        }

        return Result.Success();
    }

    // Надзор одного шарда: пробы, rebuild, UNREACHABLE/RUNNING-переходы.
    private async Task<Result> SuperviseShardAsync(
        string cluster, ClusterSnapshot snap, ShardSpec shard,
        IReadOnlyDictionary<string, NodeAddress> addresses,
        Dictionary<string, long> track, CancellationToken ct)
    {
        var scope = $"{cluster}-{shard.Name}";
        var scopeKvs = await RangeAsync($"/service/{scope}/", ct);
        if (!scopeKvs.IsSuccess)
            return scopeKvs;
        var leader = ClusterSnapshotParser.ParseService(scopeKvs.Value).FirstOrDefault()?.LeaderName;

        var alive = new List<string>();
        var dead = new List<string>();
        foreach (var node in shard.Nodes)
        {
            if (!addresses.TryGetValue($"{shard.Name}/{node.Name}", out var addr))
                continue; // без закреплённого адреса пробу не сделать
            if (await probe.IsAliveAsync(addr, ct))
                alive.Add(node.Name);
            else
                dead.Add(node.Name);
        }

        foreach (var name in dead)
        {
            var node = shard.Nodes.Single(n => n.Name == name);
            var trackKey = $"{shard.Name}/{name}";
            track.TryAdd(trackKey, Now());

            // Лидер недоступен → НИЧЕГО: failover делает Patroni (P11); лидер-призрак
            // станет репликой/умершей и обработается общим путём (arch/14 §5 C).
            var isLeader = leader == name;
            var quorum = alive.Count >= 2;
            var expired = Now() - track[trackKey] > thresholds.NodeDeadSec;

            if (!isLeader && quorum && expired)
            {
                // Rebuild (эталон rebuild-node.sh): удалить контейнер+volume,
                // пересоздать с тем же адресом; Patroni сделает pg_basebackup.
                if (!addresses.TryGetValue(trackKey, out var addr))
                    continue;
                var removed = await driver.RemoveNodeAsync(cluster, shard.Name, name, ct);
                if (!removed.IsSuccess)
                    return removed;
                var topology = TopologyOf(cluster, snap, shard.Name, addresses);
                var ensured = await driver.EnsureNodeAsync(
                    topology, name, addr, secrets,
                    new EtcdEndpoints(endpoints), ct);
                if (!ensured.IsSuccess)
                    return ensured;
                var rebuilding = await PutAsync(
                    $"/clusters/{cluster}/shards/{shard.Name}/nodes/{name}/state", "REBUILDING", ct);
                if (!rebuilding.IsSuccess)
                    return rebuilding;
                track.Remove(trackKey); // пересоздана — счётчик с нуля
                continue;
            }

            if (node.State is not (NodeState.Unreachable or NodeState.Rebuilding or NodeState.Provisioning))
            {
                var marked = await PutAsync(
                    $"/clusters/{cluster}/shards/{shard.Name}/nodes/{name}/state", "UNREACHABLE", ct);
                if (!marked.IsSuccess)
                    return marked;
            }
        }

        // Живая нода: снятие UNREACHABLE/REBUILDING → RUNNING.
        foreach (var name in alive)
        {
            var node = shard.Nodes.Single(n => n.Name == name);
            track.Remove($"{shard.Name}/{name}");
            if (node.State is not NodeState.Running)
            {
                var running = await PutAsync(
                    $"/clusters/{cluster}/shards/{shard.Name}/nodes/{name}/state", "RUNNING", ct);
                if (!running.IsSuccess)
                    return running;
            }
        }

        return Result.Success();
    }

    private static ShardTopology TopologyOf(
        string cluster, ClusterSnapshot snap, string shard,
        IReadOnlyDictionary<string, NodeAddress> addresses)
        => new(cluster, shard, $"{cluster}-{shard}",
            addresses
                .Where(p => p.Key.StartsWith($"{shard}/", StringComparison.Ordinal))
                .ToDictionary(p => p.Key.Split('/')[1], p => p.Value));

    private async Task<Result<IReadOnlyDictionary<string, NodeAddress>>> ReadPortAllocAsync(
        string cluster, CancellationToken ct)
    {
        var result = await GetAsync($"/pgworker/portalloc/{cluster}", ct);
        if (!result.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(result.Error!);
        if (result.Value is not { } kv)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(
                (IReadOnlyDictionary<string, NodeAddress>)new Dictionary<string, NodeAddress>());

        try
        {
            var parsed = JsonSerializer.Deserialize<Dictionary<string, NodeAddress>>(kv.Value);
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(
                (IReadOnlyDictionary<string, NodeAddress>)(parsed ?? []));
        }
        catch (JsonException e)
        {
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(
                new ApplicationException($"битый portalloc {cluster}: {e.Message}", e));
        }
    }

    private long Now() => clock.GetUtcNow().ToUnixTimeSeconds();

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

    private async Task<Result<IReadOnlyList<Kv>>> RangeAsync(string prefix, CancellationToken ct)
    {
        Result<IReadOnlyList<Kv>>? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.RangeAsync(endpoint, prefix, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }
}
