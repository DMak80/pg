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
    MasterKeyReconciler? masterKeys = null,
    EtcdEndpoints? etcdForNodes = null)
{
    /// <summary>
    /// Один тик надзора (не IClusterProcess: исход несёт мёртвые шарды).
    /// Мёртвые шарды возвращаются ЗНАЧЕНИЕМ, а не состоянием синглтона:
    /// процессы — синглтоны DI, кластеры обрабатываются параллельно, и общее
    /// mutable-свойство перезаписывалось бы тиками чужих кластеров (rework №1).
    /// </summary>
    public async Task<Result<SuperviseOutcome>> TickAsync(ClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;

        // Мутации — только держателем живого клэйма (инвариант spec §4.3).
        if (!claims.IsMine(cluster))
            return Fail(new ApplicationException(
                $"supervise {cluster}: клэйм не наш (или потерян) — мутации запрещены"));

        var addresses = await ReadPortAllocAsync(cluster, ct);
        if (!addresses.IsSuccess)
            return Fail(addresses.Error!);

        // 1) Сверка декларации: каждой плановой ноде — контейнер/сервис по имени;
        //    снесённый руками пересоздаётся (декларативное самовосстановление).
        var declared = await EnsureDeclaredNodesAsync(cluster, snap, addresses.Value, ct);
        if (!declared.IsSuccess)
            return Fail(declared.Error!);

        // 2) Пробы + сценарии недоступности (трек в work-журнале, план №4).
        var unreachable = await journal.ReadUnreachableAsync(cluster, ct);
        if (!unreachable.IsSuccess)
            return Fail(unreachable.Error!);
        var track = new Dictionary<string, long>(unreachable.Value);
        var deadShards = new List<string>();

        foreach (var shard in snap.Shards)
        {
            // Границы надзора (t06 §5.4): шард без dsn — домен AddShardProcess;
            // пробы/UNREACHABLE-переходы не трогаем (state нод — вход A1-гварда
            // add: ожидаемы только NOT_INITIALIZED/PROVISIONING).
            if (shard.Dsn is null)
                continue;

            // Operator-triggered recreate (TO_RECREATE): оператор панелью просит
            // пересоздать ноду — rebuild немедленно, без ожидания NodeDeadSec.
            var recreated = await RecreateMarkedNodesAsync(cluster, snap, shard, addresses.Value, ct);
            if (!recreated.IsSuccess)
                return Fail(recreated.Error!);

            var shardTrack = await SuperviseShardAsync(cluster, snap, shard, addresses.Value, track, ct);
            if (!shardTrack.IsSuccess)
                return Fail(shardTrack.Error!);

            // Весь шард недоступен (все ноды молчат) + master-ключ протух дольше
            // ShardDeadSec → кандидат на эвакуацию (spec §6.4 D).
            var allDead = shard.Nodes is { Count: > 0 }
                && shard.Nodes.All(n => track.ContainsKey($"{shard.Name}/{n.Name}"));

            // Кандидат эвакуации (t06 §5.4): только зарегистрированный шард (dsn
            // есть — add завершён) И с бакетами по routing (эвакуация пустого
            // шарда бессмысленна и карантинила бы ноды, блокируя демонтаж по G6).
            // Шард с TO_REMOVE-маркером кандидатом БЫТЬ МОЖЕТ — эвакуация
            // освобождает бакеты умирающего помеченного шарда, после чего G3
            // пропустит демонтаж (Д6).
            var hasBuckets = snap.Routing.Any(r => r.Owner == shard.Name);
            if (allDead && string.IsNullOrWhiteSpace(shard.Master) && shard.Dsn is not null && hasBuckets)
            {
                var oldest = shard.Nodes
                    .Select(n => track[$"{shard.Name}/{n.Name}"])
                    .Min();
                if (Now() - oldest > thresholds.ShardDeadSec)
                    deadShards.Add(shard.Name);
            }
        }

        // Возврат эвакуированного шарда (E3): DONE-журнал эвакуации — тоже
        // событие для эвакуатора (оживший шард останавливают и карантинят),
        // даже если REST уже жив и в deadShards он не попал.
        var evacuations = await RangeAsync($"/pgworker/evacuations/{cluster}/", ct);
        if (!evacuations.IsSuccess)
            return Fail(evacuations.Error!);
        foreach (var kv in evacuations.Value)
        {
            if (!kv.Value.Contains("\"state\":\"DONE\"", StringComparison.Ordinal))
                continue;
            var evacuated = kv.Key.Split('/')[^1];
            if (!deadShards.Contains(evacuated))
                deadShards.Add(evacuated);
        }

        await journal.WriteSupervisionAsync(cluster, claims.InstanceId, track, ct);

        // 3) P11-сверка мастер-ключей (только при рассинхроне — отдельный контур).
        if (masterKeys is not null)
        {
            var keys = await masterKeys.ReconcileAsync(snap, addresses.Value, ct);
            if (!keys.IsSuccess)
                return Fail(keys.Error!);
        }

        // Надзор не имеет терминальной фазы: успешный тик = Done (цикл повторит);
        // мёртвые шарды — значением (изоляция параллельных тиков, rework №1).
        return Result<SuperviseOutcome>.Success(new SuperviseOutcome(ProcessOutcome.Done, deadShards));
    }

    private static Result<SuperviseOutcome> Fail(Exception error)
        => Result<SuperviseOutcome>.Failed(error);

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
            // Границы надзора (t06 §5.4): шард без dsn — домен AddShardProcess;
            // TO_REMOVE — домен RemoveShardProcess (не пересоздавать демонтируемое).
            if (shard.Dsn is null || shard.ToRemove)
                continue;

            var topology = TopologyOf(cluster, snap, shard.Name, addresses);
            if (topology.Nodes.Count == 0)
                continue; // нет закреплённых адресов (внешний кластер) — не наш объект

            // Заявка ресурсов читается лениво: только если ноду правда recreate'им.
            NodeResources? resources = null;
            var resourcesLoaded = false;

            foreach (var node in shard.Nodes)
            {
                if (node.State is NodeState.Quarantined or NodeState.Removing)
                    continue; // карантин/удаление — не пересоздаём (E3/задача 22)
                if (!topology.Nodes.ContainsKey(node.Name))
                    continue;
                if (existing.Contains($"pgw-{cluster}-{shard.Name}-{node.Name}"))
                    continue; // контейнер на месте

                if (!resourcesLoaded)
                {
                    resources = await ReadShardResourcesAsync(cluster, shard.Name, ct);
                    resourcesLoaded = true;
                }

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
                    etcdForNodes ?? new EtcdEndpoints(endpoints), resources, ct);
                if (!ensured.IsSuccess)
                    return ensured;
            }
        }

        return Result.Success();
    }

    // Operator-triggered recreate: нода в TO_RECREATE — пересоздать немедленно
    // (аналог rebuild мёртвой, но по требованию оператора, без ожидания порога).
    // Guard: не лидер + жив кворум (≥1 другой ноды) — лидера не трогаем (failover
    // делает Patroni), без кворума rebuild небезопасен (нет источника для basebackup).
    private async Task<Result> RecreateMarkedNodesAsync(
        string cluster, ClusterSnapshot snap, ShardSpec shard,
        IReadOnlyDictionary<string, NodeAddress> addresses, CancellationToken ct)
    {
        var marked = shard.Nodes.Where(n => n.State == NodeState.ToRecreate).ToList();
        if (marked.Count == 0)
            return Result.Success();

        var scope = $"{cluster}-{shard.Name}";
        var scopeKvs = await RangeAsync($"/service/{scope}/", ct);
        if (!scopeKvs.IsSuccess)
            return scopeKvs;
        var leader = ClusterSnapshotParser.ParseService(scopeKvs.Value).FirstOrDefault()?.LeaderName;

        // Кворум: хотя бы одна другая нода жива (не в TO_RECREATE/QUARANTINED/REMOVING).
        var safe = shard.Nodes.Where(n => n.State != NodeState.ToRecreate
                                       && n.State is not NodeState.Quarantined
                                       && n.State is not NodeState.Removing).ToList();
        var quorum = safe.Count >= 1;

        foreach (var node in marked)
        {
            var isLeader = leader == node.Name;
            if (isLeader || !quorum)
                continue; // лидера и без кворума не трогаем — ждём failover/оживления

            if (!addresses.TryGetValue($"{shard.Name}/{node.Name}", out var addr))
                continue;

            var removed = await driver.RemoveNodeAsync(cluster, shard.Name, node.Name, ct);
            if (!removed.IsSuccess)
                return removed;

            var topology = TopologyOf(cluster, snap, shard.Name, addresses);
            var resources = await ReadShardResourcesAsync(cluster, shard.Name, ct);
            var ensured = await driver.EnsureNodeAsync(
                topology, node.Name, addr, secrets,
                etcdForNodes ?? new EtcdEndpoints(endpoints), resources, ct);
            if (!ensured.IsSuccess)
                return ensured;

            var rebuilding = await PutAsync(
                $"/clusters/{cluster}/shards/{shard.Name}/nodes/{node.Name}/state",
                "REBUILDING", ct);
            if (!rebuilding.IsSuccess)
                return rebuilding;
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
            // Карантин/демонтаж — не домен надзора (E3-инвариант arch/14 §5):
            // QUARANTINED ставится эвакуатором и держится до разбора runbook'ом
            // (возврат обрабатывает эвакуатор), REMOVING — RemoveShardProcess/
            // Deprovisioning. Проба мёртвой карантинной ноды затирала бы state
            // на UNREACHABLE — на инварианте строятся guard'ы G6/Д6 (t06).
            if (node.State is NodeState.Quarantined or NodeState.Removing)
                continue;
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
            // Guard кворума (spec §6.4 C «живых ≥2») для 2-нодовых шардов
            // обобщён: rebuild одиночной смерти допустим, пока жив кластер —
            // мертва максимум ОДНА нода (иначе — сценарий всего-шарда-мёртв).
            var quorum = alive.Count >= Math.Max(1, shard.Nodes.Count - 1);
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
                // Лимиты пересозданной ноды — из той же заявки request_* (rework №5).
                var resources = await ReadShardResourcesAsync(cluster, shard.Name, ct);
                var ensured = await driver.EnsureNodeAsync(
                    topology, name, addr, secrets,
                    etcdForNodes ?? new EtcdEndpoints(endpoints), resources, ct);
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

    // Заявки ресурсов шарда (rework №5): те же ключи, что в provisioning —
    // пересозданный (rebuild/самовосстановление) контейнер получает те же
    // лимиты. Чтение не удалось/нечитаемо — null (заявка — не контракт);
    // request_disk лимита в docker не имеет — игнор.
    private async Task<NodeResources?> ReadShardResourcesAsync(
        string cluster, string shard, CancellationToken ct)
    {
        var scope = $"{cluster}-{shard}";
        var cpu = await GetAsync($"/service/{scope}/request_cpu", ct);
        if (!cpu.IsSuccess)
            return null;
        var mem = await GetAsync($"/service/{scope}/request_mem", ct);
        return mem.IsSuccess ? NodeResourcesParser.Parse(cpu.Value?.Value, mem.Value?.Value) : null;
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
