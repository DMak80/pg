using S3Worker.Core.Model;
using S3Worker.Core.OwnS3;
using S3Worker.Docker.Drivers;
using Shared.Core.Planning;
using Shared.Etcd.Client;

namespace S3Worker.Provisioning.Processes;

/// <summary>
/// AddNode (arch/24 §5 D): заявка nodes/&lt;I&gt;/state=NOT_INITIALIZED у
/// Active-кластера (имя инстанса &lt;I&gt; — из тела запроса, arch/24 §1.1) →
/// mini-A (placement + portalloc довыделение под locks —
/// та же механика P1; volume + контейнер + state=PROVISIONING; health →
/// RUNNING; bucket) + завершающее окно: RMW endpoints + актуализация
/// config.nodes до фактического состава (txn по mod_revision, spec §2.2).
/// Прочие ноды не тронуты; nodes уже равен составу → config-txn no-op.
/// Вызывается только держателем клэйма.
/// </summary>
public sealed class AddNodeProcess(
    IEtcdGateway gateway,
    string[] endpoints,
    IS3ClusterDriver driver,
    ClaimStore claims,
    WorkJournal journal,
    PortAllocLock portLock,
    PortAllocIndex portAlloc,
    SecretProfileStore secrets,
    PkiStore pki,
    InstanceEnvComposer envComposer,
    IOwnS3NodeClient nodeClient,
    S3ProvisioningOptions options,
    Func<CancellationToken, Task<Result>>? snapshot = null)
{
    private const string Op = "add-node";

    public async Task<Result> TickAsync(S3ClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Cluster;

        // Мутации — только держателем живого клэйма (arch/17 S5).
        var claimed = S3ProcessCommon.EnsureClaimed(claims, cluster, Op);
        if (!claimed.IsSuccess)
            return claimed;

        // Заявка: ноды NOT_INITIALIZED (могло быть несколько за один тик —
        // доводим по одной за вызов, остальные следующим тиком — одна
        // docker-мутация за тик). PROVISIONING-ноды Active-кластера доводим
        // ТОЖЕ: надзор (C) идёт в тике раньше D и переводит живой контейнер
        // PROVISIONING → RUNNING — заявка исчезает из вида, а финал D
        // (RMW endpoints + config.nodes) остаётся невыполненным. Resume
        // идемпотентен по факту (arch/24 §5): PlanNode вернёт закреплённый
        // portalloc-адрес, EnsureNode/health/bucket — no-op/повтор.
        var pending = snap.Nodes
            .Where(n => n.State is S3NodeStates.NotInitialized or S3NodeStates.Provisioning)
            .OrderBy(n => n.Node, StringComparer.Ordinal)
            .ToList();
        if (pending.Count == 0)
            return Result.Success(); // заявок нет — no-op

        var node = pending[0];

        // Гварды Active-кластера: предел 1..9 (arch/23 §2) по фактическому составу.
        if (snap.Nodes.Count >= 9)
            return Fail(cluster, node.Node, new ApplicationException(
                $"add-node {cluster}/{node.Node}: actual nodes {snap.Nodes.Count} — limit 9"), "guard-limit");

        var started = await journal.WritePhaseAsync(cluster, Op, "started", claims.InstanceId, null, ct);
        if (!started.IsSuccess)
            return started;
        if (snapshot is not null)
        {
            var before = await snapshot(ct);
            if (!before.IsSuccess)
                return Fail(cluster, node.Node, before.Error!, "snapshot-before");
        }

        // Профиль: без кред нода не создаётся (permanent до починки env).
        if (!secrets.TryGet(snap.Config?.SecretProfile ?? "", out var nodeSecrets, out var secretDiagnostic))
            return Fail(cluster, node.Node, new ApplicationException(
                $"secret profile {snap.Config?.SecretProfile}: {secretDiagnostic}"), "invalid-secret-profile");

        // Mini-A фаза 1: placement + portalloc довыделение под locks (P1).
        var planned = await PlanNodeAsync(snap, node, ct);
        if (!planned.IsSuccess)
        {
            if (planned.Error is PortLockBusyException)
                return await journal.WritePhaseAsync(cluster, Op, "waiting-portalloc-lock", claims.InstanceId, null, ct);
            return Fail(cluster, node.Node, planned.Error!, "planning");
        }
        var address = planned.Value!;

        // Mini-A фаза 2: volume + контейнер + state=PROVISIONING (arch/23 §6 env).
        var marked = await S3ProcessCommon.WriteNodeStateAsync(
            gateway, endpoints, cluster, node.Node, S3NodeStates.Provisioning, ct);
        if (!marked.IsSuccess)
            return Fail(cluster, node.Node, marked.Error!, "marking");
        var placedOn = await PutAsync(S3ProcessCommon.NodePlacedOnKey(cluster, node.Node), address.Host, ct);
        if (!placedOn.IsSuccess)
            return Fail(cluster, node.Node, placedOn.Error!, "placed-on");

        // PKI (arch/23 §6): CA кластера уже есть у живого кластера —
        // EnsureClusterPkiAsync идемпотентен; материал инстанса — put-if-absent
        // (повторное создание с тем же именем — новый выпуск, §3.2).
        var clusterPki = await pki.EnsureClusterPkiAsync(cluster, ct);
        if (!clusterPki.IsSuccess)
            return Fail(cluster, node.Node, clusterPki.Error!, "pki");
        var nodePki = await pki.EnsureNodePkiAsync(cluster, node.Node, clusterPki.Value.Ca, ct);
        if (!nodePki.IsSuccess)
            return Fail(cluster, node.Node, nodePki.Error!, "pki");

        // env инстанса — единая точка сборки (§3.6); включение в mesh — G по
        // links (вызова в D нет — Active-G подхватит, arch/24 §5 D).
        var env = envComposer.Compose(
            nodeSecrets!, nodePki.Value, clusterPki.Value.Ca.Ca,
            $"http://{options.AdvertisedHost ?? address.Host}:{address.S3Port}");
        var resources = S3ProcessCommon.ParseResources(node.Resources);
        var ensured = await driver.EnsureNodeAsync(
            new S3NodeSpec(cluster, node.Node, address.Host, address.S3Port, options.OwnS3Image, env,
                resources?.Cpu, resources?.MemBytes),
            options.ContourNetwork, ct);
        if (!ensured.IsSuccess)
            return Fail(cluster, node.Node, ensured.Error!, "ensure-node");

        // Mini-A фаза 3: health → RUNNING (не ожила — InProgress, тик повторит);
        // bucket per-install (P3). Пробы — по advertised-хосту (канон P,
        // arch/23 §2.1 advertised-правило; размещение — portalloc-хост).
        var probeHost = options.AdvertisedHost ?? address.Host;
        var health = await nodeClient.HealthAsync(probeHost, address.S3Port, ct);
        if (!health.IsSuccess || !health.Value)
            return await journal.WritePhaseAsync(cluster, Op, "waiting-node", claims.InstanceId, null, ct);
        var running = await S3ProcessCommon.WriteNodeStateAsync(
            gateway, endpoints, cluster, node.Node, S3NodeStates.Running, ct);
        if (!running.IsSuccess)
            return Fail(cluster, node.Node, running.Error!, "running");
        var bucket = await nodeClient.EnsureBucketAsync(
            probeHost, address.S3Port, nodeSecrets!, options.Bucket, ct);
        if (!bucket.IsSuccess)
            return Fail(cluster, node.Node, bucket.Error!, "ensure-bucket");

        // Завершающее окно: RMW endpoints + актуализация config.nodes
        // до фактического состава (txn по mod_revision, spec §2.2; уже
        // актуален — no-op). Адреса — portalloc (закреплённые + новая).
        var converge = await ConvergeTopologyAsync(snap, ct);
        if (!converge.IsSuccess)
            return Fail(cluster, node.Node, converge.Error!, "converge-topology");

        if (snapshot is not null)
        {
            var after = await snapshot(ct);
            if (!after.IsSuccess)
                return Fail(cluster, node.Node, after.Error!, "snapshot-after");
        }

        return await journal.WritePhaseAsync(cluster, Op, "done", claims.InstanceId, null, ct);
    }

    // P1-механика для одной ноды: placement-селектор + portalloc довыделение
    // под locks/portalloc (та же лестница, что ProvisioningProcess.PlanAsync).
    private async Task<Result<NodeAddress>> PlanNodeAsync(
        S3ClusterSnapshot snap, S3NodeSnapshot node, CancellationToken ct)
    {
        var cluster = snap.Cluster;
        var pinnedRead = await ReadPortAllocWithRevisionAsync(cluster, ct);
        if (!pinnedRead.IsSuccess)
            return Result<NodeAddress>.Failed(pinnedRead.Error!);
        var (pinnedRevision, existing) = pinnedRead.Value!;
        if (existing.TryGetValue(node.Node, out var pinnedAddress))
            return Result<NodeAddress>.Success(pinnedAddress); // закреплён ранее

        var acquired = await portLock.TryAcquireAsync(ct);
        if (!acquired.IsSuccess)
            return Result<NodeAddress>.Failed(acquired.Error!);
        if (!acquired.Value)
            return Result<NodeAddress>.Failed(new PortLockBusyException(portLock.Key));
        try
        {
            var hosts = await driver.GetHostsAsync(ct);
            if (!hosts.IsSuccess)
                return Result<NodeAddress>.Failed(hosts.Error!);
            var dockerBusy = await driver.GetBusyPortsAsync(ct);
            if (!dockerBusy.IsSuccess)
                return Result<NodeAddress>.Failed(dockerBusy.Error!);
            var foreign = await portAlloc.ForeignAllocatedPortsAsync(cluster, ct);
            if (!foreign.IsSuccess)
                return Result<NodeAddress>.Failed(foreign.Error!);

            var busy = new HashSet<(string Host, int Port)>();
            foreach (var host in hosts.Value)
            {
                foreach (var port in foreign.Value)
                    busy.Add((host.Name, port));
                foreach (var pair in dockerBusy.Value)
                    busy.Add((host.Name, pair.Port));
            }

            var resolved = PlacementResolver.Resolve(
                node.Placement ?? new S3NodePlacement(null, null, null), hosts.Value, out var candidates);
            if (!resolved.IsSuccess)
                return Result<NodeAddress>.Failed(new ApplicationException(
                    $"placement {cluster}/{node.Node}: zero candidates; list: [{string.Join(", ", candidates)}]"));

            var allocated = PortAllocator.Allocate(
                new PlacementPlan([new NodePlacement(cluster, node.Node, resolved.Value)]),
                existing, busy, options.PortRangeFrom, options.PortRangeTo,
                S3Planning.PortsOf, S3Planning.HostOf, S3Planning.MakeAddress, S3Planning.KeyOf);
            if (!allocated.IsSuccess)
                return Result<NodeAddress>.Failed(allocated.Error!);
            var address = allocated.Value[node.Node];

            // RMW portalloc: ключа нет — put-if-absent (создание); есть —
            // txn по mod_revision (довыделение). Проигрыш → re-read (S5).
            var merged = new Dictionary<string, NodeAddress>(existing) { [node.Node] = address };
            var key = S3ProcessCommon.PortAllocKey(cluster);
            var compare = existing.Count > 0
                ? TxnCompare.ModRevisionEqual(key, pinnedRevision)
                : TxnCompare.NotExists(key);
            var txn = await TxnAsync(
                TxnRequest.Of([compare], [new TxnOp.Put(key, S3ProcessCommon.SerializePortAlloc(merged), null)]), ct);
            if (!txn.IsSuccess)
                return Result<NodeAddress>.Failed(txn.Error!);
            if (!txn.Value.Succeeded)
            {
                var reread = await ReadPortAllocAsync(cluster, ct);
                if (!reread.IsSuccess)
                    return Result<NodeAddress>.Failed(reread.Error!);
                if (!reread.Value.TryGetValue(node.Node, out var foreignAddress))
                    return Result<NodeAddress>.Failed(new ApplicationException(
                        $"add-node {cluster}/{node.Node}: lost txn without neighbor entry"));
                return Result<NodeAddress>.Success(foreignAddress);
            }

            return Result<NodeAddress>.Success(address);
        }
        finally
        {
            await portLock.ReleaseAsync();
        }
    }

    // Завершающее окно: фактический состав (все ноды, кроме выводимых/демонтажных)
    // × portalloc-адреса → RMW endpoints + config.nodes.
    private async Task<Result> ConvergeTopologyAsync(S3ClusterSnapshot snap, CancellationToken ct)
    {
        var pinned = await ReadPortAllocAsync(snap.Cluster, ct);
        if (!pinned.IsSuccess)
            return pinned.Error!;

        var addresses = new Dictionary<string, NodeAddress>();
        foreach (var node in snap.Nodes.Where(n =>
                     n.State is null or S3NodeStates.NotInitialized or S3NodeStates.Provisioning or S3NodeStates.Running))
        {
            if (pinned.Value.TryGetValue(node.Node, out var addr))
                addresses[node.Node] = new NodeAddress(
                    options.AdvertisedHost ?? addr.Host, addr.S3Port);
        }

        return await S3ProcessCommon.ConvergeTopologyAsync(
            gateway, endpoints, snap.Cluster, snap.Config?.Platform ?? "owns3",
            snap.Config?.SecretProfile, snap.Config?.CreatedUnix, addresses, ct);
    }

    private Result Fail(string cluster, string node, Exception error, string phase)
    {
        journal.WritePhaseAsync(cluster, Op, phase, claims.InstanceId, error.Message, CancellationToken.None)
            .GetAwaiter().GetResult();
        return Result.Failed(new ApplicationException($"add-node {cluster}/{node}: {phase}: {error.Message}", error));
    }

    private async Task<Result<IReadOnlyDictionary<string, NodeAddress>>> ReadPortAllocAsync(
        string cluster, CancellationToken ct)
    {
        var result = await S3ProcessCommon.GetWithFailoverAsync(
            gateway, endpoints, S3ProcessCommon.PortAllocKey(cluster), ct);
        if (!result.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(result.Error!);
        if (result.Value is not { } kv)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(
                (IReadOnlyDictionary<string, NodeAddress>)new Dictionary<string, NodeAddress>());
        return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(S3ProcessCommon.ParsePortAlloc(kv.Value));
    }

    // Чтение portalloc с mod_revision (для RMW довыделения).
    private async Task<Result<(long Revision, IReadOnlyDictionary<string, NodeAddress> Alloc)>>
        ReadPortAllocWithRevisionAsync(string cluster, CancellationToken ct)
    {
        var result = await S3ProcessCommon.GetWithFailoverAsync(
            gateway, endpoints, S3ProcessCommon.PortAllocKey(cluster), ct);
        if (!result.IsSuccess)
            return Result<(long, IReadOnlyDictionary<string, NodeAddress>)>.Failed(result.Error!);
        if (result.Value is not { } kv)
            return Result<(long, IReadOnlyDictionary<string, NodeAddress>)>.Success(
                (0L, (IReadOnlyDictionary<string, NodeAddress>)new Dictionary<string, NodeAddress>()));
        return Result<(long, IReadOnlyDictionary<string, NodeAddress>)>.Success(
            ((long)kv.ModRevision, S3ProcessCommon.ParsePortAlloc(kv.Value)));
    }

    private async Task<Result> PutAsync(string key, string value, CancellationToken ct)
    {
        Result last = default;
        foreach (var endpoint in endpoints)
        {
            var result = await gateway.PutAsync(endpoint, key, value, null, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last.Error ?? new EtcdUnreachableException("etcd endpoints are not set");
    }

    private async Task<Result<TxnResult>> TxnAsync(TxnRequest req, CancellationToken ct)
    {
        Result<TxnResult> last = default;
        foreach (var endpoint in endpoints)
        {
            var result = await gateway.TxnAsync(endpoint, req, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last.Error ?? new EtcdUnreachableException("etcd endpoints are not set");
    }
}
