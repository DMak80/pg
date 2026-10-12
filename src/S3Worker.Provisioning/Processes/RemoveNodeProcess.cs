using S3Worker.Core.Model;
using S3Worker.Docker.Drivers;
using Shared.Etcd.Client;

namespace S3Worker.Provisioning.Processes;

/// <summary>
/// RemoveNode (arch/24 §5 E): маркер TO_REMOVE → гварды (ДО деструктива:
/// последняя нода; нет живых RUNNING кроме выводимой — вывод не рвёт последнюю
/// точку записи пула) → REMOVING → снос контейнера+volume по placed_on →
/// del ключей ноды + исключение из portalloc (RMW под locks/portalloc) →
/// завершающее окно: RMW endpoints + актуализация config.nodes (txn по
/// mod_revision, spec §2.2; актуален — no-op) → done. Вызывается только
/// держателем клэйма.
/// </summary>
public sealed class RemoveNodeProcess(
    IEtcdGateway gateway,
    string[] endpoints,
    IS3ClusterDriver driver,
    ClaimStore claims,
    WorkJournal journal,
    PortAllocLock portLock,
    S3ProvisioningOptions options,
    Func<CancellationToken, Task<Result>>? snapshot = null)
{
    private const string Op = "remove-node";

    public async Task<Result> TickAsync(S3ClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Cluster;

        // Мутации — только держателем живого клэйма (arch/17 S5).
        var claimed = S3ProcessCommon.EnsureClaimed(claims, cluster, Op);
        if (!claimed.IsSuccess)
            return claimed;

        // Заявка: нода TO_REMOVE (по одной за тик — одна docker-мутация).
        var node = snap.Nodes
            .FirstOrDefault(n => n.State == S3NodeStates.ToRemove && n.ParseError is null);
        if (node is null)
            return Result.Success(); // заявок нет — no-op (защита повторного тика)

        // ГВАРДЫ ДО деструктива (journal-отказ, без мутаций):
        // 1) последняя нода — пул не может быть пуст (arch/23 §2: nodes 1..9);
        if (snap.Nodes.Count <= 1)
            return await RejectAsync(cluster, node.Node,
                "последняя нода кластера — вывод запрещён (снимите заявку или удалите кластер)", ct);

        // 2) «мёртвые остальные»: живых RUNNING-нод кроме выводимой нет →
        // вывод рвёт последнюю точку записи пула (arch/24 §5 E).
        var aliveOthers = snap.Nodes
            .Where(n => n.Node != node.Node && n.State == S3NodeStates.Running)
            .ToList();
        if (aliveOthers.Count == 0)
            return await RejectAsync(cluster, node.Node,
                "нет живых RUNNING-нод кроме выводимой — вывод не рвёт последнюю точку записи", ct);

        var started = await journal.WritePhaseAsync(cluster, Op, "started", claims.InstanceId, null, ct);
        if (!started.IsSuccess)
            return started;
        if (snapshot is not null)
        {
            var before = await snapshot(ct);
            if (!before.IsSuccess)
                return Fail(cluster, node.Node, before.Error!, "snapshot-before");
        }

        // REMOVING (жизненный цикл демонтажа ноды).
        var marking = await S3ProcessCommon.WriteNodeStateAsync(
            gateway, endpoints, cluster, node.Node, S3NodeStates.Removing, ct);
        if (!marking.IsSuccess)
            return Fail(cluster, node.Node, marking.Error!, "marking");

        // Снос контейнера + volume по placed_on (portalloc-адрес; при утрате
        // записи — лестница E9 у надзора; здесь объект сносим на всех хостах —
        // 404 = ок, уникальность имён s3w-<C>-node<k> глобальна).
        var removed = await driver.RemoveNodeAsync(cluster, node.Node, removeVolume: true, ct);
        if (!removed.IsSuccess)
            return Fail(cluster, node.Node, removed.Error!, "remove-node");

        // del ключей ноды.
        foreach (var key in new[]
                 {
                     S3ProcessCommon.NodePlacementKey(cluster, node.Node),
                     S3ProcessCommon.NodeResourcesKey(cluster, node.Node),
                     S3ProcessCommon.NodeStateKey(cluster, node.Node),
                     S3ProcessCommon.NodePlacedOnKey(cluster, node.Node),
                     S3ProcessCommon.NodeUsageKey(cluster, node.Node),
                 })
        {
            var del = await DeleteKeyAsync(key, ct);
            if (!del.IsSuccess)
                return Fail(cluster, node.Node, del.Error!, $"delete {key}");
        }

        // Исключение из portalloc (RMW под locks/portalloc).
        var alloc = await RemoveFromPortAllocAsync(snap, node.Node, ct);
        if (!alloc.IsSuccess)
            return Fail(cluster, node.Node, alloc.Error!, "portalloc-remove");

        // Завершающее окно: RMW endpoints + config.nodes (spec §2.2;
        // актуален — no-op).
        var pinned = await ReadPortAllocAsync(cluster, ct);
        if (!pinned.IsSuccess)
            return Fail(cluster, node.Node, pinned.Error!, "portalloc-read");
        var addresses = new Dictionary<string, NodeAddress>();
        foreach (var other in snap.Nodes.Where(n =>
                     n.Node != node.Node
                     && n.State is null or S3NodeStates.NotInitialized or S3NodeStates.Provisioning or S3NodeStates.Running))
        {
        if (pinned.Value.TryGetValue(other.Node, out var addr))
            addresses[other.Node] = new NodeAddress(
                options.AdvertisedHost ?? addr.Host, addr.S3Port);
        }

        var converge = await S3ProcessCommon.ConvergeTopologyAsync(
            gateway, endpoints, cluster, snap.Config?.Platform ?? "owns3",
            snap.Config?.SecretProfile, snap.Config?.CreatedUnix, addresses, ct);
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

    // Гвард-отказ: journal + Failed, без каких-либо мутаций.
    private async Task<Result> RejectAsync(string cluster, string node, string reason, CancellationToken ct)
    {
        var written = await journal.WritePhaseAsync(
            cluster, Op, "rejected", claims.InstanceId, reason, ct);
        return written.IsSuccess
            ? Result.Failed(new ApplicationException($"remove-node {cluster}/{node}: {reason}"))
            : written;
    }

    // Исключение ноды из portalloc: под глобальным клэймом (t91), put-if-... —
    // RMW txn по mod_revision (не NotExists: запись уже есть, меняем состав).
    private async Task<Result> RemoveFromPortAllocAsync(
        S3ClusterSnapshot snap, string node, CancellationToken ct)
    {
        var key = S3ProcessCommon.PortAllocKey(snap.Cluster);
        var current = await S3ProcessCommon.GetWithFailoverAsync(gateway, endpoints, key, ct);
        if (!current.IsSuccess)
            return current.Error!;
        if (current.Value is not { } kv)
            return Result.Success(); // записи нет — исключать нечего

        var alloc = S3ProcessCommon.ParsePortAlloc(kv.Value);
        if (!alloc.Remove(node))
            return Result.Success(); // ноды в записи нет — no-op

        var acquired = await portLock.TryAcquireAsync(ct);
        if (!acquired.IsSuccess)
            return acquired.Error!;
        if (!acquired.Value)
            return Result.Failed(new PortLockBusyException(portLock.Key));
        try
        {
            var txn = await TxnAsync(
                TxnRequest.Of(
                    [TxnCompare.ModRevisionEqual(key, (long)kv.ModRevision)],
                    [new TxnOp.Put(key, S3ProcessCommon.SerializePortAlloc(alloc), null)]),
                ct);
            if (!txn.IsSuccess)
                return txn.Error!;
            return txn.Value.Succeeded
                ? Result.Success()
                : Result.Failed(new ApplicationException(
                    $"{key} изменился под нами — ретрай тиком"));
        }
        finally
        {
            await portLock.ReleaseAsync();
        }
    }

    private Result Fail(string cluster, string node, Exception error, string phase)
    {
        journal.WritePhaseAsync(cluster, Op, phase, claims.InstanceId, error.Message, CancellationToken.None)
            .GetAwaiter().GetResult();
        return Result.Failed(new ApplicationException($"remove-node {cluster}/{node}: {phase}: {error.Message}", error));
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

    private async Task<Result> DeleteKeyAsync(string key, CancellationToken ct)
    {
        Result last = default;
        foreach (var endpoint in endpoints)
        {
            var del = await gateway.DeleteAsync(endpoint, key, prefix: false, ct);
            if (del.IsSuccess)
                return del;
            last = del;
        }

        return last.Error ?? new EtcdUnreachableException("etcd endpoints не заданы");
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

        return last.Error ?? new EtcdUnreachableException("etcd endpoints не заданы");
    }
}
