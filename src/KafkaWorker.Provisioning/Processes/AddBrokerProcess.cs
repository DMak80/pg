using System.Collections.Concurrent;
using System.Text.Json;
using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using KafkaWorker.Core.Planning;
using KafkaWorker.Docker.Drivers;
using KafkaWorker.Etcd.Client;
using KafkaWorker.Etcd.Coordination;
using KafkaWorker.Provisioning.Kafka;

namespace KafkaWorker.Provisioning.Processes;

/// <summary>
/// AddBrokerProcess (arch/16 §5 F): заявка brokers/&lt;b&gt;/state=NOT_INITIALIZED у
/// Active-кластера → план (порт; role=broker — кворум НЕ меняется) → контейнер
/// broker-only → появление в DescribeCluster → RMW endpoints (добавить адрес) →
/// state=RUNNING. Идемпотентен: RUNNING пропускается, контейнер сверяется по
/// имени. Вызывается только держателем клэйма &lt;C&gt;.
/// </summary>
public sealed class AddBrokerProcess(
    IEtcdGateway etcd,
    string[] endpoints,
    IClusterDriver driver,
    ClaimStore claims,
    WorkJournal journal,
    IKafkaAdminClientFactory adminFactory,
    ProvisioningOptions options)
{
    private const string Op = "add-broker";

    // Время первого наблюдения «новый брокер не в DescribeCluster» (бюджет
    // BrokerBootSec; диагностический — takeover начинает отсчёт заново).
    private readonly ConcurrentDictionary<string, long> _bootWaitSince = new();

    public async Task<Result> RunAsync(KafkaClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Cluster;
        if (!claims.IsMine(cluster))
            return Result.Failed(new ApplicationException(
                $"add-broker {cluster}: клэйм не наш (или потерян) — мутации запрещены"));

        // Незавершённые заявки add: NOT_INITIALIZED (свежая) и PROVISIONING (брокер
        // поднимается — тик доводит его до RUNNING по факту DescribeCluster).
        var pending = snap.Brokers.Where(b => b.State is "NOT_INITIALIZED" or "PROVISIONING").ToList();
        if (pending.Count == 0)
            return Result.Success(); // заявок нет — no-op

        var started = await journal.WriteAsync(cluster, Op, "started", claims.InstanceId, null, ct);
        if (!started.IsSuccess)
            return started;

        if (snap.AppUser is null || snap.AppPassword is null)
            return Fail(cluster,
                new ApplicationException($"add-broker {cluster}: нет app-кредов (ensure не выполнен)"),
                "no-creds");

        // План: адреса из portalloc + добор портов для новых брокеров (RMW).
        var ports = await EnsurePortsAsync(snap, pending, ct);
        if (!ports.IsSuccess)
            return Fail(cluster, ports.Error!, "planning");
        var addresses = ports.Value;

        // Контейнеры broker-only + state=PROVISIONING (существующие — сверка).
        var ensured = await EnsureNodesAsync(snap, pending, addresses, ct);
        if (!ensured.IsSuccess)
            return Fail(cluster, ensured.Error!, "ensure-nodes");

        // Готовность: DescribeCluster видит всех заявленных брокеров.
        var ready = await WaitReadyAsync(snap, ct);
        if (!ready.IsSuccess)
            return Fail(cluster, ready.Error!, "waiting-brokers");
        if (!ready.Value)
            return Result.Success(); // InProgress — следующий тик продолжит

        // RMW endpoints (добавить адреса) + state=RUNNING.
        var endpointsUpdated = await AddEndpointsAsync(snap, pending, addresses, ct);
        if (!endpointsUpdated.IsSuccess)
            return Fail(cluster, endpointsUpdated.Error!, "endpoints-rmw");

        foreach (var broker in pending)
        {
            var running = await PutAsync(BrokerStateKey(cluster, broker.Name), "RUNNING", ct);
            if (!running.IsSuccess)
                return Fail(cluster, running.Error!, "mark-running");
        }

        _bootWaitSince.TryRemove(cluster, out _);
        return await journal.WriteAsync(cluster, Op, "done", claims.InstanceId, null, ct);
    }

    // Добор адресов для новых брокеров: portalloc RMW по mod_revision (txn).
    private async Task<Result<IReadOnlyDictionary<string, NodeAddress>>> EnsurePortsAsync(
        KafkaClusterSnapshot snap, IReadOnlyList<KafkaBrokerDecl> pending, CancellationToken ct)
    {
        var cluster = snap.Cluster;
        var current = await ReadPortAllocAsync(cluster, ct);
        if (!current.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(current.Error!);
        var addresses = new Dictionary<string, NodeAddress>(current.Value.Addresses);

        var missing = pending.Select(b => b.Name).Where(n => !addresses.ContainsKey(n)).ToList();
        if (missing.Count > 0)
        {
            var hosts = await driver.GetHostsAsync(ct);
            if (!hosts.IsSuccess)
                return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(hosts.Error!);
            var busy = await driver.GetBusyPortsAsync(ct);
            if (!busy.IsSuccess)
                return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(busy.Error!);

            var plan = PlacementPlanner.Plan([.. addresses.Keys, .. missing], hosts.Value);
            var allocated = PortAllocator.Allocate(
                plan, addresses, busy.Value, options.PortFrom, options.PortTo);
            if (!allocated.IsSuccess)
                return allocated;
            foreach (var (node, addr) in allocated.Value)
                addresses[node] = addr;

            var serialized = SerializePortAlloc(addresses);
            var key = PortAllocKey(cluster);
            var txn = await TxnAsync(
                TxnRequest.Of(
                    [TxnCompare.ModRevisionEqual(key, current.Value.Revision ?? 0)],
                    [new TxnOp.Put(key, serialized, null)]),
                ct);
            if (!txn.IsSuccess)
                return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(txn.Error!);
            if (!txn.Value.Succeeded)
                return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(new ApplicationException(
                    $"portalloc {key} изменился с момента чтения — ретрай тиком"));
        }

        // Роль новых брокеров: broker-only, фиксируется навсегда (arch/15 §2).
        foreach (var broker in pending)
            if (broker.Role is null)
            {
                var put = await PutAsync(RoleKey(cluster, broker.Name), "broker", ct);
                if (!put.IsSuccess)
                    return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(put.Error!);
            }

        return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(addresses);
    }

    // Контейнер + state=PROVISIONING (env: кворум из controller-нод декларации).
    private async Task<Result> EnsureNodesAsync(
        KafkaClusterSnapshot snap,
        IReadOnlyList<KafkaBrokerDecl> pending,
        IReadOnlyDictionary<string, NodeAddress> addresses,
        CancellationToken ct)
    {
        var cluster = snap.Cluster;
        foreach (var broker in pending)
        {
            if (!addresses.TryGetValue(broker.Name, out var addr))
                return Result.Failed(new ApplicationException(
                    $"add-broker {cluster}: broker {broker.Name} не закреплён в portalloc"));

            var marked = await PutAsync(BrokerStateKey(cluster, broker.Name), "PROVISIONING", ct);
            if (!marked.IsSuccess)
                return marked;

            var env = BrokerEnvBuilder.Build(snap, broker.Name, addr, [snap.AppPassword!], options);
            var spec = new KafkaNodeSpec(
                cluster, broker.Name, addr.Host, addr.ClientPort, options.NodeImage, env,
                broker.Resources?.Cpu,
                broker.Resources is null ? null : broker.Resources.MemGi * 1024L * 1024 * 1024);
            var ensured = await driver.EnsureNodeAsync(spec, ct);
            if (!ensured.IsSuccess)
                return ensured;
        }

        return Result.Success();
    }

    // DescribeCluster: состав кластера = вся декларация (включая новых).
    private async Task<Result<bool>> WaitReadyAsync(KafkaClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Cluster;
        await using var admin = adminFactory.Create(snap.Endpoints!, snap.AppUser!, snap.AppPassword!);
        var view = await admin.DescribeClusterAsync(ct);
        var ready = view.IsSuccess && view.Value.Brokers.Count >= snap.Brokers.Count;

        if (ready)
            return Result<bool>.Success(true);

        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var since = _bootWaitSince.GetOrAdd(cluster, now);
        var reason = view.IsSuccess
            ? $"брокеров в кластере {view.Value.Brokers.Count} из {snap.Brokers.Count}"
            : view.Error!.Message;
        if (options.BrokerBootSec <= 0 || now - since > options.BrokerBootSec)
            return Result<bool>.Failed(new ApplicationException(
                $"брокер не присоединился к {cluster} за бюджет {options.BrokerBootSec} с: {reason}"));

        return Result<bool>.Success(false);
    }

    // RMW endpoints по mod_revision: добавить адреса новых брокеров к существующим.
    private async Task<Result> AddEndpointsAsync(
        KafkaClusterSnapshot snap,
        IReadOnlyList<KafkaBrokerDecl> pending,
        IReadOnlyDictionary<string, NodeAddress> addresses,
        CancellationToken ct)
    {
        var key = EndpointsKey(snap.Cluster);
        var current = await GetAsync(key, ct);
        if (!current.IsSuccess)
            return current;

        var existing = (current.Value?.Value ?? "")
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .ToList();
        var changed = false;
        foreach (var broker in pending)
        {
            var address = BrokerEnvBuilder.AdvertisedClient(snap, broker.Name, addresses[broker.Name], options);
            if (!existing.Contains(address, StringComparer.Ordinal))
            {
                existing.Add(address);
                changed = true;
            }
        }

        if (!changed)
            return Result.Success(); // адреса уже на месте (повторный тик)

        var txn = await TxnAsync(
            TxnRequest.Of(
                [TxnCompare.ModRevisionEqual(key, (long)(current.Value?.ModRevision ?? 0))],
                [new TxnOp.Put(key, string.Join(",", existing), null)]),
            ct);
        if (!txn.IsSuccess)
            return txn;
        if (!txn.Value.Succeeded)
            return Result.Failed(new ApplicationException(
                $"endpoints {key} изменился с момента чтения — ретрай тиком"));

        return Result.Success();
    }

    private sealed record PortAllocRead(
        IReadOnlyDictionary<string, NodeAddress> Addresses,
        long? Revision);

    private async Task<Result<PortAllocRead>> ReadPortAllocAsync(string cluster, CancellationToken ct)
    {
        var result = await GetAsync(PortAllocKey(cluster), ct);
        if (!result.IsSuccess)
            return Result<PortAllocRead>.Failed(result.Error!);
        var addresses = new Dictionary<string, NodeAddress>();
        if (result.Value is { } kv)
        {
            using var doc = JsonDocument.Parse(kv.Value);
            foreach (var node in doc.RootElement.EnumerateObject())
                addresses[node.Name] = new NodeAddress(
                    node.Value.GetProperty("host").GetString()!,
                    node.Value.GetProperty("client").GetInt32());
        }

        return Result<PortAllocRead>.Success(new PortAllocRead(addresses, (long?)result.Value?.ModRevision));
    }

    private static string SerializePortAlloc(IReadOnlyDictionary<string, NodeAddress> addresses)
    {
        using var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            foreach (var (node, addr) in addresses.OrderBy(p => p.Key, StringComparer.Ordinal))
            {
                writer.WritePropertyName(node);
                writer.WriteStartObject();
                writer.WriteString("host", addr.Host);
                writer.WriteNumber("client", addr.ClientPort);
                writer.WriteEndObject();
            }

            writer.WriteEndObject();
        }

        return System.Text.Encoding.UTF8.GetString(buffer.ToArray());
    }

    private Result Fail(string cluster, Exception error, string phase)
    {
        journal.WriteAsync(cluster, Op, phase, claims.InstanceId, error.Message, CancellationToken.None)
            .GetAwaiter().GetResult();
        return Result.Failed(error);
    }

    private static string BrokerStateKey(string cluster, string broker)
        => $"/kafka/clusters/{cluster}/brokers/{broker}/state";

    private static string RoleKey(string cluster, string broker)
        => $"/kafka/clusters/{cluster}/brokers/{broker}/role";

    private static string EndpointsKey(string cluster) => $"/kafka/clusters/{cluster}/endpoints";

    private static string PortAllocKey(string cluster) => $"/kafkaworker/portalloc/{cluster}";

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
