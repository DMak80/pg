using System.Globalization;
using System.Text.Json;
using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using KafkaWorker.Core.Planning;
using KafkaWorker.Core.Templates;
using KafkaWorker.Docker.Drivers;
using KafkaWorker.Etcd.Client;
using KafkaWorker.Etcd.Coordination;

namespace KafkaWorker.Provisioning.Processes;

// Лестница источников адреса при утере portalloc (E9, arch/17; t05 spec §3.3):
// 1) portalloc есть → адрес из журнала (advertise стабилен);
// 2) журнала нет, контейнер есть (положительная инспекция) → реконструкция
//    из docker inspect (published-порт + host) клэйм-txn version==0;
//    контейнер не трогаем — данные неприкосновенны;
// 3) нет ни журнала, ни контейнера → брокер мёртв по S7-свидетельству →
//    новая аллокация под клэймом locks/portalloc (S5/t90) + пересоздание +
//    RMW endpoints — клиенты перечитают дискавери тиком.
public sealed record HealedAddress(NodeAddress Address, bool Recreated);

public sealed class PortAllocHealer(
    IEtcdGateway etcd,
    string[] endpoints,
    IClusterDriver driver,
    ClaimStore claims,
    WorkJournal journal,
    PortAllocLock portLock,
    PortAllocIndex portAlloc,
    ProvisioningOptions options) : IAsyncDisposable
{
    private const string Op = "healing-portalloc";

    // Лестница для ОДНОГО безадресного брокера (spec §3.3).
    // Успех: HealedAddress (адрес + признак пересоздания); PortLockBusyException
    // → вызывающий делает waiting-portalloc-lock, следующий тик.
    public async Task<Result<HealedAddress>> ResolveAsync(
        KafkaClusterSnapshot snap, string broker,
        IReadOnlyDictionary<string, NodeAddress> addresses, CancellationToken ct)
    {
        var cluster = snap.Cluster;

        // Ветка 1: закрепление есть — advertise стабилен (rebuild по журналу).
        if (addresses.TryGetValue(broker, out var pinned))
            return Result<HealedAddress>.Success(new HealedAddress(pinned, Recreated: false));

        // Контейнер — до клэйма: положительная инспекция решает ветку.
        var inspection = await driver.InspectNodeEndpointAsync(cluster, broker, ct);
        if (!inspection.IsSuccess)
            return Result<HealedAddress>.Failed(inspection.Error!); // слепота — не лечим

        // journal-before-manipulations (spec §3.3 / arch/16 §5): фаза
        // ДО первого txn/EnsureNode — ветка известна после инспекции.
        var started = await journal.WriteAsync(cluster, Op, "started", claims.InstanceId, null, ct);
        if (!started.IsSuccess)
            return Result<HealedAddress>.Failed(started.Error!);

        if (inspection.Value is { } found)
            return await ReconstructAsync(snap, broker, found, ct); // ветка 2

        return await ReallocateAsync(snap, broker, addresses, ct); // ветка 3 (S7)
    }

    // Ветка 2: запись восстановленного закрепления put-if-absent (version==0)
    // под глобальным клэймом; проигрыш txn → re-read (первый записавший —
    // истина, S5). Контейнер НЕ трогаем. Контроль: клиентский порт advertised
    // env == published — расхождение journal-warning (канон — PortBindings).
    private async Task<Result<HealedAddress>> ReconstructAsync(
        KafkaClusterSnapshot snap, string broker, NodeEndpointInspection found, CancellationToken ct)
    {
        var cluster = snap.Cluster;
        var address = new NodeAddress(found.Host, found.ClientHostPort);

        var acquired = await portLock.TryAcquireAsync(ct);
        if (!acquired.IsSuccess)
            return Result<HealedAddress>.Failed(acquired.Error!);
        if (!acquired.Value)
            return Result<HealedAddress>.Failed(new PortLockBusyException());
        try
        {
            var key = PortAllocKey(cluster);
            var read = await ReadPortAllocAsync(cluster, ct);
            if (!read.IsSuccess)
                return Result<HealedAddress>.Failed(read.Error!);
            var merged = new Dictionary<string, NodeAddress>(read.Value);
            merged[broker] = address;
            var txn = await TxnAsync(
                TxnRequest.Of([TxnCompare.NotExists(key)], [new TxnOp.Put(key, SerializePortAlloc(merged), null)]), ct);
            if (!txn.IsSuccess)
                return Result<HealedAddress>.Failed(txn.Error!);
            if (!txn.Value.Succeeded)
            {
                // уже записал сосед — читаем его истину (S5) и дальше по ней
                var reread = await ReadPortAllocAsync(cluster, ct);
                if (!reread.IsSuccess)
                    return Result<HealedAddress>.Failed(reread.Error!);
                if (reread.Value.TryGetValue(broker, out var foreign))
                    address = foreign;
            }
        }
        finally
        {
            await portLock.ReleaseAsync();
        }

        if (found.AdvertisedClient is { } advertised
            && !advertised.EndsWith($":{found.ClientHostPort.ToString(CultureInfo.InvariantCulture)}", StringComparison.Ordinal))
            await journal.WriteAsync(cluster, Op, "reconstructed", claims.InstanceId,
                $"advertised {advertised} != published :{found.ClientHostPort} — канон PortBindings", ct);
        else
            await journal.WriteAsync(cluster, Op, "reconstructed", claims.InstanceId, null, ct);

        return Result<HealedAddress>.Success(new HealedAddress(address, Recreated: false));
    }

    // Ветка 3: новая аллокация (паттерн AddBrokerProcess.EnsurePortsAsync):
    // под клэймом busy = docker ∪ portalloc чужих ∪ свои закрепления →
    // PlacementPlanner+PortAllocator → RMW portalloc (mod_revision) →
    // EnsureNode (state=PROVISIONING пишет вызывающий supervise) →
    // RMW endpoints (mod_revision; put если ключа нет).
    private async Task<Result<HealedAddress>> ReallocateAsync(
        KafkaClusterSnapshot snap, string broker,
        IReadOnlyDictionary<string, NodeAddress> addresses, CancellationToken ct)
    {
        var cluster = snap.Cluster;

        var acquired = await portLock.TryAcquireAsync(ct);
        if (!acquired.IsSuccess)
            return Result<HealedAddress>.Failed(acquired.Error!);
        if (!acquired.Value)
            return Result<HealedAddress>.Failed(new PortLockBusyException());
        try
        {
            var read = await ReadPortAllocWithRevisionAsync(cluster, ct);
            if (!read.IsSuccess)
                return Result<HealedAddress>.Failed(read.Error!);
            var (revision, pinnedAddresses) = read.Value;
            var merged = new Dictionary<string, NodeAddress>(pinnedAddresses);
            if (merged.ContainsKey(broker))
                return Result<HealedAddress>.Success(new HealedAddress(merged[broker], Recreated: false)); // сосед успел

            var hosts = await driver.GetHostsAsync(ct);
            if (!hosts.IsSuccess)
                return Result<HealedAddress>.Failed(hosts.Error!);
            var dockerBusy = await driver.GetBusyPortsAsync(ct);
            if (!dockerBusy.IsSuccess)
                return Result<HealedAddress>.Failed(dockerBusy.Error!);
            var foreign = await portAlloc.ReadBusyAsync(cluster, ct);
            if (!foreign.IsSuccess)
                return Result<HealedAddress>.Failed(foreign.Error!);

            // План — только недостающие ноды; закреплённые адреса исключаются из
            // кандидатов явно (иначе аллокатор счёл бы их «занятыми»).
            var taken = new HashSet<(string Host, int Port)>(dockerBusy.Value);
            foreach (var p in foreign.Value)
                taken.Add(p);
            foreach (var addr in merged.Values)
                taken.Add((addr.Host, addr.ClientPort));

            var plan = PlacementPlanner.Plan([broker], hosts.Value);
            var allocated = PortAllocator.Allocate(plan, merged, taken, options.PortFrom, options.PortTo);
            if (!allocated.IsSuccess)
                return Result<HealedAddress>.Failed(allocated.Error!);
            foreach (var (node, addr) in allocated.Value)
                merged[node] = addr;

            // RMW portalloc под клэймом (compare mod_revision; отсутствие ключа —
            // revision 0 = NotExists; проигрыш — следующий тик перечитает чужую истину).
            var portTxn = await TxnAsync(TxnRequest.Of(
                [CompareKeyUnchanged(PortAllocKey(cluster), revision)],
                [new TxnOp.Put(PortAllocKey(cluster), SerializePortAlloc(merged), null)]), ct);
            if (!portTxn.IsSuccess)
                return Result<HealedAddress>.Failed(portTxn.Error!);
            if (!portTxn.Value.Succeeded)
                return Result<HealedAddress>.Failed(new ApplicationException(
                    $"portalloc {cluster} изменился под клэймом — ретрай тиком"));
            var address = merged[broker];

            // Контейнер по новому адресу (env — как EnsureNodeAsync надзора).
            var ensured = await EnsureNodeAsync(snap, broker, address, ct);
            if (!ensured.IsSuccess)
                return Result<HealedAddress>.Failed(ensured.Error!);

            // RMW endpoints: пересборка advertise-адресов всех брокеров из
            // восстановленного portalloc (AdvertisedClientHost ?? host:port).
            var endpointsError = await UpdateEndpointsAsync(snap, merged, ct);
            if (endpointsError is not null)
                return Result<HealedAddress>.Failed(endpointsError);

            await journal.WriteAsync(cluster, Op, "reallocated", claims.InstanceId, null, ct);
            return Result<HealedAddress>.Success(new HealedAddress(address, Recreated: true));
        }
        finally
        {
            await portLock.ReleaseAsync();
        }
    }

    // Пересоздание контейнера брокера по адресу (копия NodeSupervisor.EnsureNodeAsync:
    // дублирование осознанное — healer самодостаточен; state=PROVISIONING пишет
    // вызывающий supervise).
    private async Task<Result> EnsureNodeAsync(
        KafkaClusterSnapshot snap, string broker, NodeAddress addr, CancellationToken ct)
    {
        var cluster = snap.Cluster;
        var decl = snap.Brokers.FirstOrDefault(b => b.Name == broker);
        if (decl is null)
            return Result.Failed(new ApplicationException(
                $"healing-portalloc {cluster}: broker {broker} исчез из декларации"));

        if (snap.AppUser is null || snap.AppPassword is null)
            return Result.Failed(new ApplicationException(
                $"healing-portalloc {cluster}: нет app-кредов"));

        var controllers = snap.Brokers
            .Where(b => b.Role == "controller")
            .OrderBy(b => b.Name, StringComparer.Ordinal)
            .Select(b => $"{NodeId(b.Name)}@{b.Name}:9093")
            .ToList();
        var advertisedClient = $"{options.AdvertisedClientHost ?? addr.Host}:{addr.ClientPort.ToString(CultureInfo.InvariantCulture)}";

        var env = NodeEnvBuilder.Build(new NodeEnvSpec(
            cluster,
            NodeId(broker),
            broker,
            advertisedClient,
            decl.Role == "controller",
            controllers,
            snap.AppUser,
            [snap.AppPassword],
            snap.Config,
            snap.Config.Brokers,
            "/var/lib/kafka/data"));

        return await driver.EnsureNodeAsync(new KafkaNodeSpec(
            cluster, broker, addr.Host, addr.ClientPort, options.NodeImage, env,
            decl.Resources?.Cpu,
            decl.Resources is null ? null : decl.Resources.MemGi * 1024L * 1024 * 1024), ct);
    }

    // RMW /kafka/clusters/<C>/endpoints: advertise-адреса всех брокеров из
    // portalloc; ключа нет → put, есть → txn mod_revision (проигрыш — ретрай тиком).
    private async Task<Exception?> UpdateEndpointsAsync(
        KafkaClusterSnapshot snap, IReadOnlyDictionary<string, NodeAddress> merged, CancellationToken ct)
    {
        var key = EndpointsKey(snap.Cluster);
        var value = string.Join(",", merged
            .OrderBy(p => p.Key, StringComparer.Ordinal)
            .Select(p => $"{options.AdvertisedClientHost ?? p.Value.Host}:{p.Value.ClientPort.ToString(CultureInfo.InvariantCulture)}"));

        var current = await GetAsync(key, ct);
        if (!current.IsSuccess)
            return current.Error!;
        if (current.Value is null)
        {
            var put = await PutAsync(key, value, ct);
            return put.IsSuccess ? null : put.Error!;
        }

        var txn = await TxnAsync(TxnRequest.Of(
            [CompareKeyUnchanged(key, (long)current.Value.ModRevision)],
            [new TxnOp.Put(key, value, null)]), ct);
        if (!txn.IsSuccess)
            return txn.Error!;
        return txn.Value.Succeeded
            ? null
            : new ApplicationException($"endpoints {key} изменились под клэймом — ретрай тиком");
    }

    // Compare «ключ не менялся с чтения»: отсутствие ключа (revision 0) —
    // NotExists (version==0), иначе mod_revision (семантика AddBrokerProcess).
    private static TxnCompare CompareKeyUnchanged(string key, long revision)
        => revision == 0 ? TxnCompare.NotExists(key) : TxnCompare.ModRevisionEqual(key, revision);

    // portalloc под ключом с ревизией (RMW-ветка).
    private async Task<Result<(long Revision, IReadOnlyDictionary<string, NodeAddress> Addresses)>>
        ReadPortAllocWithRevisionAsync(string cluster, CancellationToken ct)
    {
        var result = await GetAsync(PortAllocKey(cluster), ct);
        if (!result.IsSuccess)
            return Result<(long, IReadOnlyDictionary<string, NodeAddress>)>.Failed(result.Error!);
        if (result.Value is not { } kv)
            return Result<(long, IReadOnlyDictionary<string, NodeAddress>)>.Success(
                (0, (IReadOnlyDictionary<string, NodeAddress>)new Dictionary<string, NodeAddress>()));

        var addresses = ParsePortAlloc(kv.Value);
        return Result<(long, IReadOnlyDictionary<string, NodeAddress>)>.Success(
            ((long)kv.ModRevision, addresses));
    }

    private async Task<Result<IReadOnlyDictionary<string, NodeAddress>>> ReadPortAllocAsync(
        string cluster, CancellationToken ct)
    {
        var result = await GetAsync(PortAllocKey(cluster), ct);
        if (!result.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(result.Error!);
        if (result.Value is not { } kv)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(
                (IReadOnlyDictionary<string, NodeAddress>)new Dictionary<string, NodeAddress>());
        return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(ParsePortAlloc(kv.Value));
    }

    // Формат arch/15 §4: {"broker<k>":{"host":"h","client":16001}}.
    private static Dictionary<string, NodeAddress> ParsePortAlloc(string json)
    {
        var addresses = new Dictionary<string, NodeAddress>();
        using var doc = JsonDocument.Parse(json);
        foreach (var node in doc.RootElement.EnumerateObject())
            addresses[node.Name] = new NodeAddress(
                node.Value.GetProperty("host").GetString()!,
                node.Value.GetProperty("client").GetInt32());
        return addresses;
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

    private static int NodeId(string nodeName)
        => int.TryParse(nodeName["broker".Length..], NumberStyles.Integer, CultureInfo.InvariantCulture, out var id)
            ? id
            : 0;

    private static string PortAllocKey(string cluster) => $"/kafkaworker/portalloc/{cluster}";

    private static string EndpointsKey(string cluster) => $"/kafka/clusters/{cluster}/endpoints";

    // Failover-обёртки: первый успешный endpoint выигрывает.
    private async Task<Result<Kv?>> GetAsync(string key, CancellationToken ct)
        => await WithFailoverAsync(endpoint => etcd.GetAsync(endpoint, key, ct));

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
        => await WithFailoverAsync(endpoint => etcd.TxnAsync(endpoint, req, ct));

    private async Task<Result<T>> WithFailoverAsync<T>(Func<string, Task<Result<T>>> call)
    {
        Result<T>? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await call(endpoint);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }

    // Страховка: клэйм не остаётся удержанным при сбое вызывающего между тиками.
    public async ValueTask DisposeAsync()
    {
        await portLock.ReleaseAsync();
    }
}
