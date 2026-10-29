using System.Globalization;
using System.Text.Json;
using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using KafkaWorker.Core.Planning;
using KafkaWorker.Core.Templates;
using KafkaWorker.Docker.Drivers;
using Shared.Etcd.Client;

namespace KafkaWorker.Provisioning.Processes;

// Лестница источников адреса при утере portalloc (E9, arch/17; spec §3.3):
// 1) portalloc есть → адрес из журнала (advertise стабилен);
// 2) журнала нет, контейнер есть (положительная инспекция) → реконструкция
//    из docker inspect (published-порт + host) RMW-merge по mod_revision
//    (merge с живыми записями соседей); контейнер не трогаем — данные
//    неприкосновенны;
// 3) нет ни журнала, ни контейнера → брокер мёртв по S7-свидетельству →
//    новая аллокация под клэймом locks/portalloc (S5/) + пересоздание +
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
    ProvisioningOptions options,
    BrokerCertificateCache certificates) : IAsyncDisposable
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
        var started = await journal.WritePhaseAsync(cluster, Op, "started", claims.InstanceId, null, ct);
        if (!started.IsSuccess)
            return Result<HealedAddress>.Failed(started.Error!);

        if (inspection.Value is { } found)
            return await ReconstructAsync(snap, broker, found, ct); // ветка 2

        return await ReallocateAsync(snap, broker, addresses, ct); // ветка 3 (S7)
    }

    // Ветка 2: запись восстановленного закрепления — RMW-merge по mod_revision
    // под глобальным клэймом (merge с живыми записями соседей; отсутствие
    // ключа — ревизия 0 = NotExists); проигрыш txn → re-read (первый
    // записавший — истина, S5; нашей записи нет — retry следующим тиком).
    // Контейнер НЕ трогаем. Контроль: клиентский порт advertised
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
            return Result<HealedAddress>.Failed(new PortLockBusyException(portLock.Key));
        try
        {
            var key = PortAllocKey(cluster);
            var read = await ReadPortAllocWithRevisionAsync(cluster, ct);
            if (!read.IsSuccess)
                return Result<HealedAddress>.Failed(read.Error!);
            var (revision, pinnedAddresses) = read.Value;
            var merged = new Dictionary<string, NodeAddress>(pinnedAddresses);
            merged[broker] = address;
            var txn = await TxnAsync(
                TxnRequest.Of([CompareKeyUnchanged(key, revision)], [new TxnOp.Put(key, SerializePortAlloc(merged), null)]), ct);
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
                else
                    return Result<HealedAddress>.Failed(new ApplicationException(
                        $"portalloc {cluster} changed under the claim — retry on next tick"));
            }
        }
        finally
        {
            await portLock.ReleaseAsync();
        }

        if (found.AdvertisedClient is { } advertised
            && !advertised.EndsWith($":{found.ClientHostPort.ToString(CultureInfo.InvariantCulture)}", StringComparison.Ordinal))
            await journal.WritePhaseAsync(cluster, Op, "reconstructed", claims.InstanceId,
                $"advertised {advertised} != published :{found.ClientHostPort} — PortBindings is canonical", ct);
        else
            await journal.WritePhaseAsync(cluster, Op, "reconstructed", claims.InstanceId, null, ct);

        return Result<HealedAddress>.Success(new HealedAddress(address, Recreated: false));
    }

    // Ветка 3: новая аллокация (паттерн AddBrokerProcess.EnsurePortsAsync).
    // Под клэймом — ТОЛЬКО узкая секция «чтение занятости → выбор портов →
    // RMW portalloc (mod_revision)» (контракт клэйма arch/15 §4: секция
    // короткая, без keepalive — docker-операции, не укладывающиеся в TTL 15 с,
    // в неё не входят). EnsureNode и RMW endpoints — ПОСЛЕ освобождения:
    // адрес закреплён в portalloc (окно «записан, контейнеров ещё нет» для
    // чужих довыделений закрывает union-чтение занятости, arch/16 §2.1),
    // endpoints строятся из СВЕЖЕГО portalloc — сосед, довыделивший порты
    // за время docker-фазы, из bootstrap-списка не выпадает.
    private async Task<Result<HealedAddress>> ReallocateAsync(
        KafkaClusterSnapshot snap, string broker,
        IReadOnlyDictionary<string, NodeAddress> addresses, CancellationToken ct)
    {
        var cluster = snap.Cluster;

        var acquired = await portLock.TryAcquireAsync(ct);
        if (!acquired.IsSuccess)
            return Result<HealedAddress>.Failed(acquired.Error!);
        if (!acquired.Value)
            return Result<HealedAddress>.Failed(new PortLockBusyException(portLock.Key));

        NodeAddress address;
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

            var plan = PlacementPlanner.Plan(KfwPlanning.Group(cluster, [broker]), hosts.Value);
            var allocated = PortAllocator.Allocate(plan, merged, taken, options.PortFrom, options.PortTo, KfwPlanning.PortsOf, KfwPlanning.HostOf, KfwPlanning.MakeAddress, KfwPlanning.KeyOf);
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
                    $"portalloc {cluster} changed under the claim — retry on next tick"));
            if (!merged.TryGetValue(broker, out var healed))
                return Result<HealedAddress>.Failed(new ApplicationException(
                    $"portalloc healing {cluster}: allocation for {broker} returned no address — retry on next tick"));
            address = healed;
        }
        finally
        {
            await portLock.ReleaseAsync();
        }

        // Контейнер по новому адресу (env — как EnsureNodeAsync надзора) — вне
        // лока; фейл docker-фазы не теряет аллокацию: следующий тик идёт по
        // ветке 1 (pinned), контейнер доезжает пересозданием надзора (§5 C).
        var ensured = await EnsureNodeAsync(snap, broker, address, ct);
        if (!ensured.IsSuccess)
            return Result<HealedAddress>.Failed(ensured.Error!);

        // RMW endpoints: пересборка advertise-адресов всех брокеров из
        // СВЕЖЕГО portalloc (AdvertisedClientHost ?? host:port).
        var endpointsError = await UpdateEndpointsAsync(snap, ct);
        if (endpointsError is not null)
            return Result<HealedAddress>.Failed(endpointsError);

        await journal.WritePhaseAsync(cluster, Op, "reallocated", claims.InstanceId, null, ct);
        return Result<HealedAddress>.Success(new HealedAddress(address, Recreated: true));
    }

    // Пересоздание контейнера брокера по адресу (копия NodeSupervisor.EnsureNodeAsync:
    // дублирование осознанное — healer самодостаточен; state=PROVISIONING пишет
    // вызывающий supervise). Env — канон BrokerEnvBuilder (креды admin/app +
    // per-cluster PKI через кеш сертов, как в надзоре).
    private async Task<Result> EnsureNodeAsync(
        KafkaClusterSnapshot snap, string broker, NodeAddress addr, CancellationToken ct)
    {
        var cluster = snap.Cluster;
        var decl = snap.Brokers.FirstOrDefault(b => b.Name == broker);
        if (decl is null)
            return Result.Failed(new ApplicationException(
                $"healing-portalloc {cluster}: broker {broker} disappeared from the declaration"));

        if (snap.AppUser is null || snap.AppPassword is null)
            return Result.Failed(new ApplicationException(
                $"healing-portalloc {cluster}: no app credentials"));

        var env = BrokerEnvBuilder.Build(
            snap, broker, addr, [snap.AppPassword!], [snap.AdminPassword!], options, certificates);

        return await driver.EnsureNodeAsync(new KafkaNodeSpec(
            cluster, broker, addr.Host, addr.ClientPort, options.NodeImage, env,
            decl.Resources?.Cpu,
            decl.Resources is null ? null : decl.Resources.MemGi * 1024L * 1024 * 1024), ct);
    }

    // RMW /kafka/clusters/<C>/endpoints: advertise-адреса всех брокеров из
    // СВЕЖЕГО portalloc (чтение после освобождения лока — включает закрепления
    // соседей за время docker-фазы, снимок секции был бы устаревшим);
    // ключа нет → put, есть → txn mod_revision (проигрыш — ретрай тиком).
    private async Task<Exception?> UpdateEndpointsAsync(
        KafkaClusterSnapshot snap, CancellationToken ct)
    {
        var read = await ReadPortAllocAsync(snap.Cluster, ct);
        if (!read.IsSuccess)
            return read.Error!;

        var key = EndpointsKey(snap.Cluster);
        var value = string.Join(",", read.Value
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
            : new ApplicationException($"endpoints {key} changed under the claim — retry on next tick");
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

    // Failover-обёртки: первый успешный endpoint выигрывает (общий хелпер).
    private Task<Result<Kv?>> GetAsync(string key, CancellationToken ct)
        => EtcdEndpointFailover.CallAsync(endpoints, (endpoint, token) => etcd.GetAsync(endpoint, key, token), ct);

    private Task<Result> PutAsync(string key, string value, CancellationToken ct)
        => EtcdEndpointFailover.CallAsync(endpoints, (endpoint, token) => etcd.PutAsync(endpoint, key, value, null, token), ct);

    private Task<Result<TxnResult>> TxnAsync(TxnRequest req, CancellationToken ct)
        => EtcdEndpointFailover.CallAsync(endpoints, (endpoint, token) => etcd.TxnAsync(endpoint, req, token), ct);

    // Страховка: клэйм не остаётся удержанным при сбое вызывающего между тиками.
    public async ValueTask DisposeAsync()
    {
        await portLock.ReleaseAsync();
    }
}
