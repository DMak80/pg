using System.Text.Json;
using Shared.Core;
using Shared.Core.Planning;
using Shared.Etcd.Client;
using ValkeyWorker.Core.Model;
using ValkeyWorker.Core.Valkey;
using ValkeyWorker.Core.Writing;
using ValkeyWorker.Docker.Drivers;

namespace ValkeyWorker.Provisioning.Processes;

/// <summary>
/// Provisioning valkey-кластера (arch/21 §5 A, фазы V0–V5): от заявки
/// NOT_INITIALIZED до рабочего кластера. Все фазы идемпотентны и перепроверяют
/// факт; перед V3 и V5 — перечитывание config (гонка TO_REMOVE посреди работы
/// безопасно прекращает процесс). Снапшоты P12 «до» (после claim) и «после»
/// (перед journal done) — через snapshot-делегат. Вызывается только держателем
/// клэйма &lt;C&gt;. Сверка V3 re-run: image + args + порт + лимиты.
/// </summary>
public sealed class ProvisioningProcess(
    IEtcdGateway gateway,
    string[] endpoints,
    IClusterDriver driver,
    ClaimStore claims,
    WorkJournal journal,
    PortAllocLock portLock,
    PortAllocIndex portIndex,
    IClusterSecretEnsurer secrets,
    NodeTlsProvisioner tlsProvisioner,
    IValkeyConnection valkey,
    ValkeyProvisioningOptions options,
    Func<CancellationToken, Task<Result>>? snapshot = null,
    TimeProvider? clock = null) // clock — тестовый бюджет V4 (FixedTimeProvider)
{
    private const string Op = "provision";

    private readonly TimeProvider _clock = clock ?? TimeProvider.System;

    public async Task<Result> TickAsync(ValkeyClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Cluster;

        // Мутации — только держателем живого клэйма (arch/21 §6).
        var claimed = ProcessCommon.EnsureClaimed(claims, cluster, Op);
        if (!claimed.IsSuccess)
            return claimed;

        // V0: journal-before-manipulations + снапшот «до».
        var started = await journal.WritePhaseAsync(cluster, Op, "started", claims.InstanceId, null, ct);
        if (!started.IsSuccess)
            return started;
        if (snapshot is not null)
        {
            var before = await snapshot(ct);
            if (!before.IsSuccess)
                return Fail(cluster, before.Error!, "snapshot-before");
        }

        // V1: план placement + порты под глобальным portalloc-клэймом.
        var plan = await PlanAsync(snap, ct);
        if (!plan.IsSuccess)
        {
            // Клэйм portalloc занят (чужой тик/инстанс) — не фейл: InProgress,
            // следующий тик повторит секцию.
            if (plan.Error is PortLockBusyException)
            {
                var waiting = await journal.WritePhaseAsync(
                    cluster, Op, "waiting-portalloc-lock", claims.InstanceId, null, ct);
                return waiting.IsSuccess ? Result.Success() : waiting;
            }

            return plan;
        }

        var planned = await journal.WritePhaseAsync(cluster, Op, "planned", claims.InstanceId, null, ct);
        if (!planned.IsSuccess)
            return planned;

        // V2: ensure кредов admin+app.
        var creds = await secrets.EnsureAsync(cluster, ct);
        if (!creds.IsSuccess)
            return Fail(cluster, creds.Error!, "ensured-secrets");
        var ensured = await journal.WritePhaseAsync(cluster, Op, "ensured-secrets", claims.InstanceId, null, ct);
        if (!ensured.IsSuccess)
            return ensured;

        // Гонка «панель пишет TO_REMOVE посреди provisioning» — перечитывание перед V3.
        if (await ConfigRemovedAsync(cluster, ct))
            return await AbortAsync(cluster);

        // V3: контейнеры нод (NodeArgsBuilder + лимиты декларации) + state=PROVISIONING.
        var containers = await EnsureContainersAsync(snap, plan.Value.Addresses, creds.Value, ct);
        if (!containers.IsSuccess)
            return Fail(cluster, containers.Error!, "container");
        var statePut = await WriteProvisioningStatesAsync(snap, ct);
        if (!statePut.IsSuccess)
            return statePut;
        var containerPhase = await journal.WritePhaseAsync(cluster, Op, "container", claims.InstanceId, null, ct);
        if (!containerPhase.IsSuccess)
            return containerPhase;

        // V4: готовность — PING с admin-кредом до PONG в бюджет NodeBootSec.
        var boot = await AwaitBootAsync(snap, plan.Value.Addresses, creds.Value, ct);
        if (!boot.IsSuccess)
        {
            // journal-before-return: boot-timeout — диагностика оператору (diag-ключ).
            await journal.WritePhaseAsync(
                cluster, Op, "boot-timeout", claims.InstanceId, boot.Error!.Message, ct);
            return Fail(cluster, boot.Error!, "boot-timeout");
        }
        var runningPut = await WriteRunningStatesAsync(snap, ct);
        if (!runningPut.IsSuccess)
            return runningPut;
        var running = await journal.WritePhaseAsync(cluster, Op, "running", claims.InstanceId, null, ct);
        if (!running.IsSuccess)
            return running;

        // Гонка TO_REMOVE — перечитывание перед V5.
        if (await ConfigRemovedAsync(cluster, ct))
            return await AbortAsync(cluster);

        // V5: endpoints (advertised-правило) + config без state (txn mod_revision).
        var endpointsPut = await PutEndpointsAsync(snap, plan.Value.Addresses, ct);
        if (!endpointsPut.IsSuccess)
            return Fail(cluster, endpointsPut.Error!, "endpoints");
        var configPut = await PutConfigWithoutStateAsync(snap, ct);
        if (!configPut.IsSuccess)
            return Fail(cluster, configPut.Error!, "config");

        if (snapshot is not null)
        {
            var after = await snapshot(ct);
            if (!after.IsSuccess)
                return Fail(cluster, after.Error!, "snapshot-after");
        }

        return await journal.WritePhaseAsync(cluster, Op, "done", claims.InstanceId, null, ct);
    }

    private Result Fail(string cluster, Exception error, string phase)
        => Result.Failed(new ApplicationException($"provision {cluster}: {phase}: {error.Message}", error));

    // Безопасная остановка: TO_REMOVE — владелец демонтажа, наш тик прекращается.
    private async Task<Result> AbortAsync(string cluster)
    {
        var aborted = await journal.WritePhaseAsync(
            cluster, Op, "aborted-state-changed", claims.InstanceId, null, CancellationToken.None);
        return aborted.IsSuccess ? Result.Success() : aborted;
    }

    // ── V1: placement + portalloc ──

    private sealed record Plan(IReadOnlyDictionary<string, NodeAddress> Addresses);

    // arch/20 §3: под глобальным portalloc-клэймом — ВСЯ секция «чтение
    // занятости → выбор портов → запись portalloc»: без него два параллельных
    // кластера читают пустую занятость до первой записи соседа и выбирают
    // одни и те же порты (docker create второго падает, portalloc закреплён
    // за коллизией — вечный failing-тик). Образец — PlanAsync kfw (t91-паттерн):
    // чтение закрепления ДО клэйма + ранний выход «всё закреплено» (тики
    // waiting-* не соперничают за глобальный клэйм); занятость и аллокация —
    // ВНУТРИ секции.
    private async Task<Result<Plan>> PlanAsync(ValkeyClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Cluster;
        var nodes = NodeNames(snap);

        // Существующее закрепление — переиспользуется аллокатором (V3-сверка порта).
        var read = await ReadPortAllocAsync(cluster, ct);
        if (!read.IsSuccess)
            return Result<Plan>.Failed(read.Error!);
        var pinned = read.Value;

        // Ранний выход ДО клэйма: всё закреплено — переиспользование без записи.
        if (nodes.All(pinned.ContainsKey))
        {
            var plannedOnly = await journal.WritePhaseAsync(cluster, Op, "planned", claims.InstanceId, null, ct);
            return plannedOnly.IsSuccess
                ? Result<Plan>.Success(new Plan(pinned))
                : Result<Plan>.Failed(plannedOnly.Error!);
        }

        // Захват глобального клэйма; занят — PortLockBusyException → тик-ретрай.
        var acquired = await portLock.TryAcquireAsync(ct);
        if (!acquired.IsSuccess)
            return Result<Plan>.Failed(acquired.Error!);
        if (!acquired.Value)
            return Result<Plan>.Failed(new PortLockBusyException(portLock.Key));
        try
        {
            // Занятость читается ВНУТРИ клэйма (буква arch/20 §3).
            var hosts = await driver.GetHostsAsync(ct);
            if (!hosts.IsSuccess)
                return Result<Plan>.Failed(hosts.Error!);
            var dockerBusy = await driver.GetBusyPortsAsync(ct);
            if (!dockerBusy.IsSuccess)
                return Result<Plan>.Failed(dockerBusy.Error!);
            var foreign = await portIndex.ForeignAllocatedPortsAsync(cluster, ct);
            if (!foreign.IsSuccess)
                return Result<Plan>.Failed(foreign.Error!);

            // Занятость: docker-публикации ∪ portalloc чужих (порт чужого
            // кластера занят консервативно на каждом известном хосте —
            // host-запись чужого размещения может не совпадать с топологией
            // этого плана).
            var taken = new HashSet<(string Host, int Port)>(dockerBusy.Value);
            foreach (var host in hosts.Value)
            foreach (var port in foreign.Value)
                taken.Add((host.Name, port));

            var plan = PlacementPlanner.Plan(ValkeyPlanning.Group(cluster, nodes), hosts.Value);
            var allocated = PortAllocator.Allocate(
                plan, pinned, taken, options.PortRangeFrom, options.PortRangeTo,
                ValkeyPlanning.PortsOf, ValkeyPlanning.HostOf, ValkeyPlanning.MakeAddress, ValkeyPlanning.KeyOf);
            if (!allocated.IsSuccess)
                return Result<Plan>.Failed(allocated.Error!);

            var merged = new Dictionary<string, NodeAddress>(pinned);
            foreach (var (node, addr) in allocated.Value)
                merged[node] = addr;

            // Закрепление portalloc (переживает пересоздание контейнера).
            var key = ProcessCommon.PortAllocKey(cluster);
            var put = await TxnAsync(
                TxnRequest.Of(
                    [TxnCompare.NotExists(key)],
                    [new TxnOp.Put(key, ProcessCommon.SerializePortAlloc(merged), null)]), ct);
            if (!put.IsSuccess)
                return Result<Plan>.Failed(put.Error!);
            if (!put.Value.Succeeded)
            {
                // Ключ появился между чтением и txn — перечитываем как истину (S5);
                // V3-сверка порта гарантирует сходимость контейнера с записью.
                var reread = await ReadPortAllocAsync(cluster, ct);
                if (!reread.IsSuccess)
                    return Result<Plan>.Failed(reread.Error!);
                foreach (var (node, addr) in reread.Value)
                    merged[node] = addr;
            }

            // journal planned — внутри секции, до release (клэйм короткий).
            var planned = await journal.WritePhaseAsync(cluster, Op, "planned", claims.InstanceId, null, ct);
            return planned.IsSuccess
                ? Result<Plan>.Success(new Plan(merged))
                : Result<Plan>.Failed(planned.Error!);
        }
        finally
        {
            await portLock.ReleaseAsync();
        }
    }

    private static IReadOnlyList<string> NodeNames(ValkeyClusterSnapshot snap)
        => [.. Enumerable.Range(1, Math.Max(1, snap.Config?.Nodes ?? 1)).Select(k => $"node{k}")];

    // ── V3: контейнеры нод ──

    private async Task<Result> EnsureContainersAsync(
        ValkeyClusterSnapshot snap,
        IReadOnlyDictionary<string, NodeAddress> addresses,
        ValkeyCredentials creds,
        CancellationToken ct)
    {
        var cluster = snap.Cluster;
        foreach (var node in NodeNames(snap))
        {
            if (!addresses.TryGetValue(node, out var address))
                return Result.Failed(new ApplicationException($"provision {cluster}: нет адреса ноды {node}"));

            var nodeSnap = snap.Nodes.GetValueOrDefault(node);
            var limits = ProcessCommon.ParseResources(nodeSnap?.Resources);
            var args = NodeArgsBuilder.Build(
                snap.Config?.MaxmemoryBytes ?? 0, snap.Config?.MaxmemoryPolicy ?? "allkeys-lru",
                creds.AdminPassword, creds.AppPassword);

            // V3 TLS (t06, arch/21 §2): серт ноды в volume ДО EnsureNodeAsync —
            // файлы обязаны существовать к старту контейнера (--tls-cert-file).
            var advertised = options.AdvertisedClientHost ?? address.Host;
            var tls = await tlsProvisioner.EnsureNodeTlsAsync(
                cluster, node, address.Host, advertised, creds.CaPem, creds.CaKey, ct);
            if (!tls.IsSuccess)
                return tls;

            // Сверка re-run (V3): image + args + порт + лимиты — полное совпадение → пропуск.
            var existingArgs = await driver.NodeArgsAsync(cluster, node, ct);
            if (!existingArgs.IsSuccess)
                return existingArgs.Error!;
            var existingResources = await driver.NodeResourcesAsync(cluster, node, ct);
            if (!existingResources.IsSuccess)
                return existingResources.Error!;
            var existingEndpoint = await driver.InspectNodeEndpointAsync(cluster, node, ct);
            if (!existingEndpoint.IsSuccess)
                return existingEndpoint.Error!;

            var matches = existingArgs.Value is { } liveArgs
                && liveArgs.SequenceEqual(args)
                && LimitsMatch(existingResources.Value, limits)
                && existingEndpoint.Value is { } liveEndpoint
                && liveEndpoint.ClientHostPort == address.ClientPort;
            if (matches)
                continue;

            // Расхождение (или иные args/лимиты/порт) → пересоздание с каноническими
            // параметрами; объекта нет → сразу создание.
            if (existingArgs.Value is not null || existingEndpoint.Value is not null)
            {
                var removed = await driver.RemoveNodeAsync(cluster, node, ct);
                if (!removed.IsSuccess)
                    return removed;
            }

            var ensured = await driver.EnsureNodeAsync(new ValkeyNodeSpec(
                cluster, node, address.Host, address.ClientPort, options.NodeImage, args,
                limits?.Cpu, limits?.MemBytes,
                TlsVolume: PlainClusterDriver.TlsVolumeName(cluster)), ct);
            if (!ensured.IsSuccess)
                return ensured;
        }

        return Result.Success();
    }

    private static bool LimitsMatch(NodeLimits? live, (decimal? Cpu, long? MemBytes)? declared)
    {
        // Нода без лимитов в декларации — сверка не требуется (нечего сходить).
        if (declared is null)
            return true;
        return NullableDecimalEquals(live?.CpuCores, declared.Value.Cpu)
               && NullableLongEquals(live?.MemoryBytes, declared.Value.MemBytes);
    }

    private static bool NullableDecimalEquals(decimal? a, decimal? b) => (a, b) switch
    {
        (null, null) => true,
        ({ } x, { } y) => x == y,
        _ => false,
    };

    private static bool NullableLongEquals(long? a, long? b) => (a, b) switch
    {
        (null, null) => true,
        ({ } x, { } y) => x == y,
        _ => false,
    };

    private async Task<Result> WriteProvisioningStatesAsync(ValkeyClusterSnapshot snap, CancellationToken ct)
    {
        foreach (var node in NodeNames(snap))
        {
            var put = await ProcessCommon.WriteNodeStateAsync(
                gateway, endpoints, snap.Cluster, node, "PROVISIONING", ct);
            if (!put.IsSuccess)
                return put;
        }

        return Result.Success();
    }

    private async Task<Result> WriteRunningStatesAsync(ValkeyClusterSnapshot snap, CancellationToken ct)
    {
        foreach (var node in NodeNames(snap))
        {
            var put = await ProcessCommon.WriteNodeStateAsync(
                gateway, endpoints, snap.Cluster, node, "RUNNING", ct);
            if (!put.IsSuccess)
                return put;
        }

        return Result.Success();
    }

    // ── V4: готовность ──

    private async Task<Result> AwaitBootAsync(
        ValkeyClusterSnapshot snap,
        IReadOnlyDictionary<string, NodeAddress> addresses,
        ValkeyCredentials creds,
        CancellationToken ct)
    {
        foreach (var node in NodeNames(snap))
        {
            var address = addresses[node];
            // Проба по advertised-адресу (правило arch/21 §2) — тот же хост,
            // что попадёт в endpoints (V5); placement-имя клиентам не видно.
            // PING — по TLS с CA из ensure (t06, V4).
            var endpoint = new ValkeyEndpoint(
                options.AdvertisedClientHost ?? address.Host, address.ClientPort,
                "admin", creds.AdminPassword, creds.CaPem);
            var startedAt = _clock.GetUtcNow();
            var budget = TimeSpan.FromSeconds(options.NodeBootSec);

            // Транзиент-толерантный цикл: ошибка пробы не прерывает — только бюджет.
            while (true)
            {
                var ping = await valkey.PingAsync(endpoint, ct);
                if (ping.IsSuccess)
                    break;
                if (_clock.GetUtcNow() - startedAt > budget)
                    return Result.Failed(new TimeoutException(
                        $"provision {snap.Cluster}/{node}: нода не отвечает {budget.TotalSeconds:F0} c " +
                        $"({ping.Error!.Message})"));
                await Task.Delay(100, ct);
            }
        }

        return Result.Success();
    }

    // ── V5: дискавери + config ──

    private async Task<Result> PutEndpointsAsync(
        ValkeyClusterSnapshot snap, IReadOnlyDictionary<string, NodeAddress> addresses, CancellationToken ct)
    {
        var value = string.Join(",", NodeNames(snap)
            .Select(n => addresses[n])
            .Select(a => ValkeyWriting.EndpointsValue(options.AdvertisedClientHost ?? a.Host, a.ClientPort)));
        return await PutWithFailoverAsync(ProcessCommon.EndpointsKey(snap.Cluster), value, ct);
    }

    // Config БЕЗ state (V5, txn compare mod_revision прочитанного config).
    private async Task<Result> PutConfigWithoutStateAsync(ValkeyClusterSnapshot snap, CancellationToken ct)
    {
        var key = ProcessCommon.ConfigKey(snap.Cluster);
        var current = await GetWithFailoverAsync(key, ct);
        if (!current.IsSuccess)
            return current.Error!;

        var value = ValkeyWriting.ConfigJson(
            snap.Config?.Nodes ?? 1,
            snap.Config?.MaxmemoryBytes ?? 0,
            snap.Config?.MaxmemoryPolicy ?? "allkeys-lru",
            snap.Config?.CreatedUnix ?? DateTimeOffset.UtcNow.ToUnixTimeSeconds());

        var compare = current.Value is { } kv
            ? TxnCompare.ModRevisionEqual(key, (long)kv.ModRevision)
            : TxnCompare.NotExists(key);
        var txn = await TxnAsync(TxnRequest.Of([compare], [new TxnOp.Put(key, value, null)]), ct);
        if (!txn.IsSuccess)
            return txn.Error!;
        return txn.Value.Succeeded
            ? Result.Success()
            : Result.Failed(new ApplicationException(
                $"{ProcessCommon.ConfigKey(snap.Cluster)} изменился под нами — ретрай тиком"));
    }

    // state сменился на TO_REMOVE (гонка с панелью).
    private async Task<bool> ConfigRemovedAsync(string cluster, CancellationToken ct)
    {
        var current = await GetWithFailoverAsync(ProcessCommon.ConfigKey(cluster), ct);
        if (!current.IsSuccess || current.Value is not { } kv)
            return false;
        return ReadConfigState(kv.Value) == "TO_REMOVE";
    }

    private static string? ReadConfigState(string json)
    {
        try
        {
            using var doc = JsonDocument.Parse(json);
            return doc.RootElement.ValueKind == JsonValueKind.Object
                   && doc.RootElement.TryGetProperty("state", out var state)
                   && state.ValueKind == JsonValueKind.String
                ? state.GetString()
                : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    // ── etcd-примитивы (failover по endpoints) ──

    internal static Task<Result<Kv?>> GetWithFailoverAsync(
        IEtcdGateway gateway, string[] endpoints, string key, CancellationToken ct)
    {
        return WithFailoverAsync(endpoints, endpoint => gateway.GetAsync(endpoint, key, ct));
    }

    private Task<Result<Kv?>> GetWithFailoverAsync(string key, CancellationToken ct)
        => GetWithFailoverAsync(gateway, endpoints, key, ct);

    // Отдельный цикл для non-generic Result (Result<T>-хелпер его не покрывает).
    private async Task<Result> PutWithFailoverAsync(string key, string value, CancellationToken ct)
    {
        Result? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await gateway.PutAsync(endpoint, key, value, null, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }

    private Task<Result<TxnResult>> TxnAsync(TxnRequest req, CancellationToken ct)
        => WithFailoverAsync(endpoints, endpoint => gateway.TxnAsync(endpoint, req, ct));

    internal static async Task<Result<T>> WithFailoverAsync<T>(
        string[] endpoints, Func<string, Task<Result<T>>> call)
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

    private async Task<Result<IReadOnlyDictionary<string, NodeAddress>>> ReadPortAllocAsync(
        string cluster, CancellationToken ct)
    {
        var result = await GetWithFailoverAsync(ProcessCommon.PortAllocKey(cluster), ct);
        if (!result.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(result.Error!);
        if (result.Value is not { } kv)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(
                (IReadOnlyDictionary<string, NodeAddress>)new Dictionary<string, NodeAddress>());
        return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(
            ProcessCommon.ParsePortAlloc(kv.Value));
    }
}

/// <summary>Valkey-инстанс обобщённых планировщиков (порт KfwPlanning): одна
/// группа на кластер, адрес = один client-порт, ключ результата — имя ноды.</summary>
public static class ValkeyPlanning
{
    public static IReadOnlyList<NodeGroup> Group(string cluster, IReadOnlyList<string> nodes)
        => [new(cluster, nodes)];

    public static IReadOnlyList<int> PortsOf(NodeAddress a) => [a.ClientPort];

    public static string HostOf(NodeAddress a) => a.Host;

    public static NodeAddress MakeAddress(string host, int port) => new(host, port);

    public static string KeyOf(NodePlacement p) => p.Node;
}
