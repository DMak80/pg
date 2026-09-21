using System.Text.Json;
using Shared.Core;
using Shared.Etcd.Client;
using ValkeyWorker.Core.Model;
// t07: NodeLimits здесь — доменная запись; docker-факт движка (Shared.Docker)
// конвертируется драйвером — Shared-имя сюда не пробрасывается.
using NodeLimits = ValkeyWorker.Core.Model.NodeLimits;
using ValkeyWorker.Core.Valkey;
using ValkeyWorker.Core.Writing;
using ValkeyWorker.Docker.Drivers;

namespace ValkeyWorker.Provisioning.Processes;

/// <summary>
/// Надзор (arch/21 §5 C): сверка декларации с docker-фактом + PING-проба
/// admin-кредом. Снесённый контейнер (положительное свидетельство) и
/// расхождение лимитов (автоконверге) → пересоздание, state=PROVISIONING;
/// молчание дольше NodeDeadSec → UNREACHABLE + пересоздание (трек first_seen
/// в work/&lt;C&gt; через WriteSupervisionAsync, стартует по УСПЕШНОМУ
/// зондированию). Зрячесть пробы — по docker-факту: inspect прочитан → ЛЮБОЙ
/// отказ пробы (таймаут, refused/reset, Running=false) = молчание ноды;
/// слепой inspect (docker-хост молчит) — ошибка тика, пересозданий вслепую
/// нет (S7). Одно пересоздание за тик; кеш-ключи домена не чистятся никогда;
/// ноды TO_REMOVE/REMOVING/PROVISIONING чужих процессов не трогаются;
/// лестница E9 до деструктива; endpoints сходится к portalloc-канону (RMW).
/// </summary>
public sealed class NodeSupervisor(
    IEtcdGateway gateway,
    string[] endpoints,
    IClusterDriver driver,
    ClaimStore claims,
    WorkJournal journal,
    IValkeyConnection valkey,
    ValkeyProvisioningOptions options,
    PortAllocHealer healer,
    NodeTlsProvisioner tlsProvisioner,
    TimeProvider? clock = null) // clock — тестовый порог NodeDeadSec (FixedTimeProvider)
{
    private const string Op = "supervise";

    private readonly TimeProvider _clock = clock ?? TimeProvider.System;

    private static readonly string[] ForeignStates = ["TO_REMOVE", "REMOVING", "PROVISIONING"];

    public async Task<Result> TickAsync(ValkeyClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Cluster;

        var claimed = ProcessCommon.EnsureClaimed(claims, cluster, Op);
        if (!claimed.IsSuccess)
            return claimed;

        // Креды/декларация: без них args не собрать — пересоздания невозможны;
        // ca_pem/ca_key (t06) обязательны — пробы по TLS, пересоздание без сертов
        // не создаётся (миграция T доиграет — warning надзора).
        if (snap.AdminPassword is null || snap.AppPassword is null || snap.CaPem is null || snap.CaKey is null)
        {
            var existingTrack = await journal.ReadUnreachableAsync(cluster, ct);
            var skip = await journal.WriteSupervisionAsync(
                cluster, claims.InstanceId, existingTrack.Value ?? new Dictionary<string, long>(),
                "нет ACL-кредов или CA в etcd — пересоздание отложено (миграция T), пробы пропущены", ct);
            return skip.IsSuccess ? Result.Success() : skip;
        }

        var recreated = false;
        var warnings = new List<string>();
        var unreachable = new Dictionary<string, long>(
            (await journal.ReadUnreachableAsync(cluster, ct)).Value ?? new Dictionary<string, long>());

        foreach (var node in snap.Nodes.Keys.OrderBy(n => n, StringComparer.Ordinal))
        {
            var state = await ProcessCommon.ReadNodeStateAsync(gateway, endpoints, cluster, node, ct);
            if (!state.IsSuccess)
                return state.Error!;
            var stateValue = state.Value;

            // Ноды демонтажа — не трогаем никогда. PROVISIONING чужих процессов
            // (provisioning A) не пересоздаётся и лимитами не сверяется, но
            // надзор переводит её в RUNNING по зрячей пробе (arch/21 §5 C).
            var isRemoving = stateValue is "TO_REMOVE" or "REMOVING";
            var isForeignProvisioning = stateValue == "PROVISIONING";
            if (isRemoving)
                continue;
            var supervisable = !isForeignProvisioning;

            // Лестница E9 ДО деструктива: адрес ноды из portalloc (или реконструкция).
            var address = await ResolveAddressAsync(cluster, node, ct);
            if (!address.IsSuccess)
                return address.Error!;

            // Docker-факт: объект есть? Слепой inspect — ошибка тика (пересозданий вслепую нет).
            var live = await driver.InspectNodeEndpointAsync(cluster, node, ct);
            if (!live.IsSuccess)
                return live.Error!;

            if (live.Value is null)
            {
                // Снесённый контейнер (положительное свидетельство) → пересоздание.
                if (!supervisable)
                    continue; // PROVISIONING чужого процесса — чужая ответственность
                if (recreated)
                    continue; // одно пересоздание за тик
                var rebuilt = await RecreateAsync(snap, node, address.Value, "container missing", ct);
                if (!rebuilt.IsSuccess)
                    return rebuilt.Error!;
                recreated = true;
                unreachable.Remove(node);
                continue;
            }

            // Автоконверге лимитов: inspect (cpu/mem) vs resources декларации;
            // disk — инфо-поле, не сверяется. Слепой inspect — ошибка тика.
            if (supervisable)
            {
                var liveLimits = await driver.NodeResourcesAsync(cluster, node, ct);
                if (!liveLimits.IsSuccess)
                    return liveLimits.Error!;
                var declared = ProcessCommon.ParseResources(snap.Nodes[node].Resources);
                if (!LimitsMatch(liveLimits.Value, declared))
                {
                    if (!recreated)
                    {
                        warnings.Add($"лимиты контейнера {node} расходятся с декларацией — пересоздание");
                        var rebuilt = await RecreateAsync(snap, node, address.Value, "limits drift", ct);
                        if (!rebuilt.IsSuccess)
                            return rebuilt.Error!;
                        recreated = true;
                        unreachable.Remove(node);
                        continue;
                    }

                    warnings.Add($"лимиты {node} расходятся — отложено до следующего тика");
                    continue;
                }
            }

            // PING-проба admin-кредом по advertised-адресу (не published), по TLS (t06).
            var ping = await valkey.PingAsync(
                new ValkeyEndpoint(
                    options.AdvertisedClientHost ?? address.Value.Host, address.Value.ClientPort,
                    "admin", snap.AdminPassword!, snap.CaPem), ct);
            if (ping.IsSuccess)
            {
                // Успешное зондирование — счётчик молчания ноды стартует заново.
                unreachable.Remove(node);
                if (stateValue != "RUNNING")
                {
                    var running = await ProcessCommon.WriteNodeStateAsync(
                        gateway, endpoints, cluster, node, "RUNNING", ct);
                    if (!running.IsSuccess)
                        return running;
                }

                continue;
            }

            // Docker-факт прочитан (inspect успешен выше) — проба зрячая: ЛЮБОЙ
            // отказ пробы (таймаут, connection refused/reset — типично после
            // docker stop; остановленный контейнер — Running=false при живом
            // объекте) = положительное свидетельство молчания ноды. Слепота
            // воркера — только недоступность docker-факта (слепой inspect —
            // ошибка тика выше): вслепую ноду не трогаем (S7).

            // Нода молчит: трек first_seen — только для supervisable (PROVISIONING
            // грузится — бюджет молчания надзора не стартует, трек заморожен).
            if (!supervisable)
            {
                warnings.Add($"PROVISIONING {node} молчит — трек надзора заморожен");
                continue;
            }

            var firstSeen = unreachable.TryGetValue(node, out var seen) ? seen : NowUnix();
            unreachable[node] = firstSeen;
            if (_clock.GetUtcNow().ToUnixTimeSeconds() - firstSeen > options.NodeDeadSec)
            {
                var dead = await ProcessCommon.WriteNodeStateAsync(
                    gateway, endpoints, cluster, node, "UNREACHABLE", ct);
                if (!dead.IsSuccess)
                    return dead;

                if (!recreated)
                {
                    var rebuilt = await RecreateAsync(snap, node, address.Value, "unreachable", ct);
                    if (!rebuilt.IsSuccess)
                        return rebuilt.Error!;
                    recreated = true;
                    unreachable.Remove(node); // пересоздание — счётчик заново
                }
            }
        }

        // endpoints сходится к portalloc-канону (RMW).
        var converge = await ConvergeEndpointsAsync(snap, ct);
        if (!converge.IsSuccess)
            return converge;

        // Стационарная запись надзора (трек first_seen + warning-и тика).
        return await journal.WriteSupervisionAsync(
            cluster, claims.InstanceId, unreachable,
            warnings.Count > 0 ? string.Join("; ", warnings) : null, ct);
    }

    private long NowUnix() => _clock.GetUtcNow().ToUnixTimeSeconds();

    private static bool LimitsMatch(NodeLimits? live, (decimal? Cpu, long? MemBytes)? declared)
    {
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

    // Адрес ноды: portalloc-запись; нет записи — лестница E9 (реконструкция из
    // inspect живого контейнера, put-if-absent под locks/portalloc).
    private async Task<Result<NodeAddress>> ResolveAddressAsync(string cluster, string node, CancellationToken ct)
    {
        var current = await GetAsync(ProcessCommon.PortAllocKey(cluster), ct);
        if (!current.IsSuccess)
            return Result<NodeAddress>.Failed(current.Error!);
        if (current.Value is { } kv)
        {
            try
            {
                using var doc = JsonDocument.Parse(kv.Value);
                if (doc.RootElement.ValueKind == JsonValueKind.Object
                    && doc.RootElement.TryGetProperty(node, out var entry))
                {
                    return Result<NodeAddress>.Success(new NodeAddress(
                        entry.GetProperty("host").GetString()!,
                        entry.GetProperty("client").GetInt32()));
                }
            }
            catch (JsonException ex)
            {
                return Result<NodeAddress>.Failed(ex);
            }
        }

        var healed = await healer.HealNodePortAsync(cluster, node, ct);
        if (!healed.IsSuccess)
            return Result<NodeAddress>.Failed(healed.Error!);

        // После реконструкции хост — из inspect; перечитываем запись (S5).
        var reread = await GetAsync(ProcessCommon.PortAllocKey(cluster), ct);
        if (!reread.IsSuccess)
            return Result<NodeAddress>.Failed(reread.Error!);
        if (reread.Value is not { } fresh)
            return Result<NodeAddress>.Failed(new ApplicationException(
                $"supervise {cluster}/{node}: порт реконструирован, записи portalloc нет"));

        using var doc2 = JsonDocument.Parse(fresh.Value);
        var nodeEntry = doc2.RootElement.GetProperty(node);
        return Result<NodeAddress>.Success(new NodeAddress(
            nodeEntry.GetProperty("host").GetString()!,
            nodeEntry.GetProperty("client").GetInt32()));
    }

    // Пересоздание ноды (те же креды/порт/лимиты — args из etcd-актуального).
    private async Task<Result> RecreateAsync(
        ValkeyClusterSnapshot snap, string node, NodeAddress address, string reason, CancellationToken ct)
    {
        var cluster = snap.Cluster;
        var nodeSnap = snap.Nodes.GetValueOrDefault(node);
        var limits = ProcessCommon.ParseResources(nodeSnap?.Resources);
        var args = NodeArgsBuilder.Build(
            snap.Config?.MaxmemoryBytes ?? 0, snap.Config?.MaxmemoryPolicy ?? "allkeys-lru",
            snap.AdminPassword!, snap.AppPassword!);

        // TLS (t06, arch/21 §5 C): серт в volume ДО EnsureNodeAsync; volume жив и
        // валиден — переиспользование, иначе перевыпуск (кеш восполним).
        var advertised = options.AdvertisedClientHost ?? address.Host;
        var tls = await tlsProvisioner.EnsureNodeTlsAsync(
            cluster, node, address.Host, advertised, snap.CaPem!, snap.CaKey!, ct);
        if (!tls.IsSuccess)
            return tls;

        // Контейнер мог остаться живым (UNREACHABLE/лимиты) — сначала снос.
        var removed = await driver.RemoveNodeAsync(cluster, node, ct);
        if (!removed.IsSuccess)
            return removed;

        var ensured = await driver.EnsureNodeAsync(new ValkeyNodeSpec(
            cluster, node, address.Host, address.ClientPort, options.NodeImage, args,
            limits?.Cpu, limits?.MemBytes,
            TlsVolume: PlainClusterDriver.TlsVolumeName(cluster)), ct);
        if (!ensured.IsSuccess)
            return ensured;

        var state = await ProcessCommon.WriteNodeStateAsync(
            gateway, endpoints, cluster, node, "PROVISIONING", ct);
        if (!state.IsSuccess)
            return state;
        return await journal.WritePhaseAsync(cluster, Op, $"recreated-{reason}", claims.InstanceId, null, ct);
    }

    // endpoints → канон portalloc (advertised-хост + клиентский порт), RMW.
    private async Task<Result> ConvergeEndpointsAsync(ValkeyClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Cluster;
        var canon = await ReadPortAllocAsync(cluster, ct);
        if (!canon.IsSuccess)
            return canon.Error!;
        if (canon.Value.Count == 0)
            return Result.Success(); // порт-план не сформирован — сходить не к чему

        var value = string.Join(",", canon.Value
            .OrderBy(p => p.Key, StringComparer.Ordinal)
            .Select(p => ValkeyWriting.EndpointsValue(
                options.AdvertisedClientHost ?? p.Value.Host, p.Value.ClientPort)));

        var key = ProcessCommon.EndpointsKey(cluster);
        var current = await GetAsync(key, ct);
        if (!current.IsSuccess)
            return current.Error!;
        if (current.Value is { } kv && kv.Value == value)
            return Result.Success(); // уже канон

        var compare = current.Value is { } existing
            ? TxnCompare.ModRevisionEqual(key, (long)existing.ModRevision)
            : TxnCompare.NotExists(key);
        var txn = await TxnAsync(TxnRequest.Of([compare], [new TxnOp.Put(key, value, null)]), ct);
        if (!txn.IsSuccess)
            return txn.Error!;
        return txn.Value.Succeeded
            ? Result.Success()
            : Result.Failed(new ApplicationException(
                $"{key} изменился под нами — ретрай тиком"));
    }

    private async Task<Result<IReadOnlyDictionary<string, NodeAddress>>> ReadPortAllocAsync(
        string cluster, CancellationToken ct)
    {
        var result = await GetAsync(ProcessCommon.PortAllocKey(cluster), ct);
        if (!result.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(result.Error!);
        if (result.Value is not { } kv)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(
                (IReadOnlyDictionary<string, NodeAddress>)new Dictionary<string, NodeAddress>());
        return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(
            ProcessCommon.ParsePortAlloc(kv.Value));
    }

    private Task<Result<Kv?>> GetAsync(string key, CancellationToken ct)
        => ProvisioningProcess.GetWithFailoverAsync(gateway, endpoints, key, ct);

    private async Task<Result<TxnResult>> TxnAsync(TxnRequest req, CancellationToken ct)
    {
        Result<TxnResult>? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await gateway.TxnAsync(endpoint, req, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }
}
