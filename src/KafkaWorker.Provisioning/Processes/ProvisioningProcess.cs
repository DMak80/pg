using System.Collections.Concurrent;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using KafkaWorker.Core.Planning;
using KafkaWorker.Core.Templates;
using KafkaWorker.Docker.Drivers;
using KafkaWorker.Etcd.Client;
using KafkaWorker.Etcd.Coordination;
using KafkaWorker.Etcd.Parsing;
using KafkaWorker.Provisioning.Kafka;

namespace KafkaWorker.Provisioning.Processes;

/// <summary>
/// Provisioning KRaft-кластера (arch/16 §5 A, фазы K0–K6): от заявки
/// NOT_INITIALIZED до рабочего кластера. Все фазы идемпотентны и перепроверяют
/// факт; перед фазами — перечитывание config (R6: TO_REMOVE посреди работы
/// безопасно прекращает процесс). Снапшоты P12 «до» (после claim) и «после»
/// (перед journal done) — порт P12 PgWorker через snapshot-делегат.
/// Вызывается только держателем клэйма &lt;C&gt;.
/// </summary>
public sealed class ProvisioningProcess(
    IEtcdGateway etcd,
    string[] endpoints,
    IClusterDriver driver,
    ClaimStore claims,
    WorkJournal journal,
    PortAllocLock portLock,
    PortAllocIndex portAlloc,
    IAppSecretEnsurer appSecret,
    IKafkaAdminClientFactory adminFactory,
    IClusterConfigConverger converger,
    ProvisioningOptions options,
    Func<CancellationToken, Task<Result>>? snapshot = null)
{
    private const string Op = "provision";

    private static readonly JsonSerializerOptions CanonicalJson = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    // Время первого наблюдения «кластер не отвечает DescribeCluster» (бюджет
    // BrokerBootSec; после takeover отсчёт начинается заново — диагностический
    // бюджет, не клэйм). ConcurrentDictionary: процесс — DI-синглтон, кластеры
    // обрабатываются параллельно.
    private readonly ConcurrentDictionary<string, long> _bootWaitSince = new();

    public async Task<Result> RunAsync(KafkaClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Cluster;

        // Мутации — только держателем живого клэйма (arch/16 §5).
        if (!claims.IsMine(cluster))
            return Result.Failed(new ApplicationException(
                $"provisioning {cluster}: клэйм не наш (или потерян) — мутации запрещены"));

        // Снапшот P12 «до» (после claim — порт P12: точка изменения).
        if (snapshot is not null)
        {
            var before = await snapshot(ct);
            if (!before.IsSuccess)
                return Fail(cluster, before.Error!, "snapshot-before");
        }

        // K0: journal-before-manipulations.
        var started = await journal.WriteAsync(cluster, Op, "started", claims.InstanceId, null, ct);
        if (!started.IsSuccess)
            return started;

        // K1: план placement + порты, закрепление portalloc, фиксация ролей.
        var planned = await PlanAsync(snap, ct);
        if (!planned.IsSuccess)
        {
            // t91: клэйм занят — не фейл, InProgress (следующий тик ~5 с);
            // сбой захвата — обычный фейл (бэкофф).
            if (planned.Error is PortLockBusyException)
                return await FinishAsync(cluster, "waiting-portalloc-lock", ct);
            return Fail(cluster, planned.Error!, "planning");
        }
        var addresses = planned.Value;
        var roles = RolesFor(snap.Config.Brokers);

        var secret = await appSecret.EnsureAsync(cluster, ct);
        if (!secret.IsSuccess)
            return Fail(cluster, secret.Error!, "ensure-app-secret");

        // K3: создать контейнеры брокеров (state=PROVISIONING; существующие — сверка).
        var ensured = await EnsureNodesAsync(snap, addresses, roles, secret.Value, ct);
        if (!ensured.IsSuccess)
            return Fail(cluster, ensured.Error!, "ensure-nodes");

        // R6: панель перевела в TO_REMOVE посреди provisioning — безопасный выход.
        if (await IsRemovedAsync(cluster, ct))
            return await FinishAsync(cluster, "aborted", ct);

        // K4: ждать готовности (DescribeCluster: брокеров = B, контроллер избран).
        var endpoints = BuildEndpoints(snap, addresses);
        var ready = await WaitReadyAsync(snap, endpoints, secret.Value, ct);
        if (!ready.IsSuccess)
            return Fail(cluster, ready.Error!, "waiting-brokers");
        if (!ready.Value)
            return await FinishAsync(cluster, "waiting-brokers", ct); // InProgress — следующий тик

        // K5: стартовый converge + endpoints + config без state.
        if (await IsRemovedAsync(cluster, ct))
            return await FinishAsync(cluster, "aborted", ct);

        var converged = await converger.ApplyAsync(cluster, endpoints, secret.Value.User, secret.Value.Password, snap.Config, ct);
        if (!converged.IsSuccess)
            return Fail(cluster, converged.Error!, "converge-configs");

        var endpointsPut = await PutAsync(EndpointsKey(cluster), endpoints, ct);
        if (!endpointsPut.IsSuccess)
            return Fail(cluster, endpointsPut.Error!, "put-endpoints");

        var committed = await CommitConfigAsync(snap, ct);
        if (!committed.IsSuccess)
            return Fail(cluster, committed.Error!, "committing-config");

        // K6: снапшот P12 «после» (перед journal done).
        if (snapshot is not null)
        {
            var after = await snapshot(ct);
            if (!after.IsSuccess)
                return Fail(cluster, after.Error!, "snapshot-after");
        }

        return await FinishAsync(cluster, "done", ct);
    }

    // Роли KRaft: broker1..broker_m — controller (m=min(3,B)), остальные broker.
    private static IReadOnlyDictionary<string, string> RolesFor(int brokers)
    {
        var controllers = Math.Min(3, brokers);
        var roles = new Dictionary<string, string>();
        for (var k = 1; k <= brokers; k++)
            roles[$"broker{k}"] = k <= controllers ? "controller" : "broker";
        return roles;
    }

    // K1: placement → порт-аллокация → закрепление /kafkaworker/portalloc/<C>
    // (txn compare version==0; конкурент закрепил первым → берём его) + роли.
    // t91 (arch/16 §2.1): довыделение портов — под глобальным клэймом
    // /kafkaworker/locks/portalloc: без него два параллельно сеемых кластера
    // читают занятость до первой записи соседа и выбирают одинаковые порты.
    // Занятость = docker-публикации ∪ portalloc ЧУЖИХ кластеров (свой —
    // закрепление, переиспользуется аллокатором); «не взял» — не ошибка:
    // waiting-portalloc-lock, следующий тик повторяет.
    private async Task<Result<IReadOnlyDictionary<string, NodeAddress>>> PlanAsync(
        KafkaClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Cluster;
        var pinned = await ReadPortAllocAsync(cluster, ct);
        if (!pinned.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(pinned.Error!);
        var existing = new Dictionary<string, NodeAddress>(pinned.Value);

        var wanted = snap.Brokers.Select(b => b.Name).ToList();

        // Ранний пред-выход ДО клэйма (порт t90): всё закреплено —
        // переиспользование без записи; тики waiting-brokers (K4) не
        // соперничают за глобальный клэйм.
        if (wanted.All(existing.ContainsKey))
            return await PlannedAsync(existing, cluster, ct);

        // t91: захват глобального клэйма; сбой — обычный фейл (бэкофф),
        // занят — PortLockBusyException → тик-ретрай.
        var acquired = await portLock.TryAcquireAsync(ct);
        if (!acquired.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(acquired.Error!);
        if (!acquired.Value)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(new PortLockBusyException());
        try
        {
            var hosts = await driver.GetHostsAsync(ct);
            if (!hosts.IsSuccess)
                return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(hosts.Error!);
            var dockerBusy = await driver.GetBusyPortsAsync(ct);
            if (!dockerBusy.IsSuccess)
                return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(dockerBusy.Error!);
            var foreign = await portAlloc.ReadBusyAsync(cluster, ct);
            if (!foreign.IsSuccess)
                return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(foreign.Error!);
            var busy = new HashSet<(string Host, int Port)>(foreign.Value);
            foreach (var p in dockerBusy.Value)
                busy.Add(p);

            var plan = PlacementPlanner.Plan(wanted, hosts.Value);
            var allocated = PortAllocator.Allocate(plan, existing, busy, options.PortFrom, options.PortTo);
            if (!allocated.IsSuccess)
                return allocated;

            foreach (var (node, addr) in allocated.Value)
                existing[node] = addr;

            // Создание ключа — только если нет (compare version==0); проигрыш → re-read.
            var key = PortAllocKey(cluster);
            var serialized = SerializePortAlloc(existing);
            var txn = await TxnAsync(
                TxnRequest.Of([TxnCompare.NotExists(key)], [new TxnOp.Put(key, serialized, null)]), ct);
            if (!txn.IsSuccess)
                return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(txn.Error!);
            if (!txn.Value.Succeeded)
            {
                var reread = await ReadPortAllocAsync(cluster, ct);
                if (!reread.IsSuccess)
                    return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(reread.Error!);
                existing = new Dictionary<string, NodeAddress>(reread.Value);
            }

            // Фиксация ролей (put только при отличии; роль навсегда, arch/15 §2) —
            // ВСЕГДА, независимо от исхода txn (порт семантики исходного кода:
            // ключ мог быть записан до t91 без ролей). Внутри секции, до release.
            foreach (var broker in snap.Brokers)
            {
                if (RolesFor(snap.Config.Brokers).GetValueOrDefault(broker.Name) is { } role && broker.Role != role)
                {
                    var put = await PutAsync(RoleKey(cluster, broker.Name), role, ct);
                    if (!put.IsSuccess)
                        return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(put.Error!);
                }
            }

            return await PlannedAsync(existing, cluster, ct);
        }
        finally
        {
            await portLock.ReleaseAsync();
        }
    }

    // Журнал planned + результат секции (порт PlannedAsync t90 — внутри try,
    // до release; клэйм короткий).
    private async Task<Result<IReadOnlyDictionary<string, NodeAddress>>> PlannedAsync(
        Dictionary<string, NodeAddress> existing, string cluster, CancellationToken ct)
    {
        var planned = await journal.WriteAsync(cluster, Op, "planned", claims.InstanceId, null, ct);
        return planned.IsSuccess
            ? Result<IReadOnlyDictionary<string, NodeAddress>>.Success(existing)
            : Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(planned.Error!);
    }

    // K3: EnsureNode всех брокеров (state != RUNNING) + state=PROVISIONING.
    private async Task<Result> EnsureNodesAsync(
        KafkaClusterSnapshot snap,
        IReadOnlyDictionary<string, NodeAddress> addresses,
        IReadOnlyDictionary<string, string> roles,
        KafkaSecrets secret,
        CancellationToken ct)
    {
        var cluster = snap.Cluster;
        var controllers = snap.Brokers
            .Where(b => roles.GetValueOrDefault(b.Name) == "controller")
            .OrderBy(b => b.Name, StringComparer.Ordinal)
            .Select(b => $"{NodeId(b.Name)}@{b.Name}:9093")
            .ToList();

        foreach (var broker in snap.Brokers.OrderBy(b => b.Name, StringComparer.Ordinal))
        {
            if (broker.State == "RUNNING")
                continue; // идемпотентность: поднята ранее — не трогаем

            if (broker.State != "PROVISIONING")
            {
                var marked = await PutAsync(BrokerStateKey(cluster, broker.Name), "PROVISIONING", ct);
                if (!marked.IsSuccess)
                    return marked;
            }

            var addr = addresses[broker.Name];
            var advertisedClient = $"{options.AdvertisedClientHost ?? addr.Host}:{addr.ClientPort}";
            var env = NodeEnvBuilder.Build(new NodeEnvSpec(
                cluster,
                NodeId(broker.Name),
                broker.Name,
                advertisedClient,
                roles.GetValueOrDefault(broker.Name) == "controller",
                controllers,
                secret.User,
                [secret.Password],
                snap.Config,
                snap.Config.Brokers,
                "/var/lib/kafka/data"));

            var ensured = await driver.EnsureNodeAsync(new KafkaNodeSpec(
                cluster, broker.Name, addr.Host, addr.ClientPort, options.NodeImage, env,
                broker.Resources?.Cpu, broker.Resources is null ? null : broker.Resources.MemGi * 1024L * 1024 * 1024), ct);
            if (!ensured.IsSuccess)
                return ensured;
        }

        return Result.Success();
    }

    // K4: DescribeCluster отвечает, контроллер избран, брокеров = B → RUNNING;
    // не готово — InProgress до бюджета BrokerBootSec (транзиент-толерантно).
    private async Task<Result<bool>> WaitReadyAsync(
        KafkaClusterSnapshot snap, string endpoints, KafkaSecrets secret, CancellationToken ct)
    {
        var cluster = snap.Cluster;
        await using var admin = adminFactory.Create(endpoints, secret.User, secret.Password);
        var view = await admin.DescribeClusterAsync(ct);
        var ready = view.IsSuccess
            && view.Value.ControllerId is not null
            && view.Value.Brokers.Count == snap.Config.Brokers;

        if (ready)
        {
            _bootWaitSince.TryRemove(cluster, out _);
            foreach (var broker in snap.Brokers.Where(b => b.State != "RUNNING"))
            {
                var running = await PutAsync(BrokerStateKey(cluster, broker.Name), "RUNNING", ct);
                if (!running.IsSuccess)
                    return Result<bool>.Failed(running.Error!);
            }

            return Result<bool>.Success(true);
        }

        // Не готов: бюджет с первого наблюдения (диагностика, не клэйм).
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var since = _bootWaitSince.GetOrAdd(cluster, now);
        var reason = view.IsSuccess
            ? $"брокеров в кластере {view.Value.Brokers.Count} из {snap.Config.Brokers}"
            : view.Error!.Message;

        if (options.BrokerBootSec <= 0 || now - since > options.BrokerBootSec)
            return Result<bool>.Failed(new ApplicationException(
                $"кластер {cluster} не собрался за бюджет {options.BrokerBootSec} с: {reason}"));

        return Result<bool>.Success(false);
    }

    // endpoints: advertised host:clientPort по нодам через запятую (arch/15 §2).
    private string BuildEndpoints(KafkaClusterSnapshot snap, IReadOnlyDictionary<string, NodeAddress> addresses)
        => string.Join(",", snap.Brokers
            .OrderBy(b => b.Name, StringComparer.Ordinal)
            .Select(b =>
            {
                var addr = addresses[b.Name];
                return $"{options.AdvertisedClientHost ?? addr.Host}:{addr.ClientPort}";
            }));

    // K5-конец: txn compare config.mod_revision → put канонического JSON без state.
    private async Task<Result> CommitConfigAsync(KafkaClusterSnapshot snap, CancellationToken ct)
    {
        var key = ConfigKey(snap.Cluster);
        var current = await GetAsync(key, ct);
        if (!current.IsSuccess)
            return current;
        if (current.Value is null)
            return Result.Success(); // ключа нет (внешняя очистка) — не наш случай

        var canonical = JsonSerializer.Serialize(new CanonicalConfig(
            snap.Config.Brokers,
            snap.Config.ReplicationFactor,
            snap.Config.MinInSyncReplicas,
            snap.Config.DefaultPartitions,
            snap.Config.DefaultRetentionMs,
            snap.Config.CreatedUnix), CanonicalJson);
        if (current.Value.Value == canonical)
            return Result.Success(); // уже закоммичен (повторные тики идемпотентны)

        var txn = await TxnAsync(
            TxnRequest.Of(
                [TxnCompare.ModRevisionEqual(key, (long)current.Value.ModRevision)],
                [new TxnOp.Put(key, canonical, null)]),
            ct);
        if (!txn.IsSuccess)
            return txn;
        if (!txn.Value.Succeeded)
            return Result.Failed(new ApplicationException(
                $"config {key} изменился с момента чтения (compare mod_revision не сошёлся) — ретрай тиком"));

        return Result.Success();
    }

    // R6: свежее чтение config — TO_REMOVE прекращает provisioning безопасно
    // (контейнеры подчистит deprovisioning).
    private async Task<bool> IsRemovedAsync(string cluster, CancellationToken ct)
    {
        var config = await GetAsync(ConfigKey(cluster), ct);
        if (!config.IsSuccess)
            return false; // чтение не удалось — фаза всё равно под клэймом, продолжаем
        return config.Value is { } kv && kv.Value.Contains("\"TO_REMOVE\"");
    }

    private static int NodeId(string nodeName)
        => int.TryParse(nodeName["broker".Length..], NumberStyles.Integer, CultureInfo.InvariantCulture, out var id)
            ? id
            : 0;

    private async Task<Result> FinishAsync(string cluster, string phase, CancellationToken ct)
    {
        var written = await journal.WriteAsync(cluster, Op, phase, claims.InstanceId, null, ct);
        return written;
    }

    private Result Fail(string cluster, Exception error, string phase)
    {
        // journal last_error + фаза (не ждём — процесс может быть уже сломан).
        journal.WriteAsync(cluster, Op, phase, claims.InstanceId, error.Message, CancellationToken.None)
            .GetAwaiter().GetResult();
        return Result.Failed(error);
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

        // Формат arch/15 §4: {"broker<k>":{"host":"h","client":16001}}.
        var addresses = new Dictionary<string, NodeAddress>();
        using var doc = JsonDocument.Parse(kv.Value);
        foreach (var node in doc.RootElement.EnumerateObject())
        {
            var host = node.Value.GetProperty("host").GetString()!;
            var client = node.Value.GetProperty("client").GetInt32();
            addresses[node.Name] = new NodeAddress(host, client);
        }

        return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(addresses);
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

    private static string ConfigKey(string cluster) => $"/kafka/clusters/{cluster}/config";

    private static string EndpointsKey(string cluster) => $"/kafka/clusters/{cluster}/endpoints";

    private static string BrokerStateKey(string cluster, string broker)
        => $"/kafka/clusters/{cluster}/brokers/{broker}/state";

    private static string RoleKey(string cluster, string broker)
        => $"/kafka/clusters/{cluster}/brokers/{broker}/role";

    private static string PortAllocKey(string cluster) => $"/kafkaworker/portalloc/{cluster}";

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

    // Канонический config после provisioning: state отсутствует (arch/15 §2.1).
    private sealed record CanonicalConfig(
        [property: JsonPropertyName("brokers")] int Brokers,
        [property: JsonPropertyName("replication_factor")] int ReplicationFactor,
        [property: JsonPropertyName("min_insync_replicas")] int MinInSyncReplicas,
        [property: JsonPropertyName("default_partitions")] int DefaultPartitions,
        [property: JsonPropertyName("default_retention_ms")] long DefaultRetentionMs,
        [property: JsonPropertyName("created_unix")] long? CreatedUnix);
}
