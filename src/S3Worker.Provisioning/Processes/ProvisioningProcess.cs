using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using S3Worker.Core.Model;
using S3Worker.Core.OwnS3;
using S3Worker.Docker.Drivers;
using S3Worker.Etcd.Parsing;
using Shared.Core.Planning;
using Shared.Etcd.Client;

namespace S3Worker.Provisioning.Processes;

/// <summary>
/// Provisioning S3-кластера (arch/24 §5 A, фазы P0–P5): от заявки
/// NOT_INITIALIZED до Active-кластера. Все фазы идемпотентны и перепроверяют
/// факт; перед фазами — перечитывание config (S-гонка: TO_REMOVE посреди
/// работы безопасно прекращает процесс, без docker-мутаций). Снапшоты P12
/// «до» (после claim) и «после» (перед journal done) — через snapshot-делегат.
/// Вызывается только держателем клэйма &lt;C&gt;.
/// </summary>
public sealed class ProvisioningProcess(
    IEtcdGateway etcd,
    string[] endpoints,
    IS3ClusterDriver driver,
    ClaimStore claims,
    WorkJournal journal,
    PortAllocLock portLock,
    PortAllocIndex portAlloc,
    SecretProfileStore secrets,
    PkiStore pki,
    InstanceEnvComposer envComposer,
    MeshGovernanceProcess mesh,
    IOwnS3NodeClient nodeClient,
    S3ProvisioningOptions options,
    BootWaitBudget bootWait,
    Func<CancellationToken, Task<Result>>? snapshot = null)
{
    private const string Op = "provision";

    private static readonly JsonSerializerOptions CanonicalJson = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    public async Task<Result> RunAsync(S3ClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Cluster;

        // Мутации — только держателем живого клэйма (arch/17 S5).
        if (!claims.IsMine(cluster))
            return Result.Failed(new ApplicationException(
                $"provisioning {cluster}: claim is not ours (or lost) — mutations forbidden"));

        // Снапшот P12 «до» (после claim — порт P12: точка изменения).
        if (snapshot is not null)
        {
            var before = await snapshot(ct);
            if (!before.IsSuccess)
                return Fail(cluster, before.Error!, "snapshot-before");
        }

        // P0: journal-before-manipulations + свежая перечитка config (гонка
        // TO_REMOVE: заявка на демонтаж — безопасная остановка ДО docker).
        var started = await journal.WritePhaseAsync(cluster, Op, "started", claims.InstanceId, null, ct);
        if (!started.IsSuccess)
            return started;
        if (await IsRemovedAsync(cluster, ct))
            return await FinishAsync(cluster, "aborted-state-changed", ct);

        // P1: план placement + порты, закрепление portalloc, placed_on, профиль.
        var planned = await PlanAsync(snap, ct);
        if (!planned.IsSuccess)
        {
            // Клэйм portalloc занят — не фейл, InProgress (следующий тик);
            // сбой захвата — обычный фейл (бэкофф).
            if (planned.Error is PortLockBusyException)
                return await FinishAsync(cluster, "waiting-portalloc-lock", ct);
            return Fail(cluster, planned.Error!, "planning");
        }

        // Профиль секретов: неполный/отсутствующий — permanent-отказ P1
        // (диагностика без значений; оператор чинит env — тик повторит).
        if (!secrets.TryGet("legacy", out var nodeSecrets, out var secretDiagnostic))
            return Fail(cluster, new ApplicationException(
                $"secret profile {"legacy"}: {secretDiagnostic}"), "invalid-secret-profile");

        // P2: создать контейнеры нод (state=PROVISIONING; существующие — сверка).
        var ensured = await EnsureNodesAsync(snap, planned.Value, nodeSecrets!, ct);
        if (!ensured.IsSuccess)
            return Fail(cluster, ensured.Error!, "ensure-nodes");

        // Гонка TO_REMOVE посреди provisioning — безопасный выход.
        if (await IsRemovedAsync(cluster, ct))
            return await FinishAsync(cluster, "aborted-state-changed", ct);

        // P2.5: ждать health всех нод (бюджет NodeBootSec; не готово — InProgress).
        var ready = await AwaitBootAsync(cluster, planned.Value, snap, ct);
        if (!ready.IsSuccess)
            return Fail(cluster, ready.Error!, "waiting-nodes");
        if (!ready.Value)
            return await FinishAsync(cluster, "waiting-nodes", ct); // InProgress — следующий тик

        // P3: bucket per-install на каждой ноде (root-кредом, идемпотентно).
        var bucket = await EnsureBucketsAsync(snap, planned.Value, nodeSecrets!, ct);
        if (!bucket.IsSuccess)
            return Fail(cluster, bucket.Error!, "ensure-buckets");

        // P4: доводка mesh по links (та же механика, что G — arch/24 §5).
        // Расхождение состава — НЕ отказ provisioning'а: отказ P4 (join/delete
        // недоступен) пишется journal waiting-mesh, а P5 продолжает — endpoints/
        // config не зависят от состава targets; mesh додоводит Active-тиками G.
        var meshTick = await mesh.TickAsync(snap, ct);
        if (!meshTick.IsSuccess)
        {
            var waiting = await journal.WritePhaseAsync(
                cluster, Op, "waiting-mesh", claims.InstanceId, meshTick.Error!.Message, ct);
            if (!waiting.IsSuccess)
                return waiting;
        }

        // Гонка TO_REMOVE перед финальным окном — безопасный выход.
        if (await IsRemovedAsync(cluster, ct))
            return await FinishAsync(cluster, "aborted-state-changed", ct);

        // P5: endpoints (advertised host:port; 0 инстансов — ключ НЕ пишется,
        // arch/23 §2: endpoints существует ⟺ ≥1 инстанс) + config без state.
        var endpoints = BuildEndpoints(snap, planned.Value);
        if (endpoints.Length > 0)
        {
            var endpointsPut = await PutAsync(S3ProcessCommon.EndpointsKey(cluster), endpoints, ct);
            if (!endpointsPut.IsSuccess)
                return Fail(cluster, endpointsPut.Error!, "put-endpoints");
        }

        var committed = await CommitConfigAsync(snap, ct);
        if (!committed.IsSuccess)
            return Fail(cluster, committed.Error!, "committing-config");

        if (snapshot is not null)
        {
            var after = await snapshot(ct);
            if (!after.IsSuccess)
                return Fail(cluster, after.Error!, "snapshot-after");
        }

        return await FinishAsync(cluster, "done", ct);
    }

    // P1: per-node placement (arch/23 §4: предикат равенства по атрибутам,
    // least-loaded + Ordinal tie-break) → порт-аллокация под глобальным клэймом
    // /s3worker/locks/portalloc (t91: без него параллельные сеяния выбирают
    // одинаковые порты) → txn put-if-absent portalloc → placed_on.
    // Журнал planned — с перечнем кандидатов каждой ноды (нуль кандидатов —
    // пустой перечень в transient-фейле).
    private async Task<Result<IReadOnlyDictionary<string, NodeAddress>>> PlanAsync(
        S3ClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Cluster;
        var pinned = await ReadPortAllocAsync(cluster, ct);
        if (!pinned.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(pinned.Error!);
        var existing = new Dictionary<string, NodeAddress>(pinned.Value);

        var wanted = snap.Nodes
            .Where(n => n.State is null or S3NodeStates.NotInitialized or S3NodeStates.Provisioning or S3NodeStates.Running)
            .Select(n => n.Node)
            .ToList();

        // Ранний пред-выход ДО клэйма: всё закреплено — переиспользование без
        // записи; тики waiting-nodes не соперничают за глобальный клэйм.
        if (wanted.All(existing.ContainsKey))
            return await PlannedAsync(existing, cluster, ct);

        var acquired = await portLock.TryAcquireAsync(ct);
        if (!acquired.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(acquired.Error!);
        if (!acquired.Value)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(new PortLockBusyException(portLock.Key));
        try
        {
            var hosts = await driver.GetHostsAsync(ct);
            if (!hosts.IsSuccess)
                return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(hosts.Error!);
            var dockerBusy = await driver.GetBusyPortsAsync(ct);
            if (!dockerBusy.IsSuccess)
                return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(dockerBusy.Error!);
            var foreign = await portAlloc.ForeignAllocatedPortsAsync(cluster, ct);
            if (!foreign.IsSuccess)
                return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(foreign.Error!);
            var busy = new HashSet<(string Host, int Port)>();
            // Порт занят ГЛОБАЛЬНО (любая docker-публикация / чужой portalloc
            // закрывает порт на каждом хосте): advertised-адрес (AdvertisedHost
            // single-host/стенд) требует уникальности порта вне зависимости
            // от хоста размещения.
            foreach (var host in hosts.Value)
            {
                foreach (var port in foreign.Value)
                    busy.Add((host.Name, port));
                foreach (var pair in dockerBusy.Value)
                    busy.Add((host.Name, pair.Port));
            }

            // Per-node резолвция селектора + перечень кандидатов (arch/23 §4).
            var placements = new List<NodePlacement>();
            foreach (var node in snap.Nodes.Where(n => wanted.Contains(n.Node)).OrderBy(n => n.Node, StringComparer.Ordinal))
            {
                var resolved = PlacementResolver.Resolve(
                    node.Placement ?? new S3NodePlacement(null, null, null), hosts.Value, out var candidates);
                if (!resolved.IsSuccess)
                    return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(
                        new ApplicationException(
                            $"placement {cluster}/{node.Node}: zero candidates; list: [{string.Join(", ", candidates)}]"));
                placements.Add(new NodePlacement(cluster, node.Node, resolved.Value));
                var placedOn = await PutAsync(S3ProcessCommon.NodePlacedOnKey(cluster, node.Node), resolved.Value, ct);
                if (!placedOn.IsSuccess)
                    return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(placedOn.Error!);
            }

            // Порт выделяется уникально ГЛОБАЛЬНО (advertised-адрес с общим
            // AdvertisedHost требует уникальности порта вне зависимости от
            // хоста): Allocate — по ноде за раз, после каждого выделения порт
            // закрывается на всех хостах (семантика Allocate — per-host).
            foreach (var placement in placements)
            {
                var one = PortAllocator.Allocate(
                    new PlacementPlan([placement]), existing, busy,
                    options.PortRangeFrom, options.PortRangeTo,
                    S3Planning.PortsOf, S3Planning.HostOf, S3Planning.MakeAddress, S3Planning.KeyOf);
                if (!one.IsSuccess)
                    return one;
                existing[placement.Node] = one.Value[placement.Node];
                foreach (var host in hosts.Value)
                    busy.Add((host.Name, one.Value[placement.Node].S3Port));
            }

            // Создание ключа — только если нет (compare version==0); проигрыш →
            // re-read (первый записавший — истина, S5).
            var key = S3ProcessCommon.PortAllocKey(cluster);
            var serialized = S3ProcessCommon.SerializePortAlloc(existing);
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

            return await PlannedAsync(existing, cluster, ct);
        }
        finally
        {
            await portLock.ReleaseAsync();
        }
    }

    // Журнал planned + результат секции (внутри try, до release; клэйм короткий).
    private async Task<Result<IReadOnlyDictionary<string, NodeAddress>>> PlannedAsync(
        Dictionary<string, NodeAddress> existing, string cluster, CancellationToken ct)
    {
        var planned = await journal.WritePhaseAsync(cluster, Op, "planned", claims.InstanceId, null, ct);
        return planned.IsSuccess
            ? Result<IReadOnlyDictionary<string, NodeAddress>>.Success(existing)
            : Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(planned.Error!);
    }

    // P2: EnsureNode всех нод (state != RUNNING) + state=PROVISIONING; env ноды
    // — OWNS3_* профиля + OWNS3_DATA_DIR=/data (arch/23 §6).
    private async Task<Result> EnsureNodesAsync(
        S3ClusterSnapshot snap,
        IReadOnlyDictionary<string, NodeAddress> addresses,
        OwnS3NodeSecrets nodeSecrets,
        CancellationToken ct)
    {
        var cluster = snap.Cluster;
        foreach (var node in snap.Nodes.OrderBy(n => n.Node, StringComparer.Ordinal))
        {
            if (node.State == S3NodeStates.Running)
                continue; // идемпотентность: поднята ранее — не трогаем
            if (node.State is S3NodeStates.ToRemove or S3NodeStates.Removing)
                continue; // демонтажные — не provisioning

            if (node.State != S3NodeStates.Provisioning)
            {
                var marked = await PutAsync(S3ProcessCommon.NodeStateKey(cluster, node.Node), S3NodeStates.Provisioning, ct);
                if (!marked.IsSuccess)
                    return marked;
            }

            // PKI лениво при первом использовании (arch/23 §6): пара CA+оркестратор,
            // затем материал инстанса — txn put-if-absent, идемпотентно на тиках.
            var clusterPki = await pki.EnsureClusterPkiAsync(cluster, ct);
            if (!clusterPki.IsSuccess)
                return Fail(cluster, clusterPki.Error!, "pki");
            var nodePki = await pki.EnsureNodePkiAsync(cluster, node.Node, clusterPki.Value.Ca, ct);
            if (!nodePki.IsSuccess)
                return Fail(cluster, nodePki.Error!, "pki");

            var addr = addresses[node.Node];
            var env = envComposer.Compose(
                nodeSecrets, nodePki.Value, clusterPki.Value.Ca.Ca,
                $"http://{options.AdvertisedHost ?? addr.Host}:{addr.S3Port}");
            var resources = S3ProcessCommon.ParseResources(node.Resources);
            var ensured = await driver.EnsureNodeAsync(
                new S3NodeSpec(
                    cluster, node.Node, addr.Host, addr.S3Port, options.OwnS3Image, env,
                    resources?.Cpu, resources?.MemBytes),
                options.ContourNetwork, ct);
            if (!ensured.IsSuccess)
                return ensured;
        }

        return Result.Success();
    }

    // P2.5: health всех нод отвечает → RUNNING; не готово — InProgress до
    // бюджета NodeBootSec (транзиент-толерантно; таймаут одиночной пробы — 7.5 с).
    private async Task<Result<bool>> AwaitBootAsync(
        string cluster,
        IReadOnlyDictionary<string, NodeAddress> addresses,
        S3ClusterSnapshot snap,
        CancellationToken ct)
    {
        var pending = snap.Nodes
            .Where(n => n.State != S3NodeStates.Running
                && n.State is null or S3NodeStates.NotInitialized or S3NodeStates.Provisioning)
            .Select(n => n.Node)
            .ToList();
        foreach (var node in snap.Nodes.Where(n => n.State == S3NodeStates.Running))
            pending.Add(node.Node); // проверяем и «уже RUNNING» — факт важнее стейта

        foreach (var node in pending.Distinct().OrderBy(n => n, StringComparer.Ordinal))
        {
            var addr = addresses[node];
            var health = await nodeClient.HealthAsync(
                options.AdvertisedHost ?? addr.Host, addr.S3Port, ct);
            if (!health.IsSuccess || !health.Value)
            {
                // Не готов: бюджет с первого наблюдения (диагностика, не клэйм).
                if (bootWait.Exceeded(cluster, options.NodeBootSec))
                    return Result<bool>.Failed(new ApplicationException(
                        $"cluster {cluster}: node {node} did not come up within budget {options.NodeBootSec} s"));
                return Result<bool>.Success(false);
            }
        }

        // Все живы: state=RUNNING (put только не-RUNNING).
        bootWait.Reset(cluster);
        foreach (var node in snap.Nodes.Where(n => n.State != S3NodeStates.Running))
        {
            var running = await PutAsync(S3ProcessCommon.NodeStateKey(cluster, node.Node), S3NodeStates.Running, ct);
            if (!running.IsSuccess)
                return Result<bool>.Failed(running.Error!);
        }

        return Result<bool>.Success(true);
    }

    // P3: bucket per-install на каждой ноде (root-кредом; 409 already-owned —
    // успех, owns3/02).
    private async Task<Result> EnsureBucketsAsync(
        S3ClusterSnapshot snap,
        IReadOnlyDictionary<string, NodeAddress> addresses,
        OwnS3NodeSecrets nodeSecrets,
        CancellationToken ct)
    {
        foreach (var node in snap.Nodes.OrderBy(n => n.Node, StringComparer.Ordinal))
        {
            var addr = addresses[node.Node];
            var ensured = await nodeClient.EnsureBucketAsync(
                options.AdvertisedHost ?? addr.Host, addr.S3Port, nodeSecrets, options.Bucket, ct);
            if (!ensured.IsSuccess)
                return Result.Failed(ensured.Error!);
        }

        return Result.Success();
    }

    // endpoints: advertised host:port по нодам через запятую (arch/23 §2.1).
    private string BuildEndpoints(S3ClusterSnapshot snap, IReadOnlyDictionary<string, NodeAddress> addresses)
        => string.Join(",", snap.Nodes
            .Where(n => n.State is null or S3NodeStates.NotInitialized or S3NodeStates.Provisioning or S3NodeStates.Running)
            .OrderBy(n => n.Node, StringComparer.Ordinal)
            .Select(n =>
            {
                var addr = addresses[n.Node];
                return $"{options.AdvertisedHost ?? addr.Host}:{addr.S3Port}";
            }));

    // P5-конец: txn compare config.mod_revision → put канонического JSON без state
    // (Active-пример arch/23 §2.1: platform/nodes/etcd_backup?/created_unix).
    private async Task<Result> CommitConfigAsync(S3ClusterSnapshot snap, CancellationToken ct)
    {
        var key = S3ProcessCommon.ConfigKey(snap.Cluster);
        var current = await GetAsync(key, ct);
        if (!current.IsSuccess)
            return current;
        if (current.Value is null)
            return Result.Success(); // ключа нет (внешняя очистка) — не наш случай

        var canonical = JsonSerializer.Serialize(
            new CanonicalConfig(
                snap.Config!.Platform!,
                snap.Config.Nodes!.Value,
                snap.Config.EtcdBackup ?? false ? true : null,
                snap.Config.CreatedUnix),
            CanonicalJson);
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
                $"config {key} changed since read (compare mod_revision mismatch) — retried next tick"));

        return Result.Success();
    }

    // Гонка: свежее чтение config — TO_REMOVE прекращает provisioning безопасно
    // (контейнеры подчистит deprovisioning).
    private async Task<bool> IsRemovedAsync(string cluster, CancellationToken ct)
    {
        var config = await GetAsync(S3ProcessCommon.ConfigKey(cluster), ct);
        if (!config.IsSuccess)
            return false; // чтение не удалось — фаза всё равно под клэймом, продолжаем
        return config.Value is { } kv && kv.Value.Contains("\"TO_REMOVE\"", StringComparison.Ordinal);
    }

    private async Task<Result> FinishAsync(string cluster, string phase, CancellationToken ct)
    {
        // Выход из provisioning (done / aborted-state-changed → демонтаж):
        // бюджет boot-ожидания — заново; waiting-* кластер из provisioning не
        // выводит, отсчёт продолжается.
        if (phase is "done" or "aborted-state-changed")
            bootWait.Reset(cluster);
        return await journal.WritePhaseAsync(cluster, Op, phase, claims.InstanceId, null, ct);
    }

    private Result Fail(string cluster, Exception error, string phase)
    {
        // journal last_error + фаза (не ждём — процесс может быть уже сломан).
        journal.WritePhaseAsync(cluster, Op, phase, claims.InstanceId, error.Message, CancellationToken.None)
            .GetAwaiter().GetResult();
        return Result.Failed(error);
    }

    private async Task<Result<IReadOnlyDictionary<string, NodeAddress>>> ReadPortAllocAsync(
        string cluster, CancellationToken ct)
    {
        var result = await GetAsync(S3ProcessCommon.PortAllocKey(cluster), ct);
        if (!result.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(result.Error!);
        if (result.Value is not { } kv)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(
                (IReadOnlyDictionary<string, NodeAddress>)new Dictionary<string, NodeAddress>());

        return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(S3ProcessCommon.ParsePortAlloc(kv.Value));
    }

    // Failover-обёртки: первый успешный endpoint выигрывает.
    private async Task<Result<Kv?>> GetAsync(string key, CancellationToken ct)
        => await WithFailoverAsync(endpoint => etcd.GetAsync(endpoint, key, ct));

    private async Task<Result> PutAsync(string key, string value, CancellationToken ct)
    {
        Result last = default;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.PutAsync(endpoint, key, value, null, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last.Error ?? new EtcdUnreachableException("etcd endpoints are not set");
    }

    private async Task<Result<TxnResult>> TxnAsync(TxnRequest req, CancellationToken ct)
        => await WithFailoverAsync(endpoint => etcd.TxnAsync(endpoint, req, ct));

    private async Task<Result<T>> WithFailoverAsync<T>(Func<string, Task<Result<T>>> call)
    {
        Result<T> last = default;
        foreach (var endpoint in endpoints)
        {
            var result = await call(endpoint);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last.Error ?? new EtcdUnreachableException("etcd endpoints are not set");
    }

    // Канонический config после provisioning: state отсутствует (arch/23 §2.1,
    // порядок полей канона).
    private sealed record CanonicalConfig(
        [property: JsonPropertyName("platform")] string Platform,
        [property: JsonPropertyName("nodes")] int Nodes,
        [property: JsonPropertyName("etcd_backup")] bool? EtcdBackup,
        [property: JsonPropertyName("created_unix")] long? CreatedUnix);
}
