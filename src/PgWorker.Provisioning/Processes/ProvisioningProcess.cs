using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.Json.Serialization;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Planning;
using PgWorker.Core.Templates;
using PgWorker.Docker.Drivers;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Probes;
using PgWorker.Provisioning.Sql;

namespace PgWorker.Provisioning.Processes;

/// <summary>
/// Provisioning — главная машина состояний (задача 19; spec §6.4 A, arch/14
/// §5 A; эталон init-cluster.sh). Фазы P0–P5, каждая идемпотентна и
/// перепроверяет факт; перед фазами — перечитывание config (R6: смена state
/// посреди работы безопасно прекращает процесс) и проверка клэйма (мутации
/// /clusters/ и docker — только держателем). Один тик доводит кластер насколько
/// возможно: ожидания (Patroni, мастер) возвращают InProgress — цикл задачи 23
/// продолжит следующим тиком с записанной фазы из /pgworker/work/&lt;C&gt;.
/// </summary>
public sealed class ProvisioningProcess(
    IEtcdGateway etcd,
    string[] endpoints,
    IClusterDriver driver,
    ISqlExecutor db,
    ShardProbe probe,
    ClaimStore claims,
    WorkJournal journal,
    PlacementOptions placementOpts,
    InstallSecrets secrets,
    IAppSecretEnsurer appSecret,
    IAppParamsEnsurer appParams,
    EtcdEndpoints etcdEndpoints,
    PortAllocIndex portAlloc,
    Func<CancellationToken, Task<Result>>? snapshot = null) : IClusterProcess
{
    private const int TxnBatchSize = 128; // лимит ops в txn (P3)
    private const string Op = "provision";

    private static readonly JsonSerializerOptions CanonicalJson = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    // Время первого наблюдения «шард без живого Patroni» (бюджет P2.2; memory —
    // после takeover отсчёт начнётся заново: диагностический бюджет, не клэйм).
    // ConcurrentDictionary (rework №1): процесс — синглтон DI, кластеры
    // обрабатываются параллельно — обычный Dictionary не потокобезопасен.
    private readonly ConcurrentDictionary<string, long> _patroniWaitSince = new();

    // Исход P2.2-ожидания: ждём / готово / починили HA-scope (Д3 — тик завершается
    // фазой reset-scope, Patroni бутстрапится заново).
    private enum WaitPatroniOutcome { Waiting, Ready, ResetScope }

    public async Task<Result<ProcessOutcome>> TickAsync(ClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;

        // Мутации — только держателем живого клэйма (инвариант spec §4.3).
        if (!claims.IsMine(cluster))
            return Result<ProcessOutcome>.Failed(new ApplicationException(
                $"provisioning {cluster}: клэйм не наш (или потерян) — мутации запрещены"));

        // Guard входа (arch/14 §5 A): полный набор ключей панели, иначе
        // полуфабрикат NOT_INITIALIZED не provisioning'уем.
        if (!HasFullDeclaration(snap))
            return await Finish(cluster, "waiting-keys", ProcessOutcome.InProgress, ct);

        // Бэкофф ретраев (spec §3.5 E2): серия фейлов в журнале — до retry_not_before
        // тик процесса пропускается (без записи: журнал несёт последний фейл).
        var priorWork = await journal.ReadAsync(cluster, ct);
        if (!priorWork.IsSuccess)
            return Result<ProcessOutcome>.Failed(priorWork.Error!);
        var series = priorWork.Value is { Op: Op, FailCount: > 0, FailFirstUnix: > 0 } pw
            ? new RetrySeries(pw.FailCount!.Value, pw.FailFirstUnix!.Value, pw.RetryNotBeforeUnix ?? 0)
            : null;
        if (series is { RetryNotBeforeUnix: > 0 } s
            && s.RetryNotBeforeUnix > DateTimeOffset.UtcNow.ToUnixTimeSeconds())
            return Result<ProcessOutcome>.Success(ProcessOutcome.InProgress);

        // P0: journal-before-manipulations (P7); серия переносится фазами тика
        // (ревью Ф4-2: пропуск series стирал бы поля — provision-stuck мигает).
        var started = await journal.WritePhaseAsync(cluster, Op, "started", claims.InstanceId, null, ct, series);
        if (!started.IsSuccess)
            return Result<ProcessOutcome>.Failed(started.Error!);

        // P1: план placement + порты, закрепление portalloc.
        var allocation = await PlanPortsAsync(snap, series, ct);
        if (!allocation.IsSuccess)
            return await FailAsync(cluster, allocation.Error!, "planning", ct, series);
        var addresses = allocation.Value;

        // Per-cluster credentials: переопределение bucket_admin user/password
        // из config кластера (fallback на глобальные InstallSecrets).
        var clusterSecrets = secrets with
        {
            BucketAdminUser = snap.Config.BucketAdminUser ?? secrets.BucketAdminUser,
            BucketAdminPassword = snap.Config.BucketAdminPassword ?? secrets.BucketAdminPassword,
        };

        // P1.5 (spec §3.3): ensure per-cluster app-секрета — до любых контейнеров/ролей:
        // приложение получает креды в etcd раньше, чем поднимутся ноды.
        var appCreds = await appSecret.EnsureAsync(cluster, ct);
        if (!appCreds.IsSuccess)
            return await FailAsync(cluster, appCreds.Error!, "ensure-app-secret", ct, series);

        // P2.1: EnsureNode всех нод ВСЕХ шардов ПАРАЛЛЕЛЬНО (контейнеры стартуют
        // одновременно, ожидание Patroni — следующим проходом) + nodes/<n>/state=PROVISIONING.
        var topologies = new ConcurrentDictionary<string, ShardTopology>();
        var orderedShards = snap.Shards.OrderBy(s => s.Name, StringComparer.Ordinal).ToList();

        var ensureErrors = new ConcurrentQueue<Exception>();
        await Parallel.ForEachAsync(orderedShards, ct, async (shard, token) =>
        {
            if (await IsRemovedAsync(cluster, token))
                return;

            var topology = Topology(cluster, shard.Name, addresses);
            topologies[shard.Name] = topology;
            var resources = await ReadShardResourcesAsync(cluster, shard, token);
            var ensured = await EnsureNodesAsync(cluster, shard, topology, resources, clusterSecrets, token);
            if (!ensured.IsSuccess)
                ensureErrors.Enqueue(ensured.Error!);
        });

        if (ensureErrors.TryDequeue(out var ensureError))
            return await FailAsync(cluster, ensureError, "ensure-nodes", ct, series);
        if (await IsRemovedAsync(cluster, ct))
            return await Finish(cluster, "aborted", ProcessOutcome.InProgress, ct, series);

        // P2.2–P2.5: по каждому шарду ПАРАЛЛЕЛЬНО — ожидание Patroni, master, БД/роли/схемы, dsn.
        var shardErrors = new ConcurrentQueue<Exception>();
        var resetScopes = new ConcurrentQueue<string>();
        await Parallel.ForEachAsync(orderedShards, ct, async (shard, token) =>
        {
            // R6: перечитываем config перед фазами шарда.
            if (await IsRemovedAsync(cluster, token))
                return;

            var topology = topologies[shard.Name];
            var booted = await WaitPatroniAsync(cluster, shard, topology, token);
            if (!booted.IsSuccess)
            {
                shardErrors.Enqueue(booted.Error!);
                return;
            }
            if (booted.Value == WaitPatroniOutcome.ResetScope)
            {
                resetScopes.Enqueue(shard.Name); // Д3: тик завершится фазой reset-scope
                return;
            }
            if (booted.Value == WaitPatroniOutcome.Waiting)
                return; // InProgress — не ошибка, следующий тик

            var master = await ResolveMasterAsync(shard, topology, token);
            if (master is null)
                return; // waiting-master — InProgress

            var sqlDone = await ProvisionShardSqlAsync(snap, shard, topology, master, appCreds.Value, token);
            if (!sqlDone.IsSuccess)
                shardErrors.Enqueue(sqlDone.Error!);
        });

        if (shardErrors.TryDequeue(out var firstError))
            return await FailAsync(cluster, firstError, "shard-provision", ct, series);

        // Д3: чистка HA-scope выполнена — тик завершаем журналом reset-scope (одна
        // фаза на тик; серию переносим — прогресс, не фейл).
        if (resetScopes.TryDequeue(out _))
            return await Finish(cluster, "reset-scope", ProcessOutcome.InProgress, ct, series);

        // Если хоть один шард не доведён (Patroni/master ещё не готовы) — InProgress.
        foreach (var shard in orderedShards)
        {
            if (shard.Nodes.Any(n => n.State != NodeState.Running))
                return await Finish(cluster, "waiting-patroni", ProcessOutcome.InProgress, ct, series);
            if (string.IsNullOrEmpty(shard.Dsn))
                return await Finish(cluster, "waiting-shard-sql", ProcessOutcome.InProgress, ct, series);
        }

        // R6 перед финальными мутациями контрол-плейна.
        if (await IsRemovedAsync(cluster, ct))
            return await Finish(cluster, "aborted", ProcessOutcome.InProgress, ct, series);

        // P3: снять ВСЕ status-ключи (txn-пакетами ≤128) — бакеты ACTIVE.
        var cleared = await ClearStatusKeysAsync(snap, ct);
        if (!cleared.IsSuccess)
            return await FailAsync(cluster, cleared.Error!, "clear-status", ct, series);

        // P4: config txn (compare mod_revision) → канонический JSON без state (Д1).
        var committed = await CommitConfigAsync(snap, ct);
        if (!committed.IsSuccess)
            return await FailAsync(cluster, committed.Error!, "committing-config", ct, series);

        // P5: снапшот P12 (делегат SnapshotJob, задача 22) + journal phase=done.
        if (snapshot is not null)
        {
            var shot = await snapshot(ct);
            if (!shot.IsSuccess)
                return await FailAsync(cluster, shot.Error!, "snapshot", ct, series);
        }

        return await Finish(cluster, "done", ProcessOutcome.Done, ct);
    }

    // Guard входа: config с константами, replicas/nodes у каждого шарда,
    // routing всех N бакетов заполнен (arch/14 §5 A).
    private static bool HasFullDeclaration(ClusterSnapshot snap) =>
        snap.Config.Buckets > 0
        && !string.IsNullOrWhiteSpace(snap.Config.DbName)
        && snap.Shards is { Count: > 0 }
        && snap.Shards.All(s => s.Replicas > 0 && s.Nodes.Count > 0)
        && snap.Routing.Count == snap.Config.Buckets
        && snap.Routing.All(r => !string.IsNullOrWhiteSpace(r.Owner));

    // P1: усыновление факта → busy (docker ∪ чужие portalloc) → аллокация недобора →
    // закрепление /pgworker/portalloc/<C> (spec §3.1). Факт над записью: живой
    // канонический контейнер — канон записи; нода без контейнера — обычная аллокация.
    private async Task<Result<IReadOnlyDictionary<string, NodeAddress>>> PlanPortsAsync(
        ClusterSnapshot snap, RetrySeries? series, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;
        var pinned = await ReadPortAllocAsync(cluster, ct);
        if (!pinned.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(pinned.Error!);
        var existing = new Dictionary<string, NodeAddress>(pinned.Value);

        var wanted = snap.Shards.SelectMany(s => s.Nodes.Select(n => $"{s.Name}/{n.Name}")).ToList();

        // Усыновление факта — КАЖДЫЙ тик provision (ревью Ф4-1, spec §3.1 шаг 3):
        // расхождение нельзя узнать без инспекции, а ПОЛНЫЙ portalloc может быть
        // расходящимся (потерян и выделен заново — состояние стенда canon10/smoke).
        var adopted = await AdoptRunningContainersAsync(cluster, snap, existing, ct);
        if (!adopted.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(adopted.Error!);
        var skipped = adopted.Value.Skipped;

        // Д1 (spec §3.7, живой-Ф7): занятость = ВСЯ фактическая — docker-публикации
        // (чужие И своих соседей по кластеру: дубликат внутри кластера — такой же
        // конфликт) ∪ portalloc соседей. Читается до проверки полноты: полный
        // portalloc может нести коллизию — «закреплено и переиспользуется» не должно
        // давать вечный фейл-цикл «port is already allocated».
        var dockerBusy = await driver.GetBusyPortsAsync(ct);
        if (!dockerBusy.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(dockerBusy.Error!);
        var foreignAlloc = await portAlloc.ReadBusyAsync(cluster, ct);
        if (!foreignAlloc.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(foreignAlloc.Error!);
        var busy = new HashSet<(string, int)>(foreignAlloc.Value);
        foreach (var p in dockerBusy.Value)
            busy.Add(p);
        var detached = PortPlanConvergence.DetachColliding(existing, adopted.Value.SelfFactByNode, busy);

        // Ранний выход (идемпотентность, spec §3.1 шаг 4 + §3.7 Д1): всё закреплено,
        // merge и detach ничего не изменили — записи portalloc нет.
        if (wanted.All(existing.ContainsKey) && !adopted.Value.Changed && !detached)
            return await PlannedAsync(existing, cluster, ct, series, skipped);

        if (wanted.All(existing.ContainsKey))
        {
            var commit = await CommitPortAllocAsync(cluster, existing, pinned.Value.Count > 0, ct);
            if (!commit.IsSuccess)
                return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(commit.Error!);
            return await PlannedAsync(existing, cluster, ct, series, skipped);
        }

        var hosts = await driver.GetHostsAsync(ct);
        if (!hosts.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(hosts.Error!);
        // Занятость для аллокации — busy минус факты ПОДТВЕРЖДЁННЫХ записей (запись,
        // совпадающая с живым контейнером своей ноды, — закрепление, не запрет:
        // попади её факт в taken, allocator не переиспользовал бы валидную запись →
        // EnsureNode пересоздавал бы живые контейнеры, spec §8.10).
        var taken = new HashSet<(string, int)>(busy);
        foreach (var p in PortPlanConvergence.ConfirmedFact(existing, adopted.Value.SelfFactByNode))
            taken.Remove(p);
        var plan = PlacementPlanner.Plan(snap.Shards, hosts.Value);
        var allocated = PortAllocator.Allocate(plan, existing, taken, placementOpts.PortFrom, placementOpts.PortTo);
        if (!allocated.IsSuccess)
            return allocated;

        foreach (var (merged, addr) in allocated.Value)
            existing[merged] = addr;

        var commitAll = await CommitPortAllocAsync(cluster, existing, pinned.Value.Count > 0, ct);
        if (!commitAll.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(commitAll.Error!);

        return await PlannedAsync(existing, cluster, ct, series, skipped);
    }

    // Результат усыновления: имена пропущенных находок (journal-заметка), признак
    // «merge изменил existing» (ранний выход без записи) и порты ФАКТА своих
    // контейнеров ПО НОДАМ (Д1: подтверждение per-node — факт контейнера ноды
    // подтверждает только её запись; агрегат маскировал дубликаты внутри кластера).
    private sealed record Adoption(
        IReadOnlyList<string> Skipped,
        bool Changed,
        IReadOnlyDictionary<string, IReadOnlySet<(string Host, int Port)>> SelfFactByNode);

    // Инспекция живых канонических контейнеров: фактические public-порты становятся
    // каноном записей — добавление отсутствующих, перезапись при расхождении
    // (только записей без object), совпадение — не пишем (NodeAddress/NodePorts —
    // record, Equals по значению).
    private async Task<Result<Adoption>> AdoptRunningContainersAsync(
        string cluster, ClusterSnapshot snap, Dictionary<string, NodeAddress> existing, CancellationToken ct)
    {
        var byNode = snap.Shards
            .SelectMany(s => s.Nodes.Select(n => (Key: $"{s.Name}/{n.Name}", Name: n.Name)))
            .ToList();
        var discovered = await driver.InspectNodesAsync(
            cluster, byNode.Select(p => p.Name).Distinct().ToList(), ct);
        if (!discovered.IsSuccess)
            return Result<Adoption>.Failed(discovered.Error!);

        var skipped = new List<string>();
        var changed = false;
        var selfFactByNode = new Dictionary<string, IReadOnlySet<(string, int)>>();
        foreach (var (key, nodeName) in byNode)
        {
            if (!discovered.Value.TryGetValue(nodeName, out var node))
                continue; // контейнера нет — аллокация недобором
            var canonicalObject = $"pgw-{cluster}-{key.Replace('/', '-')}";
            if (node.Object != canonicalObject || node.Pg <= 0 || node.Patroni <= 0)
            {
                skipped.Add(key); // чужой/частичная публикация — не наша находка (spec §3.1 guard'ы)
                continue;
            }

            // Д1 (живой-Ф7): факт контейнера ноды — ПОДТВЕРЖДЕНИЕ её записи per-node
            // (не агрегат: контейнер соседней ноды кластера на том же порту — конфликт).
            var factPorts = new HashSet<(string, int)>();
            foreach (var p in new[] { node.Pg, node.Patroni, node.Doorman })
                if (p > 0)
                    factPorts.Add((node.Host, p));
            selfFactByNode[key] = factPorts;

            var fact = new NodeAddress(node.Host, new NodePorts(node.Pg, node.Patroni, node.Doorman));
            if (existing.TryGetValue(key, out var current))
            {
                if (current.Object is not null)
                    continue; // object-запись (усыновлённая ранее) не перезаписываем
                if (current.Equals(fact))
                    continue; // совпадение записи с фактом — не пишем (идемпотентность)
            }

            existing[key] = fact;
            changed = true;
        }

        return Result<Adoption>.Success(new Adoption(skipped, changed, selfFactByNode));
    }

    // Закрепление: первый ключ — txn NotExists (конкурент создал → берём его перезаписью
    // merge под клэймом); существующий — put (read-modify-write, паттерн AddShard A2).
    private async Task<Result> CommitPortAllocAsync(
        string cluster, IReadOnlyDictionary<string, NodeAddress> addresses, bool keyExisted, CancellationToken ct)
    {
        var key = PortAllocKey(cluster);
        var value = SerializePortAlloc(addresses);
        if (keyExisted)
            return await PutAsync(key, value, ct);

        var txn = await TxnAsync(
            TxnRequest.Of(
                [TxnCompare.NotExists(key)],
                [new TxnOp.Put(key, value, null)]),
            ct);
        if (!txn.IsSuccess)
            return txn;
        if (txn.Value.Succeeded)
            return Result.Success();

        // Проигрыш txn (ключ появился после чтения) — канон другой инстанс уже записал:
        // под клэймом безопасно перезаписать нашим merge (факт свежего чтения).
        return await PutAsync(key, value, ct);
    }

    private async Task<Result<IReadOnlyDictionary<string, NodeAddress>>> PlannedAsync(
        Dictionary<string, NodeAddress> addresses, string cluster, CancellationToken ct,
        RetrySeries? series = null, IReadOnlyList<string>? skipped = null)
    {
        // Пропуски усыновления — journal-заметка (эфемерна: журнал — одна фаза на тик).
        var phase = skipped is { Count: > 0 } s
            ? $"planned, adopt-skipped: {string.Join(", ", s)}"
            : "planned";
        var planned = await journal.WritePhaseAsync(cluster, Op, phase, claims.InstanceId, null, ct, series);
        return planned.IsSuccess
            ? Result<IReadOnlyDictionary<string, NodeAddress>>.Success(addresses)
            : Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(planned.Error!);
    }

    // P2.1: EnsureNode всех нод шарда (state != RUNNING) + nodes/<n>/state=PROVISIONING.
    private async Task<Result> EnsureNodesAsync(
        string cluster, ShardSpec shard, ShardTopology topology, NodeResources? resources,
        InstallSecrets clusterSecrets, CancellationToken ct)
    {
        foreach (var node in shard.Nodes)
        {
            if (node.State == NodeState.Running)
                continue; // идемпотентность: поднята ранее (контейнер есть) — не трогаем

            if (node.State != NodeState.Provisioning)
            {
                var marked = await PutAsync(NodeStateKey(cluster, shard.Name, node.Name), "PROVISIONING", ct);
                if (!marked.IsSuccess)
                    return marked;
            }

            var ensured = await driver.EnsureNodeAsync(
                topology, node.Name, topology.Nodes[node.Name], clusterSecrets, etcdEndpoints, resources, ct);
            if (!ensured.IsSuccess)
                return ensured;
        }

        return Result.Success();
    }

    // Заявки ресурсов шарда (rework №5): /service/<scope>/request_{cpu,mem} →
    // лимиты контейнера/сервиса нод (NanoCPUs/Memory). Чтение не удалось или
    // значение нечитаемо — null: заявка — не контракт, кластер обязан подняться
    // и без лимита. request_disk примитива лимита в docker не имеет — игнор.
    private async Task<NodeResources?> ReadShardResourcesAsync(
        string cluster, ShardSpec shard, CancellationToken ct)
    {
        var scope = $"{cluster}-{shard.Name}";
        var cpu = await GetAsync($"/service/{scope}/request_cpu", ct);
        if (!cpu.IsSuccess)
            return null;
        var mem = await GetAsync($"/service/{scope}/request_mem", ct);
        return mem.IsSuccess ? NodeResourcesParser.Parse(cpu.Value?.Value, mem.Value?.Value) : null;
    }

    // P2.2: scope initialized + leader + Patroni REST всех нод отвечает →
    // nodes/<n>/state=RUNNING; иначе InProgress (бюджет PatroniBootSec, P7-толерантно).
    // Д3: бюджет-ветка — трёхуровневая проба данных (Present/Absent/Unknown).
    private async Task<Result<WaitPatroniOutcome>> WaitPatroniAsync(
        string cluster, ShardSpec shard, ShardTopology topology, CancellationToken ct)
    {
        var scope = $"{cluster}-{shard.Name}";

        var scopeKvs = await RangeAsync($"/service/{scope}/", ct);
        if (!scopeKvs.IsSuccess)
            return Result<WaitPatroniOutcome>.Failed(scopeKvs.Error!);
        var scopeState = ClusterSnapshotParser.ParseService(scopeKvs.Value).FirstOrDefault();
        var scopeReady = scopeState is { Initialized: true, LeaderName: not null };

        // Д1б (spec §3.7): проба обязана подтвердить ИМЕННО нашу ноду — GET /patroni
        // несёт scope+name; чужой ответ по коллизионному порту ≠ наша нода
        // (фальш-RUNNING/фальш-dsn на чужие данные исключены).
        var probesOurs = true;
        foreach (var node in topology.Nodes.Keys)
        {
            var identity = await probe.IdentifyAsync(topology.Nodes[node], ct);
            if (!identity.IsSuccess
                || identity.Value is not { } id
                || id.Scope != scope
                || id.Name != node)
            {
                probesOurs = false;
                break;
            }
        }

        if (!scopeReady || !probesOurs)
        {
            var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            // GetOrAdd — атомарно при параллельных тиках разных кластеров (rework №1).
            var since = _patroniWaitSince.GetOrAdd(scope, now);
            if (now - since > placementOpts.PatroniBootSec)
            {
                // Бюджет исчерпан: сброс трекера — новая попытка получает полный бюджет
                // заново (E3); далее — лечение HA-scope при доказанной утрате (Д3).
                _patroniWaitSince.TryRemove(scope, out _);

                // Д3 (spec §3.7, arch/14 R11): трёхуровневая проба данных нод scope.
                var presences = new List<DataPresence>();
                foreach (var node in shard.Nodes)
                {
                    var presence = await driver.NodeDataPresenceAsync(cluster, shard.Name, node.Name, ct);
                    presences.Add(presence.IsSuccess ? presence.Value : DataPresence.Unknown);
                }

                if (presences.All(p => p == DataPresence.Absent))
                {
                    var reset = await ResetScopeAsync(scope, ct);
                    if (!reset.IsSuccess)
                        return Result<WaitPatroniOutcome>.Failed(reset.Error!);
                    return Result<WaitPatroniOutcome>.Success(WaitPatroniOutcome.ResetScope);
                }

                var alive = string.Join(",", shard.Nodes
                    .Where((n, i) => presences[i] == DataPresence.Present).Select(n => n.Name));
                if (alive.Length > 0)
                    return Result<WaitPatroniOutcome>.Failed(new ApplicationException(
                        $"{scope}: данные есть (ноды {alive}), лидера нет {placementOpts.PatroniBootSec} с — разбор оператора: чистка scope уничтожила бы данные"));

                return Result<WaitPatroniOutcome>.Success(WaitPatroniOutcome.Waiting); // Unknown: утрата не доказана — новый бюджет
            }

            return Result<WaitPatroniOutcome>.Success(WaitPatroniOutcome.Waiting);
        }

        _patroniWaitSince.TryRemove(scope, out _);
        foreach (var node in shard.Nodes.Where(n => n.State != NodeState.Running))
        {
            var running = await PutAsync(NodeStateKey(cluster, shard.Name, node.Name), "RUNNING", ct);
            if (!running.IsSuccess)
                return Result<WaitPatroniOutcome>.Failed(running.Error!);
        }

        return Result<WaitPatroniOutcome>.Success(WaitPatroniOutcome.Ready);
    }

    // Д3: чистка HA-scope (Patroni бутстрапится заново): точечные initialize/leader/
    // sync + префиксы optime//members/; request_* — декларации панели — НЕ трогаем
    // (spec §3.7 Д3, arch/14 §5 A P2.2/R11). Одна чистка на scope за бюджет —
    // трекер сброшен, следующая не раньше нового бюджета.
    private async Task<Result> ResetScopeAsync(string scope, CancellationToken ct)
    {
        foreach (var key in new[] { "initialize", "leader", "sync" })
        {
            var del = await DeleteAsync($"/service/{scope}/{key}", prefix: false, ct);
            if (!del.IsSuccess)
                return del;
        }

        foreach (var prefix in new[] { $"/service/{scope}/optime/", $"/service/{scope}/members/" })
        {
            var del = await DeleteAsync(prefix, prefix: true, ct);
            if (!del.IsSuccess)
                return del;
        }

        return Result.Success();
    }

    // Адрес master-ноды шарда для SQL-фаз: master-ключ (host|имяНоды:clientPort —
    // порт ключа клиентский, SQL-порт берём из portalloc) → fallback Patroni REST.
    private async Task<NodeAddress?> ResolveMasterAsync(ShardSpec shard, ShardTopology topology, CancellationToken ct)
    {
        if (!string.IsNullOrWhiteSpace(shard.Master))
        {
            var left = shard.Master.Split(':')[0];
            var byKey = topology.Nodes.FirstOrDefault(p => p.Value.Host == left || p.Key == left);
            if (byKey.Value is not null)
                return byKey.Value;
        }

        foreach (var node in topology.Nodes.Keys)
        {
            var members = await probe.GetClusterAsync(topology.Nodes[node], ct);
            if (!members.IsSuccess)
                continue;
            // Patroni 3.x в /cluster называет мастера "leader" (legacy: "master").
            var master = members.Value.FirstOrDefault(m =>
                m.Role is "master" or "leader" or "primary" && m.State == "running");
            if (master is not null && topology.Nodes.TryGetValue(master.Name, out var addr))
                return addr;
        }

        return null;
    }

    // P2.3–P2.5: БД/роли на мастере, схемы по routing шарда, dsn (multi-host).
    private async Task<Result> ProvisionShardSqlAsync(
        ClusterSnapshot snap, ShardSpec shard, ShardTopology topology, NodeAddress master,
        AppCredentials app, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;
        var dbname = snap.Config.DbName;
        var bucketAdminUser = snap.Config.BucketAdminUser ?? "bucket_admin";
        var bucketAdminPassword = snap.Config.BucketAdminPassword ?? secrets.BucketAdminPassword;

        var adminDsn = DatabaseProvisioner.BuildAdminDsn(master.Host, master.Ports.Pg, "postgres", secrets);
        var ensured = await db.EnsureDatabaseAsync(adminDsn, dbname, ct);
        if (!ensured.IsSuccess)
            return ensured;

        var dbDsn = DatabaseProvisioner.BuildAdminDsn(master.Host, master.Ports.Pg, dbname, secrets);
        // Роли — guard-SELECT → CREATE отдельной командой (gexec-паттерн).
        foreach (var guard in DatabaseProvisioner.BuildRoleGuardsSql(secrets, app, bucketAdminUser, bucketAdminPassword))
        {
            var probe = await db.ExecuteScalarAsync(dbDsn, guard, ct);
            if (!probe.IsSuccess)
                return probe;
            if (probe.Value is string create)
            {
                var created = await db.ExecuteAsync(dbDsn, create, ct);
                if (!created.IsSuccess)
                    return created;
            }
        }

        // pg_monitor — через ExecuteAsync (DO-блок, не guard-SELECT).
        foreach (var exec in DatabaseProvisioner.BuildRoleExecSql(bucketAdminUser))
        {
            var executed = await db.ExecuteAsync(dbDsn, exec, ct);
            if (!executed.IsSuccess)
                return executed;
        }

        // Выравнивание app-роли паролю из etcd-ключа (идемпотентно; spec §4.1):
        // кластеры, созданные до app-секрета, и rebuild нод получают актуальный пароль.
        var alterApp = await db.ExecuteAsync(dbDsn, DatabaseProvisioner.BuildAlterAppPasswordSql(app), ct);
        if (!alterApp.IsSuccess)
            return alterApp;

        var bucketIds = snap.Routing
            .Where(r => r.Owner == shard.Name)
            .Select(r => r.Id)
            .OrderBy(i => i)
            .ToList();
        var schemas = await db.ExecuteAsync(
            dbDsn, DatabaseProvisioner.BuildSchemasSql(dbname, bucketIds, bucketAdminUser, app.User), ct);
        if (!schemas.IsSuccess)
            return schemas;

        // P2.5: dsn = write-эндпоинт шарда — HAProxy :5432 каждой ноды (P2):
        // multi-host по нодам в порядке имени, pg-порты из portalloc.
        // Per-cluster credentials: user+password из config кластера.
        var nodes = shard.Nodes.OrderBy(n => n.Name, StringComparer.Ordinal).ToList();
        var hosts = string.Join(",", nodes.Select(n => topology.Nodes[n.Name].Host));
        var ports = string.Join(",", nodes.Select(n => topology.Nodes[n.Name].Ports.Pg));
        var dsn = $"host={hosts} port={ports} dbname={dbname} user={bucketAdminUser} password={bucketAdminPassword}";
        if (shard.Dsn != dsn)
        {
            var dsnPut = await PutAsync($"/clusters/{cluster}/shards/{shard.Name}/dsn", dsn, ct);
            if (!dsnPut.IsSuccess)
                return dsnPut;
        }

        // P2.5' (spec §4.2, arch/14 §5 A): ensure app_params КАЖДОЙ ноды шарда —
        // put-if-absent дефолта; существующие (ручные) значения не трогаем.
        // Выполняется и при уже записанном dsn (повторные тики доводят миграцию).
        var appParamsEnsured = await appParams.EnsureShardAsync(
            cluster, shard.Name, shard.Nodes.Select(n => n.Name), ct);
        if (!appParamsEnsured.IsSuccess)
            return appParamsEnsured;

        return Result.Success();
    }

    // P3: del всех status/bucket_<i> (txn с пустым compare — безусловный, пакетами ≤128).
    private async Task<Result> ClearStatusKeysAsync(ClusterSnapshot snap, CancellationToken ct)
    {
        var statusIds = snap.Routing
            .Where(r => r.Status is not null)
            .Select(r => r.Id)
            .OrderBy(i => i)
            .ToList();
        foreach (var batch in statusIds.Chunk(TxnBatchSize))
        {
            var ops = batch
                .Select(id => new TxnOp.Delete(
                    $"/clusters/{snap.Config.Cluster}/buckets/status/bucket_{id}", Prefix: false))
                .ToList();
            var txn = await TxnAsync(TxnRequest.Of([], ops), ct);
            if (!txn.IsSuccess)
                return txn;
        }

        return Result.Success();
    }

    // P4: txn compare config.mod_revision → put канонического JSON без state (Д1).
    private async Task<Result> CommitConfigAsync(ClusterSnapshot snap, CancellationToken ct)
    {
        var key = $"/clusters/{snap.Config.Cluster}/config";
        var current = await GetAsync(key, ct);
        if (!current.IsSuccess)
            return current;
        if (current.Value is null)
            return Result.Success(); // ключа нет (внешняя очистка) — не наш случай

        var canonical = JsonSerializer.Serialize(
            new CanonicalConfig(snap.Config.Buckets, snap.Config.DbName, snap.Config.CreatedUnix),
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
                $"config {key} изменился с момента чтения (compare mod_revision не сошёлся) — ретрай тиком"));

        return Result.Success();
    }

    // R6: свежее чтение config — TO_REMOVE прекращает provisioning безопасно
    // (контейнеры подчистит deprovisioning, spec §12 R6).
    private async Task<bool> IsRemovedAsync(string cluster, CancellationToken ct)
    {
        var config = await GetAsync($"/clusters/{cluster}/config", ct);
        if (!config.IsSuccess)
            return false; // чтение не удалось — фаза всё равно под клэймом, продолжаем
        return config.Value is { } kv && kv.Value.Contains("\"TO_REMOVE\"");
    }

    // Топология шарда из полного закрепления адресов.
    private static ShardTopology Topology(
        string cluster, string shard, IReadOnlyDictionary<string, NodeAddress> addresses)
        => new(cluster, shard, $"{cluster}-{shard}",
            addresses
                .Where(p => p.Key.StartsWith($"{shard}/", StringComparison.Ordinal))
                .ToDictionary(p => p.Key.Split('/')[1], p => p.Value));

    private async Task<Result<ProcessOutcome>> Finish(
        string cluster, string phase, ProcessOutcome outcome, CancellationToken ct, RetrySeries? series = null)
    {
        // series = null (в т.ч. фаза done) — сброс контекста серии: успех чинит всё.
        var written = await journal.WritePhaseAsync(cluster, Op, phase, claims.InstanceId, null, ct, series);
        return written.IsSuccess
            ? Result<ProcessOutcome>.Success(outcome)
            : Result<ProcessOutcome>.Failed(written.Error!);
    }

    private async Task<Result<ProcessOutcome>> FailAsync(
        string cluster, Exception error, string phase, CancellationToken ct, RetrySeries? prior = null)
    {
        // Серия подряд идущих фейлов (без разбора текста — простота; spec §8.8):
        // новая ошибка после успеха начинает серию заново (series=null после Done).
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var n = prior is null ? 1 : prior.FailCount + 1;
        var shift = Math.Min(n - 1, 20);
        var delay = Math.Min(placementOpts.ProvisionRetryBaseSec * (1L << shift), placementOpts.ProvisionRetryMaxSec);
        var next = new RetrySeries(n, prior?.FailFirstUnix ?? now, now + delay);
        await journal.WritePhaseAsync(cluster, Op, phase, claims.InstanceId, error.Message, ct, next);
        return Result<ProcessOutcome>.Failed(error);
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

        // Контрактный плоский формат (spec §4.3) — см. Core.Model.Portalloc.
        return Portalloc.Parse(cluster, kv.Value);
    }

    private static string SerializePortAlloc(IReadOnlyDictionary<string, NodeAddress> addresses)
        => Portalloc.Serialize(addresses); // плоский контрактный формат §4.3

    private static string PortAllocKey(string cluster) => $"/pgworker/portalloc/{cluster}";

    private static string NodeStateKey(string cluster, string shard, string node)
        => $"/clusters/{cluster}/shards/{shard}/nodes/{node}/state";

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

    private async Task<Result> DeleteAsync(string keyOrPrefix, bool prefix, CancellationToken ct)
    {
        Result? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.DeleteAsync(endpoint, keyOrPrefix, prefix, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }

    private async Task<Result<IReadOnlyList<Kv>>> RangeAsync(string prefix, CancellationToken ct)
        => await WithFailoverAsync(endpoint => etcd.RangeAsync(endpoint, prefix, ct));

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

    // Канонический config после provisioning (Д1): state отсутствует.
    private sealed record CanonicalConfig(
        [property: JsonPropertyName("buckets")] int Buckets,
        [property: JsonPropertyName("dbname")] string DbName,
        [property: JsonPropertyName("created_unix")] long? CreatedUnix);
}
