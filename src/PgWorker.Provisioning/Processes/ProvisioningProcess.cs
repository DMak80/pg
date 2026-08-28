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
    EtcdEndpoints etcdEndpoints,
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

        // P0: journal-before-manipulations (P7).
        var started = await journal.WritePhaseAsync(cluster, Op, "started", claims.InstanceId, null, ct);
        if (!started.IsSuccess)
            return Result<ProcessOutcome>.Failed(started.Error!);

        // P1: план placement + порты, закрепление portalloc.
        var allocation = await PlanPortsAsync(snap, ct);
        if (!allocation.IsSuccess)
            return await FailAsync(cluster, allocation.Error!, "planning", ct);
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
            return await FailAsync(cluster, appCreds.Error!, "ensure-app-secret", ct);

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
            return await FailAsync(cluster, ensureError, "ensure-nodes", ct);
        if (await IsRemovedAsync(cluster, ct))
            return await Finish(cluster, "aborted", ProcessOutcome.InProgress, ct);

        // P2.2–P2.5: по каждому шарду ПАРАЛЛЕЛЬНО — ожидание Patroni, master, БД/роли/схемы, dsn.
        var shardErrors = new ConcurrentQueue<Exception>();
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
            if (!booted.Value)
                return; // InProgress — не ошибка, следующий тик

            var master = await ResolveMasterAsync(shard, topology, token);
            if (master is null)
                return; // waiting-master — InProgress

            var sqlDone = await ProvisionShardSqlAsync(snap, shard, topology, master, appCreds.Value, token);
            if (!sqlDone.IsSuccess)
                shardErrors.Enqueue(sqlDone.Error!);
        });

        if (shardErrors.TryDequeue(out var firstError))
            return await FailAsync(cluster, firstError, "shard-provision", ct);

        // Если хоть один шард не доведён (Patroni/master ещё не готовы) — InProgress.
        foreach (var shard in orderedShards)
        {
            if (shard.Nodes.Any(n => n.State != NodeState.Running))
                return await Finish(cluster, "waiting-patroni", ProcessOutcome.InProgress, ct);
            if (string.IsNullOrEmpty(shard.Dsn))
                return await Finish(cluster, "waiting-shard-sql", ProcessOutcome.InProgress, ct);
        }

        // R6 перед финальными мутациями контрол-плейна.
        if (await IsRemovedAsync(cluster, ct))
            return await Finish(cluster, "aborted", ProcessOutcome.InProgress, ct);

        // P3: снять ВСЕ status-ключи (txn-пакетами ≤128) — бакеты ACTIVE.
        var cleared = await ClearStatusKeysAsync(snap, ct);
        if (!cleared.IsSuccess)
            return await FailAsync(cluster, cleared.Error!, "clear-status", ct);

        // P4: config txn (compare mod_revision) → канонический JSON без state (Д1).
        var committed = await CommitConfigAsync(snap, ct);
        if (!committed.IsSuccess)
            return await FailAsync(cluster, committed.Error!, "committing-config", ct);

        // P5: снапшот P12 (делегат SnapshotJob, задача 22) + journal phase=done.
        if (snapshot is not null)
        {
            var shot = await snapshot(ct);
            if (!shot.IsSuccess)
                return await FailAsync(cluster, shot.Error!, "snapshot", ct);
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

    // P1: placement → порт-аллокация → закрепление /pgworker/portalloc/<C>
    // (txn compare version==0 — только создание; конкурент создал → берём его).
    private async Task<Result<IReadOnlyDictionary<string, NodeAddress>>> PlanPortsAsync(
        ClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;
        var pinned = await ReadPortAllocAsync(cluster, ct);
        if (!pinned.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(pinned.Error!);
        var existing = new Dictionary<string, NodeAddress>(pinned.Value);

        // Всё закреплено — план переиспользуется (portalloc переживает rebuild).
        var wanted = snap.Shards.SelectMany(s => s.Nodes.Select(n => $"{s.Name}/{n.Name}")).ToList();
        if (wanted.All(existing.ContainsKey))
            return await PlannedAsync(existing, cluster, ct);

        var hosts = await driver.GetHostsAsync(ct);
        if (!hosts.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(hosts.Error!);
        var busy = await driver.GetBusyPortsAsync(ct);
        if (!busy.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(busy.Error!);

        var plan = PlacementPlanner.Plan(snap.Shards, hosts.Value);
        var allocated = PortAllocator.Allocate(plan, existing, busy.Value, placementOpts.PortFrom, placementOpts.PortTo);
        if (!allocated.IsSuccess)
            return allocated;

        // Merge: закреплённое сохраняется, новые ноды добавляются.
        foreach (var (merged, addr) in allocated.Value)
            existing[merged] = addr;

        // Создание ключа — только если его нет (compare version==0); проигрыш
        // txn → перечитать актуальный (другой инстанс закрепил первым).
        var portAllocKey = PortAllocKey(cluster);
        var txn = await TxnAsync(
            TxnRequest.Of(
                [TxnCompare.NotExists(portAllocKey)],
                [new TxnOp.Put(portAllocKey, SerializePortAlloc(existing), null)]),
            ct);
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

    private async Task<Result<IReadOnlyDictionary<string, NodeAddress>>> PlannedAsync(
        Dictionary<string, NodeAddress> addresses, string cluster, CancellationToken ct)
    {
        var planned = await journal.WritePhaseAsync(cluster, Op, "planned", claims.InstanceId, null, ct);
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
    private async Task<Result<bool>> WaitPatroniAsync(
        string cluster, ShardSpec shard, ShardTopology topology, CancellationToken ct)
    {
        var scope = $"{cluster}-{shard.Name}";

        var scopeKvs = await RangeAsync($"/service/{scope}/", ct);
        if (!scopeKvs.IsSuccess)
            return Result<bool>.Failed(scopeKvs.Error!);
        var scopeState = ClusterSnapshotParser.ParseService(scopeKvs.Value).FirstOrDefault();
        var scopeReady = scopeState is { Initialized: true, LeaderName: not null };

        var probesAlive = true;
        foreach (var node in topology.Nodes.Keys)
        {
            if (!await probe.IsAliveAsync(topology.Nodes[node], ct))
            {
                probesAlive = false;
                break;
            }
        }

        if (!scopeReady || !probesAlive)
        {
            var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            // GetOrAdd — атомарно при параллельных тиках разных кластеров (rework №1).
            var since = _patroniWaitSince.GetOrAdd(scope, now);
            if (now - since > placementOpts.PatroniBootSec)
                return Result<bool>.Failed(new ApplicationException(
                    $"Patroni шарда {scope} не поднялся за бюджет {placementOpts.PatroniBootSec} с"));

            return Result<bool>.Success(false);
        }

        _patroniWaitSince.TryRemove(scope, out _);
        foreach (var node in shard.Nodes.Where(n => n.State != NodeState.Running))
        {
            var running = await PutAsync(NodeStateKey(cluster, shard.Name, node.Name), "RUNNING", ct);
            if (!running.IsSuccess)
                return Result<bool>.Failed(running.Error!);
        }

        return Result<bool>.Success(true);
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
            return await PutAsync($"/clusters/{cluster}/shards/{shard.Name}/dsn", dsn, ct);

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
        string cluster, string phase, ProcessOutcome outcome, CancellationToken ct)
    {
        var written = await journal.WritePhaseAsync(cluster, Op, phase, claims.InstanceId, null, ct);
        return written.IsSuccess
            ? Result<ProcessOutcome>.Success(outcome)
            : Result<ProcessOutcome>.Failed(written.Error!);
    }

    private async Task<Result<ProcessOutcome>> FailAsync(
        string cluster, Exception error, string phase, CancellationToken ct)
    {
        await journal.WritePhaseAsync(cluster, Op, phase, claims.InstanceId, error.Message, ct);
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
