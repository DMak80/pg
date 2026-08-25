using System.Collections.Concurrent;
using System.Text.RegularExpressions;
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
/// AddShardProcess — подъём ОТДЕЛЬНОГО пустого шарда в Active-кластере
/// (t06 spec §5.2; arch/14 §5 G). Панель заявила декларацию (replicas +
/// nodes/NOT_INITIALIZED + request_*); процесс доводит шард до dsn/RUNNING,
/// НЕ трогая routing/status/схемы бакетов (граница §2.1). Механика —
/// ProvisioningProcess в scoped-to-shard виде; идемпотентность каждого шага,
/// R6-перечитывание config, фазы в /pgworker/work/&lt;C&gt;.
/// </summary>
public sealed partial class AddShardProcess(
    IEtcdGateway etcd,
    string[] endpoints,
    IClusterDriver driver,
    ISqlExecutor db,
    ShardProbe probe,
    ClaimStore claims,
    WorkJournal journal,
    PlacementOptions placementOpts,
    InstallSecrets secrets,
    EtcdEndpoints etcdEndpoints,
    Func<CancellationToken, Task<Result>>? snapshot = null)
{
    private const string Op = "add-shard";

    // Паттерн имени шарда (t06 §4.1): без дефиса — scope <C>-<X> и имена нод однозначны.
    [GeneratedRegex("^[a-z][a-z0-9_]{0,30}$")]
    private static partial Regex ShardNamePattern();

    // Время первого наблюдения «scope без живого Patroni» (бюджет PatroniBootSec).
    private readonly ConcurrentDictionary<string, long> _patroniWaitSince = new();

    public async Task<Result<ProcessOutcome>> TickAsync(ClusterSnapshot snap, string shardName, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;

        // Мутации — только держателем живого клэйма (инвариант §4.3).
        if (!claims.IsMine(cluster))
            return Result<ProcessOutcome>.Failed(new ApplicationException(
                $"add-shard {cluster}/{shardName}: клэйм не наш (или потерян) — мутации запрещены"));

        var shard = snap.Shards.FirstOrDefault(s => s.Name == shardName);
        if (shard is null)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done); // шарда уже нет

        // A0: journal-before-manipulations (P7).
        var started = await journal.WritePhaseAsync(cluster, Op, "started", claims.InstanceId, null, ct);
        if (!started.IsSuccess)
            return Result<ProcessOutcome>.Failed(started.Error!);

        // A1: guard'ы add (§4.4). R6: свежий config — смена state прекращает add.
        if (await ClusterStateChangedAsync(cluster, ct))
            return await Finish(cluster, "aborted", ProcessOutcome.InProgress, ct);
        if (shard.Dsn is not null)
            return await Finish(cluster, "done", ProcessOutcome.Done, ct); // уже поднят
        if (shard.ToRemove)
            return await Finish(cluster, "blocked-removing", ProcessOutcome.InProgress, ct);
        if (!ShardNamePattern().IsMatch(shardName))
            return await FailAsync(cluster,
                new ApplicationException(
                    $"имя шарда '{shardName}' неканоническое (^[a-z][a-z0-9_]{{0,30}}$) — разбор оператором (etcdctl)"),
                "invalid-name", ct);
        var scope = $"{cluster}-{shardName}";
        // Guard коллизии имён (§4.4): initialize scope'а с ЧУЖИМ лидером (или без
        // лидера) = живой чужой Patroni-кластер. Leader из ИМЁН нод шарда — наш
        // поднимающийся Patroni (идемпотентность повторных тиков после A3).
        var scopeState = await ReadScopeStateAsync(scope, ct);
        if (!scopeState.IsSuccess)
            return await FailAsync(cluster, scopeState.Error!, "scope-check", ct);
        if (scopeState.Value is { Initialized: true } takenState
            && (takenState.LeaderName is null || shard.Nodes.All(n => n.Name != takenState.LeaderName)))
            return await FailAsync(cluster,
                new ApplicationException(
                    $"scope {scope} занят живым Patroni-кластером (initialize существует) — коллизия имён, разбор оператором"),
                "scope-taken", ct);
        if (!IsFullyDeclared(shard))
            return await Finish(cluster, "waiting-keys", ProcessOutcome.InProgress, ct);

        // A2: план placement (только ноды нового шарда; занятость живыми шардами —
        // UsedSlots хостов + busy-порты драйвера) + порт-аллокация; merge в
        // существующий /pgworker/portalloc/<C> (read-modify-write под клэймом).
        var planned = await PlanShardPortsAsync(cluster, shard, ct);
        if (!planned.IsSuccess)
            return await FailAsync(cluster, planned.Error!, "planning", ct);
        var topology = Topology(cluster, shardName, planned.Value);

        // A3: EnsureNode каждой ноды + state=PROVISIONING (идемпотентно).
        var resources = await ReadShardResourcesAsync(cluster, shardName, ct);
        var clusterSecrets = secrets with
        {
            BucketAdminUser = snap.Config.BucketAdminUser ?? secrets.BucketAdminUser,
            BucketAdminPassword = snap.Config.BucketAdminPassword ?? secrets.BucketAdminPassword,
        };
        var ensured = await EnsureNodesAsync(cluster, shard, topology, resources, clusterSecrets, ct);
        if (!ensured.IsSuccess)
            return await FailAsync(cluster, ensured.Error!, "ensure-nodes", ct);

        // R6 перед ожиданиями/SQL.
        if (await ClusterStateChangedAsync(cluster, ct))
            return await Finish(cluster, "aborted", ProcessOutcome.InProgress, ct);

        // A4: ждать Patroni (scope initialize+leader + REST всех нод) → RUNNING.
        var booted = await WaitPatroniAsync(cluster, shard, topology, ct);
        if (!booted.IsSuccess)
            return await FailAsync(cluster, booted.Error!, "waiting-patroni", ct);
        if (!booted.Value)
            return await Finish(cluster, "waiting-patroni", ProcessOutcome.InProgress, ct);

        var master = await ResolveMasterAsync(shard, topology, ct);
        if (master is null)
            return await Finish(cluster, "waiting-master", ProcessOutcome.InProgress, ct);

        // A5: БД/роли на мастере НОВОГО шарда; СХЕМЫ БАКЕТОВ НЕ СОЗДАЮТСЯ (§2.1);
        // dsn multi-host (порты portalloc, без пароля).
        var sqlDone = await ProvisionShardSqlAsync(snap, shard, topology, master, ct);
        if (!sqlDone.IsSuccess)
            return await FailAsync(cluster, sqlDone.Error!, "sql", ct);

        // A6: снапшот P12 (точка изменения) + journal done — шард в надзоре.
        if (snapshot is not null)
        {
            var shot = await snapshot(ct);
            if (!shot.IsSuccess)
                return await FailAsync(cluster, shot.Error!, "snapshot", ct);
        }

        return await Finish(cluster, "done", ProcessOutcome.Done, ct);
    }

    // A1: полное объявление шарда (панель доустанила ключи? ждём — waiting-keys).
    private static bool IsFullyDeclared(ShardSpec shard) =>
        shard.Replicas > 0
        && shard.Nodes.Count == shard.Replicas
        && shard.Nodes.All(n => n.State is NodeState.NotInitialized or NodeState.Provisioning);

    // R6: перечитывание config — NOT_INITIALIZED/TO_REMOVE безопасно прекращает add
    // (provisioning поднимет декларацию как обычный шард / deprovisioning снесёт).
    private async Task<bool> ClusterStateChangedAsync(string cluster, CancellationToken ct)
    {
        var config = await GetAsync($"/clusters/{cluster}/config", ct);
        if (!config.IsSuccess || config.Value is not { } kv)
            return false;
        return kv.Value.Contains("\"NOT_INITIALIZED\"") || kv.Value.Contains("\"TO_REMOVE\"");
    }

    // A2: порт-аллокация нод нового шарда; merge в существующий portalloc
    // (просто put — read-modify-write под клэймом, Д10).
    private async Task<Result<IReadOnlyDictionary<string, NodeAddress>>> PlanShardPortsAsync(
        string cluster, ShardSpec shard, CancellationToken ct)
    {
        var pinned = await ReadPortAllocAsync(cluster, ct);
        if (!pinned.IsSuccess)
            return pinned;
        var existing = new Dictionary<string, NodeAddress>(pinned.Value);

        // Всё закреплено — план переиспользуется (portalloc переживает rebuild).
        var wanted = shard.Nodes.Select(n => $"{shard.Name}/{n.Name}").ToList();
        if (wanted.All(existing.ContainsKey))
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(existing);

        var hosts = await driver.GetHostsAsync(ct);
        if (!hosts.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(hosts.Error!);
        var busy = await driver.GetBusyPortsAsync(ct);
        if (!busy.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(busy.Error!);

        // Список из ОДНОГО шарда: анти-аффинити внутри нового; занятость живыми
        // шардами уже учтена (UsedSlots хостов + фактические busy-порты драйвера).
        var plan = PlacementPlanner.Plan([shard], hosts.Value);
        var allocated = PortAllocator.Allocate(
            plan, existing, busy.Value, placementOpts.PortFrom, placementOpts.PortTo);
        if (!allocated.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(new ApplicationException(
                $"порт-диапазон исчерпан — расширьте PortRange (PgWorker:Docker:PortRange): {allocated.Error!.Message}"));

        foreach (var (merged, addr) in allocated.Value)
            existing[merged] = addr;

        var put = await PutAsync(PortAllocKey(cluster), Portalloc.Serialize(existing), ct);
        if (!put.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(put.Error!);

        var plannedPhase = await journal.WritePhaseAsync(cluster, Op, "planned", claims.InstanceId, null, ct);
        return plannedPhase.IsSuccess
            ? Result<IReadOnlyDictionary<string, NodeAddress>>.Success(existing)
            : Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(plannedPhase.Error!);
    }

    // Состояние HA-scope Patroni (initialize/leader) для guard'а коллизии имён.
    private async Task<Result<ClusterSnapshotParser.HaScopeState?>> ReadScopeStateAsync(
        string scope, CancellationToken ct)
    {
        var kvs = await RangeAsync($"/service/{scope}/", ct);
        if (!kvs.IsSuccess)
            return Result<ClusterSnapshotParser.HaScopeState?>.Failed(kvs.Error!);
        return Result<ClusterSnapshotParser.HaScopeState?>.Success(
            ClusterSnapshotParser.ParseService(kvs.Value).FirstOrDefault());
    }

    // A3: EnsureNode всех нод шарда (state != RUNNING) + state=PROVISIONING.
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

    // Заявки ресурсов нод шарда: /service/<scope>/request_{cpu,mem} → лимиты.
    private async Task<NodeResources?> ReadShardResourcesAsync(
        string cluster, string shardName, CancellationToken ct)
    {
        var scope = $"{cluster}-{shardName}";
        var cpu = await GetAsync($"/service/{scope}/request_cpu", ct);
        if (!cpu.IsSuccess)
            return null;
        var mem = await GetAsync($"/service/{scope}/request_mem", ct);
        return mem.IsSuccess ? NodeResourcesParser.Parse(cpu.Value?.Value, mem.Value?.Value) : null;
    }

    // A4: scope initialized + leader + Patroni REST всех нод отвечает →
    // nodes/<n>/state=RUNNING; иначе InProgress (бюджет PatroniBootSec).
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

    // Адрес master-ноды шарда для SQL-фаз: master-ключ → fallback Patroni REST.
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
            var master = members.Value.FirstOrDefault(m =>
                m.Role is "master" or "leader" or "primary" && m.State == "running");
            if (master is not null && topology.Nodes.TryGetValue(master.Name, out var addr))
                return addr;
        }

        return null;
    }

    // A5: БД/роли — ТОЛЬКО (идемпотентные гварды); СХЕМЫ БАКЕТОВ НЕ СОЗДАЮТСЯ:
    // шард стартует пустым (§2.1) — routing на него не указывает. dsn — multi-host.
    private async Task<Result> ProvisionShardSqlAsync(
        ClusterSnapshot snap, ShardSpec shard, ShardTopology topology, NodeAddress master, CancellationToken ct)
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
        foreach (var guard in DatabaseProvisioner.BuildRoleGuardsSql(secrets, bucketAdminUser, bucketAdminPassword))
        {
            var probeResult = await db.ExecuteScalarAsync(dbDsn, guard, ct);
            if (!probeResult.IsSuccess)
                return probeResult;
            if (probeResult.Value is string create)
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

        // НИКАКИХ BuildSchemasSql/routing/status-записей (граница §2.1).

        var nodes = shard.Nodes.OrderBy(n => n.Name, StringComparer.Ordinal).ToList();
        var hosts = string.Join(",", nodes.Select(n => topology.Nodes[n.Name].Host));
        var ports = string.Join(",", nodes.Select(n => topology.Nodes[n.Name].Ports.Pg));
        var dsn = $"host={hosts} port={ports} dbname={dbname} user={bucketAdminUser} password={bucketAdminPassword}";
        if (shard.Dsn == dsn)
            return Result.Success();
        return await PutAsync($"/clusters/{cluster}/shards/{shard.Name}/dsn", dsn, ct);
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

        return Portalloc.Parse(cluster, kv.Value);
    }

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

    private async Task<Result<IReadOnlyList<Kv>>> RangeAsync(string prefix, CancellationToken ct)
        => await WithFailoverAsync(endpoint => etcd.RangeAsync(endpoint, prefix, ct));

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
}
