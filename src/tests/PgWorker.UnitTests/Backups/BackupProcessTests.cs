using Microsoft.Extensions.Logging.Abstractions;
using PgWorker.Backups;
using PgWorker.Backups.Job;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Planning;
using PgWorker.Core.Templates;
using PgWorker.Core.Tuning;
using PgWorker.Docker.Drivers;
using PgWorker.Docker.Engine;
using Shared.Etcd.Client;
using PgWorker.Etcd.Coordination;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Processes;
using PgWorker.Provisioning.Probes;
using PgWorker.Provisioning.Sql;
using PgWorker.UnitTests.Provisioning;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Планировщик полных бэкапов (t02, arch/19 §2): G0 выключен → no-op; G1
// ensure backup_password; G2 гвард роли backup_exec — КАЖДЫЙ тик (до
// due-гвардов); G3 при due+бэкоффе: PLANNED → джоб-контейнер → RUNNING
// (journal-before-manipulations). Клэйм-инвариант мутаций префикса.
public class BackupProcessTests
{
    private const string Ep = "http://etcd:2379";
    private static readonly InstallSecrets Secrets = new("su-pw", "sb-pw", "adm-pw", "mov-pw");

    private static long Unix(DateTimeOffset t) => t.ToUnixTimeSeconds();

    private sealed class DeadHandler : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken ct)
            => Task.FromResult(new HttpResponseMessage(System.Net.HttpStatusCode.ServiceUnavailable));
    }

    // Сид: Active-кластер shop, шард shard1 с dsn + master-ключ (мастер —
    // shard1a на h1:15000) + portalloc (формат ClusterSecretRotatorTests).
    private static void SeedCluster(Fakes.FakeEtcd etcd)
    {
        etcd.Seed("/clusters/shop/config",
            """{"buckets":1,"dbname":"shop","created_unix":1755900000}""");
        etcd.Seed("/clusters/shop/shards/shard1/replicas", "2");
        etcd.Seed("/clusters/shop/shards/shard1/nodes/shard1a/state", "RUNNING");
        etcd.Seed("/clusters/shop/shards/shard1/nodes/shard1b/state", "RUNNING");
        etcd.Seed("/clusters/shop/shards/shard1/dsn",
            "host=h1 port=15000 dbname=shop user=bucket_admin password=x");
        etcd.Seed("/clusters/shop/shards/shard1/master", "h1:16500");
        etcd.Seed("/clusters/shop/shards/shard1/nodes/shard1a/app_params", "sslmode=require");
        etcd.Seed("/clusters/shop/shards/shard1/nodes/shard1b/app_params", "sslmode=require");

        var alloc = new Dictionary<string, NodeAddress>
        {
            ["shard1/shard1a"] = new("h1", new NodePorts(15000, 18000, 16500)),
            ["shard1/shard1b"] = new("h2", new NodePorts(15002, 18002, 16502)),
        };
        etcd.Seed("/pgworker/portalloc/shop", Portalloc.Serialize(alloc));
        etcd.Seed("/clusters/shop/buckets/routing/bucket_0", "shard1");
    }

    private static async Task<ClusterSnapshot> Snapshot(Fakes.FakeEtcd etcd)
    {
        var range = await etcd.RangeAsync(Ep, "/clusters/", CancellationToken.None);
        var parsed = ClusterSnapshotParser.ParseClusters(range.Value, out _);
        return parsed.Value.Single(c => c.Config.Cluster == "shop");
    }

    // Бэкапы шарда для параметра тика (тик читает префикс из аргумента, не из etcd).
    private static IReadOnlyList<ClusterBackups> BackupsOf(params FullBackupState[] fulls)
        => [new ClusterBackups("shop", null,
            new Dictionary<string, ShardBackups> { ["shard1"] = new(fulls, null) })];

    internal sealed class FakeBackupEngine : IDockerEngine
    {
        internal sealed record ContainerRec(string Id, string State, int ExitCode, string Logs);

        public readonly Dictionary<string, ContainerRec> Containers = [];
        public readonly List<(string Name, ContainerSpec Spec)> Created = [];
        public readonly List<string> Started = [];
        public readonly List<string> Removed = [];
        public readonly List<string> RemovedVolumes = [];

        // transport-отказы docker (S-ветки): список/логи/инспект — статус не меняем.
        public bool ListFails { get; set; }
        public bool LogsFails { get; set; }
        public bool InspectFails { get; set; }

        public Task<Result> PingAsync(CancellationToken ct) => Task.FromResult(Result.Success());

        public Task<Result<IReadOnlyList<DockerContainer>>> ListContainersAsync(
            string namePrefix, bool all, CancellationToken ct)
        {
            if (ListFails)
                return Task.FromResult(Result<IReadOnlyList<DockerContainer>>.Failed(
                    new ApplicationException("docker: list failed")));
            // Names — БЕЗ ведущего "/" (реальный движок триммит, матчинг по имени)
            var list = Containers
                .Where(p => p.Key.Contains(namePrefix, StringComparison.Ordinal))
                .Select(p => new DockerContainer(p.Value.Id, [p.Key], p.Value.State, "img"))
                .ToList();
            return Task.FromResult(Result<IReadOnlyList<DockerContainer>>.Success(
                (IReadOnlyList<DockerContainer>)list));
        }

        public Task<Result<DockerContainerInspect>> InspectContainerAsync(string id, CancellationToken ct)
        {
            if (InspectFails)
                return Task.FromResult(Result<DockerContainerInspect>.Failed(
                    new ApplicationException("docker: inspect failed")));
            var found = Containers.FirstOrDefault(p => p.Value.Id == id);
            return Task.FromResult(found.Key is null
                ? Result<DockerContainerInspect>.Failed(new KeyNotFoundException(id))
                : Result<DockerContainerInspect>.Success(new DockerContainerInspect(
                    id, found.Key, [], [], [],
                    found.Value.State == "running", found.Value.ExitCode)));
        }

        public Task<Result<string>> GetContainerLogsAsync(string idOrName, int tail, CancellationToken ct)
        {
            if (LogsFails)
                return Task.FromResult(Result<string>.Failed(new ApplicationException("docker: logs failed")));
            return Task.FromResult(Containers.TryGetValue(idOrName, out var rec)
                ? Result<string>.Success(rec.Logs)
                : Result<string>.Failed(new KeyNotFoundException(idOrName)));
        }

        public Task<Result> CreateContainerAsync(ContainerSpec spec, string name, CancellationToken ct)
        {
            Created.Add((name, spec));
            Containers[name] = new ContainerRec(Guid.NewGuid().ToString("N"), "created", -1, "");
            return Task.FromResult(Result.Success());
        }

        public Task<Result> StartContainerAsync(string idOrName, CancellationToken ct)
        {
            Started.Add(idOrName);
            if (Containers.TryGetValue(idOrName, out var rec))
                Containers[idOrName] = rec with { State = "running" };
            return Task.FromResult(Result.Success());
        }

        public Task<Result> StopContainerAsync(string idOrName, int timeoutSec, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result> RemoveContainerAsync(string idOrName, bool force, CancellationToken ct)
        {
            Removed.Add(idOrName);
            Containers.Remove(idOrName);
            return Task.FromResult(Result.Success());
        }

        public Task<Result> RemoveVolumeAsync(string name, CancellationToken ct)
        {
            RemovedVolumes.Add(name);
            return Task.FromResult(Result.Success());
        }

        public Task<Result<string>> ExecAsync(string containerId, IReadOnlyList<string> cmd, CancellationToken ct)
            => Task.FromResult(Result<string>.Success(string.Empty));

        public Task<Result> EnsureNetworkAsync(string name, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result<IReadOnlyList<DockerSwarmNode>>> ListNodesAsync(CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<DockerSwarmNode>>.Success([]));

        public Task<Result> CreateServiceAsync(ServiceSpec spec, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result> RemoveServiceAsync(string name, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result<IReadOnlyList<string>>> ListServicesAsync(string namePrefix, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<string>>.Success([]));

        public Task<Result<IReadOnlyList<DockerTask>>> ListTasksAsync(string serviceName, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<DockerTask>>.Success([]));

        public Task<Result<IReadOnlySet<(string Host, int Port)>>> BusyPortsAsync(CancellationToken ct)
            => Task.FromResult(Result<IReadOnlySet<(string Host, int Port)>>.Success(
                (IReadOnlySet<(string Host, int Port)>)new HashSet<(string, int)>()));

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    // Драйвер с единственным хостом h1: джобы бэкапов создаются его движком.
    internal sealed class FakeBackupDriver(FakeBackupEngine engine) : IClusterDriver
    {
        public bool SupportsRunningInspection => true;

        public IDockerEngine? EngineFor(string host) => engine;

        // pg_hba-гвард (t02 G2): exec-патч по нодам шарда — помним вызовы.
        public readonly List<(string Shard, string Node, IReadOnlyList<string> Cmd)> ExecNodeCalls = [];

        // pg_hba-гвард усвоенных нод: exec по имени чужого контейнера (object).
        public readonly List<(string Container, IReadOnlyList<string> Cmd)> ContainerExecCalls = [];

        public Task<Result<string>> ExecNodeAsync(string cluster, string shard, string node,
            IReadOnlyList<string> cmd, CancellationToken ct)
        {
            ExecNodeCalls.Add((shard, node, cmd));
            return Task.FromResult(Result<string>.Success(string.Empty));
        }

        public Task<Result<string>> ExecContainerAsync(string containerName, IReadOnlyList<string> cmd, CancellationToken ct)
        {
            ContainerExecCalls.Add((containerName, cmd));
            return Task.FromResult(Result<string>.Success(string.Empty));
        }

        public Task<Result> RemoveBackupJobsAsync(string cluster, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result> RemoveRestoreJobsAsync(string cluster, string shard, CancellationToken ct)
            => Task.FromResult(Result.Success());

        private static NotSupportedException NotSupported() => new("не используется в тестах бэкапов");

        public Task<Result<IReadOnlyList<HostInfo>>> GetHostsAsync(CancellationToken ct) => throw NotSupported();
        public Task<Result<IReadOnlySet<(string Host, int Port)>>> GetBusyPortsAsync(CancellationToken ct) => throw NotSupported();
        public Task<Result> EnsureNodeAsync(ShardTopology topology, string nodeName, NodeAddress addr,
            InstallSecrets secrets, EtcdEndpoints etcd, NodeResources? resources, PgTuneResult? tuning, CancellationToken ct) => throw NotSupported();
        public Task<Result> RemoveNodeAsync(string cluster, string shard, string nodeName, CancellationToken ct) => throw NotSupported();
        public Task<Result> StopNodeAsync(string cluster, string shard, string nodeName, CancellationToken ct) => throw NotSupported();
        public Task<Result<DataPresence>> NodeDataPresenceAsync(string cluster, string shard, string node, CancellationToken ct) => throw NotSupported();
        public Task<Result<IReadOnlyDictionary<string, DiscoveredNode>>> InspectNodesAsync(
            string cluster, IReadOnlyCollection<string> nodeNames, CancellationToken ct) => throw NotSupported();
        // ExecContainerAsync — реализован выше (pg_hba-гвард object-нод)
        public Task<Result<IReadOnlyList<string>>> ListNodeObjectsAsync(string cluster, CancellationToken ct) => throw NotSupported();

        // WAL-агенты (t03): в тестах джобов не используются.
        public Task<Result> EnsureBackupAgentAsync(
            string cluster, string shard, ContainerSpec spec, string host, CancellationToken ct) => throw NotSupported();
        public Task<Result> RemoveBackupAgentsAsync(string cluster, string? shard, CancellationToken ct) => throw NotSupported();
        public Task<Result<IReadOnlyList<DockerContainer>>> ListBackupAgentsAsync(string cluster, CancellationToken ct) => throw NotSupported();
    }

    // G1-двойник: пароль backup_exec фиксированный, вызовы считаются (G0-кейс).
    internal sealed class FakeSecretEnsurer : IClusterSecretEnsurer
    {
        public int Calls { get; private set; }

        public Task<Result<ClusterCredentials>> EnsureAsync(string cluster, ClusterConfig config, CancellationToken ct)
        {
            Calls++;
            return Task.FromResult(Result<ClusterCredentials>.Success(new ClusterCredentials(
                new AppCredentials("app", "app-pw"), "mover-pw",
                new AppCredentials("bucket_admin", "admin-pw"), "pw0000000000000000000000000000A")));
        }
    }

    // SQL-двойник: скаляр-гвард по умолчанию → null («роль уже есть»).
    internal sealed class FakeSql : ISqlExecutor
    {
        public readonly List<(string Dsn, string Sql)> Executed = [];
        public readonly List<(string Dsn, string Sql)> Scalars = [];

        public Task<Result> ExecuteAsync(string dsn, string sql, CancellationToken ct)
        {
            Executed.Add((dsn, sql));
            return Task.FromResult(Result.Success());
        }

        public Task<Result<object?>> ExecuteScalarAsync(string dsn, string sql, CancellationToken ct)
        {
            Scalars.Add((dsn, sql));
            return Task.FromResult(Result<object?>.Success(null));
        }

        public Task<Result> EnsureDatabaseAsync(string dsn, string dbname, CancellationToken ct)
            => Task.FromResult(Result.Success());
    }

    private sealed record Rig(Fakes.FakeEtcd Etcd, FakeSql Sql, FakeBackupEngine Engine,
        FakeBackupDriver Driver, FakeSecretEnsurer Ensurer, ClaimStore Claims,
        WorkJournal Journal, BackupProcess Process);

    private static async Task<Rig> NewRig(
        bool claim = true, bool seedPortalloc = true, BackupsRuntimeOptions? options = null,
        Fakes.FakeEtcd? etcdOverride = null)
    {
        var store = etcdOverride ?? new Fakes.FakeEtcd();
        SeedCluster(store);
        if (!seedPortalloc)
            store.Store.Remove("/pgworker/portalloc/shop");
        var sql = new FakeSql();
        var engine = new FakeBackupEngine();
        var ensurer = new FakeSecretEnsurer();
        var driver = new FakeBackupDriver(engine);
        var claims = new ClaimStore([Ep], store, TimeProvider.System);
        if (claim)
            await claims.TryClaimClusterAsync("shop", CancellationToken.None);
        store.Txns.Clear(); // отсечь claim-txn: ассерты — только про тик бэкапов
        var journal = new WorkJournal(store, [Ep]);
        var probe = new ShardProbe(new HttpClient(new DeadHandler()));
        var process = new BackupProcess(
            store, [Ep], driver,
            new ShardEndpoints(store, [Ep], probe), sql, ensurer,
            claims, journal, Secrets, options ?? new BackupsRuntimeOptions { Enabled = true },
            TimeProvider.System, NullLogger<BackupProcess>.Instance, snapshot: null);
        return new Rig(store, sql, engine, driver, ensurer, claims, journal, process);
    }

    // AAA: G0 — подсистема выключена: тик Done без ensure и мутаций префикса
    [Fact]
    public async Task Disabled_Noop()
    {
        // Arrange — Enabled=false (дефолт поставки)
        var rig = await NewRig(options: new BackupsRuntimeOptions { Enabled = false });

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), [], CancellationToken.None);

        // Assert — Done; ensure не вызван; префикс /pgworker/backups/ пуст
        outcome.IsSuccess.Should().BeTrue();
        outcome.Value.Should().Be(ProcessOutcome.Done);
        rig.Ensurer.Calls.Should().Be(0);
        rig.Etcd.Store.Keys.Should().NotContain(k => k.StartsWith("/pgworker/backups/"));
    }

    // AAA: G3 — due (COMPLETED нет): PLANNED → контейнер (env backup_exec,
    // extra_hosts) → RUNNING; journal phase=started
    [Fact]
    public async Task DueNoCompleted_PlansRunsAndStartsContainer()
    {
        // Arrange — активный кластер, бэкапов ещё не было
        var rig = await NewRig();

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), [], CancellationToken.None);

        // Assert — запись RUNNING с node/role источника (мастер shard1a —
        // Patroni недоступен, fallback мастер-ключа)
        outcome.IsSuccess.Should().BeTrue(outcome.Error?.ToString());
        var fullKey = rig.Etcd.Store.Keys.Single(k => k.StartsWith("/pgworker/backups/shop/shard1/full/"));
        var id = fullKey.Split('/')[^1];
        var parsed = BackupsParser.Parse(
            (IReadOnlyList<Kv>)[new Kv(fullKey, rig.Etcd.Store[fullKey].Value, 1)], out var errors);
        errors.Should().BeEmpty();
        var full = parsed.Value[0].Shards["shard1"].Full.Single();
        full.State.Should().Be(FullBackupStatus.Running);
        full.Node.Should().Be("shard1a");
        full.Role.Should().Be(BackupSourceRole.Master);

        // джоб создан и запущен: env от backup_exec + host-gateway
        var (name, spec) = rig.Engine.Created.Should().ContainSingle().Subject;
        name.Should().Be(BackupNames.ContainerName("shop", "shard1", id));
        spec.Env["PGW_BK_DSN"].Should().Contain("user=backup_exec password=pw0000000000000000000000000000A");
        spec.ExtraHosts.Should().Contain("host.docker.internal:host-gateway");
        rig.Engine.Started.Should().Contain(name);

        // pg_hba-гвард прошёл по обеим нодам шарда (G2 каждый тик)
        rig.Driver.ExecNodeCalls.Select(c => c.Node).Should().BeEquivalentTo(["shard1a", "shard1b"]);

        // journal-before-manipulations: phase started/shard1/<id>
        (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.Phase
            .Should().Be($"started/shard1/{id}");
    }

    // AAA: pg_hba-гвард на усвоенной ноде (object) — exec в её контейнер,
    // а не по pgw-имени (у стендовых/усвоенных нод pgw-контейнера нет)
    [Fact]
    public async Task AdoptedNodes_HbaGuard_ExecsObjectContainer()
    {
        // Arrange — portalloc с object-именами (усвоенные ноды, arch/14 §5 J);
        // сид поверх NewRig (иначе SeedCluster перезапишет portalloc/master)
        var etcd = new Fakes.FakeEtcd();
        var rig = await NewRig(etcdOverride: etcd);
        var alloc = new Dictionary<string, NodeAddress>
        {
            ["shard1/shard1a"] = new("local", new NodePorts(15433, 18011, 0), Object: "as-shard1a"),
            ["shard1/shard1b"] = new("local", new NodePorts(15434, 18012, 0), Object: "as-shard1b"),
        };
        etcd.Seed("/pgworker/portalloc/shop", Portalloc.Serialize(alloc));
        // master-ключ усвоенного стенда: <имя ноды>:<порт> (doorman=0 — ключ
        // резолвится byName; E2eScenarios, arch/14 §2.4 п.5)
        etcd.Seed("/clusters/shop/shards/shard1/master", "shard1a:0");

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(etcd), [], CancellationToken.None);

        // Assert — exec ушёл в object-контейнеры, pgw-путь не звался
        outcome.IsSuccess.Should().BeTrue(outcome.Error?.ToString());
        outcome.Value.Should().Be(ProcessOutcome.Done);
        rig.Sql.Scalars.Should().NotBeEmpty("гвард G2 исполнен (мастер резолвится по byName)");
        rig.Driver.ContainerExecCalls.Select(c => c.Container).Should().BeEquivalentTo(["as-shard1a", "as-shard1b"]);
        rig.Driver.ExecNodeCalls.Should().BeEmpty();
    }

    // AAA (AC5): RUNNING-полный с «вечным» контейнером старше 6 ч → контейнер/volume
    // удалены, статус FAILED "job-timeout…", переснятие по бэкоффу
    [Fact]
    public async Task Супервиз_полный_старше_бюджета_FAILED_jobtimeout_и_переснятие()
    {
        // Arrange — активный RUNNING started = now-7h (бюджет 6 ч дефолт);
        // FakeBackupEngine держит running-контейнер без result-логов («вечный»)
        var rig = await NewRig();
        var now = TimeProvider.System.GetUtcNow();
        var staleStarted = Unix(now.AddHours(-7));
        var active = new FullBackupState("20260908030000Z", FullBackupStatus.Running,
            "shard1a", BackupSourceRole.Master, staleStarted, null, null, null, null, null);
        var name = BackupNames.ContainerName("shop", "shard1", "20260908030000Z");
        rig.Engine.Containers[name] = new("cnt-timeout", "running", -1, "{\"phase\":\"basebackup\"}");

        // Act 1 — тик супервиза
        var outcome = await rig.Process.TickAsync(
            await Snapshot(rig.Etcd), BackupsOf(active), CancellationToken.None);

        // Assert 1 — статус FAILED с error "job-timeout"; контейнер и volume
        // удалены; journal содержит phase job-timeout/<shard>/<id>
        outcome.IsSuccess.Should().BeTrue(outcome.Error?.ToString());
        var fullKey = FullKey("20260908030000Z");
        var parsed = BackupsParser.Parse(
            (IReadOnlyList<Kv>)[new Kv(fullKey, rig.Etcd.Store[fullKey].Value, 1)], out var errors);
        errors.Should().BeEmpty();
        var failed = parsed.Value[0].Shards["shard1"].Full.Single();
        failed.State.Should().Be(FullBackupStatus.Failed);
        failed.Error.Should().Contain("job-timeout");
        rig.Engine.Removed.Should().Contain(name);
        rig.Engine.RemovedVolumes.Should().Contain(BackupNames.VolumeName("shop", "shard1", "20260908030000Z"));
        (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.Phase
            .Should().Be("job-timeout/shard1/20260908030000Z");

        // Act 2 — повторный тик: FAILED-попытка 7-часовой давности — бэкофф
        // (Base·2^0 от started) давно прошёл → переснятие НОВЫМ id
        var outcome2 = await rig.Process.TickAsync(
            await Snapshot(rig.Etcd), BackupsOf(failed), CancellationToken.None);

        // Assert 2 — новый PLANNED/RUNNING с другим id (переснятие по общему правилу)
        outcome2.IsSuccess.Should().BeTrue();
        rig.Etcd.Store.Keys
            .Where(k => k.StartsWith("/pgworker/backups/shop/shard1/full/"))
            .Should().HaveCount(2, "переснятие — НОВЫЙ id, старая FAILED-запись — история");
    }

    // AAA: инвариант одного активного — при RUNNING (живой джоб) новую попытку
    // не создаём: супервизия поллит, G3 молчит
    [Fact]
    public async Task ActiveExists_DoesNotPlanSecond()
    {
        // Arrange — активная RUNNING-попытка + живой контейнер (поллинг без мутаций)
        var rig = await NewRig();
        var now = TimeProvider.System.GetUtcNow();
        var active = new FullBackupState("20260908030000Z", FullBackupStatus.Running,
            "shard1a", BackupSourceRole.Master, Unix(now.AddMinutes(-2)), null, null, null, null, null);
        var name = BackupNames.ContainerName("shop", "shard1", "20260908030000Z");
        rig.Engine.Containers[name] = new("cnt1", "running", -1, "{\"phase\":\"basebackup\"}");

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), BackupsOf(active), CancellationToken.None);

        // Assert — ни нового ключа, ни (пере)создания контейнера
        outcome.IsSuccess.Should().BeTrue();
        rig.Etcd.Store.Keys.Should().NotContain(k => k.StartsWith("/pgworker/backups/shop/shard1/full/"));
        rig.Engine.Created.Should().BeEmpty();
    }

    // AAA: restore-гвард (t05 §3.4) — шард с активной restore-заявкой при due-
    // условиях новый полный НЕ планирует (шард демонтируется restore-процессом,
    // контуры бэкапов его не трогают)
    [Fact]
    public async Task ActiveRestore_SkipsShard_NoNewFull()
    {
        // Arrange — COMPLETED давно (due по возрасту) + PLANNED restore шарда
        var rig = await NewRig();
        var now = TimeProvider.System.GetUtcNow();
        var old = new FullBackupState("20260908030000Z", FullBackupStatus.Completed,
            "shard1a", BackupSourceRole.Master, Unix(now.AddDays(-2)),
            Unix(now.AddDays(-2).AddMinutes(5)), null, null, null, null);
        var restoring = new ShardBackups([old], null,
            [new RestoreOperationState("20260910025900Z", RestoreStatus.Planned,
                "", "shop/shard1", "latest", "shard1a", Unix(now), "operator")]);
        IReadOnlyList<ClusterBackups> backups =
            [new ClusterBackups("shop", null,
                new Dictionary<string, ShardBackups> { ["shard1"] = restoring })];

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), backups, CancellationToken.None);

        // Assert — тик Done, новых полных и джобов нет (гвард сработал до due)
        outcome.IsSuccess.Should().BeTrue();
        rig.Etcd.Store.Keys.Should().NotContain(k => k.StartsWith("/pgworker/backups/shop/shard1/full/"));
        rig.Engine.Created.Should().BeEmpty();
    }

    // AAA: свежий COMPLETED + живой wal-ключ — не due, новых записей нет, но
    // гвард backup_exec исполнен (G2 — каждый тик, до due-гвардов; spec §3.1,
    // ревью Ф4 finding 1). t05 §3.5: без wal-ключа свежий полный — уже due
    // (инвариант цепочки), поэтому «не due» обязан включать живой ключ.
    [Fact]
    public async Task FreshCompleted_NotDue_RoleStillEnsured()
    {
        // Arrange — COMPLETED час назад + активный wal-поток (цепочка жива)
        var rig = await NewRig();
        var now = TimeProvider.System.GetUtcNow();
        var completed = new FullBackupState("20260908030000Z", FullBackupStatus.Completed,
            "shard1a", BackupSourceRole.Master, Unix(now.AddHours(-1).AddMinutes(-5)),
            Unix(now.AddHours(-1)), "000000010000000000000042", 1048576, null, null);
        var wal = new WalStreamState(WalStreamStatus.Active, "slot_shop_shard1", "shard1a",
            "000000010000000000000042", "000000010000000000000043", "000000010000000000000043",
            Unix(now), null, null);
        var backups = new IReadOnlyList<ClusterBackups>[]
        {
            [new ClusterBackups("shop", null, new Dictionary<string, ShardBackups>
            {
                ["shard1"] = new([completed], wal),
            })],
        }[0];

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), backups, CancellationToken.None);

        // Assert — новых записей НЕТ; гвард исполнен; тик Done
        outcome.IsSuccess.Should().BeTrue();
        rig.Etcd.Store.Keys.Should().NotContain(k => k.StartsWith("/pgworker/backups/shop/shard1/full/"));
        rig.Engine.Created.Should().BeEmpty();
        var guard = rig.Sql.Scalars.Should().ContainSingle().Subject;
        guard.Sql.Should().Contain("backup_exec");
    }

    // AAA (AC1): wal-ключ BROKEN → планировщик создаёт новый полный, даже если
    // последний COMPLETED свежий (инвариант одного активного/бэкофф — как всегда)
    [Fact]
    public async Task Тик_при_BROKEN_wal_планирует_пересъём()
    {
        // Arrange — COMPLETED ..42 (5 мин назад) + BROKEN с границей ..44 ВЫШЕ
        // старого полного (дыра выше ..44): ..42 разрыв НЕ покрывает — пересъём нужен
        var rig = await NewRig();
        var now = TimeProvider.System.GetUtcNow();
        var completed = new FullBackupState("20260908030000Z", FullBackupStatus.Completed,
            "shard1a", BackupSourceRole.Master, Unix(now.AddMinutes(-6)),
            Unix(now.AddMinutes(-5)), "000000010000000000000042", 1048576, null, null);
        var brokenWal = new WalStreamState(WalStreamStatus.Broken, "slot_shop_shard1", "shard1a",
            "000000010000000000000044", "000000010000000000000044", "000000010000000000000044",
            Unix(now.AddMinutes(-30)), null, "дыра WAL-цепочки");
        var backups = new IReadOnlyList<ClusterBackups>[]
        {
            [new ClusterBackups("shop", null, new Dictionary<string, ShardBackups>
            {
                ["shard1"] = new([completed], brokenWal),
            })],
        }[0];

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), backups, CancellationToken.None);

        // Assert — появился новый полный (PLANNED/RUNNING) с ДРУГИМ id:
        // BROKEN лечится пересъёмом независимо от свежести полного (spec §3.2)
        outcome.IsSuccess.Should().BeTrue(outcome.Error?.ToString());
        var fullKeys = rig.Etcd.Store.Keys
            .Where(k => k.StartsWith("/pgworker/backups/shop/shard1/full/")).ToList();
        fullKeys.Should().ContainSingle("новый полный запланирован");
        fullKeys.Single().Should().NotContain("20260908030000Z", "id новый — не переснятый старый");
        rig.Engine.Created.Should().NotBeEmpty("джоб пересъёма запущен");
    }

    // AAA (t07, arch/19 §2 — прогон 2026-09-13): COMPLETED-полный с wal_start ≥
    // границы разрыва УЖЕ покрывает BROKEN — повторный пересъём не планируется
    // (иначе шторм пересъёмов каждый тик, контроль не успевает заживить)
    [Fact]
    public async Task Тик_при_BROKEN_покрытом_полным_пересъёма_нет()
    {
        // Arrange — COMPLETED ..46 (старт выше границы разрыва ..42) + BROKEN ..42
        var rig = await NewRig();
        var now = TimeProvider.System.GetUtcNow();
        var reshot = new FullBackupState("20260908030500Z", FullBackupStatus.Completed,
            "shard1a", BackupSourceRole.Master, Unix(now.AddMinutes(-6)),
            Unix(now.AddMinutes(-5)), "000000010000000000000046", 1048576, null, null);
        var brokenWal = new WalStreamState(WalStreamStatus.Broken, "slot_shop_shard1", "shard1a",
            "000000010000000000000042", "000000010000000000000042", "000000010000000000000042",
            Unix(now.AddMinutes(-30)), null, "дыра WAL-цепочки");
        var backups = new IReadOnlyList<ClusterBackups>[]
        {
            [new ClusterBackups("shop", null, new Dictionary<string, ShardBackups>
            {
                ["shard1"] = new([reshot], brokenWal),
            })],
        }[0];

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), backups, CancellationToken.None);

        // Assert — нового полного нет: разрыв покрыт, заживление — за контролем (§3)
        outcome.IsSuccess.Should().BeTrue(outcome.Error?.ToString());
        rig.Etcd.Store.Keys.Should().NotContain(k => k.StartsWith("/pgworker/backups/shop/shard1/full/"),
            "пересъём поверх покрывающего полного — шторм");
        rig.Engine.Created.Should().BeEmpty();
    }

    // AAA: недавний FAILED — бэкофф (Base=300 > 100 c) держит, новой попытки нет
    [Fact]
    public async Task FailedRecent_BackoffBlocks()
    {
        // Arrange — FAILED 100 c назад, RetryBaseSec=300 (дефолт)
        var rig = await NewRig();
        var now = TimeProvider.System.GetUtcNow();
        var failed = new FullBackupState("20260910025820Z", FullBackupStatus.Failed,
            "shard1a", BackupSourceRole.Master, Unix(now) - 100, Unix(now) - 90, null, null,
            "pg_basebackup failed", null);

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), BackupsOf(failed), CancellationToken.None);

        // Assert — нового PLANNED/RUNNING нет
        outcome.IsSuccess.Should().BeTrue();
        rig.Etcd.Store.Keys.Should().NotContain(k => k.StartsWith("/pgworker/backups/shop/shard1/full/"));
        rig.Engine.Created.Should().BeEmpty();
    }

    // AAA: мастер не резолвится (пустой portalloc) — шард skip: гвард НЕ исполнен,
    // ключей нет, тик Done (супервизия без мутаций — активных нет)
    [Fact]
    public async Task MasterUnresolved_SkipsShard()
    {
        // Arrange — portalloc-ключ отсутствует
        var rig = await NewRig(seedPortalloc: false);

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), [], CancellationToken.None);

        // Assert
        outcome.IsSuccess.Should().BeTrue();
        rig.Sql.Scalars.Should().BeEmpty();
        rig.Etcd.Store.Keys.Should().NotContain(k => k.StartsWith("/pgworker/backups/"));
        rig.Engine.Created.Should().BeEmpty();
    }

    // AAA: клэйм не наш — отказ до любых мутаций (FakeEtcd.Txns пуст)
    [Fact]
    public async Task NotClaimed_Refuses()
    {
        // Arrange — клэйм не взят
        var rig = await NewRig(claim: false);

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), [], CancellationToken.None);

        // Assert — Failed; txn нет
        outcome.IsSuccess.Should().BeFalse();
        rig.Etcd.Txns.Should().BeEmpty();
    }

    // ─── S-ветки супервизии (Task 12) ───

    private const string WalSeg = "000000010000000000000042";

    // Хелпер: активная запись шарда (источник — мастер shard1a, как в сидах G).
    private static FullBackupState Active(string id, FullBackupStatus state, string? wal = null,
        string? error = null, long? finished = null, long? size = null, BackupVerify? verify = null)
        => new(id, state, "shard1a", BackupSourceRole.Master,
            TimeProvider.System.GetUtcNow().ToUnixTimeSeconds() - 60, finished, wal, size, error, verify);

    // Хелпер: активная запись шарда как аргумент тика (как после прошлого тика).
    private static IReadOnlyList<ClusterBackups> Seeded(string id,
        FullBackupStatus state, string? wal = null, string? error = null, long? finished = null,
        long? size = null, BackupVerify? verify = null)
        => BackupsOf(Active(id, state, wal, error, finished, size, verify));

    private static string FullKey(string id) => BackupNames.FullKey("shop", "shard1", id);

    // AAA: PLANNED без контейнера (сбой create прошлого тика) → create+start,
    // state=RUNNING, тот же id; бэкофф-штрафа нет — новых ключей не появилось
    [Fact]
    public async Task PlannedWithoutContainer_Relaunches()
    {
        // Arrange — PLANNED-запись, движок пуст
        var rig = await NewRig();
        var backups = Seeded("20260908030000Z", FullBackupStatus.Planned);

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), backups, CancellationToken.None);

        // Assert — тот же id стал RUNNING; джоб создан и запущен
        outcome.IsSuccess.Should().BeTrue(outcome.Error?.ToString());
        rig.Etcd.Store.Keys.Should().ContainSingle(k => k.StartsWith("/pgworker/backups/shop/shard1/full/"))
            .Which.Should().Be(FullKey("20260908030000Z"));
        var parsed = BackupsParser.Parse(
            (IReadOnlyList<Kv>)[new Kv(FullKey("20260908030000Z"), rig.Etcd.Store[FullKey("20260908030000Z")].Value, 1)],
            out var errors);
        errors.Should().BeEmpty();
        parsed.Value[0].Shards["shard1"].Full.Single().State.Should().Be(FullBackupStatus.Running);
        rig.Engine.Created.Should().ContainSingle();
        rig.Engine.Started.Should().Contain(BackupNames.ContainerName("shop", "shard1", "20260908030000Z"));
        (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.Phase
            .Should().Be("started/shard1/20260908030000Z");
    }

    // AAA: PLANNED + контейнер created (сбой start прошлого тика) → start,
    // state=RUNNING — wedge исключён
    [Fact]
    public async Task PlannedWithCreatedContainer_GetsStarted()
    {
        // Arrange — PLANNED + контейнер в created
        var rig = await NewRig();
        var backups = Seeded("20260908030000Z", FullBackupStatus.Planned);
        var name = BackupNames.ContainerName("shop", "shard1", "20260908030000Z");
        rig.Engine.Containers[name] = new("cnt1", "created", -1, "");

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), backups, CancellationToken.None);

        // Assert — стартован, статус RUNNING (не «жив» в created)
        outcome.IsSuccess.Should().BeTrue();
        rig.Engine.Started.Should().Contain(name);
        rig.Engine.Containers[name].State.Should().Be("running");
        var parsed = BackupsParser.Parse(
            (IReadOnlyList<Kv>)[new Kv(FullKey("20260908030000Z"), rig.Etcd.Store[FullKey("20260908030000Z")].Value, 1)],
            out _);
        parsed.Value[0].Shards["shard1"].Full.Single().State.Should().Be(FullBackupStatus.Running);
        rig.Engine.Created.Should().BeEmpty("повторного create быть не должно");
    }

    // AAA: running-джоб напечатал uploading-маркер → state=UPLOADING +
    // wal_start_segment (node/role/started сохранены)
    [Fact]
    public async Task RunningJob_UploadingMarker_MovesToUploading()
    {
        // Arrange — RUNNING + контейнер running с uploading-маркером
        var rig = await NewRig();
        var backups = Seeded("20260908030000Z", FullBackupStatus.Running);
        var name = BackupNames.ContainerName("shop", "shard1", "20260908030000Z");
        rig.Engine.Containers[name] = new("cnt1", "running", -1,
            $"{{\"phase\":\"uploading\",\"wal_start_segment\":\"{WalSeg}\"}}");

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), backups, CancellationToken.None);

        // Assert
        outcome.IsSuccess.Should().BeTrue();
        var parsed = BackupsParser.Parse(
            (IReadOnlyList<Kv>)[new Kv(FullKey("20260908030000Z"), rig.Etcd.Store[FullKey("20260908030000Z")].Value, 1)],
            out _);
        var full = parsed.Value[0].Shards["shard1"].Full.Single();
        full.State.Should().Be(FullBackupStatus.Uploading);
        full.WalStartSegment.Should().Be(WalSeg);
        full.Node.Should().Be("shard1a");
        full.Role.Should().Be(BackupSourceRole.Master);
    }

    // AAA: exited 0 + ok:true → COMPLETED (finished/wal/size, verify PENDING),
    // контейнер и staging volume удалены
    [Fact]
    public async Task ExitedZero_OkResult_CompletesAndCleans()
    {
        // Arrange — RUNNING + exited 0 + result ok
        var rig = await NewRig();
        var backups = Seeded("20260908030000Z", FullBackupStatus.Running);
        var name = BackupNames.ContainerName("shop", "shard1", "20260908030000Z");
        rig.Engine.Containers[name] = new("cnt1", "exited", 0,
            $"{{\"ok\":true,\"wal_start_segment\":\"{WalSeg}\",\"size_bytes\":1048576}}");

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), backups, CancellationToken.None);

        // Assert
        outcome.IsSuccess.Should().BeTrue();
        var parsed = BackupsParser.Parse(
            (IReadOnlyList<Kv>)[new Kv(FullKey("20260908030000Z"), rig.Etcd.Store[FullKey("20260908030000Z")].Value, 1)],
            out _);
        var full = parsed.Value[0].Shards["shard1"].Full.Single();
        full.State.Should().Be(FullBackupStatus.Completed);
        full.FinishedUnix.Should().NotBeNull();
        full.WalStartSegment.Should().Be(WalSeg);
        full.SizeBytes.Should().Be(1048576);
        full.Verify!.State.Should().Be(BackupVerifyStatus.Pending);
        rig.Engine.Removed.Should().Contain(name);
        rig.Engine.RemovedVolumes.Should().Contain(BackupNames.VolumeName("shop", "shard1", "20260908030000Z"));
    }

    // AAA: exit 1 + ok:false с ошибкой → FAILED с error; контейнер/volume удалены
    [Fact]
    public async Task ExitedNonZero_FailsWithError()
    {
        // Arrange — RUNNING + exited 1 + result ok:false
        var rig = await NewRig();
        var backups = Seeded("20260908030000Z", FullBackupStatus.Running);
        var name = BackupNames.ContainerName("shop", "shard1", "20260908030000Z");
        rig.Engine.Containers[name] = new("cnt1", "exited", 1,
            "{\"ok\":false,\"error\":\"upload: connection refused\"}");

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), backups, CancellationToken.None);

        // Assert
        outcome.IsSuccess.Should().BeTrue();
        var parsed = BackupsParser.Parse(
            (IReadOnlyList<Kv>)[new Kv(FullKey("20260908030000Z"), rig.Etcd.Store[FullKey("20260908030000Z")].Value, 1)],
            out _);
        var full = parsed.Value[0].Shards["shard1"].Full.Single();
        full.State.Should().Be(FullBackupStatus.Failed);
        full.Error.Should().Be("upload: connection refused");
        full.FinishedUnix.Should().NotBeNull();
        rig.Engine.Removed.Should().Contain(name);
        rig.Engine.RemovedVolumes.Should().Contain(BackupNames.VolumeName("shop", "shard1", "20260908030000Z"));
    }

    // AAA: exit 2 без result-JSON → FAILED с error «exit 2» (exit-код — истина)
    [Fact]
    public async Task ExitedWithoutResult_FailsWithExitCode()
    {
        // Arrange — RUNNING + exited 2, логи без result
        var rig = await NewRig();
        var backups = Seeded("20260908030000Z", FullBackupStatus.Running);
        var name = BackupNames.ContainerName("shop", "shard1", "20260908030000Z");
        rig.Engine.Containers[name] = new("cnt1", "exited", 2, "sh: pg_basebackup: not found\n");

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), backups, CancellationToken.None);

        // Assert
        outcome.IsSuccess.Should().BeTrue();
        var parsed = BackupsParser.Parse(
            (IReadOnlyList<Kv>)[new Kv(FullKey("20260908030000Z"), rig.Etcd.Store[FullKey("20260908030000Z")].Value, 1)],
            out _);
        var full = parsed.Value[0].Shards["shard1"].Full.Single();
        full.State.Should().Be(FullBackupStatus.Failed);
        full.Error.Should().Be("exit 2");
    }

    // AAA: RUNNING без контейнера (рестарт docker-хоста) → FAILED
    // container-vanished; осиротевший staging volume удалён
    [Fact]
    public async Task RunningWithoutContainer_MarksFailedContainerVanished()
    {
        // Arrange — RUNNING, ListContainers пуст (all=true)
        var rig = await NewRig();
        var backups = Seeded("20260908030000Z", FullBackupStatus.Running);

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), backups, CancellationToken.None);

        // Assert
        outcome.IsSuccess.Should().BeTrue();
        var parsed = BackupsParser.Parse(
            (IReadOnlyList<Kv>)[new Kv(FullKey("20260908030000Z"), rig.Etcd.Store[FullKey("20260908030000Z")].Value, 1)],
            out _);
        var full = parsed.Value[0].Shards["shard1"].Full.Single();
        full.State.Should().Be(FullBackupStatus.Failed);
        full.Error.Should().Be("container-vanished");
        full.FinishedUnix.Should().NotBeNull();
        rig.Engine.RemovedVolumes.Should().Contain(BackupNames.VolumeName("shop", "shard1", "20260908030000Z"));
        (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.Phase
            .Should().Be("vanished/shard1/20260908030000Z");
    }

    // AAA: transport-отказ docker (list) — статус в etcd НЕ изменён
    [Fact]
    public async Task TransportError_KeepsStatus()
    {
        // Arrange — RUNNING + ListContainers падает
        var rig = await NewRig();
        var backups = Seeded("20260908030000Z", FullBackupStatus.Running);
        rig.Engine.ListFails = true;

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), backups, CancellationToken.None);

        // Assert — попытка не потеряна: статус и ключи нетронуты
        outcome.IsSuccess.Should().BeTrue();
        rig.Etcd.Store.Should().NotContainKey(FullKey("20260908030000Z"),
            "статус живёт в аргументе тика; в etcd его пишет только успешная супервизия");
        rig.Engine.Removed.Should().BeEmpty();
        rig.Engine.RemovedVolumes.Should().BeEmpty();
    }

    // AAA: exited, но logs/inspect transport-отказ → статус не изменён, чистки нет
    [Theory]
    [InlineData(true, false)]  // logs недоступен
    [InlineData(false, true)]  // inspect недоступен
    public async Task Exited_InspectOrLogsTransportError_KeepsStatus(bool logsFails, bool inspectFails)
    {
        // Arrange — RUNNING + exited 0 + ok:true, но транспорт логов/инспекта упал
        var rig = await NewRig();
        var backups = Seeded("20260908030000Z", FullBackupStatus.Running);
        var name = BackupNames.ContainerName("shop", "shard1", "20260908030000Z");
        rig.Engine.Containers[name] = new("cnt1", "exited", 0,
            $"{{\"ok\":true,\"wal_start_segment\":\"{WalSeg}\",\"size_bytes\":1}}");
        rig.Engine.LogsFails = logsFails;
        rig.Engine.InspectFails = inspectFails;

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), backups, CancellationToken.None);

        // Assert — статус не тронут, CleanupJobAsync не зван: следующий тик повторит
        outcome.IsSuccess.Should().BeTrue();
        rig.Etcd.Store.Should().NotContainKey(FullKey("20260908030000Z"));
        rig.Engine.Removed.Should().BeEmpty();
        rig.Engine.RemovedVolumes.Should().BeEmpty();
    }

    // AAA (t13 AC2): источник джоба исчез из portalloc НАВСЕГДА (replace ноды/
    // рассинхрон при живом шарде), RUNNING старше бюджета: вердикт FAILED по
    // возрасту — самостоятельный факт etcd (started_unix + часы воркера),
    // docker-доступ не нужен → тик ставит FAILED job-timeout, cleanup
    // пропускается (движок не тронут), journal несёт пометку. До t13 такой
    // джоб застревал в вечном RUNNING и держал инвариант «один активный».
    [Fact]
    public async Task Источник_исчез_при_возрасте_свыше_бюджета_FAILED_без_cleanup()
    {
        // Arrange — RUNNING started = now-7h (бюджет 6 ч дефолт), node = shard1z:
        // ноды нет в portalloc (источник исчез навсегда), порт-аллок жив
        var rig = await NewRig();
        var now = TimeProvider.System.GetUtcNow();
        var active = new FullBackupState("20260908030000Z", FullBackupStatus.Running,
            "shard1z", BackupSourceRole.Master, Unix(now.AddHours(-7)), null, null, null, null, null);

        // Act
        var outcome = await rig.Process.TickAsync(
            await Snapshot(rig.Etcd), BackupsOf(active), CancellationToken.None);

        // Assert — FAILED job-timeout по факту возраста; cleanup пропущен
        // (контейнер/движок не тронуты — Engine.Removed пуст); journal —
        // job-timeout/<shard>/<id> с пометкой о пропущенном cleanup
        outcome.IsSuccess.Should().BeTrue(outcome.Error?.ToString());
        var parsed = BackupsParser.Parse(
            (IReadOnlyList<Kv>)[new Kv(FullKey("20260908030000Z"),
                rig.Etcd.Store[FullKey("20260908030000Z")].Value, 1)], out var errors);
        errors.Should().BeEmpty();
        var failed = parsed.Value[0].Shards["shard1"].Full.Single();
        failed.State.Should().Be(FullBackupStatus.Failed);
        failed.Error.Should().Contain("job-timeout");
        rig.Engine.Removed.Should().BeEmpty("источник недоступен — cleanup пропущен (t13 AC3)");
        rig.Engine.RemovedVolumes.Should().BeEmpty();
        var entry = (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!;
        entry.Phase.Should().Be("job-timeout/shard1/20260908030000Z");
        entry.LastError.Should().Contain("job-timeout").And.Contain("cleanup пропущен");
    }

    // AAA (t13 AC3): возраст свыше бюджета + list-отказ (transient источника
    // при доступном portalloc/engine): FAILED всё равно ставится — возраст
    // самодостаточен; cleanup пропускается с пометкой в той же journal-записи
    [Fact]
    public async Task List_отказ_при_возрасте_свыше_бюджета_FAILED_и_cleanup_пропущен()
    {
        // Arrange — RUNNING started = now-7h на живой ноде shard1a, контейнер
        // running («вечный» джоб), но list по движку падает (transport-отказ)
        var rig = await NewRig();
        var now = TimeProvider.System.GetUtcNow();
        var active = new FullBackupState("20260908030000Z", FullBackupStatus.Running,
            "shard1a", BackupSourceRole.Master, Unix(now.AddHours(-7)), null, null, null, null, null);
        var name = BackupNames.ContainerName("shop", "shard1", "20260908030000Z");
        rig.Engine.Containers[name] = new("cnt-listfail", "running", -1, "{\"phase\":\"basebackup\"}");
        rig.Engine.ListFails = true;

        // Act
        var outcome = await rig.Process.TickAsync(
            await Snapshot(rig.Etcd), BackupsOf(active), CancellationToken.None);

        // Assert — FAILED job-timeout (transient list больше не откладывает
        // бюджет); контейнер не тронут (cleanup пропущен); journal — пометка
        outcome.IsSuccess.Should().BeTrue(outcome.Error?.ToString());
        var parsed = BackupsParser.Parse(
            (IReadOnlyList<Kv>)[new Kv(FullKey("20260908030000Z"),
                rig.Etcd.Store[FullKey("20260908030000Z")].Value, 1)], out var errors);
        errors.Should().BeEmpty();
        var failed = parsed.Value[0].Shards["shard1"].Full.Single();
        failed.State.Should().Be(FullBackupStatus.Failed);
        failed.Error.Should().Contain("job-timeout");
        rig.Engine.Removed.Should().BeEmpty("list-отказ — cleanup пропущен (t13 AC3)");
        rig.Engine.RemovedVolumes.Should().BeEmpty();
        var entry = (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!;
        entry.Phase.Should().Be("job-timeout/shard1/20260908030000Z");
        entry.LastError.Should().Contain("cleanup пропущен");
    }
}
