using Microsoft.Extensions.Logging.Abstractions;
using PgWorker.Backups;
using PgWorker.Backups.Job;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Planning;
using PgWorker.Core.Templates;
using PgWorker.Core.Tuning;
using PgWorker.Docker.Drivers;
using Shared.Etcd.Client;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Processes;
using PgWorker.Provisioning.Probes;
using PgWorker.Provisioning.Sql;
using PgWorker.UnitTests.Provisioning;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Планировщик полных бэкапов (t02/t26, arch/19 §2): G0 выключен → no-op; G1
// ensure backup_password; G2 гвард роли backup_exec — КАЖДЫЙ тик (до
// due-гвардов); G3 при due+бэкоффе: PLANNED → StartFullAsync приёмнику ноды
// (alias:8080) → RUNNING (journal-before-manipulations). Супервизия —
// GetFullAsync (uploading/succeeded/failed/404 vanished), бюджет+cancel.
// Клэйм-инвариант мутаций префикса. AAA-комментарии.
public class BackupProcessTests
{
    private const string Ep = "http://etcd:2379";
    private static readonly InstallSecrets Secrets = new("su-pw", "sb-pw", "adm-pw", "mov-pw");
    private const string ReceiverUrlMaster = "http://pgw-shop-shard1-shard1a:8080";

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
        public Task<Result> PingAsync(CancellationToken ct) => Task.FromResult(Result.Success());

        public Task<Result<IReadOnlyList<DockerContainer>>> ListContainersAsync(
            string namePrefix, bool all, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<DockerContainer>>.Success([]));

        public Task<Result<DockerContainerInspect>> InspectContainerAsync(string id, CancellationToken ct)
            => Task.FromResult(Result<DockerContainerInspect>.Failed(new ApplicationException("fake")));
        public Task<Result<string>> GetContainerLogsAsync(string idOrName, int tail, CancellationToken ct)
            => Task.FromResult(Result<string>.Failed(new ApplicationException("fake")));

        public Task<Result> CreateContainerAsync(ContainerSpec spec, string name, CancellationToken ct)
            => Task.FromResult(Result.Success());
        public Task<Result> StartContainerAsync(string idOrName, CancellationToken ct)
            => Task.FromResult(Result.Success());
        public Task<Result> StopContainerAsync(string idOrName, int timeoutSec, CancellationToken ct)
            => Task.FromResult(Result.Success());
        public Task<Result> KillContainerAsync(string idOrName, CancellationToken ct)
            => Task.FromResult(Result.Success());
        public Task<Result> RemoveContainerAsync(string idOrName, bool force, CancellationToken ct)
            => Task.FromResult(Result.Success());
        public Task<Result> RemoveVolumeAsync(string name, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result<string>> ExecAsync(string containerId, IReadOnlyList<string> cmd, CancellationToken ct)
            => Task.FromResult(Result<string>.Success(""));
        public Task<Result> EnsureNetworkAsync(string name, CancellationToken ct)
            => Task.FromResult(Result.Success());
        public Task<Result> DeleteNetworkAsync(string name, CancellationToken ct) => Task.FromResult(Result.Success());
        public Task<Result> NetworkConnectAsync(string network, string container, CancellationToken ct) => Task.FromResult(Result.Success());
        public Task<Result<bool>> VolumeExistsAsync(string name, CancellationToken ct) => Task.FromResult(Result<bool>.Success(false));
        public Task<Result<IReadOnlyList<string>>> ListVolumesAsync(string namePrefix, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<string>>.Success([]));
        public Task<Result> EnsureVolumeAsync(string name, string labelKey, string label, CancellationToken ct)
            => Task.FromResult(Result.Success());
        public Task<Result> EnsureVolumeAsync(string name, CancellationToken ct) => Task.FromResult(Result.Success());
        public Task<Result> DeleteVolumeAsync(string name, CancellationToken ct) => Task.FromResult(Result.Success());
        public Task<Result<NodeLimits?>> InspectContainerResourcesAsync(string name, CancellationToken ct) => Task.FromResult(Result<NodeLimits?>.Success(null));
        public Task<Result<NodeLimits?>> InspectServiceResourcesAsync(string name, CancellationToken ct) => Task.FromResult(Result<NodeLimits?>.Success(null));
        public Task<Result<IReadOnlyDictionary<string, string>?>> InspectContainerEnvAsync(string idOrName, CancellationToken ct) => Task.FromResult(Result<IReadOnlyDictionary<string, string>?>.Success(null));
        public Task<Result<IReadOnlyDictionary<string, string>?>> InspectServiceEnvAsync(string name, CancellationToken ct) => Task.FromResult(Result<IReadOnlyDictionary<string, string>?>.Success(null));
        public Task<Result<IReadOnlyList<string>?>> InspectContainerCmdAsync(string idOrName, CancellationToken ct) => Task.FromResult(Result<IReadOnlyList<string>?>.Success(null));
        public Task<Result<IReadOnlyList<string>?>> InspectServiceCmdAsync(string name, CancellationToken ct) => Task.FromResult(Result<IReadOnlyList<string>?>.Success(null));
        public Task<Result<DockerNodeEndpoint?>> InspectNodeEndpointAsync(string name, int containerPort, CancellationToken ct) => Task.FromResult(Result<DockerNodeEndpoint?>.Success(null));
        public Task<Result<IReadOnlyList<DockerSwarmNode>>> ListNodesAsync(CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<DockerSwarmNode>>.Success([]));
        public Task<Result> CreateServiceAsync(ServiceSpec spec, CancellationToken ct)
            => Task.FromResult(Result.Success());
        public Task<Result> RemoveServiceAsync(string name, CancellationToken ct)
            => Task.FromResult(Result.Success());
        public Task<Result<IReadOnlyList<DockerService>>> ListServiceRecordsAsync(string namePrefix, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<DockerService>>.Success([]));
        public Task<Result<IReadOnlyList<string>>> ListServicesAsync(string namePrefix, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<string>>.Success([]));
        public Task<Result<IReadOnlyList<DockerTask>>> ListTasksAsync(string serviceName, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<DockerTask>>.Success([]));
        public Task<Result<IReadOnlySet<(string Host, int Port)>>> BusyPortsAsync(CancellationToken ct)
            => Task.FromResult(Result<IReadOnlySet<(string Host, int Port)>>.Success(new HashSet<(string, int)>()));

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    internal sealed class FakeBackupDriver(FakeBackupEngine engine) : IClusterDriver
    {
        public Task<Result> RemoveBackupJobsAsync(string cluster, CancellationToken ct)
            => Task.FromResult(Result.Success());
        public Task<Result> RemoveRestoreJobsAsync(string cluster, string shard, CancellationToken ct)
            => Task.FromResult(Result.Success());
        public Task<Result<IReadOnlyList<HostInfo>>> GetHostsAsync(CancellationToken ct) => throw NotSupported();
        public Task<Result<IReadOnlySet<(string Host, int Port)>>> GetBusyPortsAsync(CancellationToken ct) => throw NotSupported();
        public Task<Result> EnsureNodeAsync(ShardTopology topology, string nodeName, NodeAddress addr,
            InstallSecrets secrets, EtcdEndpoints etcd, NodeResources? resources,
            PgTuneResult? tuning, bool syncStrict, string restPassword, CancellationToken ct) => throw NotSupported();
        public Task<Result> RemoveNodeAsync(string cluster, string shard, string nodeName, CancellationToken ct) => throw NotSupported();
        public Task<Result> StopNodeAsync(string cluster, string shard, string nodeName, CancellationToken ct) => throw NotSupported();
        public Task<Result<IReadOnlyDictionary<string, DiscoveredNode>>> InspectNodesAsync(
            string cluster, IReadOnlyCollection<string> nodeNames, CancellationToken ct) => throw NotSupported();
        public Task<Result<IReadOnlyDictionary<string, string>>> InspectNodeEnvAsync(
            string cluster, string shard, string node, CancellationToken ct) => throw NotSupported();
        public bool SupportsRunningInspection => true;
        public Task<Result<IReadOnlyList<string>>> ListNodeObjectsAsync(string cluster, CancellationToken ct) => throw NotSupported();
        public Task<Result> RemoveBackupAgentsAsync(string cluster, string? shard, CancellationToken ct)
            => Task.FromResult(Result.Success());
        public IDockerEngine? EngineFor(string host) => engine;

        private static NotSupportedException NotSupported() => new("not used in this test");
    }

    internal sealed class FakeSecretEnsurer : IClusterSecretEnsurer
    {
        public int Calls;
        public Task<Result<ClusterCredentials>> EnsureAsync(string cluster, ClusterConfig config, CancellationToken ct)
        {
            Calls++;
            return Task.FromResult(Result<ClusterCredentials>.Success(new ClusterCredentials(
                new("app", "app-pw"), "mov-pw", new("bucket_admin", "sb-pw"),
                "pw0000000000000000000000000000A", "rest-pw")));
        }
    }

    internal sealed class FakeSql : ISqlExecutor
    {
        public readonly List<(string Dsn, string Sql)> Scalars = [];

        public Task<Result> ExecuteAsync(string dsn, string sql, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result<object?>> ExecuteScalarAsync(string dsn, string sql, CancellationToken ct)
        {
            Scalars.Add((dsn, sql));
            // Гвард роли: строка CREATE ROLE — роль ещё не создана (идемпотентный путь).
            return Task.FromResult(Result<object?>.Success(
                sql.Contains("CREATE ROLE") ? "CREATE ROLE backup_exec ..." : null));
        }

        public Task<Result> EnsureDatabaseAsync(string dsn, string dbname, CancellationToken ct)
            => Task.FromResult(Result.Success());
    }

    /// <summary>Фейк клиента API приёмника (шаг 16): программируемые ответы по
    /// виду вызова, счётчики/журнал URL и id — ассерты адресации alias:8080.</summary>
    internal sealed class FakeReceiverClient : IWalReceiverApiClient
    {
        public ReceiverReply StartFullReply { get; set; } = Accepted();
        public ReceiverReply CancelFullReply { get; set; } = new(202, null, null);
        public readonly Queue<ReceiverReply> GetFullReplies = new();

        public readonly List<string> StartFullUrls = [];
        public readonly List<string> StartFullIds = [];
        public int CancelCalls;
        public int GetCalls;

        public static ReceiverReply Accepted() =>
            new(202, new ReceiverStatus("running", "basebackup"), null);
        // activeId — id активной операции в теле 409 (корреляция «своя/чужая»);
        // null — тело без id (приёмник без корреляции — прежнее поведение).
        public static ReceiverReply Conflict(string? activeId = null) => new(409,
            activeId is null
                ? null
                : new ReceiverStatus("running", "basebackup", Id: activeId,
                    Error: "operation already active"),
            null);
        public static ReceiverReply Status(string state, string phase,
            string? wal = null, long? size = null, string? error = null) =>
            new(200, new ReceiverStatus(state, phase, null, wal, size, null, null, null, error), null);
        public static ReceiverReply Transport() => new(0, null, "connection refused");
        public static ReceiverReply ServerError() => new(502, null, "bad gateway");
        public static ReceiverReply Rejected() => new(404, null, null);

        public Task<ReceiverReply> StartFullAsync(string baseUrl, string id, CancellationToken ct)
        {
            StartFullUrls.Add(baseUrl);
            StartFullIds.Add(id);
            return Task.FromResult(StartFullReply);
        }

        public Task<ReceiverReply> GetFullAsync(string baseUrl, CancellationToken ct)
        {
            GetCalls++;
            return Task.FromResult(GetFullReplies.Count > 0 ? GetFullReplies.Dequeue() : Accepted());
        }

        public Task<ReceiverReply> CancelFullAsync(string baseUrl, CancellationToken ct)
        {
            CancelCalls++;
            return Task.FromResult(CancelFullReply);
        }

        public Task<ReceiverReply> StartVerifyAsync(string baseUrl, StartVerifyRequest body, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<ReceiverReply> GetVerifyAsync(string baseUrl, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<ReceiverReply> StartRestoreAsync(string baseUrl, StartRestoreRequest body, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<ReceiverReply> GetRestoreAsync(string baseUrl, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<ReceiverReply> CancelOpsAsync(string baseUrl, CancellationToken ct)
            => throw new NotSupportedException();

        // hba-гвард (arch/19 §7): ответ программируется, вызовы — в журнал URL.
        public NodeReply<HbaEnsureResponse> HbaReply { get; set; }
            = new(200, new HbaEnsureResponse(true, false), null);
        public readonly List<string> HbaUrls = [];

        public Task<NodeReply<HbaEnsureResponse>> EnsureHbaAsync(string baseUrl, CancellationToken ct)
        {
            HbaUrls.Add(baseUrl);
            return Task.FromResult(HbaReply);
        }

        public Task<NodeReply<PgDumpResponse>> PgDumpAsync(string baseUrl, StartPgDumpRequest body, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<NodeReply<PgDataResponse>> GetPgDataAsync(string baseUrl, CancellationToken ct)
            => throw new NotSupportedException();
    }

    private sealed record Rig(Fakes.FakeEtcd Etcd, FakeSql Sql, FakeBackupEngine Engine,
        FakeBackupDriver Driver, FakeSecretEnsurer Ensurer, ClaimStore Claims,
        WorkJournal Journal, FakeReceiverClient Receiver, BackupProcess Process);

    private static async Task<Rig> NewRig(
        bool claim = true, bool seedPortalloc = true, BackupsRuntimeOptions? options = null,
        Fakes.FakeEtcd? etcdOverride = null,
        Action<string, IReadOnlyDictionary<string, (long? LastValidUnix, long MaxAgeSec)>>? observer = null)
    {
        var store = etcdOverride ?? new Fakes.FakeEtcd();
        SeedCluster(store);
        if (!seedPortalloc)
            store.Store.Remove("/pgworker/portalloc/shop");
        var sql = new FakeSql();
        var engine = new FakeBackupEngine();
        var ensurer = new FakeSecretEnsurer();
        var driver = new FakeBackupDriver(engine);
        var receiver = new FakeReceiverClient();
        var claims = new ClaimStore("/pgworker", [Ep], store, TimeProvider.System);
        if (claim)
            await claims.TryClaimClusterAsync("shop", CancellationToken.None);
        store.Txns.Clear(); // отсечь claim-txn: ассерты — только про тик бэкапов
        var journal = new WorkJournal("/pgworker", store, [Ep]);
        var probe = new ShardProbe(new HttpClient(new DeadHandler()));
        var process = new BackupProcess(
            store, [Ep],
            new ShardEndpoints(store, [Ep], probe), sql, ensurer,
            claims, journal, Secrets, options ?? new BackupsRuntimeOptions { Enabled = true },
            TestPoolFactory.Single(),
            receiver,
            TimeProvider.System, NullLogger<BackupProcess>.Instance, snapshot: null,
            fullAgeObserver: observer);
        return new Rig(store, sql, engine, driver, ensurer, claims, journal, receiver, process);
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

    // AAA: t14 — тик передаёт наблюдателю набор бэкапимых шардов: валидный
    // COMPLETED → LastValidUnix его finished; verify-FAILED → null; max_age —
    // эффективная политика тика (дефолт конфига при отсутствии policy-ключа)
    [Fact]
    public async Task Tick_PassesObserverFullsAge()
    {
        // Arrange — шард с валидным COMPLETED (finished=T); дефолт FullMaxAgeSec=86400
        var seen = new List<(string Cluster,
            IReadOnlyDictionary<string, (long? LastValidUnix, long MaxAgeSec)> Shards)>();
        var rig = await NewRig(observer: (c, shards) => seen.Add((c, shards)));
        const long finished = 1_757_500_300;
        var valid = new FullBackupState("20260910120000Z", FullBackupStatus.Completed, "shard1a",
            BackupSourceRole.Replica, 1_757_500_000, finished, "000000010000000000000001", 1024, null, null);
        var broken = valid with
        {
            Id = "20260911090000Z", StartedUnix = 1_757_586_000, FinishedUnix = 1_757_586_300,
            Verify = new BackupVerify(BackupVerifyStatus.Failed, 1_757_586_300, "bad"),
        };

        // Act — тик с валидным, затем тик, где валидного нет (новейший COMPLETED verify-FAILED)
        (await rig.Process.TickAsync(await Snapshot(rig.Etcd), BackupsOf(valid), CancellationToken.None))
            .IsSuccess.Should().BeTrue();
        (await rig.Process.TickAsync(await Snapshot(rig.Etcd), BackupsOf(broken), CancellationToken.None))
            .IsSuccess.Should().BeTrue();

        // Assert — один вызов на тик; замещение семантики набора проверяет
        // instrumentation (WorkerMetricsInstrumentationTests), здесь — содержимое
        seen.Should().HaveCount(2);
        seen[0].Cluster.Should().Be("shop");
        seen[0].Shards.Should().ContainKey("shard1")
            .WhoseValue.LastValidUnix.Should().Be(finished);
        seen[0].Shards["shard1"].MaxAgeSec.Should().Be(86_400);
        seen[1].Shards["shard1"].LastValidUnix.Should().BeNull("verify FAILED — not valid");
        seen[1].Shards["shard1"].MaxAgeSec.Should().Be(86_400, "the threshold is always written");
    }

    // AAA: G3 — due (COMPLETED нет): PLANNED → StartFullAsync (202) → RUNNING;
    // адрес приёмника — alias:8080 имени ноды; journal phase=started
    [Fact]
    public async Task DueNoCompleted_PlansAndStartsViaApi()
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

        // Assert — команда ушла приёмнику выбранной ноды по alias:8080
        rig.Receiver.StartFullUrls.Should().ContainSingle().Which.Should().Be(ReceiverUrlMaster);
        rig.Receiver.StartFullIds.Should().ContainSingle().Which.Should().Be(id);

        // pg_hba-гвард прошёл по обеим нодам шарда (G2 каждый тик) — команды
        // приёмникам нод по alias:8080 (канал arch/25 №22)
        rig.Receiver.HbaUrls.Select(u => u.Split("//")[^1].TrimEnd('/')).Should().BeEquivalentTo(
            ["pgw-shop-shard1-shard1a:8080", "pgw-shop-shard1-shard1b:8080"]);

        // journal-before-manipulations: phase started/shard1/<id>
        (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.Phase
            .Should().Be($"started/shard1/{id}");
    }

    // AAA: object-нода (усыновлённая) — URL приёмника — имя её контейнера
    // (NodeAddress.Object), НЕ каноническое pgw-имя (WalReceiverAddress.ForNode).
    [Fact]
    public async Task StartFull_Url_UsesObjectContainerName()
    {
        // Arrange — portalloc с object-нодами; мастер-ключ byName
        var etcd = new Fakes.FakeEtcd();
        var rig = await NewRig(etcdOverride: etcd);
        var alloc = new Dictionary<string, NodeAddress>
        {
            ["shard1/shard1a"] = new("local", new NodePorts(15433, 18011, 0), Object: "as-shard1a"),
            ["shard1/shard1b"] = new("local", new NodePorts(15434, 18012, 0), Object: "as-shard1b"),
        };
        etcd.Seed("/pgworker/portalloc/shop", Portalloc.Serialize(alloc));
        etcd.Seed("/clusters/shop/shards/shard1/master", "shard1a:0");

        // Act — тик планировщика (COMPLETED нет → due)
        (await rig.Process.TickAsync(await Snapshot(rig.Etcd), [], CancellationToken.None))
            .IsSuccess.Should().BeTrue();

        // Assert — StartFull ушёл на объект-контейнер, не на pgw-имя
        rig.Receiver.StartFullUrls.Should().ContainSingle().Which.Should().Be("http://as-shard1a:8080");
    }

    // AAA: 409 на StartFull при PLANNED (пере-старт прошлого тика) — операция
    // уже жива ⇒ PLANNED → RUNNING, статус не портится
    [Fact]
    public async Task StartFull_Conflict409_PlannedBecomesRunning()
    {
        // Arrange — PLANNED-запись; приёмник отвечает 409
        var rig = await NewRig();
        rig.Receiver.StartFullReply = FakeReceiverClient.Conflict();
        var backups = Seeded("20260908030000Z", FullBackupStatus.Planned);

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), backups, CancellationToken.None);

        // Assert — тот же id стал RUNNING; новых ключей не появилось
        outcome.IsSuccess.Should().BeTrue(outcome.Error?.ToString());
        rig.Etcd.Store.Keys.Should().ContainSingle(k => k.StartsWith("/pgworker/backups/shop/shard1/full/"))
            .Which.Should().Be(FullKey("20260908030000Z"));
        FullOf(rig.Etcd, "20260908030000Z").State.Should().Be(FullBackupStatus.Running);
        (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.Phase
            .Should().Be("started/shard1/20260908030000Z");
    }

    // AAA: 502 (5xx) на StartFull при PLANNED — PLANNED остаётся (следующий тик
    // повторит), journal-факт receiver-unavailable с адресом (spec §3.1)
    [Fact]
    public async Task StartFull_ServerError_PlannedStays_JournalUnavailable()
    {
        // Arrange — PLANNED; приёмник отвечает 502
        var rig = await NewRig();
        rig.Receiver.StartFullReply = FakeReceiverClient.ServerError();
        var backups = Seeded("20260908030000Z", FullBackupStatus.Planned);

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), backups, CancellationToken.None);

        // Assert — статус PLANNED сохранён (статус живёт в аргументе тика —
        // etcd-запись не появлялась); journal — receiver-unavailable с адресом
        outcome.IsSuccess.Should().BeTrue();
        rig.Etcd.Store.Should().NotContainKey(FullKey("20260908030000Z"));
        var entry = (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!;
        entry.Phase.Should().Be("receiver-unavailable/shard1/20260908030000Z");
        entry.LastError.Should().Contain(ReceiverUrlMaster);
    }

    // AAA: 503 стоп-гейтов на StartFull — операция не начинается: PLANNED
    // остаётся молча (стоп-семантика подсистемы), статус не портится
    [Fact]
    public async Task StartFull_Gated503_PlannedStays()
    {
        // Arrange — PLANNED; приёмник отвечает 503 (стоп-гейты)
        var rig = await NewRig();
        rig.Receiver.StartFullReply = new ReceiverReply(503, null, null);
        var backups = Seeded("20260908030000Z", FullBackupStatus.Planned);

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), backups, CancellationToken.None);

        // Assert — PLANNED остался (etcd-запись не появлялась); journal НЕ про
        // недостижимость (503 — не transient: гейт-ветка молчалива)
        outcome.IsSuccess.Should().BeTrue();
        rig.Etcd.Store.Should().NotContainKey(FullKey("20260908030000Z"));
        var entry503 = (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value;
        entry503?.Phase.Should().NotStartWith("receiver-unavailable");
    }

    // AAA: 409 со СВОИМ id в теле (пере-старт прошлого тика, приёмник новой
    // версии) — как раньше: PLANNED → RUNNING
    [Fact]
    public async Task StartFull_Conflict409OwnId_PlannedBecomesRunning()
    {
        // Arrange — PLANNED; приёмник отвечает 409 с id нашей операции
        var rig = await NewRig();
        rig.Receiver.StartFullReply = FakeReceiverClient.Conflict("20260908030000Z");
        var backups = Seeded("20260908030000Z", FullBackupStatus.Planned);

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), backups, CancellationToken.None);

        // Assert — своя операция: тот же id стал RUNNING
        outcome.IsSuccess.Should().BeTrue(outcome.Error?.ToString());
        FullOf(rig.Etcd, "20260908030000Z").State.Should().Be(FullBackupStatus.Running);
        (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.Phase
            .Should().Be("started/shard1/20260908030000Z");
    }

    // AAA: 409 с ЧУЖИМ id (после job-timeout с недоехавшим cancel старая
    // операция живёт) — не-наша операция: transient с журналом, PLANNED
    // остаётся (фантомный COMPLETED чужого снимка исключён)
    [Fact]
    public async Task StartFull_Conflict409ForeignId_PlannedStays_JournalForeign()
    {
        // Arrange — PLANNED; приёмник отвечает 409 с ЧУЖИМ id активной операции
        var rig = await NewRig();
        rig.Receiver.StartFullReply = FakeReceiverClient.Conflict("20250101000000Z");
        var backups = Seeded("20260908030000Z", FullBackupStatus.Planned);

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), backups, CancellationToken.None);

        // Assert — статус не тронут (ключа нет), journal — foreign-active с чужим id
        outcome.IsSuccess.Should().BeTrue();
        rig.Etcd.Store.Should().NotContainKey(FullKey("20260908030000Z"));
        var entry = (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!;
        entry.Phase.Should().Be("foreign-active/shard1/20260908030000Z");
        entry.LastError.Should().Contain("20250101000000Z");
    }

    // AAA: неклассифицированный код (404 — нода на до-t26 образе) в G3 —
    // НЕ теряется молча: journal receiver-rejected с кодом, PLANNED остаётся
    [Fact]
    public async Task StartFull_Unclassified404_G3JournalsAndStaysPlanned()
    {
        // Arrange — тик планировщика (бэкапов нет); приёмник отвечает 404
        var rig = await NewRig();
        rig.Receiver.StartFullReply = FakeReceiverClient.Rejected();

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), [], CancellationToken.None);

        // Assert — PLANNED записан ДО команды (journal-before-manipulations)
        // и остался; journal — receiver-rejected с кодом и адресом
        outcome.IsSuccess.Should().BeTrue();
        var fullKey = rig.Etcd.Store.Keys.Single(k => k.StartsWith("/pgworker/backups/shop/shard1/full/"));
        var id = fullKey.Split('/')[^1];
        FullOf(rig.Etcd, id).State.Should().Be(FullBackupStatus.Planned);
        var entry = (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!;
        entry.Phase.Should().Be($"receiver-rejected/shard1/{id}");
        entry.LastError.Should().Contain("404").And.Contain(ReceiverUrlMaster);
    }

    // AAA: неклассифицированный код (404) в PLANNED-ветке супервиза — journal
    // receiver-rejected, PLANNED остаётся (симметрия 502-transient-теста выше)
    [Fact]
    public async Task PlannedRepost_Unclassified404_JournalsAndStaysPlanned()
    {
        // Arrange — PLANNED; приёмник отвечает 404
        var rig = await NewRig();
        rig.Receiver.StartFullReply = FakeReceiverClient.Rejected();
        var backups = Seeded("20260908030000Z", FullBackupStatus.Planned);

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), backups, CancellationToken.None);

        // Assert — статус не тронут (ключа нет), journal — receiver-rejected
        outcome.IsSuccess.Should().BeTrue();
        rig.Etcd.Store.Should().NotContainKey(FullKey("20260908030000Z"));
        var entry = (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!;
        entry.Phase.Should().Be("receiver-rejected/shard1/20260908030000Z");
        entry.LastError.Should().Contain("404");
    }

    // AAA: GET uploading → state=UPLOADING + wal_start_segment (node/role сохранены)
    [Fact]
    public async Task GetUploading_MovesToUploading()
    {
        // Arrange — RUNNING; GET отвечает uploading + wal_start
        var rig = await NewRig();
        rig.Receiver.GetFullReplies.Enqueue(FakeReceiverClient.Status(
            "uploading", "uploading", wal: WalSeg));
        var backups = Seeded("20260908030000Z", FullBackupStatus.Running);

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), backups, CancellationToken.None);

        // Assert
        outcome.IsSuccess.Should().BeTrue();
        var full = FullOf(rig.Etcd, "20260908030000Z");
        full.State.Should().Be(FullBackupStatus.Uploading);
        full.WalStartSegment.Should().Be(WalSeg);
        full.Node.Should().Be("shard1a");
        full.Role.Should().Be(BackupSourceRole.Master);
    }

    // AAA: GET succeeded → COMPLETED (finished/wal/size, verify PENDING при on_create)
    [Fact]
    public async Task GetSucceeded_CompletesWithVerifyPending()
    {
        // Arrange — RUNNING; GET отвечает succeeded
        var rig = await NewRig();
        rig.Receiver.GetFullReplies.Enqueue(FakeReceiverClient.Status(
            "succeeded", "uploading", wal: WalSeg, size: 1048576));
        var backups = Seeded("20260908030000Z", FullBackupStatus.Running);

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), backups, CancellationToken.None);

        // Assert
        outcome.IsSuccess.Should().BeTrue();
        var full = FullOf(rig.Etcd, "20260908030000Z");
        full.State.Should().Be(FullBackupStatus.Completed);
        full.FinishedUnix.Should().NotBeNull();
        full.WalStartSegment.Should().Be(WalSeg);
        full.SizeBytes.Should().Be(1048576);
        full.Verify!.State.Should().Be(BackupVerifyStatus.Pending);
        (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.Phase
            .Should().Be("completed/shard1/20260908030000Z");
    }

    // AAA: GET failed → FAILED + однострочная причина приёмника
    [Fact]
    public async Task GetFailed_FailsWithError()
    {
        // Arrange — RUNNING; GET отвечает failed
        var rig = await NewRig();
        rig.Receiver.GetFullReplies.Enqueue(FakeReceiverClient.Status(
            "failed", "basebackup", error: "pg_basebackup: connection refused"));
        var backups = Seeded("20260908030000Z", FullBackupStatus.Running);

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), backups, CancellationToken.None);

        // Assert
        outcome.IsSuccess.Should().BeTrue();
        var full = FullOf(rig.Etcd, "20260908030000Z");
        full.State.Should().Be(FullBackupStatus.Failed);
        full.Error.Should().Be("pg_basebackup: connection refused");
        full.FinishedUnix.Should().NotBeNull();
        (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.Phase
            .Should().Be("failed/shard1/20260908030000Z");
    }

    // AAA: GET 404 при RUNNING (рестарт приёмника/ноды) → FAILED
    // operation-vanished; переснятие по G3 с бэкоффом (Р9)
    [Fact]
    public async Task GetNotFound_MarksFailedOperationVanished()
    {
        // Arrange — RUNNING; GET отвечает 404
        var rig = await NewRig();
        rig.Receiver.GetFullReplies.Enqueue(new ReceiverReply(404, null, null));
        var backups = Seeded("20260908030000Z", FullBackupStatus.Running);

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), backups, CancellationToken.None);

        // Assert
        outcome.IsSuccess.Should().BeTrue();
        var full = FullOf(rig.Etcd, "20260908030000Z");
        full.State.Should().Be(FullBackupStatus.Failed);
        full.Error.Should().Be("operation-vanished");
        full.FinishedUnix.Should().NotBeNull();
        (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.Phase
            .Should().Be("vanished/shard1/20260908030000Z");
    }

    // AAA: транспортный отказ GET (0/5xx) — transient тика: статус в etcd не тронут
    [Fact]
    public async Task GetTransient_KeepsStatus()
    {
        // Arrange — RUNNING; GET — транспортный отказ
        var rig = await NewRig();
        rig.Receiver.GetFullReplies.Enqueue(FakeReceiverClient.Transport());
        var backups = Seeded("20260908030000Z", FullBackupStatus.Running);

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), backups, CancellationToken.None);

        // Assert — статус и ключи нетронуты (статус живёт в аргументе тика)
        outcome.IsSuccess.Should().BeTrue();
        rig.Etcd.Store.Should().NotContainKey(FullKey("20260908030000Z"));
        rig.Receiver.CancelCalls.Should().Be(0);
    }

    // AAA (AC5): RUNNING старше 6 ч → FAILED "job-timeout…" + cancel API вызван
    // (best-effort); повторный тик — переснятие НОВЫМ id (бэкофф давно прошёл)
    [Fact]
    public async Task Supervise_FullOlderThanBudget_FailedAndCancelCalled()
    {
        // Arrange — активный RUNNING started = now-7h (бюджет 6 ч дефолт)
        var rig = await NewRig();
        var now = TimeProvider.System.GetUtcNow();
        var active = new FullBackupState("20260908030000Z", FullBackupStatus.Running,
            "shard1a", BackupSourceRole.Master, Unix(now.AddHours(-7)), null, null, null, null, null);

        // Act 1 — тик супервиза
        var outcome = await rig.Process.TickAsync(
            await Snapshot(rig.Etcd), BackupsOf(active), CancellationToken.None);

        // Assert 1 — FAILED job-timeout; cancel вызван по адресу ноды
        outcome.IsSuccess.Should().BeTrue(outcome.Error?.ToString());
        var failed = FullOf(rig.Etcd, "20260908030000Z");
        failed.State.Should().Be(FullBackupStatus.Failed);
        failed.Error.Should().Contain("job-timeout");
        rig.Receiver.CancelCalls.Should().Be(1);
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
            .Should().HaveCount(2, "a reshoot uses a NEW id — the old FAILED record is history");
    }

    // AAA (t13 AC2): нода исчезла из portalloc навсегда, RUNNING старше бюджета:
    // FAILED job-timeout — самостоятельный факт etcd; cancel пропущен (ноды нет
    // — адрес не собрать) с journal-пометкой в той же записи
    [Fact]
    public async Task SourceVanished_AgeOverBudget_FailedCancelSkipped()
    {
        // Arrange — RUNNING started = now-7h, node = shard1z (нет в portalloc)
        var rig = await NewRig();
        var now = TimeProvider.System.GetUtcNow();
        var active = new FullBackupState("20260908030000Z", FullBackupStatus.Running,
            "shard1z", BackupSourceRole.Master, Unix(now.AddHours(-7)), null, null, null, null, null);

        // Act
        var outcome = await rig.Process.TickAsync(
            await Snapshot(rig.Etcd), BackupsOf(active), CancellationToken.None);

        // Assert — FAILED job-timeout; cancel пропущен с пометкой
        outcome.IsSuccess.Should().BeTrue(outcome.Error?.ToString());
        FullOf(rig.Etcd, "20260908030000Z").Error.Should().Contain("job-timeout");
        rig.Receiver.CancelCalls.Should().Be(0, "node unavailable — cancel skipped");
        var entry = (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!;
        entry.Phase.Should().Be("job-timeout/shard1/20260908030000Z");
        entry.LastError.Should().Contain("job-timeout").And.Contain("cancel skipped");
    }

    // AAA (t13 AC3): возраст свыше бюджета + cancel-транспортный отказ — FAILED
    // всё равно ставится; journal-пометка «cancel пропущен» в той же записи
    [Fact]
    public async Task CancelFails_AgeOverBudget_FailedAndCancelSkipped()
    {
        // Arrange — RUNNING старше бюджета на живой ноде; cancel недостижим
        var rig = await NewRig();
        var now = TimeProvider.System.GetUtcNow();
        var active = new FullBackupState("20260908030000Z", FullBackupStatus.Running,
            "shard1a", BackupSourceRole.Master, Unix(now.AddHours(-7)), null, null, null, null, null);
        rig.Receiver.CancelFullReply = FakeReceiverClient.Transport();

        // Act
        var outcome = await rig.Process.TickAsync(
            await Snapshot(rig.Etcd), BackupsOf(active), CancellationToken.None);

        // Assert — FAILED job-timeout; cancel попытался и не достучался (пометка)
        outcome.IsSuccess.Should().BeTrue(outcome.Error?.ToString());
        FullOf(rig.Etcd, "20260908030000Z").Error.Should().Contain("job-timeout");
        rig.Receiver.CancelCalls.Should().Be(1);
        var entry = (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!;
        entry.LastError.Should().Contain("cancel skipped");
    }

    // AAA: инвариант одного активного — при RUNNING новую попытку не создаём
    [Fact]
    public async Task ActiveExists_DoesNotPlanSecond()
    {
        // Arrange — активная RUNNING-попытка (GET отвечает running)
        var rig = await NewRig();
        var now = TimeProvider.System.GetUtcNow();
        var active = new FullBackupState("20260908030000Z", FullBackupStatus.Running,
            "shard1a", BackupSourceRole.Master, Unix(now.AddMinutes(-2)), null, null, null, null, null);
        rig.Receiver.GetFullReplies.Enqueue(FakeReceiverClient.Status("running", "basebackup"));

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), BackupsOf(active), CancellationToken.None);

        // Assert — ни нового ключа, ни второго StartFull (Get — один)
        outcome.IsSuccess.Should().BeTrue();
        rig.Etcd.Store.Keys.Should().NotContain(k => k.StartsWith("/pgworker/backups/shop/shard1/full/"));
        rig.Receiver.StartFullUrls.Should().BeEmpty();
        rig.Receiver.GetCalls.Should().Be(1);
    }

    // AAA: restore-гвард (t05 §3.4) — шард с активной restore-заявкой при due-
    // условиях новый полный НЕ планирует (шард демонтируется restore-процессом)
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

        // Assert — тик Done, новых полных и команд нет (гвард сработал до due)
        outcome.IsSuccess.Should().BeTrue();
        rig.Etcd.Store.Keys.Should().NotContain(k => k.StartsWith("/pgworker/backups/shop/shard1/full/"));
        rig.Receiver.StartFullUrls.Should().BeEmpty();
    }

    // AAA: свежий COMPLETED + живой wal-ключ — не due, новых записей нет, но
    // гвард backup_exec исполнен (G2 — каждый тик, до due-гвардов). t05 §3.5:
    // без wal-ключа свежий полный — уже due (инвариант цепочки).
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
        rig.Receiver.StartFullUrls.Should().BeEmpty();
        var guard = rig.Sql.Scalars.Should().ContainSingle().Subject;
        guard.Sql.Should().Contain("backup_exec");
    }

    // AAA (AC1): wal-ключ WAL_CHAIN_ERROR → планировщик создаёт новый полный, даже если
    // последний COMPLETED свежий (инвариант одного активного/бэкофф — как всегда)
    [Fact]
    public async Task Tick_OnBrokenWal_PlansReshoot()
    {
        // Arrange — COMPLETED ..42 (5 мин назад) + WAL_CHAIN_ERROR с границей ..44 ВЫШЕ
        // старого полного (дыра выше ..44): ..42 разрыв НЕ покрывает — пересъём нужен
        var rig = await NewRig();
        var now = TimeProvider.System.GetUtcNow();
        var completed = new FullBackupState("20260908030000Z", FullBackupStatus.Completed,
            "shard1a", BackupSourceRole.Master, Unix(now.AddMinutes(-6)),
            Unix(now.AddMinutes(-5)), "000000010000000000000042", 1048576, null, null);
        var brokenWal = new WalStreamState(WalStreamStatus.WalChainError, "slot_shop_shard1", "shard1a",
            "000000010000000000000044", "000000010000000000000044", "000000010000000000000044",
            Unix(now.AddMinutes(-30)), null, "WAL chain gap");
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
        // WAL_CHAIN_ERROR лечится пересъёмом независимо от свежести полного (spec §3.2)
        outcome.IsSuccess.Should().BeTrue(outcome.Error?.ToString());
        var fullKeys = rig.Etcd.Store.Keys
            .Where(k => k.StartsWith("/pgworker/backups/shop/shard1/full/")).ToList();
        fullKeys.Should().ContainSingle("a new full is planned");
        fullKeys.Single().Should().NotContain("20260908030000Z", "a new id — not the re-shot old one");
        rig.Receiver.StartFullUrls.Should().ContainSingle("the reshoot is started via the receiver API");
    }

    // AAA (t07): COMPLETED-полный с wal_start ≥ границы разрыва УЖЕ покрывает
    // WAL_CHAIN_ERROR — повторный пересъём не планируется (иначе шторм пересъёмов)
    [Fact]
    public async Task Tick_OnBrokenCoveredByFull_NoReshoot()
    {
        // Arrange — COMPLETED ..46 (старт выше границы разрыва ..42) + WAL_CHAIN_ERROR ..42
        var rig = await NewRig();
        var now = TimeProvider.System.GetUtcNow();
        var reshot = new FullBackupState("20260908030500Z", FullBackupStatus.Completed,
            "shard1a", BackupSourceRole.Master, Unix(now.AddMinutes(-6)),
            Unix(now.AddMinutes(-5)), "000000010000000000000046", 1048576, null, null);
        var brokenWal = new WalStreamState(WalStreamStatus.WalChainError, "slot_shop_shard1", "shard1a",
            "000000010000000000000042", "000000010000000000000042", "000000010000000000000042",
            Unix(now.AddMinutes(-30)), null, "WAL chain gap");
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
            "a reshoot on top of a covering full is a storm");
        rig.Receiver.StartFullUrls.Should().BeEmpty();
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
        rig.Receiver.StartFullUrls.Should().BeEmpty();
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
        rig.Receiver.StartFullUrls.Should().BeEmpty();
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

    // AAA: pg_hba-гвард на усвоенной ноде (object) — команда приёмнику по имени
    // её фактического контейнера (WalReceiverAddress.ForNode), не по pgw-имени
    [Fact]
    public async Task AdoptedNodes_HbaGuard_AddressesObjectContainer()
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

        // Assert — команды ушли приёмникам object-контейнеров, pgw-путь не звался
        outcome.IsSuccess.Should().BeTrue(outcome.Error?.ToString());
        outcome.Value.Should().Be(ProcessOutcome.Done);
        rig.Sql.Scalars.Should().NotBeEmpty("the G2 guard executed (master is resolved byName)");
        rig.Receiver.HbaUrls.Should().BeEquivalentTo(
            ["http://as-shard1a:8080", "http://as-shard1b:8080"]);
    }

    // ─── Хелперы ───

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

    // Хелпер: разбор статуса полного из etcd-фейка после тика.
    private static FullBackupState FullOf(Fakes.FakeEtcd etcd, string id)
    {
        var parsed = BackupsParser.Parse(
            (IReadOnlyList<Kv>)[new Kv(FullKey(id), etcd.Store[FullKey(id)].Value, 1)], out var errors);
        errors.Should().BeEmpty();
        return parsed.Value[0].Shards["shard1"].Full.Single();
    }
}
