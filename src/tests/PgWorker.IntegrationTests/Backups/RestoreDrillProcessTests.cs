using System.Text.Json;
using FluentAssertions;
using PgWorker.Backups;
using PgWorker.Backups.Drill;
using PgWorker.Backups.Job;
using PgWorker.Backups.Process;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Core.Tuning;
using PgWorker.Docker.Drivers;
using Shared.Etcd.Client;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Probes;
using Xunit;

namespace PgWorker.IntegrationTests.Backups;

// Интеграции RestoreDrillProcess (reliability t02, spec §3.3 п.2): у КАЖДОГО
// Fact своё etcd-окружение OwnEtcd (guid-имя pgw-ee-*, динамический порт,
// own-only teardown с ассертом чистоты — docs/e2e-isolation.md) + фейки
// docker-движка и S3 (паттерны BackupVerifyProcessTests/FakeBackupDeps).
[Collection(NonE2eCollection.Name)]
public class RestoreDrillProcessTests
{
    // Окружение Fact'а (свой etcd); создаётся в начале каждого сценария.
    private OwnEtcd Fx = null!;

    // ClaimStore ОДИН на окружение: InstanceId фиксируется клэймом SeedAsync.
    private ClaimStore? _claimsStore;

    private ClaimStore Claims => _claimsStore ??= new("/pgworker", [Fx.Endpoint], Fx.Gateway, TimeProvider.System);

    // ── Фейк docker-движка (копия FakeVerifyEngine + RemoveFailsOnce —
    //    transient-сценарий сноса: rm не прошёл, ключ остаётся с cleaning) ──
    internal sealed class FakeDrillEngine : IDockerEngine
    {
        // ── union-члены t07 (kfw/vwk-методы): pg-доменом не используются — стабы ──
        public Task<Result> DeleteNetworkAsync(string name, CancellationToken ct) => Task.FromResult(Result.Success());
        public Task<Result> NetworkConnectAsync(string network, string container, CancellationToken ct) => Task.FromResult(Result.Success());
        public Task<Result<bool>> VolumeExistsAsync(string name, CancellationToken ct) => Task.FromResult(Result<bool>.Success(false));
        public Task<Result> EnsureVolumeAsync(string name, CancellationToken ct) => Task.FromResult(Result.Success());
        public Task<Result> DeleteVolumeAsync(string name, CancellationToken ct) => Task.FromResult(Result.Success());
        public Task<Result> PutVolumeArchiveAsync(string name, byte[] tar, string image, CancellationToken ct) => Task.FromResult(Result.Success());
        public Task<Result<byte[]?>> GetVolumeArchiveAsync(string name, string image, CancellationToken ct) => Task.FromResult(Result<byte[]?>.Success(null));
        public Task<Result<NodeLimits?>> InspectContainerResourcesAsync(string name, CancellationToken ct) => Task.FromResult(Result<NodeLimits?>.Success(null));
        public Task<Result<NodeLimits?>> InspectServiceResourcesAsync(string name, CancellationToken ct) => Task.FromResult(Result<NodeLimits?>.Success(null));
        public Task<Result<IReadOnlyDictionary<string, string>?>> InspectContainerEnvAsync(string idOrName, CancellationToken ct) => Task.FromResult(Result<IReadOnlyDictionary<string, string>?>.Success(null));
        public Task<Result<IReadOnlyDictionary<string, string>?>> InspectServiceEnvAsync(string name, CancellationToken ct) => Task.FromResult(Result<IReadOnlyDictionary<string, string>?>.Success(null));
        public Task<Result<IReadOnlyList<string>?>> InspectContainerCmdAsync(string idOrName, CancellationToken ct) => Task.FromResult(Result<IReadOnlyList<string>?>.Success(null));
        public Task<Result<IReadOnlyList<string>?>> InspectServiceCmdAsync(string name, CancellationToken ct) => Task.FromResult(Result<IReadOnlyList<string>?>.Success(null));
        public Task<Result<DockerNodeEndpoint?>> InspectNodeEndpointAsync(string name, int containerPort, CancellationToken ct) => Task.FromResult(Result<DockerNodeEndpoint?>.Success(null));

        internal sealed record ContainerRec(string Id, string State, int ExitCode, string Logs,
            long? StartedUnix = null);

        public readonly Dictionary<string, ContainerRec> Containers = [];
        public readonly List<(string Name, ContainerSpec Spec)> Created = [];
        public readonly List<string> Started = [];
        public readonly List<string> Removed = [];
        public readonly List<string> RemovedVolumes = [];
        public bool ListFails { get; set; }
        public bool LogsFails { get; set; }
        public bool InspectFails { get; set; }

        // Один transient-сбой rm (снос: ключ остаётся с cleaning, тик повторяет).
        public bool RemoveFailsOnce { get; set; }

        public Task<Result> PingAsync(CancellationToken ct) => Task.FromResult(Result.Success());

        public Task<Result<IReadOnlyList<DockerContainer>>> ListContainersAsync(
            string namePrefix, bool all, CancellationToken ct)
        {
            if (ListFails)
                return Task.FromResult(Result<IReadOnlyList<DockerContainer>>.Failed(
                    new ApplicationException("docker: list failed")));
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
            var found = Containers.FirstOrDefault(p => p.Value.Id == id || p.Key == id);
            return Task.FromResult(found.Key is null
                ? Result<DockerContainerInspect>.Failed(new KeyNotFoundException(id))
                : Result<DockerContainerInspect>.Success(new DockerContainerInspect(
                    found.Value.Id, found.Key, [], [], [],
                    found.Value.State == "running", found.Value.ExitCode,
                    StartedAtUnix: found.Value.StartedUnix)));
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
            if (RemoveFailsOnce)
            {
                RemoveFailsOnce = false;
                return Task.FromResult(Result.Failed(new ApplicationException("docker: rm failed")));
            }

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
        public Task<Result> EnsureNetworkAsync(string name, CancellationToken ct) => Task.FromResult(Result.Success());
        public Task<Result<IReadOnlyList<DockerSwarmNode>>> ListNodesAsync(CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<DockerSwarmNode>>.Success([]));
        public Task<Result> CreateServiceAsync(ServiceSpec spec, CancellationToken ct) => Task.FromResult(Result.Success());
        public Task<Result> RemoveServiceAsync(string name, CancellationToken ct) => Task.FromResult(Result.Success());
        public Task<Result<IReadOnlyList<string>>> ListServicesAsync(string namePrefix, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<string>>.Success([]));
        public Task<Result<IReadOnlyList<DockerTask>>> ListTasksAsync(string serviceName, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<DockerTask>>.Success([]));
        public Task<Result<IReadOnlySet<(string Host, int Port)>>> BusyPortsAsync(CancellationToken ct)
            => Task.FromResult(Result<IReadOnlySet<(string Host, int Port)>>.Success(
                (IReadOnlySet<(string Host, int Port)>)new HashSet<(string, int)>()));
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    // Драйвер: один хост h1 (ctor по образцу verify) или два h1+h2 (мульти-хост
    // кейсы доводки/супервиза — Finding 1 ревью). GetHostsAsync — таблица
    // Docker:Hosts = ключи словаря движков (порядок Ordinal).
    internal sealed class FakeDrillDriver : IClusterDriver
    {
        private readonly Dictionary<string, IDockerEngine> _engines;

        public FakeDrillDriver(FakeDrillEngine engine)
            : this(engine, null)
        {
        }

        public FakeDrillDriver(FakeDrillEngine h1Engine, FakeDrillEngine? h2Engine)
        {
            _engines = new Dictionary<string, IDockerEngine> { ["h1"] = h1Engine };
            if (h2Engine is not null)
                _engines["h2"] = h2Engine;
        }

        public bool SupportsRunningInspection => true;

        public IDockerEngine? EngineFor(string host)
            => _engines.TryGetValue(host, out var engine) ? engine : null;

        public IReadOnlyDictionary<string, IDockerEngine> Engines => _engines;
        public Task<Result> RemoveBackupJobsAsync(string cluster, CancellationToken ct)
            => Task.FromResult(Result.Success());
        public Task<Result> RemoveRestoreJobsAsync(string cluster, string shard, CancellationToken ct)
            => Task.FromResult(Result.Success());
        // Сбой таблицы Docker:Hosts (transient-окно после вердикта, F1).
        public bool HostsFails { get; set; }

        public Task<Result<IReadOnlyList<HostInfo>>> GetHostsAsync(CancellationToken ct)
        {
            if (HostsFails)
                return Task.FromResult(Result<IReadOnlyList<HostInfo>>.Failed(
                    new ApplicationException("docker: hosts table failed")));
            return Task.FromResult(Result<IReadOnlyList<HostInfo>>.Success(
                (IReadOnlyList<HostInfo>)[.. _engines.Keys.OrderBy(k => k, StringComparer.Ordinal)
                    .Select(k => new HostInfo(k, 0))]));
        }
        public Task<Result> EnsureBackupAgentAsync(
            string cluster, string shard, string node, ContainerSpec spec, string host, CancellationToken ct)
            => Task.FromResult(Result.Success());
        public Task<Result> RemoveBackupAgentsAsync(string cluster, string? shard, CancellationToken ct)
            => Task.FromResult(Result.Success());
        public Task<Result<IReadOnlyList<DockerContainer>>> ListBackupAgentsAsync(string cluster, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<DockerContainer>>.Success([]));

        private static NotSupportedException NotSupported() => new("не используется в тестах дрилла");
        public Task<Result<IReadOnlySet<(string Host, int Port)>>> GetBusyPortsAsync(CancellationToken ct) => throw NotSupported();
        public Task<Result> EnsureNodeAsync(ShardTopology topology, string nodeName, NodeAddress addr,
            InstallSecrets secrets, EtcdEndpoints etcd, NodeResources? resources, PgTuneResult? tuning,
            bool syncStrict, CancellationToken ct) => throw NotSupported();
        public Task<Result> RemoveNodeAsync(string cluster, string shard, string nodeName, CancellationToken ct) => throw NotSupported();
        public Task<Result> StopNodeAsync(string cluster, string shard, string nodeName, CancellationToken ct) => throw NotSupported();
        public Task<Result<DataPresence>> NodeDataPresenceAsync(string cluster, string shard, string node, CancellationToken ct) => throw NotSupported();
        public Task<Result<IReadOnlyDictionary<string, DiscoveredNode>>> InspectNodesAsync(
            string cluster, IReadOnlyCollection<string> nodeNames, CancellationToken ct) => throw NotSupported();
        public Task<Result<string>> ExecNodeAsync(string cluster, string shard, string node,
            IReadOnlyList<string> cmd, CancellationToken ct) => throw NotSupported();
        public Task<Result<string>> ExecContainerAsync(string containerName, IReadOnlyList<string> cmd, CancellationToken ct) => throw NotSupported();
        public Task<Result<IReadOnlyList<string>>> ListNodeObjectsAsync(string cluster, CancellationToken ct) => throw NotSupported();
    }

    // ── Сид-хелперы ──
    private static ClusterSnapshot BuildSnap(string cluster) => new(
        new ClusterConfig(cluster, 2, cluster, null, ClusterState.Active),
        [new ShardSpec("shard1", 1, $"host=shard1a dbname={cluster}", "shard1a:17001",
            [new NodeSpec("shard1", "shard1a", NodeState.Running)])],
        []);

    private const string Shard = "shard1";
    private const string DrillId = "20261001120000Z";

    private static string DrillKey(string cluster) => BackupNames.DrillKey(cluster, Shard);

    private async Task SeedAsync(string cluster)
    {
        var ct = TestContext.Current.CancellationToken;
        await Fx.Gateway.DeleteAsync(Fx.Endpoint, $"/pgworker/backups/{cluster}/", prefix: true, ct);
        await Fx.Gateway.DeleteAsync(Fx.Endpoint, $"/pgworker/claims/{cluster}", prefix: false, ct);
        await Fx.Gateway.PutAsync(Fx.Endpoint, $"/pgworker/portalloc/{cluster}",
            Portalloc.Serialize(new Dictionary<string, NodeAddress>
            {
                ["shard1/shard1a"] = new("h1", new NodePorts(16001, 18001, 17001)),
            }), null, ct);
        (await Claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue("клэйм — предусловие тика");
    }

    private RestoreDrillProcess BuildProcess(
        string cluster, FakeDrillDriver driver, IBackupS3 s3, BackupsRuntimeOptions? options = null,
        Action<string, string, string>? drillObserver = null)
        => new(
            Fx.Gateway, [Fx.Endpoint], driver,
            new ShardEndpoints(Fx.Gateway, [Fx.Endpoint], new ShardProbe(new HttpClient())),
            s3, Claims, new WorkJournal("/pgworker", Fx.Gateway, [Fx.Endpoint]),
            options ?? new BackupsRuntimeOptions(
                Enabled: true, S3Endpoint: "http://minio", S3Bucket: "bkt",
                S3AccessKey: "ak", S3SecretKey: "sk", JobImage: "pgworker-backup:test"),
            TimeProvider.System,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<RestoreDrillProcess>.Instance,
            drillObserver);

    // Сид drill-ключа шарда (значение — сериализатор канона §4).
    private async Task SeedDrillAsync(string cluster, DrillState drill)
    {
        var ct = TestContext.Current.CancellationToken;
        await Fx.Gateway.PutAsync(Fx.Endpoint, DrillKey(cluster), DrillStatusJson.Serialize(drill), null, ct);
    }

    private static long NowUnix() => DateTimeOffset.UtcNow.ToUnixTimeSeconds();

    // Сид контейнера drill-джоба в фейк-движке.
    private static void SeedContainer(FakeDrillEngine engine, string name, string state, int exitCode, string logs)
        => engine.Containers[name] = new FakeDrillEngine.ContainerRec(
            Guid.NewGuid().ToString("N"), state, exitCode, logs);

    private async Task<string> ReadDrillAsync(string cluster)
    {
        var kv = await Fx.Gateway.GetAsync(Fx.Endpoint, DrillKey(cluster),
            TestContext.Current.CancellationToken);
        kv.Value.Should().NotBeNull("статус дрилла пишется в ключ /drill");
        return kv.Value!.Value;
    }

    // Backups-аргумент тика — снапшот префикса кластера через снапшотный
    // BackupsParser (как в ReconcileLoop: процесс читает модель, не etcd).
    private async Task<IReadOnlyList<ClusterBackups>> SnapshotBackupsAsync(string cluster)
    {
        var ct = TestContext.Current.CancellationToken;
        var range = await Fx.Gateway.RangeAsync(Fx.Endpoint, $"/pgworker/backups/{cluster}/", ct);
        range.IsSuccess.Should().BeTrue();
        var parsed = BackupsParser.Parse(range.Value, out var errors);
        errors.Should().BeEmpty("сид-ключи обязаны парситься");
        return parsed.Value;
    }

    // AAA: RUNNING + контейнер running → phase из логов пишется в ключ при изменении.
    [Fact]
    public async Task Supervise_RunningContainer_UpdatesPhaseFromLogs()
    {
        // Arrange — свой etcd; RUNNING-ключ + running-контейнер с фазой recovering.
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr1";
        await SeedAsync(cluster);
        var engine = new FakeDrillEngine();
        var driver = new FakeDrillDriver(engine);
        SeedContainer(engine, BackupNames.DrillContainerName(cluster, Shard, DrillId),
            "running", -1, "{\"phase\":\"recovering\"}");
        await SeedDrillAsync(cluster,
            new DrillState(DrillId, DrillStatus.Running, "20261001090000Z", NowUnix()));
        var process = BuildProcess(cluster, driver, new FakeBackupS3());

        // Act
        var result = await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct);

        // Assert — ключ содержит phase recovering (из логов джоба).
        result.IsSuccess.Should().BeTrue();
        (await ReadDrillAsync(cluster)).Should().Contain("\"phase\":\"recovering\"");
    }

    // AAA: exited 0 + result ok:true → SUCCEEDED + finished_unix + restored_to_lsn
    // → cleaning → контейнер и volume удалены, phase снят.
    [Fact]
    public async Task Supervise_ExitOk_SucceedsAndCleansUp()
    {
        // Arrange — exited(0) с result-JSON ок.
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr2";
        await SeedAsync(cluster);
        var engine = new FakeDrillEngine();
        var driver = new FakeDrillDriver(engine);
        var name = BackupNames.DrillContainerName(cluster, Shard, DrillId);
        SeedContainer(engine, name, "exited", 0,
            "{\"phase\":\"recovering\"}\n{\"ok\":true,\"restored_to_lsn\":\"0/42\"}");
        await SeedDrillAsync(cluster,
            new DrillState(DrillId, DrillStatus.Running, "20261001090000Z", NowUnix()));
        var process = BuildProcess(cluster, driver, new FakeBackupS3());

        // Act — один тик: вердикт + доводимый снос до чистого итога.
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct)).IsSuccess.Should().BeTrue();

        // Assert — SUCCEEDED с LSN, phase снят, контейнер и volume снесены.
        var key = await ReadDrillAsync(cluster);
        key.Should().Contain("\"SUCCEEDED\"").And.Contain("\"restored_to_lsn\":\"0/42\"");
        key.Should().NotContain("phase", "чистый терминальный итог — контур снесён");
        key.Should().Contain("finished_unix");
        engine.Removed.Should().Contain(name);
        engine.RemovedVolumes.Should().Contain(BackupNames.DrillVolumeName(cluster, Shard, DrillId));
    }

    // AAA: exited 1 → FAILED с error из result; снос тот же.
    [Fact]
    public async Task Supervise_ExitFail_FailsWithErrorAndCleansUp()
    {
        // Arrange — exited(1) с result-JSON ошибки.
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr3";
        await SeedAsync(cluster);
        var engine = new FakeDrillEngine();
        var driver = new FakeDrillDriver(engine);
        var name = BackupNames.DrillContainerName(cluster, Shard, DrillId);
        SeedContainer(engine, name, "exited", 1, "{\"ok\":false,\"error\":\"boom\"}");
        await SeedDrillAsync(cluster,
            new DrillState(DrillId, DrillStatus.Running, "20261001090000Z", NowUnix()));
        var process = BuildProcess(cluster, driver, new FakeBackupS3());

        // Act
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct)).IsSuccess.Should().BeTrue();

        // Assert — FAILED с причиной, контейнер/volume снесены, phase снят.
        var key = await ReadDrillAsync(cluster);
        key.Should().Contain("\"FAILED\"").And.Contain("boom");
        key.Should().NotContain("phase");
        engine.Removed.Should().Contain(name);
        engine.RemovedVolumes.Should().Contain(BackupNames.DrillVolumeName(cluster, Shard, DrillId));
    }

    // AAA: рестарт посреди сноса: терминальный ключ с phase=cleaning + живые
    // контейнер/volume → следующий тик дочищает до чистого итога.
    [Fact]
    public async Task Cleanup_RestartMidCleaning_NextTickFinishes()
    {
        // Arrange — SUCCEEDED+cleaning в ключе (вердикт прошлого инстанса),
        // exited-контейнер жив (снос не доведён).
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr4";
        await SeedAsync(cluster);
        var engine = new FakeDrillEngine();
        var driver = new FakeDrillDriver(engine);
        var name = BackupNames.DrillContainerName(cluster, Shard, DrillId);
        SeedContainer(engine, name, "exited", 0, "{\"ok\":true}");
        await SeedDrillAsync(cluster,
            new DrillState(DrillId, DrillStatus.Succeeded, "20261001090000Z", NowUnix() - 600,
                FinishedUnix: NowUnix() - 300, Phase: "cleaning"));
        var process = BuildProcess(cluster, driver, new FakeBackupS3());

        // Act — тик «нового инстанса» дочищает снос.
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct)).IsSuccess.Should().BeTrue();

        // Assert — контейнер и volume удалены, phase снят.
        engine.Removed.Should().Contain(name);
        engine.RemovedVolumes.Should().Contain(BackupNames.DrillVolumeName(cluster, Shard, DrillId));
        var key = await ReadDrillAsync(cluster);
        key.Should().Contain("\"SUCCEEDED\"").And.NotContain("phase");

        // Act — второй тик: no-op (Removed не растёт).
        var removedBefore = engine.Removed.Count;
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct)).IsSuccess.Should().BeTrue();

        // Assert
        engine.Removed.Count.Should().Be(removedBefore, "чистый итог — повторных rm нет");
    }

    // AAA: transient docker rm — ключ остаётся с phase=cleaning, следующий тик повторяет.
    [Fact]
    public async Task Cleanup_TransientRemove_KeepsCleaningPhase()
    {
        // Arrange — SUCCEEDED+cleaning; первый rm падает (transient).
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr5";
        await SeedAsync(cluster);
        var engine = new FakeDrillEngine();
        var driver = new FakeDrillDriver(engine);
        var name = BackupNames.DrillContainerName(cluster, Shard, DrillId);
        SeedContainer(engine, name, "exited", 0, "{\"ok\":true}");
        engine.RemoveFailsOnce = true;
        await SeedDrillAsync(cluster,
            new DrillState(DrillId, DrillStatus.Succeeded, "20261001090000Z", NowUnix() - 600,
                FinishedUnix: NowUnix() - 300, Phase: "cleaning"));
        var process = BuildProcess(cluster, driver, new FakeBackupS3());

        // Act — тик с падшим rm.
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct)).IsSuccess.Should().BeTrue();

        // Assert — контейнер жив, ключ остался с cleaning (доводка не «забыта»).
        engine.Containers.Should().ContainKey(name, "rm не прошёл — контур на месте");
        (await ReadDrillAsync(cluster)).Should().Contain("\"phase\":\"cleaning\"",
            "transient сноса виден в ключе — следующий тик повторит");

        // Act — следующий тик повторяет rm (идемпотентная доводка).
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct)).IsSuccess.Should().BeTrue();

        // Assert — контур снесён, phase снят.
        engine.Removed.Should().Contain(name);
        (await ReadDrillAsync(cluster)).Should().NotContain("phase");
    }


    // ── Отбор / валидация / запуск (Task 7, spec §3.3 п.3–5) ──

    private const string FullId = "20261001090000Z";

    // Сид COMPLETED-полного шарда в etcd — кандидат отбора.
    private async Task SeedFullAsync(string cluster, string? walStart = "000000010000000000000001")
    {
        var ct = TestContext.Current.CancellationToken;
        var full = new FullBackupState(FullId, FullBackupStatus.Completed, "shard1a",
            BackupSourceRole.Replica, NowUnix() - 3600, NowUnix() - 3500, walStart, 1024, null, null);
        await Fx.Gateway.PutAsync(Fx.Endpoint, $"/pgworker/backups/{cluster}/{Shard}/full/{FullId}",
            BackupStatusJson.Serialize(full), null, ct);
    }

    // Непрерывная WAL-цепочка 1..3 + манифест кандидата (валидация проходит).
    private static void SeedValidCandidate(FakeBackupS3 s3, string cluster)
    {
        s3.Objects.Add((cluster, Shard, "000000010000000000000001"));
        s3.Objects.Add((cluster, Shard, "000000010000000000000002"));
        s3.Objects.Add((cluster, Shard, "000000010000000000000003"));
        s3.Texts[$"{cluster}/{Shard}/full/{FullId}/backup_manifest"] = "MANIFEST";
    }

    private async Task<string> ReadWorkPhaseAsync(string cluster)
    {
        var kv = await Fx.Gateway.GetAsync(Fx.Endpoint, $"/pgworker/work/{cluster}",
            TestContext.Current.CancellationToken);
        return kv.Value?.Value ?? "";
    }

    // AAA: первый дрилл стартует немедленно: COMPLETED-полный есть, ключа нет
    // → тик пишет RUNNING + создаёт+стартует контейнер pgw-backup-drill-<C>-<X>-<id>.
    [Fact]
    public async Task Tick_NoDrillYet_StartsFirstDrill()
    {
        // Arrange — свой etcd; полный + цепочка + манифест валидны.
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr6";
        await SeedAsync(cluster);
        await SeedFullAsync(cluster);
        var engine = new FakeDrillEngine();
        var driver = new FakeDrillDriver(engine);
        var s3 = new FakeBackupS3();
        SeedValidCandidate(s3, cluster);
        var process = BuildProcess(cluster, driver, s3);

        // Act
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — ключ RUNNING; джоб создан+запущен (inline bash); журнал started.
        (await ReadDrillAsync(cluster)).Should().Contain("\"RUNNING\"");
        engine.Created.Should().ContainSingle();
        engine.Created[0].Name.Should().StartWith(BackupNames.DrillJobContainerPrefix(cluster));
        var spec = engine.Created[0].Spec;
        spec.Cmd![0].Should().Be("bash");
        engine.Started.Should().Contain(engine.Created[0].Name);
        (await ReadWorkPhaseAsync(cluster)).Should().Contain("started/shard1/");
    }

    // AAA: свежий SUCCEEDED (finished_unix=now-60) → новых запусков нет до interval_days.
    [Fact]
    public async Task Tick_RecentSuccess_NoNewStart()
    {
        // Arrange
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr7";
        await SeedAsync(cluster);
        await SeedFullAsync(cluster);
        var engine = new FakeDrillEngine();
        var driver = new FakeDrillDriver(engine);
        var s3 = new FakeBackupS3();
        SeedValidCandidate(s3, cluster);
        await SeedDrillAsync(cluster,
            new DrillState(DrillId, DrillStatus.Succeeded, FullId, NowUnix() - 120,
                FinishedUnix: NowUnix() - 60, RestoredToLsn: "0/42"));
        var process = BuildProcess(cluster, driver, s3);

        // Act
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — джоба нет, ключ не изменился (интервал сутки не истёк).
        engine.Created.Should().BeEmpty();
        (await ReadDrillAsync(cluster)).Should().Contain("\"SUCCEEDED\"").And.Contain("\"restored_to_lsn\":\"0/42\"");
    }

    // AAA: interval_days=0 (конфиг) — запусков нет.
    [Fact]
    public async Task Tick_IntervalZero_NoStart()
    {
        // Arrange
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr8";
        await SeedAsync(cluster);
        await SeedFullAsync(cluster);
        var engine = new FakeDrillEngine();
        var driver = new FakeDrillDriver(engine);
        var s3 = new FakeBackupS3();
        SeedValidCandidate(s3, cluster);
        var process = BuildProcess(cluster, driver, s3, new BackupsRuntimeOptions(
            Enabled: true, S3Endpoint: "http://minio", S3Bucket: "bkt",
            S3AccessKey: "ak", S3SecretKey: "sk", JobImage: "pgworker-backup:test",
            DrillIntervalDays: 0));

        // Act
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — ключа дрилла не появилось, джобов нет.
        engine.Created.Should().BeEmpty();
        var kv = await Fx.Gateway.GetAsync(Fx.Endpoint, DrillKey(cluster), ct);
        kv.Value.Should().BeNull("интервал 0 — дрилл выключен");
    }

    // AAA: активная restore-заявка шарда — шард не кандидат.
    [Fact]
    public async Task Tick_ActiveRestore_ShardExcluded()
    {
        // Arrange — restore-ключ RUNNING шарда (restore владеет ЖЦ, arch/19 §3.5).
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr9";
        await SeedAsync(cluster);
        await SeedFullAsync(cluster);
        await Fx.Gateway.PutAsync(Fx.Endpoint,
            $"/pgworker/backups/{cluster}/{Shard}/restore/20261001100000Z",
            """{"state":"RUNNING","backup_id":"20261001090000Z","source":"c/x","target":"latest","node":"n1","requested_unix":1,"requested_by":"operator"}""",
            null, ct);
        var engine = new FakeDrillEngine();
        var driver = new FakeDrillDriver(engine);
        var s3 = new FakeBackupS3();
        SeedValidCandidate(s3, cluster);
        var process = BuildProcess(cluster, driver, s3);

        // Act
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert
        engine.Created.Should().BeEmpty("restore владеет ЖЦ шарда — дрилл не стартует");
    }

    // AAA: валидационный провал (манифеста нет) → FAILED без запуска джоба:
    // started/finished поставлены, error причина, ключ БЕЗ phase, journal failed.
    [Fact]
    public async Task Tick_ManifestMissing_FailsWithoutJob()
    {
        // Arrange — полный есть, манифест НЕ докачан (в S3 его нет).
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr10";
        await SeedAsync(cluster);
        await SeedFullAsync(cluster);
        var engine = new FakeDrillEngine();
        var driver = new FakeDrillDriver(engine);
        var s3 = new FakeBackupS3();
        s3.Objects.Add((cluster, Shard, "000000010000000000000001"));
        s3.Objects.Add((cluster, Shard, "000000010000000000000002"));
        s3.Objects.Add((cluster, Shard, "000000010000000000000003"));
        var process = BuildProcess(cluster, driver, s3);

        // Act
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — FAILED с причиной; started/finished; контейнера/volume нет; phase нет.
        var key = await ReadDrillAsync(cluster);
        key.Should().Contain("\"FAILED\"").And.Contain("backup_manifest");
        key.Should().Contain("started_unix").And.Contain("finished_unix");
        key.Should().NotContain("phase", "без джоба — чистый терминальный итог");
        engine.Created.Should().BeEmpty();
        engine.RemovedVolumes.Should().BeEmpty();
        (await ReadWorkPhaseAsync(cluster)).Should().Contain("failed/shard1/");
    }

    // AAA: дыра WAL-цепочки → FAILED с границами от WalChain, без джоба.
    [Fact]
    public async Task Tick_WalGap_FailsWithGapError()
    {
        // Arrange — wal/: 1, 3 (нет 2) — цепочка от 001 дырявая.
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr11";
        await SeedAsync(cluster);
        await SeedFullAsync(cluster);
        var engine = new FakeDrillEngine();
        var driver = new FakeDrillDriver(engine);
        var s3 = new FakeBackupS3();
        s3.Texts[$"{cluster}/{Shard}/full/{FullId}/backup_manifest"] = "MANIFEST";
        s3.Objects.Add((cluster, Shard, "000000010000000000000001"));
        s3.Objects.Add((cluster, Shard, "000000010000000000000003"));
        var process = BuildProcess(cluster, driver, s3);

        // Act
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — FAILED с границами дыры; джоб не запускался; чистый итог.
        var key = await ReadDrillAsync(cluster);
        key.Should().Contain("\"FAILED\"").And.Contain("000000010000000000000002");
        key.Should().NotContain("phase");
        engine.Created.Should().BeEmpty();
    }

    // AAA: takeover: ключ RUNNING + живой контейнер от «прошлого инстанса» —
    // новый процесс супервизит до исхода (полный путь RUNNING → SUCCEEDED).
    [Fact]
    public async Task Tick_Takeover_SeesJobThrough()
    {
        // Arrange — RUNNING-ключ и running-контейнер (инстанс сменился).
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr12";
        await SeedAsync(cluster);
        var engine = new FakeDrillEngine();
        var driver = new FakeDrillDriver(engine);
        var name = BackupNames.DrillContainerName(cluster, Shard, DrillId);
        SeedContainer(engine, name, "running", -1, "{\"phase\":\"downloading\"}");
        await SeedDrillAsync(cluster,
            new DrillState(DrillId, DrillStatus.Running, FullId, NowUnix()));
        var process = BuildProcess(cluster, driver, new FakeBackupS3());

        // Act 1 — супервиз: фаза из логов.
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert 1 — ключ видит фазу чужого джоба.
        (await ReadDrillAsync(cluster)).Should().Contain("\"phase\":\"downloading\"");

        // Act 2 — джоб «прошлого инстанса» дошёл до исхода (exited 0, ok).
        SeedContainer(engine, name, "exited", 0,
            "{\"phase\":\"recovering\"}\n{\"ok\":true,\"restored_to_lsn\":\"0/500\"}");
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert 2 — SUCCEEDED, restored_to_lsn, контур снесён (takeover доведён).
        var key = await ReadDrillAsync(cluster);
        key.Should().Contain("\"SUCCEEDED\"").And.Contain("\"restored_to_lsn\":\"0/500\"");
        key.Should().NotContain("phase");
        engine.Removed.Should().Contain(name);
    }

    // AAA: RUNNING возраст > DrillTimeoutSec (бюджет мал в options) → FAILED
    // drill-timeout + kill+rm контейнера/volume через cleaning.
    [Fact]
    public async Task Tick_RunningTooLong_FailsTimeoutAndCleans()
    {
        // Arrange — бюджет 60 с, ключ RUNNING возрастом 120 с, контейнер running.
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr13";
        await SeedAsync(cluster);
        var engine = new FakeDrillEngine();
        var driver = new FakeDrillDriver(engine);
        var name = BackupNames.DrillContainerName(cluster, Shard, DrillId);
        SeedContainer(engine, name, "running", -1, "{\"phase\":\"recovering\"}");
        await SeedDrillAsync(cluster,
            new DrillState(DrillId, DrillStatus.Running, FullId, NowUnix() - 120));
        var process = BuildProcess(cluster, driver, new FakeBackupS3(),
            new BackupsRuntimeOptions(
                Enabled: true, S3Endpoint: "http://minio", S3Bucket: "bkt",
                S3AccessKey: "ak", S3SecretKey: "sk", JobImage: "pgworker-backup:test",
                DrillTimeoutSec: 60));

        // Act
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — FAILED drill-timeout; контейнер и volume снесены; чистый итог.
        var key = await ReadDrillAsync(cluster);
        key.Should().Contain("\"FAILED\"").And.Contain("drill-timeout");
        key.Should().NotContain("phase");
        engine.Removed.Should().Contain(name);
        engine.RemovedVolumes.Should().Contain(BackupNames.DrillVolumeName(cluster, Shard, DrillId));
    }

    // AAA: RUNNING без контейнера, возраст > бюджета → FAILED drill-vanished
    // + volume сносится.
    [Fact]
    public async Task Tick_Vanished_FailsAndRemovesVolume()
    {
        // Arrange — бюджет 60 с, ключ RUNNING возрастом 120 с, контейнера НЕТ.
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr14";
        await SeedAsync(cluster);
        var engine = new FakeDrillEngine();
        var driver = new FakeDrillDriver(engine);
        await SeedDrillAsync(cluster,
            new DrillState(DrillId, DrillStatus.Running, FullId, NowUnix() - 120));
        var process = BuildProcess(cluster, driver, new FakeBackupS3(),
            new BackupsRuntimeOptions(
                Enabled: true, S3Endpoint: "http://minio", S3Bucket: "bkt",
                S3AccessKey: "ak", S3SecretKey: "sk", JobImage: "pgworker-backup:test",
                DrillTimeoutSec: 60));

        // Act
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — FAILED drill-vanished; volume снесён; джоб не создавался.
        var key = await ReadDrillAsync(cluster);
        key.Should().Contain("\"FAILED\"").And.Contain("drill-vanished");
        key.Should().NotContain("phase");
        engine.Created.Should().BeEmpty("vanished — досоздачи нет: возраст за бюджетом");
        engine.RemovedVolumes.Should().Contain(BackupNames.DrillVolumeName(cluster, Shard, DrillId));
    }

    // AAA: RUNNING без контейнера, возраст < бюджета → transient-ожидание
    // (статус не меняется); джоб досоздаётся по детерминированному имени
    // (create прошедшего тика не дошёл — канон §3.3 п.5).
    [Fact]
    public async Task Tick_VanishedYoung_Waits()
    {
        // Arrange — ключ RUNNING свежий, контейнера нет.
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr15";
        await SeedAsync(cluster);
        var engine = new FakeDrillEngine();
        var driver = new FakeDrillDriver(engine);
        await SeedDrillAsync(cluster,
            new DrillState(DrillId, DrillStatus.Running, FullId, NowUnix()));
        var process = BuildProcess(cluster, driver, new FakeBackupS3());

        // Act
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — статус не изменился (RUNNING без error/финала); джоб досоздан.
        var key = await ReadDrillAsync(cluster);
        key.Should().Contain("\"RUNNING\"").And.NotContain("drill-vanished").And.NotContain("\"FAILED\"");
        engine.Created.Should().ContainSingle(
            c => c.Name == BackupNames.DrillContainerName(cluster, Shard, DrillId));
        engine.Started.Should().Contain(BackupNames.DrillContainerName(cluster, Shard, DrillId));
    }

    // AAA: Enabled=false — доводка активного и сноса работает, новых запусков нет.
    [Fact]
    public async Task Tick_Disabled_FinishesActive_NoNewStarts()
    {
        // Arrange — полный валиден (кандидат есть), активный RUNNING-контейнер
        // дошёл до исхода; подсистема выключена.
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr16";
        await SeedAsync(cluster);
        await SeedFullAsync(cluster);
        var engine = new FakeDrillEngine();
        var driver = new FakeDrillDriver(engine);
        var name = BackupNames.DrillContainerName(cluster, Shard, DrillId);
        SeedContainer(engine, name, "exited", 0,
            "{\"ok\":true,\"restored_to_lsn\":\"0/600\"}");
        var s3 = new FakeBackupS3();
        SeedValidCandidate(s3, cluster);
        await SeedDrillAsync(cluster,
            new DrillState(DrillId, DrillStatus.Running, FullId, NowUnix() - 30));
        var process = BuildProcess(cluster, driver, s3, new BackupsRuntimeOptions(
            Enabled: false, S3Endpoint: "http://minio", S3Bucket: "bkt",
            S3AccessKey: "ak", S3SecretKey: "sk", JobImage: "pgworker-backup:test"));

        // Act 1 — доводка активного до терминала/сноса (стоп-семантика).
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert 1 — итог записан, контур снесён.
        (await ReadDrillAsync(cluster)).Should().Contain("\"SUCCEEDED\"").And.NotContain("phase");
        engine.Removed.Should().Contain(name);

        // Act 2 — новых запусков при Enabled=false нет.
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert 2
        engine.Created.Should().BeEmpty("выключение — только доводка, новых запусков нет");
    }

    // AAA: интервал = policy.drill.interval_days ?? конфига (policy перекрывает).
    [Fact]
    public async Task Tick_PolicyInterval_OverridesConfig()
    {
        // Arrange — policy интервал 0 (выключен), конфиг 1: policy перекрывает.
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr17";
        await SeedAsync(cluster);
        await SeedFullAsync(cluster);
        await Fx.Gateway.PutAsync(Fx.Endpoint, $"/pgworker/backups/{cluster}/policy",
            """{"retention":{"days":7,"weeks":4,"months":6},"full_max_age_sec":86400,"drill":{"interval_days":0}}""",
            null, ct);
        var engine = new FakeDrillEngine();
        var driver = new FakeDrillDriver(engine);
        var s3 = new FakeBackupS3();
        SeedValidCandidate(s3, cluster);
        var process = BuildProcess(cluster, driver, s3);

        // Act
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — запуска нет (policy 0 перекрыла дефолт конфига 1).
        engine.Created.Should().BeEmpty();
    }

    // ── Мульти-хост: доводка сноса и супервиз fallback-путём (Finding 1 ревью) ──

    // AAA (AC4, мульти-хост): терминальный ключ с phase=cleaning, контейнер+volume
    // на ВТОРОМ хосте → доводка (снос стартует с engine=null) перебирает хосты,
    // находит и сносит на h2, ключ — чистый итог.
    [Fact]
    public async Task Cleanup_TwoHosts_JobOnSecondHost_FindsAndCleans()
    {
        // Arrange — два хоста; джоб (exited) и volume живут на h2.
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr18";
        await SeedAsync(cluster);
        var engineH1 = new FakeDrillEngine();
        var engineH2 = new FakeDrillEngine();
        var driver = new FakeDrillDriver(engineH1, engineH2);
        var name = BackupNames.DrillContainerName(cluster, Shard, DrillId);
        SeedContainer(engineH2, name, "exited", 0, "{\"ok\":true}");
        await SeedDrillAsync(cluster,
            new DrillState(DrillId, DrillStatus.Succeeded, "20261001090000Z", NowUnix() - 600,
                FinishedUnix: NowUnix() - 300, Phase: "cleaning"));
        var process = BuildProcess(cluster, driver, new FakeBackupS3());

        // Act — тик доводки (краш-рекавери: снос без движка из супервиза).
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — контур снесён на h2 (не «404 чужого хоста»), ключ чист.
        engineH2.Removed.Should().Contain(name, "доводка обязана найти джоб на втором хосте");
        engineH2.RemovedVolumes.Should().Contain(BackupNames.DrillVolumeName(cluster, Shard, DrillId));
        (await ReadDrillAsync(cluster)).Should().Contain("\"SUCCEEDED\"").And.NotContain("phase");
    }

    // AAA (AC4, мульти-хост): джоб был на ПЕРВОМ хосте, снос стартует с engine=null
    // (тик активностей) → доводка сносит на h1 и подтверждает отсутствие на h2.
    [Fact]
    public async Task Cleanup_TwoHosts_JobOnFirstHost_CleansAndConfirmsBoth()
    {
        // Arrange
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr19";
        await SeedAsync(cluster);
        var engineH1 = new FakeDrillEngine();
        var engineH2 = new FakeDrillEngine();
        var driver = new FakeDrillDriver(engineH1, engineH2);
        var name = BackupNames.DrillContainerName(cluster, Shard, DrillId);
        SeedContainer(engineH1, name, "exited", 1, "{\"ok\":false,\"error\":\"boom\"}");
        await SeedDrillAsync(cluster,
            new DrillState(DrillId, DrillStatus.Succeeded, "20261001090000Z", NowUnix() - 600,
                FinishedUnix: NowUnix() - 300, Phase: "cleaning"));
        var process = BuildProcess(cluster, driver, new FakeBackupS3());

        // Act
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — снос на h1; h2 — только 404-проход (чистого итога не ломает).
        engineH1.Removed.Should().Contain(name);
        engineH1.RemovedVolumes.Should().Contain(BackupNames.DrillVolumeName(cluster, Shard, DrillId));
        (await ReadDrillAsync(cluster)).Should().NotContain("phase");
    }

    // AAA (AC9, мульти-хост): portalloc без шарда (шард демонтируется) —
    // fallback-супервиз ищет RUNNING-джоб на остальных хостах и супервизит
    // найденный на h2 (фаза из логов пишется в ключ).
    [Fact]
    public async Task Supervise_Fallback_JobOnSecondHost_Supervised()
    {
        // Arrange — portalloc БЕЗ shard1 (fallback-путь); джоб running на h2.
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr20";
        await SeedAsync(cluster);
        await Fx.Gateway.PutAsync(Fx.Endpoint, $"/pgworker/portalloc/{cluster}",
            Portalloc.Serialize(new Dictionary<string, NodeAddress>
            {
                // shard1 демонтируется — записи нет; остался чужой shard2
                ["shard2/shard2a"] = new("h1", new NodePorts(16011, 18011, 17011)),
            }), null, ct);
        var engineH1 = new FakeDrillEngine();
        var engineH2 = new FakeDrillEngine();
        var driver = new FakeDrillDriver(engineH1, engineH2);
        var name = BackupNames.DrillContainerName(cluster, Shard, DrillId);
        SeedContainer(engineH2, name, "running", -1, "{\"phase\":\"recovering\"}");
        await SeedDrillAsync(cluster,
            new DrillState(DrillId, DrillStatus.Running, "20261001090000Z", NowUnix()));
        var process = BuildProcess(cluster, driver, new FakeBackupS3());

        // Act
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — фаза найденного на h2 джоба в ключе; джоб НЕ считался vanished
        // (ни досоздачи на h1, ни FAILED).
        (await ReadDrillAsync(cluster)).Should().Contain("\"phase\":\"recovering\"")
            .And.NotContain("drill-vanished").And.NotContain("\"FAILED\"");
        engineH1.Created.Should().BeEmpty("досоздача-дубликат на чужом хосте запрещена");
    }

    // AAA (AC4, окно вердикт→cleaning, F1): вердикт пишется ОДНИМ put сразу с
    // phase=cleaning — transient-сбой сноса (GetHosts недоступен) после вердикта
    // оставляет ключ терминальным С cleaning (фильтр тика его видит); следующий
    // тик дочищает до чистого итога. Двух-put код оставлял бы ключ БЕЗ phase —
    // снос терялся навсегда (утечка контейнера/volume до D1).
    [Fact]
    public async Task Supervise_VerdictThenTransientCleanup_KeyKeepsCleaningPhase()
    {
        // Arrange — RUNNING + exited ok; таблица хостов недоступна (снос сорвётся).
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr22";
        await SeedAsync(cluster);
        var engineH1 = new FakeDrillEngine();
        var driver = new FakeDrillDriver(engineH1) { HostsFails = true };
        var name = BackupNames.DrillContainerName(cluster, Shard, DrillId);
        SeedContainer(engineH1, name, "exited", 0,
            "{\"ok\":true,\"restored_to_lsn\":\"0/700\"}");
        await SeedDrillAsync(cluster,
            new DrillState(DrillId, DrillStatus.Running, "20261001090000Z", NowUnix()));
        var process = BuildProcess(cluster, driver, new FakeBackupS3());

        // Act 1 — вердикт одним put; снос падает на GetHosts (transient).
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert 1 — ключ SUCCEEDED С phase=cleaning: инвариант «терминал ⇒
        // cleaning» (снос не потерян, фильтр тика выбирает); контейнер ещё жив.
        var key = await ReadDrillAsync(cluster);
        key.Should().Contain("\"SUCCEEDED\"").And.Contain("\"phase\":\"cleaning\"")
            .And.Contain("\"restored_to_lsn\":\"0/700\"");
        engineH1.Containers.Should().ContainKey(name, "снос сорвался — контур на месте");

        // Act 2 — хосты доступны: следующий тик дочищает.
        driver.HostsFails = false;
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert 2 — чистый итог: phase снят, контур снесён.
        (await ReadDrillAsync(cluster)).Should().Contain("\"SUCCEEDED\"").And.NotContain("phase");
        engineH1.Removed.Should().Contain(name);
        engineH1.RemovedVolumes.Should().Contain(BackupNames.DrillVolumeName(cluster, Shard, DrillId));
    }

    // AAA: t14 — оба пути чистого терминального итога зовут наблюдателя:
    // FAILED-валидация без джоба ("failed") и доведённый SUCCEEDED ("ok")
    [Fact]
    public async Task ТерминальныеИсходы_зовут_наблюдателя()
    {
        // Arrange 1 — полный есть, манифеста нет: FAILED-валидация без джоба
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill-obs", ct);
        Fx = fx;
        const string cluster = "dro1";
        await SeedAsync(cluster);
        await SeedFullAsync(cluster);
        var s3 = new FakeBackupS3();
        s3.Objects.Add((cluster, Shard, "000000010000000000000001"));
        s3.Objects.Add((cluster, Shard, "000000010000000000000002"));
        s3.Objects.Add((cluster, Shard, "000000010000000000000003"));
        var outcomes = new List<(string C, string S, string Result)>();
        var process = BuildProcess(cluster, new FakeDrillDriver(new FakeDrillEngine()), s3,
            drillObserver: (c, s, r) => outcomes.Add((c, s, r)));

        // Act 1 — тик валидации
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert 1 — чистый итог без джоба
        outcomes.Should().ContainSingle().Which.Should().Be((cluster, Shard, "failed"));

        // Arrange 2 — RUNNING-джоб exited(0) ok → чистый итог SUCCEEDED
        const string cluster2 = "dro2";
        await SeedAsync(cluster2);
        var engine = new FakeDrillEngine();
        var driver = new FakeDrillDriver(engine);
        var name = BackupNames.DrillContainerName(cluster2, Shard, DrillId);
        SeedContainer(engine, name, "exited", 0,
            "{\"phase\":\"recovering\"}\n{\"ok\":true,\"restored_to_lsn\":\"0/42\"}");
        await SeedDrillAsync(cluster2,
            new DrillState(DrillId, DrillStatus.Running, "20261001090000Z", NowUnix()));
        outcomes.Clear();
        var process2 = BuildProcess(cluster2, driver, new FakeBackupS3(),
            drillObserver: (c, s, r) => outcomes.Add((c, s, r)));

        // Act 2 — вердикт + доводка сноса одним тиком
        (await process2.TickAsync(BuildSnap(cluster2), await SnapshotBackupsAsync(cluster2), ct))
            .IsSuccess.Should().BeTrue();

        // Assert 2 — ровно один вызов: ok после снятия cleaning
        outcomes.Should().ContainSingle().Which.Should().Be((cluster2, Shard, "ok"));
    }
}
