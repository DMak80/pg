using System.Text.Json;
using FluentAssertions;
using PgWorker.Backups;
using PgWorker.Backups.Drill;
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

    // Драйвер с единственным хостом h1 (portalloc-сид даёт shard1/shard1a → h1).
    internal sealed class FakeDrillDriver(FakeDrillEngine engine) : IClusterDriver
    {
        public bool SupportsRunningInspection => true;
        public IDockerEngine? EngineFor(string host) => engine;
        public Task<Result> RemoveBackupJobsAsync(string cluster, CancellationToken ct)
            => Task.FromResult(Result.Success());
        public Task<Result> RemoveRestoreJobsAsync(string cluster, string shard, CancellationToken ct)
            => Task.FromResult(Result.Success());
        public Task<Result<IReadOnlyList<HostInfo>>> GetHostsAsync(CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<HostInfo>>.Success(
                (IReadOnlyList<HostInfo>)[new HostInfo("h1", 0)]));
        public Task<Result> EnsureBackupAgentAsync(
            string cluster, string shard, ContainerSpec spec, string host, CancellationToken ct)
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
        string cluster, FakeDrillDriver driver, IBackupS3 s3, BackupsRuntimeOptions? options = null)
        => new(
            Fx.Gateway, [Fx.Endpoint], driver,
            new ShardEndpoints(Fx.Gateway, [Fx.Endpoint], new ShardProbe(new HttpClient())),
            s3, Claims, new WorkJournal("/pgworker", Fx.Gateway, [Fx.Endpoint]),
            options ?? new BackupsRuntimeOptions(
                Enabled: true, S3Endpoint: "http://minio", S3Bucket: "bkt",
                S3AccessKey: "ak", S3SecretKey: "sk", JobImage: "pgworker-backup:test"),
            TimeProvider.System,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<RestoreDrillProcess>.Instance);

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

}
