using System.Text.Json;
using FluentAssertions;
using PgWorker.Backups;
using PgWorker.Backups.Job;
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

// Интеграции BackupVerifyProcess (t04 spec Ф3): у КАЖДОГО Fact своё etcd-
// окружение OwnEtcd (guid-имя pgw-ee-*, динамический порт, own-only teardown
// с ассертом чистоты — docs/e2e-isolation.md §1/§3; ключи умирают с контейнером)
// + фейки docker-движка и S3 (паттерны FakeBackupDeps/FakeBackupEngine).
[Collection(NonE2eCollection.Name)]
public class BackupVerifyProcessTests
{
    // Окружение Fact'а (свой etcd); создаётся в начале каждого сценария.
    private OwnEtcd Fx = null!;

    // ClaimStore ОДИН на окружение: InstanceId фиксируется клэймом SeedAsync —
    // новый экземпляр на вызов давал бы чужой InstanceId в тике (guard «клэйм не наш»).
    private ClaimStore? _claimsStore;

    private ClaimStore Claims => _claimsStore ??= new("/pgworker", [Fx.Endpoint], Fx.Gateway, TimeProvider.System);

    // ── Фейк docker-движка (по образцу BackupProcessTests.FakeBackupEngine;
    //    тест управляет State/ExitCode/Logs — супервиз-ветки задачи 9) ──
    // ── t02 (arch/19 §2 «Наблюдаемость итога verify»): spy-логгер — снимает
    //    форматированные строки (итог verify наблюдаем из host-лога воркера) ──
    internal sealed class SpyLogger : Microsoft.Extensions.Logging.ILogger<BackupVerifyProcess>
    {
        public readonly List<string> Lines = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => true;

        public void Log<TState>(
            Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId,
            TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => Lines.Add(formatter(state, exception));
    }

    internal sealed class FakeVerifyEngine : IDockerEngine
    {

        // ── union-члены t07 (kfw/vwk-методы): pg-доменом не используются — стабы ──
        public Task<Result> DeleteNetworkAsync(string name, CancellationToken ct) => Task.FromResult(Result.Success());
        public Task<Result> NetworkConnectAsync(string network, string container, CancellationToken ct) => Task.FromResult(Result.Success());
        public Task<Result<bool>> VolumeExistsAsync(string name, CancellationToken ct) => Task.FromResult(Result<bool>.Success(false));
    public Task<Result<IReadOnlyList<DockerService>>> ListServiceRecordsAsync(string namePrefix, CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyList<DockerService>>.Success([]));

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
                    found.Value.Id, found.Key, [], [], [new PortMap(8080, 31999)],
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

        public Task<Result> KillContainerAsync(string idOrName, CancellationToken ct)
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

        // Не используется в тестах verify — заглушки по образцу FakeBackupEngine.
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
                (IReadOnlySet<(string, int)>)new HashSet<(string, int)>()));
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    // Драйвер с единственным хостом h1 (portalloc-сид даёт shard1/shard1a → h1);
    // GetHostsAsync — «таблица Docker:Hosts» (fallback EngineForShard, spec §3.2).
    internal sealed class FakeVerifyDriver(FakeVerifyEngine engine) : IClusterDriver
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
        public Task<Result> RemoveBackupAgentsAsync(string cluster, string? shard, CancellationToken ct)
            => Task.FromResult(Result.Success());

        private static NotSupportedException NotSupported() => new("not used in the verify tests");
        public Task<Result<IReadOnlySet<(string Host, int Port)>>> GetBusyPortsAsync(CancellationToken ct) => throw NotSupported();
        public Task<Result> EnsureNodeAsync(ShardTopology topology, string nodeName, NodeAddress addr,
            InstallSecrets secrets, EtcdEndpoints etcd, NodeResources? resources, PgTuneResult? tuning,
            bool syncStrict, string restPassword, CancellationToken ct) => throw NotSupported();
        public Task<Result> RemoveNodeAsync(string cluster, string shard, string nodeName, CancellationToken ct) => throw NotSupported();
        public Task<Result> StopNodeAsync(string cluster, string shard, string nodeName, CancellationToken ct) => throw NotSupported();
        public Task<Result<DataPresence>> NodeDataPresenceAsync(string cluster, string shard, string node, CancellationToken ct) => throw NotSupported();
        public Task<Result<IReadOnlyDictionary<string, DiscoveredNode>>> InspectNodesAsync(
            string cluster, IReadOnlyCollection<string> nodeNames, CancellationToken ct) => throw NotSupported();
        public Task<Result<string>> ExecNodeAsync(string cluster, string shard, string node,
            IReadOnlyList<string> cmd, CancellationToken ct) => throw NotSupported();
        public Task<Result<string>> ExecContainerAsync(string containerName, IReadOnlyList<string> cmd, CancellationToken ct) => throw NotSupported();
            public Task<Result<IReadOnlyDictionary<string, string>>> InspectNodeEnvAsync(
        string cluster, string shard, string nodeName, CancellationToken ct) => Task.FromResult(Result<IReadOnlyDictionary<string, string>>.Success(
            (IReadOnlyDictionary<string, string>)new Dictionary<string, string>()));

public Task<Result<IReadOnlyList<string>>> ListNodeObjectsAsync(string cluster, CancellationToken ct) => throw NotSupported();
    }

    // ── Сид-хелперы ──
    private static ClusterSnapshot BuildSnap(string cluster) => new(
        new ClusterConfig(cluster, 2, cluster, null, ClusterState.Active),
        [new ShardSpec("shard1", 1, $"host=shard1a dbname={cluster}", "shard1a:17001",
            [new NodeSpec("shard1", "shard1a", NodeState.Running)])],
        []);

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
        (await Claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue("claim is a tick precondition");
    }

    // Фейк API приёмника: ответы GetVerify программируются тестами.
    private readonly FakeReceiverClient Receiver = new();

    // Ответы GET /api/ops/verify (маркер транзиента — пара state/phase, spec §3.2).
    private static ReceiverReply ApiOk() =>
        new(200, new ReceiverStatus("succeeded", "verifying"), null);
    private static ReceiverReply ApiDownloadTransient(string error) =>
        new(200, new ReceiverStatus("failed", "downloading", Error: error), null);
    private static ReceiverReply ApiVerifyFailed(string error) =>
        new(200, new ReceiverStatus("failed", "verifying", Error: error), null);

    /// <summary>Мини-фейк API приёмника (verify-сценарии): StartVerify — 202
    /// (тело в журнал), GetVerify — программируемый ответ.</summary>
    private sealed class FakeReceiverClient : IWalReceiverApiClient
    {
        public ReceiverReply StartReply { get; set; } =
            new(202, new ReceiverStatus("running", "downloading"), null);
        public ReceiverReply GetReply { get; set; } =
            new(200, new ReceiverStatus("running", "downloading"), null);
        public readonly List<StartVerifyRequest> StartBodies = [];

        public Task<ReceiverReply> StartVerifyAsync(string baseUrl, StartVerifyRequest body, CancellationToken ct)
        {
            StartBodies.Add(body);
            return Task.FromResult(StartReply);
        }

        public Task<ReceiverReply> GetVerifyAsync(string baseUrl, CancellationToken ct)
            => Task.FromResult(GetReply);

        public Task<ReceiverReply> StartFullAsync(string baseUrl, string id, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<ReceiverReply> GetFullAsync(string baseUrl, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<ReceiverReply> CancelFullAsync(string baseUrl, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<ReceiverReply> StartRestoreAsync(string baseUrl, StartRestoreRequest body, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<ReceiverReply> GetRestoreAsync(string baseUrl, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<ReceiverReply> CancelOpsAsync(string baseUrl, CancellationToken ct)
            => throw new NotSupportedException();
    }

    private BackupVerifyProcess BuildProcess(
        string cluster, FakeVerifyDriver driver, IBackupS3 s3, BackupsRuntimeOptions? options = null,
        Action<string, string, string>? verifyObserver = null,
        Microsoft.Extensions.Logging.ILogger<BackupVerifyProcess>? logger = null)
        => new(
            Fx.Gateway, [Fx.Endpoint], driver,
            new ShardEndpoints(Fx.Gateway, [Fx.Endpoint], new ShardProbe(new HttpClient())),
            FakePoolFactory.Single(s3), Receiver, Claims,
            new WorkJournal("/pgworker", Fx.Gateway, [Fx.Endpoint]),
            options ?? new BackupsRuntimeOptions(
                Enabled: true, S3Bucket: "bkt",
                StagingDir: "/home/postgres/pgdata/backup-staging"),
            TimeProvider.System,
            logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<BackupVerifyProcess>.Instance,
            verifyObserver);

    private static IReadOnlyList<ClusterBackups> BackupsOf(string cluster, params FullBackupState[] fulls)
        => [new ClusterBackups(cluster, null,
            new Dictionary<string, ShardBackups> { ["shard1"] = new(fulls, null) })];

    private static FullBackupState Completed(string id, string? walStart, BackupVerify? verify = null) =>
        new(id, FullBackupStatus.Completed, "shard1a", BackupSourceRole.Replica,
            1757500000, 1757500300, walStart, 1024, null, verify);

    private async Task<JsonElement> ReadVerifyAsync(string cluster, string id)
    {
        var kv = await Fx.Gateway.GetAsync(
            Fx.Endpoint, $"/pgworker/backups/{cluster}/shard1/full/{id}",
            TestContext.Current.CancellationToken);
        kv.Value.Should().NotBeNull("the verify outcome is written to the full backup key");
        return JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(kv.Value!.Value)!["verify"];
    }

    // AAA: PENDING-кандидат при on_create — цепочка цела → verify-джоб создан и
    // запущен (AC1-юнитная часть; итог exited — задача 9)
    [Fact]
    public async Task TickPendingCandidateIntactChainStartsJob()
    {
        // Arrange — full COMPLETED verify=PENDING; wal/: сегменты 1..3 + history нет;
        // pg_wal набора: сегмент 3 (end)
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("vc1");
        var engine = new FakeVerifyEngine();
        var driver = new FakeVerifyDriver(engine);
        var s3 = new FakeBackupS3();
        s3.Objects.Add(("vc1", "shard1", "000000010000000000000001"));
        s3.Objects.Add(("vc1", "shard1", "000000010000000000000002"));
        s3.Objects.Add(("vc1", "shard1", "000000010000000000000003"));
        s3.PrefixedObjects.Add(("vc1", "shard1", "full/20260911120000Z/pg_wal/000000010000000000000003"));
        var process = BuildProcess("vc1", driver, s3);
        var backups = BackupsOf("vc1", Completed("20260911120000Z", "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Pending, null)));

        // Act
        var result = await process.TickAsync(BuildSnap("vc1"), backups, ct);

        // Assert — tool-контейнер создан+запущен (образ нод в tool-режиме,
        // старт БЕЗ команд — весь жизненный цикл через API приёмника)
        result.IsSuccess.Should().BeTrue();
        var (name, spec) = engine.Created.Should().ContainSingle(c => c.Name == "pgw-backup-verify-vc1-shard1-20260911120000Z").Subject;
        engine.Started.Should().Contain(name);
        spec.Image.Should().Be(new BackupsRuntimeOptions().NodeImage);
        spec.Env!["PGW_NODE_TOOL"].Should().Be("1");
        spec.Cmd.Should().BeNull();
        Receiver.StartBodies.Should().ContainSingle().Which.BackupId.Should().Be("20260911120000Z");
        Receiver.StartBodies[0].S3.Prefix.Should().Be("vc1/shard1");
    }

    // AAA: дыра в wal/ внутри [start..end] → permanent FAILED с границами,
    // verify-джоб НЕ создаётся (AC3: контейнер не создаётся)
    [Fact]
    public async Task TickChainGapFailedWithoutJob()
    {
        // Arrange — wal/: 1,3 (нет 2); набор: pg_wal/3 → end=3
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("vc2");
        var engine = new FakeVerifyEngine();
        var driver = new FakeVerifyDriver(engine);
        var s3 = new FakeBackupS3();
        s3.Objects.Add(("vc2", "shard1", "000000010000000000000001"));
        s3.Objects.Add(("vc2", "shard1", "000000010000000000000003"));
        s3.PrefixedObjects.Add(("vc2", "shard1", "full/20260911120000Z/pg_wal/000000010000000000000003"));
        var process = BuildProcess("vc2", driver, s3);
        var backups = BackupsOf("vc2", Completed("20260911120000Z", "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Pending, null)));

        // Act
        (await process.TickAsync(BuildSnap("vc2"), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — verify.state=FAILED + error с границами + checked_unix; джоба нет
        var verify = await ReadVerifyAsync("vc2", "20260911120000Z");
        verify.GetProperty("state").GetString().Should().Be("FAILED");
        verify.GetProperty("error").GetString().Should().Contain("000000010000000000000002");
        verify.GetProperty("checked_unix").GetInt64().Should().BeGreaterThan(0);
        engine.Created.Should().BeEmpty("the verdict is determined by the chain — no download is spent (spec §3.1)");
    }

    // AAA: COMPLETED без wal_start_segment (аномалия) — permanent FAILED без джоба
    [Fact]
    public async Task TickNoWalStartFailedWithoutJob()
    {
        // Arrange — wal_start_segment = null (ранний упавший UPLOADING не бывает
        // COMPLETED — аномалия; но guard обязателен, spec §3.7)
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("vc3");
        var engine = new FakeVerifyEngine();
        var driver = new FakeVerifyDriver(engine);
        var process = BuildProcess("vc3", driver, new FakeBackupS3());
        var backups = BackupsOf("vc3", Completed("20260911120000Z", null,
            new BackupVerify(BackupVerifyStatus.Pending, null)));

        // Act
        (await process.TickAsync(BuildSnap("vc3"), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert
        var verify = await ReadVerifyAsync("vc3", "20260911120000Z");
        verify.GetProperty("state").GetString().Should().Be("FAILED");
        verify.GetProperty("error").GetString().Should().Contain("wal_start_segment");
        engine.Created.Should().BeEmpty();
    }

    // AAA: строгий TLI-переход с валидным switchWALLSN (GET history из S3) —
    // цепочка цела, джоб запускается (AC4: verify использует содержимое history)
    [Fact]
    public async Task TickStrictTliValidPointStartsJob()
    {
        // Arrange — wal/: tli1/seg1, history, tli2/seg2; набор: pg_wal/tli2/seg2
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("vc4");
        var engine = new FakeVerifyEngine();
        var driver = new FakeVerifyDriver(engine);
        var s3 = new FakeBackupS3();
        s3.Objects.Add(("vc4", "shard1", "000000010000000000000001"));
        s3.Objects.Add(("vc4", "shard1", "00000002.history"));
        s3.Objects.Add(("vc4", "shard1", "000000020000000000000002"));
        s3.PrefixedObjects.Add(("vc4", "shard1", "full/20260911120000Z/pg_wal/000000020000000000000002"));
        s3.Contents[("vc4", "shard1", "wal/00000002.history")] = "1\t0/2000000\n";
        var process = BuildProcess("vc4", driver, s3);
        var backups = BackupsOf("vc4", Completed("20260911120000Z", "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Pending, null)));

        // Act
        (await process.TickAsync(BuildSnap("vc4"), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert
        engine.Created.Should().ContainSingle(c => c.Name == "pgw-backup-verify-vc4-shard1-20260911120000Z");
    }

    // AAA: строгий TLI-переход с НЕвалидной точкой (switchWALLSN вне границы) —
    // FAILED с упоминанием history, без джоба (AC4)
    [Fact]
    public async Task TickStrictTliPointOffBoundaryFailed()
    {
        // Arrange — как vc4, но history говорит parent=1, lsn=0/5000000 (сегмент 5)
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("vc5");
        var engine = new FakeVerifyEngine();
        var driver = new FakeVerifyDriver(engine);
        var s3 = new FakeBackupS3();
        s3.Objects.Add(("vc5", "shard1", "000000010000000000000001"));
        s3.Objects.Add(("vc5", "shard1", "00000002.history"));
        s3.Objects.Add(("vc5", "shard1", "000000020000000000000002"));
        s3.PrefixedObjects.Add(("vc5", "shard1", "full/20260911120000Z/pg_wal/000000020000000000000002"));
        s3.Contents[("vc5", "shard1", "wal/00000002.history")] = "1\t0/5000000\n";
        var process = BuildProcess("vc5", driver, s3);
        var backups = BackupsOf("vc5", Completed("20260911120000Z", "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Pending, null)));

        // Act
        (await process.TickAsync(BuildSnap("vc5"), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert
        var verify = await ReadVerifyAsync("vc5", "20260911120000Z");
        verify.GetProperty("state").GetString().Should().Be("FAILED");
        verify.GetProperty("error").GetString().Should().Contain("00000002.history");
        engine.Created.Should().BeEmpty();
    }

    // AAA: transient S3 (list wal/ недоступен) — шард-skip тиком: тик успешен,
    // джоб не создаётся, ключ полного НЕ изменён (spec §3.1: статус не трогаем)
    [Fact]
    public async Task TickTransientS3StatusUntouchedNoJob()
    {
        // Arrange — PENDING-ключ в etcd (записан руками — ассерт «не изменён»);
        // S3 «лежит» (FakeBackupS3.Fails)
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("vc6");
        var engine = new FakeVerifyEngine();
        var driver = new FakeVerifyDriver(engine);
        var s3 = new FakeBackupS3 { Fails = true };
        var candidate = Completed("20260911120000Z", "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Pending, null));
        var key = $"/pgworker/backups/vc6/shard1/full/20260911120000Z";
        var before = BackupStatusJson.Serialize(candidate);
        await Fx.Gateway.PutAsync(Fx.Endpoint, key, before, null, ct);
        var process = BuildProcess("vc6", driver, s3);
        var backups = BackupsOf("vc6", candidate);

        // Act
        var result = await process.TickAsync(BuildSnap("vc6"), backups, ct);

        // Assert — transient: тик без ошибки (шард-skip), ничего не создано/не записано
        result.IsSuccess.Should().BeTrue("transient S3 — not a tick error (spec §3.1)");
        engine.Created.Should().BeEmpty("a job without a chain does not start");
        var after = (await Fx.Gateway.GetAsync(Fx.Endpoint, key, ct)).Value!.Value;
        after.Should().Be(before, "the PENDING status is untouched — retry on the next tick");
    }

    // AAA: transient GET history (spec §3.1): цепочка с TLI-переходом требует
    // скачивания .history → GET падает → шард-skip, джоба нет, статус не тронут
    [Fact]
    public async Task TickTransientGetHistoryStatusUntouchedNoJob()
    {
        // Arrange — сид как в vc4 (TLI-переход в диапазоне), но GET падает;
        // PENDING-ключ в etcd для ассерта «не изменён»
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("vc7");
        var engine = new FakeVerifyEngine();
        var driver = new FakeVerifyDriver(engine);
        var s3 = new FakeBackupS3 { FailsGetObject = true };
        s3.Objects.Add(("vc7", "shard1", "000000010000000000000001"));
        s3.Objects.Add(("vc7", "shard1", "00000002.history"));
        s3.Objects.Add(("vc7", "shard1", "000000020000000000000002"));
        s3.PrefixedObjects.Add(("vc7", "shard1", "full/20260911120000Z/pg_wal/000000020000000000000002"));
        s3.Contents[("vc7", "shard1", "wal/00000002.history")] = "1\t0/2000000\n";
        var candidate = Completed("20260911120000Z", "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Pending, null));
        var key = "/pgworker/backups/vc7/shard1/full/20260911120000Z";
        var before = BackupStatusJson.Serialize(candidate);
        await Fx.Gateway.PutAsync(Fx.Endpoint, key, before, null, ct);
        var process = BuildProcess("vc7", driver, s3);

        // Act
        var result = await process.TickAsync(BuildSnap("vc7"), BackupsOf("vc7", candidate), ct);

        // Assert — list прошёл (переход в диапазоне найден), GET упал → transient
        result.IsSuccess.Should().BeTrue("transient GET — not a tick error (spec §3.1)");
        engine.Created.Should().BeEmpty("without the history content the strict parse is impossible — the job does not start");
        var after = (await Fx.Gateway.GetAsync(Fx.Endpoint, key, ct)).Value!.Value;
        after.Should().Be(before, "the PENDING status is untouched — retry on the next tick");
    }

    // Локальный хелпер: PENDING-кандидат + непрерывная цепочка 1..3 + джоб запущен первым тиком.
    private async Task<FakeVerifyEngine> StartJobAsync(
        string cluster, FakeBackupS3 s3, string id = "20260911120000Z")
    {
        var engine = new FakeVerifyEngine();
        var driver = new FakeVerifyDriver(engine);
        s3.Objects.Add((cluster, "shard1", "000000010000000000000001"));
        s3.Objects.Add((cluster, "shard1", "000000010000000000000002"));
        s3.Objects.Add((cluster, "shard1", "000000010000000000000003"));
        s3.PrefixedObjects.Add((cluster, "shard1", $"full/{id}/pg_wal/000000010000000000000003"));
        var process = BuildProcess(cluster, driver, s3);
        var backups = BackupsOf(cluster, Completed(id, "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Pending, null)));
        (await process.TickAsync(BuildSnap(cluster), backups, TestContext.Current.CancellationToken))
            .IsSuccess.Should().BeTrue();
        engine.Created.Should().NotBeEmpty("the job is started by the first tick (precondition)");
        return engine;
    }

    // AAA: exited exit=0 + ok:true → verify OK + checked_unix; контейнер и volume снесены (AC1)
    [Fact]
    public async Task SupervisorExit0OkCleansUpJob()
    {
        // Arrange — джоб exited с ok:true
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("sv1");
        var s3 = new FakeBackupS3();
        var engine = await StartJobAsync("sv1", s3);
        var name = "pgw-backup-verify-sv1-shard1-20260911120000Z";
        engine.Containers[name] = engine.Containers[name] with { State = "running" };
        Receiver.GetReply = ApiOk();

        // Act — второй тик (супервиз итога по API)
        var process = BuildProcess("sv1", new FakeVerifyDriver(engine), s3);
        (await process.TickAsync(BuildSnap("sv1"), BackupsOf("sv1",
            Completed("20260911120000Z", "000000010000000000000001",
                new BackupVerify(BackupVerifyStatus.Pending, null))), ct)).IsSuccess.Should().BeTrue();

        // Assert
        var verify = await ReadVerifyAsync("sv1", "20260911120000Z");
        verify.GetProperty("state").GetString().Should().Be("OK");
        verify.GetProperty("checked_unix").GetInt64().Should().BeGreaterThan(0);
        engine.Removed.Should().Contain(name);
        engine.RemovedVolumes.Should().Contain(name); // volume имя == имени контейнера
        var journal = await Fx.Gateway.GetAsync(Fx.Endpoint, "/pgworker/work/sv1", ct);
        journal.Value!.Value.Should().Contain("verified-ok/shard1/20260911120000Z");
    }

    // AAA: exited + phase=verify → permanent FAILED с error от pg_verifybackup (AC2)
    [Fact]
    public async Task SupervisorVerifyPhaseFailedFailed()
    {
        // Arrange — джоб exited с result-JSON phase=verify
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("sv2");
        var s3 = new FakeBackupS3();
        var engine = await StartJobAsync("sv2", s3);
        var name = "pgw-backup-verify-sv2-shard1-20260911120000Z";
        engine.Containers[name] = engine.Containers[name] with { State = "running" };
        Receiver.GetReply = ApiVerifyFailed("checksum mismatch failed");

        // Act
        var process = BuildProcess("sv2", new FakeVerifyDriver(engine), s3);
        (await process.TickAsync(BuildSnap("sv2"), BackupsOf("sv2",
            Completed("20260911120000Z", "000000010000000000000001",
                new BackupVerify(BackupVerifyStatus.Pending, null))), ct)).IsSuccess.Should().BeTrue();

        // Assert
        var verify = await ReadVerifyAsync("sv2", "20260911120000Z");
        verify.GetProperty("state").GetString().Should().Be("FAILED");
        verify.GetProperty("error").GetString().Should().Contain("checksum mismatch");
        verify.GetProperty("checked_unix").GetInt64().Should().BeGreaterThan(0);
    }

    // AAA: exited + phase=download → transient: контейнер снесён, статус ОСТАЛСЯ PENDING (AC2)
    [Fact]
    public async Task SupervisorDownloadPhaseTransientStaysPending()
    {
        // Arrange — джоб exited с result-JSON phase=download (mc/staging ENOSPC);
        // ключ кандидата в etcd записан руками (transient итог НЕ пишет — паттерн vc6/vc7)
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("sv3");
        var s3 = new FakeBackupS3();
        var engine = await StartJobAsync("sv3", s3);
        var name = "pgw-backup-verify-sv3-shard1-20260911120000Z";
        engine.Containers[name] = engine.Containers[name] with { State = "running" };
        Receiver.GetReply = ApiDownloadTransient("mc cp failed");
        var key = "/pgworker/backups/sv3/shard1/full/20260911120000Z";
        var before = BackupStatusJson.Serialize(Completed("20260911120000Z", "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Pending, null)));
        await Fx.Gateway.PutAsync(Fx.Endpoint, key, before, null, ct);

        // Act — супервиз
        var process = BuildProcess("sv3", new FakeVerifyDriver(engine), s3);
        (await process.TickAsync(BuildSnap("sv3"), BackupsOf("sv3",
            Completed("20260911120000Z", "000000010000000000000001",
                new BackupVerify(BackupVerifyStatus.Pending, null))), ct)).IsSuccess.Should().BeTrue();

        // Assert — статус НЕ изменён (остался PENDING), контейнер/volume снесены
        var after = (await Fx.Gateway.GetAsync(Fx.Endpoint, key, ct)).Value!.Value;
        after.Should().Be(before, "download-phase transient: the PENDING status is untouched");
        engine.Removed.Should().Contain(name);

        // Act 2 — следующий тик: PENDING снова due → джоб перезапущен (ретрай тиками)
        (await process.TickAsync(BuildSnap("sv3"), BackupsOf("sv3",
            Completed("20260911120000Z", "000000010000000000000001",
                new BackupVerify(BackupVerifyStatus.Pending, null))), ct)).IsSuccess.Should().BeTrue();

        // Assert 2
        engine.Created.Should().HaveCount(2, "a transient is retried on the next tick (spec §3.1)");
    }

    // AAA: vanished-джоб (контейнера нет при PENDING) → перезапуск со шага цепочки (AC8)
    [Fact]
    public async Task SupervisorVanishedRestart()
    {
        // Arrange — PENDING-кандидат, движок ПУСТ (контейнер исчез после рестарта docker-хоста)
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("sv4");
        var s3 = new FakeBackupS3();
        s3.Objects.Add(("sv4", "shard1", "000000010000000000000001"));
        s3.PrefixedObjects.Add(("sv4", "shard1", "full/20260911120000Z/pg_wal/000000010000000000000001"));
        var engine = new FakeVerifyEngine();
        var process = BuildProcess("sv4", new FakeVerifyDriver(engine), s3);

        // Act — тик без контейнера
        (await process.TickAsync(BuildSnap("sv4"), BackupsOf("sv4",
            Completed("20260911120000Z", "000000010000000000000001",
                new BackupVerify(BackupVerifyStatus.Pending, null))), ct)).IsSuccess.Should().BeTrue();

        // Assert — идемпотентный запуск заново (цепочка → create+start), не FAILED
        engine.Created.Should().ContainSingle(c => c.Name == "pgw-backup-verify-sv4-shard1-20260911120000Z");
    }

    // AAA: два due-кандидата — по одному за тик; PENDING раньше периодики (AC8)
    [Fact]
    public async Task TwoDueOnePerTickPendingEarlier()
    {
        // Arrange — full A (старее): verify=OK, CheckedUnix=now-7200 (периодика due при
        // interval 3600); full B (свежее): verify=PENDING (on_create)
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("sv5");
        var s3 = new FakeBackupS3();
        s3.Objects.Add(("sv5", "shard1", "000000010000000000000001"));
        s3.PrefixedObjects.Add(("sv5", "shard1", "full/20260911090000Z/pg_wal/000000010000000000000001"));
        s3.PrefixedObjects.Add(("sv5", "shard1", "full/20260911120000Z/pg_wal/000000010000000000000001"));
        var engine = new FakeVerifyEngine();
        var process = BuildProcess("sv5", new FakeVerifyDriver(engine), s3);
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var backups = new ClusterBackups("sv5",
            new BackupPolicy(7, 4, 6, 86400, true, VerifyIntervalSec: 3600),
            new Dictionary<string, ShardBackups>
            {
                ["shard1"] = new(
                [
                    Completed("20260911090000Z", "000000010000000000000001",
                        new BackupVerify(BackupVerifyStatus.Ok, now - 7200)),
                    Completed("20260911120000Z", "000000010000000000000001",
                        new BackupVerify(BackupVerifyStatus.Pending, null)),
                ], null),
            });

        // Act — тик 1: только B (PENDING-очередь раньше периодики)
        (await process.TickAsync(BuildSnap("sv5"), [backups], ct)).IsSuccess.Should().BeTrue();

        // Assert — один джоб, и это B
        engine.Created.Should().ContainSingle()
            .Which.Name.Should().Be("pgw-backup-verify-sv5-shard1-20260911120000Z");
    }

    // AAA: периодика (AC5): OK-полный перепроверяется по interval_sec — цикл
    // ДОКАНЦА: exited ok → checked_unix РАСТЁТ; interval_sec<=0 — только
    // on_create; verify=null («никогда не проверялся») — периодика due;
    // FAILED не перепроверяется (терминален)
    [Fact]
    public async Task PeriodicityByIntervalDisabledAndFailedTerminality()
    {
        // Arrange — OK-полный CheckedUnix=now-7200; policy interval_sec=3600 → due
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("sv6");
        var s3 = new FakeBackupS3();
        s3.Objects.Add(("sv6", "shard1", "000000010000000000000001"));
        s3.PrefixedObjects.Add(("sv6", "shard1", "full/20260911120000Z/pg_wal/000000010000000000000001"));
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var okBackup = Completed("20260911120000Z", "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Ok, now - 7200));
        ClusterBackups WithInterval(long? interval, FullBackupState? full = null) => new("sv6",
            new BackupPolicy(7, 4, 6, 86400, true, interval),
            new Dictionary<string, ShardBackups> { ["shard1"] = new([full ?? okBackup], null) });

        // Act 1 — interval=3600: джоб запущен (перепроверка due)
        var engine = new FakeVerifyEngine();
        var process = BuildProcess("sv6", new FakeVerifyDriver(engine), s3);
        (await process.TickAsync(BuildSnap("sv6"), [WithInterval(3600)], ct)).IsSuccess.Should().BeTrue();
        engine.Created.Should().ContainSingle("the OK full is older than the interval — re-verify (AC5)");

        // Act 1b — операция завершилась (GET succeeded) → супервиз пишет итог
        Receiver.GetReply = ApiOk();
        (await process.TickAsync(BuildSnap("sv6"), [WithInterval(3600)], ct)).IsSuccess.Should().BeTrue();

        // Assert 1b — verify OK и checked_unix РАСТЁТ (было now-7200, стало ~now)
        var verify = await ReadVerifyAsync("sv6", "20260911120000Z");
        verify.GetProperty("state").GetString().Should().Be("OK");
        verify.GetProperty("checked_unix").GetInt64().Should().BeGreaterThan(now - 7200,
            "the periodic re-verify refreshes checked_unix (AC5)");

        // Act 2 / Assert 2 — interval=0: не due (только on_create)
        var engineOff = new FakeVerifyEngine();
        var processOff = BuildProcess("sv6", new FakeVerifyDriver(engineOff), s3);
        (await processOff.TickAsync(BuildSnap("sv6"), [WithInterval(0)], ct)).IsSuccess.Should().BeTrue();
        engineOff.Created.Should().BeEmpty("interval_sec<=0 — periodicity is off (AC5)");

        // Act 2b / Assert 2b — verify=null («никогда не проверялся», spec §3.1
        // due-periodic): ловится ТОЛЬКО периодикой (не on_create — verify нет)
        var engineNull = new FakeVerifyEngine();
        var processNull = BuildProcess("sv6", new FakeVerifyDriver(engineNull), s3);
        var neverVerified = Completed("20260911120000Z", "000000010000000000000001", verify: null);
        (await processNull.TickAsync(BuildSnap("sv6"), [WithInterval(3600, neverVerified)], ct))
            .IsSuccess.Should().BeTrue();
        engineNull.Created.Should().ContainSingle("an unverified full (verify=null) is due by periodicity (§3.1)");

        // Act 3 / Assert 3 — FAILED не перепроверяется ни при каком interval
        var failed = new ClusterBackups("sv6",
            new BackupPolicy(7, 4, 6, 86400, true, 1),
            new Dictionary<string, ShardBackups>
            {
                ["shard1"] = new([Completed("20260911120000Z", "000000010000000000000001",
                    new BackupVerify(BackupVerifyStatus.Failed, now, "bad"))], null),
            });
        var engineFailed = new FakeVerifyEngine();
        var processFailed = BuildProcess("sv6", new FakeVerifyDriver(engineFailed), s3);
        (await processFailed.TickAsync(BuildSnap("sv6"), [failed], ct)).IsSuccess.Should().BeTrue();
        engineFailed.Created.Should().BeEmpty("FAILED is terminal (spec §3.1)");
    }

    // AAA: транспорт-отказ docker (list) — статус не меняем (transient, spec §3.2)
    [Fact]
    public async Task SupervisorTransportFailureStatusUntouched()
    {
        // Arrange — джоб exited с ok:true, но list падает; ключ кандидата в etcd
        // записан руками (никто другой его не пишет до итога — паттерн vc6/vc7)
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("sv7");
        var s3 = new FakeBackupS3();
        var engine = await StartJobAsync("sv7", s3);
        var name = "pgw-backup-verify-sv7-shard1-20260911120000Z";
        engine.Containers[name] = engine.Containers[name] with { State = "exited", ExitCode = 0, Logs = "{\"ok\":true}" };
        engine.ListFails = true;
        var key = "/pgworker/backups/sv7/shard1/full/20260911120000Z";
        await Fx.Gateway.PutAsync(Fx.Endpoint, key, BackupStatusJson.Serialize(
            Completed("20260911120000Z", "000000010000000000000001",
                new BackupVerify(BackupVerifyStatus.Pending, null))), null, ct);
        var before = (await Fx.Gateway.GetAsync(Fx.Endpoint, key, ct)).Value!.Value;

        // Act
        var process = BuildProcess("sv7", new FakeVerifyDriver(engine), s3);
        (await process.TickAsync(BuildSnap("sv7"), BackupsOf("sv7",
            Completed("20260911120000Z", "000000010000000000000001",
                new BackupVerify(BackupVerifyStatus.Pending, null))), ct)).IsSuccess.Should().BeTrue();

        // Assert — ключ не изменён, контейнер не тронут (следующий тик повторит супервиз)
        var after = (await Fx.Gateway.GetAsync(
            Fx.Endpoint, "/pgworker/backups/sv7/shard1/full/20260911120000Z", ct)).Value!.Value;
        after.Should().Be(before);
        engine.Removed.Should().NotContain(name);
    }

    // AAA: инвариант «максимум один verify-джоб на шард» (AC8): running-джоб
    // кандидата A жив + кандидат B due (PENDING) → новый джоб НЕ стартуется,
    // статус B не тронут (spec §3.3 п.1)
    [Fact]
    public async Task LiveShardJobBlocksNewOneStatusUntouched()
    {
        // Arrange — в движке уже running-контейнер джоба кандидата A; B — PENDING;
        // ключи обоих кандидатов записаны в etcd руками (ассерт «не тронут»)
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("sv8");
        var s3 = new FakeBackupS3();
        s3.Objects.Add(("sv8", "shard1", "000000010000000000000001"));
        s3.PrefixedObjects.Add(("sv8", "shard1", "full/20260911090000Z/pg_wal/000000010000000000000001"));
        var engine = new FakeVerifyEngine();
        var nameA = "pgw-backup-verify-sv8-shard1-20260911090000Z";
        engine.Containers[nameA] = new(
            Guid.NewGuid().ToString("N"), "running", -1, ""); // ContainerRec: Id, State, ExitCode, Logs
        var a = Completed("20260911090000Z", "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Pending, null));
        var b = Completed("20260911120000Z", "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Pending, null));
        var keyB = "/pgworker/backups/sv8/shard1/full/20260911120000Z";
        var beforeB = BackupStatusJson.Serialize(b);
        await Fx.Gateway.PutAsync(Fx.Endpoint,
            "/pgworker/backups/sv8/shard1/full/20260911090000Z", BackupStatusJson.Serialize(a), null, ct);
        await Fx.Gateway.PutAsync(Fx.Endpoint, keyB, beforeB, null, ct);
        var process = BuildProcess("sv8", new FakeVerifyDriver(engine), s3);

        // Act — тик: супервиз видит живой джоб A (running → ждать), B due
        (await process.TickAsync(BuildSnap("sv8"), BackupsOf("sv8", a, b), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — новый джоб не создан; ключ B не изменён
        engine.Created.Should().BeEmpty("a live verify job of the shard blocks starting a new one (§3.3 invariant)");
        var afterB = (await Fx.Gateway.GetAsync(Fx.Endpoint, keyB, ct)).Value!.Value;
        afterB.Should().Be(beforeB, "the due-candidate status is untouched while a foreign job of the shard is alive");
    }

    // AAA (AC5): verify-джоб running дольше бюджета → kill+rm, кандидат PENDING с
    // checked_unix=now; лив-лок исключён — следующий due только через interval
    [Fact]
    public async Task SupervisorVerifyOlderThanBudgetKillAndAttemptQuota()
    {
        // Arrange — COMPLETED-полный с Verify=PENDING; FakeVerifyEngine держит
        // running-контейнер verify-джоба с StartedUnix = now-7h (бюджет 6 ч дефолт)
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("sv9");
        var now = TimeProvider.System.GetUtcNow().ToUnixTimeSeconds();
        var s3 = new FakeBackupS3();
        var engine = new FakeVerifyEngine();
        var name = "pgw-backup-verify-sv9-shard1-20260911090000Z";
        engine.Containers[name] = new(
            Guid.NewGuid().ToString("N"), "running", -1, "", StartedUnix: now - 7 * 3600);
        var a = Completed("20260911090000Z", "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Pending, null));
        await Fx.Gateway.PutAsync(Fx.Endpoint,
            "/pgworker/backups/sv9/shard1/full/20260911090000Z", BackupStatusJson.Serialize(a), null, ct);
        var process = BuildProcess("sv9", new FakeVerifyDriver(engine), s3);

        // Act — тик: супервиз видит running-джоб старше бюджета
        (await process.TickAsync(BuildSnap("sv9"), BackupsOf("sv9", a), ct))
            .IsSuccess.Should().BeTrue();

        // Assert 1 — контейнер и volume удалены; ключ полного: verify.state=PENDING,
        // checked_unix ≈ now (попытка зачтена, вердикта FAILED нет)
        engine.Removed.Should().Contain(name);
        engine.RemovedVolumes.Should().Contain(name);
        var verify = await ReadVerifyAsync("sv9", "20260911090000Z");
        ((string?)verify.GetProperty("state").GetString()).Should().Be("PENDING");
        var checkedUnix = verify.GetProperty("checked_unix").GetInt64();
        checkedUnix.Should().BeGreaterOrEqualTo(now - 60, "the attempt quota — the supervisor time");
        verify.TryGetProperty("error", out _).Should().BeFalse("the data is not at fault — FAILED is not set");

        // Act 2 — повторный тик немедленно (джобов живых больше нет)
        var b = a with { Verify = new BackupVerify(BackupVerifyStatus.Pending, checkedUnix) };
        (await process.TickAsync(BuildSnap("sv9"), BackupsOf("sv9", b), ct))
            .IsSuccess.Should().BeTrue();

        // Assert 3 — НОВЫЙ джоб НЕ создан (checked только что; pending due — по interval)
        engine.Created.Should().BeEmpty("the live-lock of immediate restarts is excluded (t07)");
    }

    // AAA: нода-источник исчезла из portalloc → джоб стартует на ПЕРВОМ хосте
    // таблицы Docker:Hosts (GetHostsAsync) + journal-факт engine-fallback (spec §3.2)
    [Fact]
    public async Task TickNodeGoneFromPortallocJobOnFirstHostOfTable()
    {
        // Arrange — portalloc ПУСТ (узел shard1a исчез); PENDING-кандидат с node=shard1a
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("vc8");
        await Fx.Gateway.PutAsync(Fx.Endpoint, "/pgworker/portalloc/vc8",
            Portalloc.Serialize(new Dictionary<string, NodeAddress>()), null, ct);
        var engine = new FakeVerifyEngine();
        var driver = new FakeVerifyDriver(engine); // GetHostsAsync → [h1]
        var s3 = new FakeBackupS3();
        s3.Objects.Add(("vc8", "shard1", "000000010000000000000001"));
        s3.PrefixedObjects.Add(("vc8", "shard1", "full/20260911120000Z/pg_wal/000000010000000000000001"));
        var process = BuildProcess("vc8", driver, s3);
        var backups = BackupsOf("vc8", Completed("20260911120000Z", "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Pending, null)));

        // Act
        var result = await process.TickAsync(BuildSnap("vc8"), backups, ct);

        // Assert — джоб создан fallback-движком; выбор хоста зафиксирован журналом
        result.IsSuccess.Should().BeTrue();
        engine.Created.Should().ContainSingle(c => c.Name == "pgw-backup-verify-vc8-shard1-20260911120000Z");
        var journal = await Fx.Gateway.GetAsync(Fx.Endpoint, "/pgworker/work/vc8", ct);
        journal.Value!.Value.Should().Contain("engine-fallback/shard1");
    }

    // AAA: t14 — observe-точки verify: S3 недоступен → transient; битый
    // wal_start → permanent FAILED → failed; exited(0) ok:true → ok
    // (процесс не меняется — тест фиксирует вызовы всех трёх исходов)
    [Fact]
    public async Task VerifyOutcomesCallObserverTransientFailedOk()
    {
        // Arrange 1 — COMPLETED PENDING-кандидат; S3-транспорт лежит
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify-obs", ct);
        Fx = fx;
        const string cluster = "vo1";
        await SeedAsync(cluster);
        var s3 = new FakeBackupS3 { Fails = true };
        var outcomes = new List<(string C, string S, string Result)>();
        var process = BuildProcess(cluster, new FakeVerifyDriver(new FakeVerifyEngine()), s3,
            verifyObserver: (c, s, r) => outcomes.Add((c, s, r)));

        // Act 1
        (await process.TickAsync(BuildSnap(cluster), BackupsOf(cluster,
                Completed("20260911120000Z", "000000010000000000000001",
                    new BackupVerify(BackupVerifyStatus.Pending, null))), ct))
            .IsSuccess.Should().BeTrue();

        // Assert 1 — list S3 упал: transient, статус кандидата не тронут
        outcomes.Should().ContainSingle().Which.Should().Be((cluster, "shard1", "transient"));

        // Arrange 2 — wal_start не разбирается: permanent FAILED сразу
        const string cluster2 = "vo2";
        await SeedAsync(cluster2);
        var outcomes2 = new List<(string C, string S, string Result)>();
        var process2 = BuildProcess(cluster2, new FakeVerifyDriver(new FakeVerifyEngine()), new FakeBackupS3(),
            verifyObserver: (c, s, r) => outcomes2.Add((c, s, r)));

        // Act 2
        (await process2.TickAsync(BuildSnap(cluster2), BackupsOf(cluster2,
                Completed("20260911120000Z", "garbage-wal-start",
                    new BackupVerify(BackupVerifyStatus.Pending, null))), ct))
            .IsSuccess.Should().BeTrue();

        // Assert 2 — вердикт permanent: failed после успешной записи FAILED в etcd
        outcomes2.Should().ContainSingle().Which.Should().Be((cluster2, "shard1", "failed"));

        // Arrange 3 — джоб стартует первым тиком (StartJobAsync без наблюдателя:
        // старт-тик исходов не эмитит), операция завершилась успешно (GET Ok)
        const string cluster3 = "vo3";
        await SeedAsync(cluster3);
        var s3ok = new FakeBackupS3();
        var engine = await StartJobAsync(cluster3, s3ok);
        var jobName = $"pgw-backup-verify-{cluster3}-shard1-20260911120000Z";
        Receiver.GetReply = ApiOk();
        var outcomes3 = new List<(string C, string S, string Result)>();
        var process3 = BuildProcess(cluster3, new FakeVerifyDriver(engine), s3ok,
            verifyObserver: (c, s, r) => outcomes3.Add((c, s, r)));

        // Act 3 — тик супервиза итога
        (await process3.TickAsync(BuildSnap(cluster3), BackupsOf(cluster3,
                Completed("20260911120000Z", "000000010000000000000001",
                    new BackupVerify(BackupVerifyStatus.Pending, null))), ct))
            .IsSuccess.Should().BeTrue();

        // Assert 3 — GET succeeded → observe ok после успешной записи OK в etcd
        outcomes3.Should().ContainSingle().Which.Should().Be((cluster3, "shard1", "ok"));
    }

    // ---- t02 (arch/19 §2 «Наблюдаемость итога verify»): итог — строка host-лога ----

    // AAA: итог OK от джоба (exit 0 + ok:true) — строка host-лога воркера с
    // кластером/шардом и id полного (журнал остаётся летописью после
    // deprovisioning'а etcd-ключей).
    [Fact]
    public async Task SupervisorOkLogsOutcomeToHostLog()
    {
        // Arrange — джоб exited с ok:true; spy-логгер снимает строки
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("hl1");
        var s3 = new FakeBackupS3();
        var engine = await StartJobAsync("hl1", s3);
        var name = "pgw-backup-verify-hl1-shard1-20260911120000Z";
        engine.Containers[name] = engine.Containers[name] with { State = "running" };
        Receiver.GetReply = ApiOk();
        var logger = new SpyLogger();
        var process = BuildProcess("hl1", new FakeVerifyDriver(engine), s3, logger: logger);

        // Act — тик супервиза итога
        (await process.TickAsync(BuildSnap("hl1"), BackupsOf("hl1",
            Completed("20260911120000Z", "000000010000000000000001",
                new BackupVerify(BackupVerifyStatus.Pending, null))), ct)).IsSuccess.Should().BeTrue();

        // Assert — строка итога OK с <C>/<X> и id полного
        logger.Lines.Should().Contain(l =>
            l.Contains("hl1/shard1", StringComparison.Ordinal)
            && l.Contains("verify 20260911120000Z OK", StringComparison.Ordinal));
    }

    // AAA: итог FAILED от джоба (phase=verify, ненулевой pg_verifybackup) —
    // строка host-лога с причиной из verify.error.
    [Fact]
    public async Task SupervisorVerifyPhaseFailedLogsOutcome()
    {
        // Arrange — джоб exited с result-JSON phase=verify
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("hl2");
        var s3 = new FakeBackupS3();
        var engine = await StartJobAsync("hl2", s3);
        var name = "pgw-backup-verify-hl2-shard1-20260911120000Z";
        engine.Containers[name] = engine.Containers[name] with { State = "running" };
        Receiver.GetReply = ApiVerifyFailed("checksum mismatch failed");
        var logger = new SpyLogger();
        var process = BuildProcess("hl2", new FakeVerifyDriver(engine), s3, logger: logger);

        // Act — тик супервиза итога
        (await process.TickAsync(BuildSnap("hl2"), BackupsOf("hl2",
            Completed("20260911120000Z", "000000010000000000000001",
                new BackupVerify(BackupVerifyStatus.Pending, null))), ct)).IsSuccess.Should().BeTrue();

        // Assert — строка итога FAILED с причиной
        logger.Lines.Should().Contain(l =>
            l.Contains("verify 20260911120000Z FAILED", StringComparison.Ordinal)
            && l.Contains("checksum mismatch failed", StringComparison.Ordinal));
    }

    // AAA: итог FAILED цепочечной проверки (без джоба) — строка host-лога с
    // причиной (границы дыры); РОВНО один итог на полный — transient-пути
    // итоговой строки не пишут.
    [Fact]
    public async Task ChainFailedLogsOutcome()
    {
        // Arrange — wal/: 1,3 (дыра на 2); набор pg_wal/3; spy-логгер
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("hl3");
        var engine = new FakeVerifyEngine();
        var driver = new FakeVerifyDriver(engine);
        var s3 = new FakeBackupS3();
        s3.Objects.Add(("hl3", "shard1", "000000010000000000000001"));
        s3.Objects.Add(("hl3", "shard1", "000000010000000000000003"));
        s3.PrefixedObjects.Add(("hl3", "shard1", "full/20260911120000Z/pg_wal/000000010000000000000003"));
        var logger = new SpyLogger();
        var process = BuildProcess("hl3", driver, s3, logger: logger);

        // Act — тик: дыра цепочки → permanent FAILED без джоба
        (await process.TickAsync(BuildSnap("hl3"), BackupsOf("hl3",
            Completed("20260911120000Z", "000000010000000000000001",
                new BackupVerify(BackupVerifyStatus.Pending, null))), ct)).IsSuccess.Should().BeTrue();

        // Assert — ровно одна итоговая строка FAILED с границей дыры
        logger.Lines.Should().ContainSingle(l =>
                l.Contains("verify 20260911120000Z FAILED", StringComparison.Ordinal)
                && l.Contains("000000010000000000000002", StringComparison.Ordinal),
            "exactly one terminal outcome line per full — transient paths log no outcome");
    }
}
