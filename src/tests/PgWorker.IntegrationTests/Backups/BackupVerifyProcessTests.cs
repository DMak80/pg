using System.Text.Json;
using FluentAssertions;
using PgWorker.Backups;
using PgWorker.Backups.Job;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Planning;
using PgWorker.Core.Templates;
using PgWorker.Core.Tuning;
using PgWorker.Docker.Drivers;
using PgWorker.Docker.Engine;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Probes;
using Xunit;

namespace PgWorker.IntegrationTests.Backups;

// Интеграции BackupVerifyProcess (t04 spec Ф3): у КАЖДОГО Fact своё etcd-
// окружение OwnEtcd (guid-имя pgw-ee-*, динамический порт, own-only teardown
// с ассертом чистоты — docs/e2e-isolation.md §1/§3; ключи умирают с контейнером)
// + фейки docker-движка и S3 (паттерны FakeBackupDeps/FakeBackupEngine).
public class BackupVerifyProcessTests
{
    // Окружение Fact'а (свой etcd); создаётся в начале каждого сценария.
    private OwnEtcd Fx = null!;

    // ClaimStore ОДИН на окружение: InstanceId фиксируется клэймом SeedAsync —
    // новый экземпляр на вызов давал бы чужой InstanceId в тике (guard «клэйм не наш»).
    private ClaimStore? _claimsStore;

    private ClaimStore Claims => _claimsStore ??= new([Fx.Endpoint], Fx.Gateway, TimeProvider.System);

    // ── Фейк docker-движка (по образцу BackupProcessTests.FakeBackupEngine;
    //    тест управляет State/ExitCode/Logs — супервиз-ветки задачи 9) ──
    internal sealed class FakeVerifyEngine : IDockerEngine
    {
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
        public Task<Result> EnsureBackupAgentAsync(
            string cluster, string shard, ContainerSpec spec, string host, CancellationToken ct)
            => Task.FromResult(Result.Success());
        public Task<Result> RemoveBackupAgentsAsync(string cluster, string? shard, CancellationToken ct)
            => Task.FromResult(Result.Success());
        public Task<Result<IReadOnlyList<DockerContainer>>> ListBackupAgentsAsync(string cluster, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<DockerContainer>>.Success([]));

        private static NotSupportedException NotSupported() => new("не используется в тестах verify");
        public Task<Result<IReadOnlySet<(string Host, int Port)>>> GetBusyPortsAsync(CancellationToken ct) => throw NotSupported();
        public Task<Result> EnsureNodeAsync(ShardTopology topology, string nodeName, NodeAddress addr,
            InstallSecrets secrets, EtcdEndpoints etcd, NodeResources? resources, PgTuneResult? tuning, CancellationToken ct) => throw NotSupported();
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

    private BackupVerifyProcess BuildProcess(
        string cluster, FakeVerifyDriver driver, IBackupS3 s3, BackupsRuntimeOptions? options = null)
        => new(
            Fx.Gateway, [Fx.Endpoint], driver,
            new ShardEndpoints(Fx.Gateway, [Fx.Endpoint], new ShardProbe(new HttpClient())),
            s3, Claims, new WorkJournal(Fx.Gateway, [Fx.Endpoint]),
            options ?? new BackupsRuntimeOptions(
                Enabled: true, S3Endpoint: "http://minio", S3Bucket: "bkt",
                S3AccessKey: "ak", S3SecretKey: "sk", JobImage: "pgworker-backup:test",
                StagingDir: "/backup-staging"),
            TimeProvider.System,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<BackupVerifyProcess>.Instance);

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
        kv.Value.Should().NotBeNull("итог verify пишется в ключ полного");
        return JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(kv.Value!.Value)!["verify"];
    }

    // AAA: PENDING-кандидат при on_create — цепочка цела → verify-джоб создан и
    // запущен (AC1-юнитная часть; итог exited — задача 9)
    [Fact]
    public async Task Тик_PendingКандидат_ЦепочкаЦела_ЗапускаетДжоб()
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

        // Assert — контейнер pgw-backup-verify-vc1-shard1-20260911120000Z создан+запущен
        result.IsSuccess.Should().BeTrue();
        engine.Created.Should().ContainSingle(c => c.Name == "pgw-backup-verify-vc1-shard1-20260911120000Z");
        engine.Started.Should().Contain("pgw-backup-verify-vc1-shard1-20260911120000Z");
        engine.Created.Single().Spec.Cmd.Should().BeEquivalentTo(VerifyJobCommand.Build(), o => o.WithStrictOrdering());
    }

    // AAA: дыра в wal/ внутри [start..end] → permanent FAILED с границами,
    // verify-джоб НЕ создаётся (AC3: контейнер не создаётся)
    [Fact]
    public async Task Тик_ДыраЦепочки_FAILED_безДжоба()
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
        engine.Created.Should().BeEmpty("вердикт определён цепочкой — скачивание не тратим (spec §3.1)");
    }

    // AAA: COMPLETED без wal_start_segment (аномалия) — permanent FAILED без джоба
    [Fact]
    public async Task Тик_НетWalStart_FAILED_безДжоба()
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
    public async Task Тик_СтрогийTLI_ВалиднаяТочка_ЗапускаетДжоб()
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
    public async Task Тик_СтрогийTLI_ТочкаВнеГраниц_FAILED()
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
    public async Task Тик_TransientS3_СтатусНеТрогаем_ДжобаНет()
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
        result.IsSuccess.Should().BeTrue("transient S3 — не ошибка тика (spec §3.1)");
        engine.Created.Should().BeEmpty("джоб без цепочки не стартует");
        var after = (await Fx.Gateway.GetAsync(Fx.Endpoint, key, ct)).Value!.Value;
        after.Should().Be(before, "статус PENDING не трогаем — ретрай следующим тиком");
    }

    // AAA: transient GET history (spec §3.1): цепочка с TLI-переходом требует
    // скачивания .history → GET падает → шард-skip, джоба нет, статус не тронут
    [Fact]
    public async Task Тик_TransientGetHistory_СтатусНеТрогаем_ДжобаНет()
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
        result.IsSuccess.Should().BeTrue("transient GET — не ошибка тика (spec §3.1)");
        engine.Created.Should().BeEmpty("без содержимого history строгий разбор невозможен — джоб не стартует");
        var after = (await Fx.Gateway.GetAsync(Fx.Endpoint, key, ct)).Value!.Value;
        after.Should().Be(before, "статус PENDING не трогаем — ретрай следующим тиком");
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
        engine.Created.Should().NotBeEmpty("джоб запущен первым тиком (предусловие)");
        return engine;
    }

    // AAA: exited exit=0 + ok:true → verify OK + checked_unix; контейнер и volume снесены (AC1)
    [Fact]
    public async Task Супервиз_Exit0_Ok_чисткаДжоба()
    {
        // Arrange — джоб exited с ok:true
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("sv1");
        var s3 = new FakeBackupS3();
        var engine = await StartJobAsync("sv1", s3);
        var name = "pgw-backup-verify-sv1-shard1-20260911120000Z";
        engine.Containers[name] = engine.Containers[name] with { State = "exited", ExitCode = 0, Logs = "{\"ok\":true}" };

        // Act — второй тик (супервиз итога)
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
    public async Task Супервиз_VerifyPhaseFailed_FAILED()
    {
        // Arrange — джоб exited с result-JSON phase=verify
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("sv2");
        var s3 = new FakeBackupS3();
        var engine = await StartJobAsync("sv2", s3);
        var name = "pgw-backup-verify-sv2-shard1-20260911120000Z";
        engine.Containers[name] = engine.Containers[name] with { State = "exited", ExitCode = 1,
            Logs = "{\"ok\":false,\"phase\":\"verify\",\"error\":\"checksum mismatch failed\"}" };

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
    public async Task Супервиз_DownloadPhase_Transient_ОстаетсяPending()
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
        engine.Containers[name] = engine.Containers[name] with { State = "exited", ExitCode = 1,
            Logs = "{\"ok\":false,\"phase\":\"download\",\"error\":\"mc cp failed\"}" };
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
        after.Should().Be(before, "download-phase transient: статус PENDING не трогаем");
        engine.Removed.Should().Contain(name);

        // Act 2 — следующий тик: PENDING снова due → джоб перезапущен (ретрай тиками)
        (await process.TickAsync(BuildSnap("sv3"), BackupsOf("sv3",
            Completed("20260911120000Z", "000000010000000000000001",
                new BackupVerify(BackupVerifyStatus.Pending, null))), ct)).IsSuccess.Should().BeTrue();

        // Assert 2
        engine.Created.Should().HaveCount(2, "transient ретраится следующим тиком (spec §3.1)");
    }

    // AAA: vanished-джоб (контейнера нет при PENDING) → перезапуск со шага цепочки (AC8)
    [Fact]
    public async Task Супервиз_Vanished_Перезапуск()
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
    public async Task ДваDue_ПоОдномуЗаТик_PendingРаньше()
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
    public async Task Периодика_ПоInterval_Отключение_и_ТерминальностьFAILED()
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
        engine.Created.Should().ContainSingle("OK-полный старше interval — перепроверка (AC5)");

        // Act 1b — джоб завершился ok:true → супервиз пишет итог
        var name = "pgw-backup-verify-sv6-shard1-20260911120000Z";
        engine.Containers[name] = engine.Containers[name] with { State = "exited", ExitCode = 0, Logs = "{\"ok\":true}" };
        (await process.TickAsync(BuildSnap("sv6"), [WithInterval(3600)], ct)).IsSuccess.Should().BeTrue();

        // Assert 1b — verify OK и checked_unix РАСТЁТ (было now-7200, стало ~now)
        var verify = await ReadVerifyAsync("sv6", "20260911120000Z");
        verify.GetProperty("state").GetString().Should().Be("OK");
        verify.GetProperty("checked_unix").GetInt64().Should().BeGreaterThan(now - 7200,
            "периодическая перепроверка обновляет checked_unix (AC5)");

        // Act 2 / Assert 2 — interval=0: не due (только on_create)
        var engineOff = new FakeVerifyEngine();
        var processOff = BuildProcess("sv6", new FakeVerifyDriver(engineOff), s3);
        (await processOff.TickAsync(BuildSnap("sv6"), [WithInterval(0)], ct)).IsSuccess.Should().BeTrue();
        engineOff.Created.Should().BeEmpty("interval_sec<=0 — периодика выключена (AC5)");

        // Act 2b / Assert 2b — verify=null («никогда не проверялся», spec §3.1
        // due-periodic): ловится ТОЛЬКО периодикой (не on_create — verify нет)
        var engineNull = new FakeVerifyEngine();
        var processNull = BuildProcess("sv6", new FakeVerifyDriver(engineNull), s3);
        var neverVerified = Completed("20260911120000Z", "000000010000000000000001", verify: null);
        (await processNull.TickAsync(BuildSnap("sv6"), [WithInterval(3600, neverVerified)], ct))
            .IsSuccess.Should().BeTrue();
        engineNull.Created.Should().ContainSingle("непроверенный полный (verify=null) due по периодике (§3.1)");

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
        engineFailed.Created.Should().BeEmpty("FAILED терминален (spec §3.1)");
    }

    // AAA: транспорт-отказ docker (list) — статус не меняем (transient, spec §3.2)
    [Fact]
    public async Task Супервиз_TransportОтказ_СтатусНеТрогаем()
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
    public async Task ЖивойДжобШарда_БлокируетНовый_СтатусBTакойЖе()
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
        engine.Created.Should().BeEmpty("живой verify-джоб шарда блокирует запуск нового (инвариант §3.3)");
        var afterB = (await Fx.Gateway.GetAsync(Fx.Endpoint, keyB, ct)).Value!.Value;
        afterB.Should().Be(beforeB, "статус due-кандидата не трогаем, пока жив чужой джоб шарда");
    }

    // AAA (AC5): verify-джоб running дольше бюджета → kill+rm, кандидат PENDING с
    // checked_unix=now; лив-лок исключён — следующий due только через interval
    [Fact]
    public async Task Супервиз_verify_старше_бюджета_kill_и_квота_попытки()
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
        checkedUnix.Should().BeGreaterOrEqualTo(now - 60, "квота попытки — время супервиза");
        verify.TryGetProperty("error", out _).Should().BeFalse("данные не виноваты — FAILED не ставится");

        // Act 2 — повторный тик немедленно (джобов живых больше нет)
        var b = a with { Verify = new BackupVerify(BackupVerifyStatus.Pending, checkedUnix) };
        (await process.TickAsync(BuildSnap("sv9"), BackupsOf("sv9", b), ct))
            .IsSuccess.Should().BeTrue();

        // Assert 3 — НОВЫЙ джоб НЕ создан (checked только что; pending due — по interval)
        engine.Created.Should().BeEmpty("лив-лок немедленных перезапусков исключён (t07)");
    }

    // AAA: нода-источник исчезла из portalloc → джоб стартует на ПЕРВОМ хосте
    // таблицы Docker:Hosts (GetHostsAsync) + journal-факт engine-fallback (spec §3.2)
    [Fact]
    public async Task Тик_УзелИсчезИзPortalloc_ДжобНаПервомХостеТаблицы()
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
}
