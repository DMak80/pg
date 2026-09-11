using System.Text.Json;
using FluentAssertions;
using PgWorker.Backups;
using PgWorker.Backups.Job;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Planning;
using PgWorker.Core.Templates;
using PgWorker.Docker.Drivers;
using PgWorker.Docker.Engine;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;
using PgWorker.Etcd.Parsing;
using PgWorker.IntegrationTests.Etcd;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Probes;
using Xunit;

namespace PgWorker.IntegrationTests.Backups;

// Интеграции BackupVerifyProcess (t04 spec Ф3): реальный etcd (статусы/журнал)
// + фейки docker-движка и S3 (паттерны FakeBackupDeps/FakeBackupEngine).
[Collection(EtcdCollection.Name)]
public class BackupVerifyProcessTests(EtcdFixture fixture)
{
    private readonly ClaimStore _claims = new([fixture.Endpoint], fixture.Gateway, TimeProvider.System);

    // ── Фейк docker-движка (по образцу BackupProcessTests.FakeBackupEngine;
    //    тест управляет State/ExitCode/Logs — супервиз-ветки задачи 9) ──
    internal sealed class FakeVerifyEngine : IDockerEngine
    {
        internal sealed record ContainerRec(string Id, string State, int ExitCode, string Logs);

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
            InstallSecrets secrets, EtcdEndpoints etcd, NodeResources? resources, CancellationToken ct) => throw NotSupported();
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
        await fixture.Gateway.DeleteAsync(fixture.Endpoint, $"/pgworker/backups/{cluster}/", prefix: true, ct);
        await fixture.Gateway.DeleteAsync(fixture.Endpoint, $"/pgworker/claims/{cluster}", prefix: false, ct);
        await fixture.Gateway.PutAsync(fixture.Endpoint, $"/pgworker/portalloc/{cluster}",
            Portalloc.Serialize(new Dictionary<string, NodeAddress>
            {
                ["shard1/shard1a"] = new("h1", new NodePorts(16001, 18001, 17001)),
            }), null, ct);
        (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue("клэйм — предусловие тика");
    }

    private BackupVerifyProcess BuildProcess(
        string cluster, FakeVerifyDriver driver, IBackupS3 s3, BackupsRuntimeOptions? options = null)
        => new(
            fixture.Gateway, [fixture.Endpoint], driver,
            new ShardEndpoints(fixture.Gateway, [fixture.Endpoint], new ShardProbe(new HttpClient())),
            s3, _claims, new WorkJournal(fixture.Gateway, [fixture.Endpoint]),
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
        var kv = await fixture.Gateway.GetAsync(
            fixture.Endpoint, $"/pgworker/backups/{cluster}/shard1/full/{id}",
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
        await SeedAsync("vc6");
        var engine = new FakeVerifyEngine();
        var driver = new FakeVerifyDriver(engine);
        var s3 = new FakeBackupS3 { Fails = true };
        var candidate = Completed("20260911120000Z", "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Pending, null));
        var key = $"/pgworker/backups/vc6/shard1/full/20260911120000Z";
        var before = BackupStatusJson.Serialize(candidate);
        await fixture.Gateway.PutAsync(fixture.Endpoint, key, before, null, ct);
        var process = BuildProcess("vc6", driver, s3);
        var backups = BackupsOf("vc6", candidate);

        // Act
        var result = await process.TickAsync(BuildSnap("vc6"), backups, ct);

        // Assert — transient: тик без ошибки (шард-skip), ничего не создано/не записано
        result.IsSuccess.Should().BeTrue("transient S3 — не ошибка тика (spec §3.1)");
        engine.Created.Should().BeEmpty("джоб без цепочки не стартует");
        var after = (await fixture.Gateway.GetAsync(fixture.Endpoint, key, ct)).Value!.Value;
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
        await fixture.Gateway.PutAsync(fixture.Endpoint, key, before, null, ct);
        var process = BuildProcess("vc7", driver, s3);

        // Act
        var result = await process.TickAsync(BuildSnap("vc7"), BackupsOf("vc7", candidate), ct);

        // Assert — list прошёл (переход в диапазоне найден), GET упал → transient
        result.IsSuccess.Should().BeTrue("transient GET — не ошибка тика (spec §3.1)");
        engine.Created.Should().BeEmpty("без содержимого history строгий разбор невозможен — джоб не стартует");
        var after = (await fixture.Gateway.GetAsync(fixture.Endpoint, key, ct)).Value!.Value;
        after.Should().Be(before, "статус PENDING не трогаем — ретрай следующим тиком");
    }

    // AAA: нода-источник исчезла из portalloc → джоб стартует на ПЕРВОМ хосте
    // таблицы Docker:Hosts (GetHostsAsync) + journal-факт engine-fallback (spec §3.2)
    [Fact]
    public async Task Тик_УзелИсчезИзPortalloc_ДжобНаПервомХостеТаблицы()
    {
        // Arrange — portalloc ПУСТ (узел shard1a исчез); PENDING-кандидат с node=shard1a
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("vc8");
        await fixture.Gateway.PutAsync(fixture.Endpoint, "/pgworker/portalloc/vc8",
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
        var journal = await fixture.Gateway.GetAsync(fixture.Endpoint, "/pgworker/work/vc8", ct);
        journal.Value!.Value.Should().Contain("engine-fallback/shard1");
    }
}
