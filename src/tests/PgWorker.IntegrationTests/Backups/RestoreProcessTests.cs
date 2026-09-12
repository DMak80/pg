using FluentAssertions;
using PgWorker.Backups;
using PgWorker.Backups.Process;
using PgWorker.Backups.Restore;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Planning;
using PgWorker.Docker.Drivers;
using PgWorker.Docker.Engine;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;
using PgWorker.Etcd.Parsing;
using PgWorker.IntegrationTests.Etcd;
using PgWorker.Core.Templates;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Processes;
using PgWorker.Provisioning.Probes;
using Xunit;

namespace PgWorker.IntegrationTests.Backups;

// Интеграции RestoreProcess (t05 spec Ф2): реальный etcd (статусы/журнал) +
// фейки docker/S3. Снапшот кластера и бэкапы строятся руками. PLANNED-фаза:
// валидация (усыновление/полный/manifest/цепочка) и гвард дублей.
[Collection(EtcdCollection.Name)]
public class RestoreProcessTests(EtcdFixture fixture)
{
    private readonly ClaimStore _claims = new([fixture.Endpoint], fixture.Gateway, TimeProvider.System);

    // ── Хелперы Arrange ──

    // Активный кластер c1/shard1 с канонической нодой shard1a.
    private static ClusterSnapshot BuildSnap(string cluster = "c1", string shard = "shard1") => new(
        new ClusterConfig(cluster, 2, cluster, null, ClusterState.Active),
        [new ShardSpec(shard, 1, $"host=shard1a dbname={cluster}", "shard1a:17001",
            [new NodeSpec(shard, "shard1a", NodeState.Running)])],
        []);

    private static BackupsRuntimeOptions Options() => new(
        Enabled: true,
        S3Endpoint: "http://minio",
        S3Bucket: "bkt",
        S3AccessKey: "ak",
        S3SecretKey: "sk",
        JobImage: "pgworker-backup:test",
        RestoreRecoveryTimeoutSec: 1800);

    // Сид portalloc под конкретный тест-кластер + чистка чужих следов
    // (own-only: только префиксы этого кластера).
    private async Task SeedAsync(string cluster, string shard = "shard1",
        Dictionary<string, NodeAddress>? alloc = null)
    {
        var ct = TestContext.Current.CancellationToken;
        await fixture.Gateway.DeleteAsync(fixture.Endpoint, $"/pgworker/backups/{cluster}/", prefix: true, ct);
        await fixture.Gateway.DeleteAsync(fixture.Endpoint, $"/pgworker/claims/{cluster}", prefix: false, ct);
        await fixture.Gateway.DeleteAsync(fixture.Endpoint, $"/pgworker/work/{cluster}", prefix: false, ct);
        await fixture.Gateway.PutAsync(fixture.Endpoint, $"/pgworker/portalloc/{cluster}",
            Portalloc.Serialize(alloc ?? new Dictionary<string, NodeAddress>
            {
                [$"{shard}/shard1a"] = new("localhost", new NodePorts(16001, 18001, 17001)),
            }), null, ct);
    }

    private RestoreProcess BuildProcess(FakeBackupS3 s3, IClusterDriver driver,
        TimeProvider? clock = null, HttpMessageHandler? patroni = null)
        => new(fixture.Gateway, [fixture.Endpoint], driver, s3, _claims,
            new WorkJournal(fixture.Gateway, [fixture.Endpoint]), Options(),
            new InstallSecrets("su-pw", "sb-pw", "adm-pw", "mov-pw"),
            new EtcdEndpoints([fixture.Endpoint]), new StubAppSecret(),
            new ShardProbe(new HttpClient(patroni ?? new DeadHandler())),
            new ThresholdsOptions(600, 1800, PatroniBootSec: 600),
            clock ?? TimeProvider.System);

    // Patroni-фейк: /cluster отвечает членами шарда (Ready — running/старт).
    private sealed class PatroniHandler : HttpMessageHandler
    {
        public bool Ready { get; set; }

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken ct)
        {
            var json = Ready
                ? """{"members":[{"name":"shard1a","state":"running","role":"leader"},{"name":"shard1b","state":"running","role":"replica"}]}"""
                : """{"members":[{"name":"shard1a","state":"start","role":"leader"}]}""";
            return Task.FromResult(new HttpResponseMessage(System.Net.HttpStatusCode.OK)
            {
                Content = new StringContent(json, System.Text.Encoding.UTF8, "application/json"),
            });
        }
    }

    // Мёртвый обработчик (плейсхолдер пробы там, где Patroni не зовётся).
    private sealed class DeadHandler : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken ct)
            => Task.FromResult(new HttpResponseMessage(System.Net.HttpStatusCode.ServiceUnavailable));
    }

    // Фиксированные часы: управление бюджетом rejoin-ожидания (AAA).
    private sealed class MutableClock : TimeProvider
    {
        public DateTimeOffset Now { get; set; } = new(2026, 9, 11, 12, 0, 0, TimeSpan.Zero);

        public override DateTimeOffset GetUtcNow() => Now;
    }

    // Заявка restore: PUT PLANNED-ключа в etcd + тождественный объект в
    // backsupply-параметре тика (ReconcileLoop парсит префикс один раз).
    private async Task<RestoreOperationState> SeedRestoreAsync(
        string cluster, string shard, string id, string backupId = "", string source = "",
        string target = "latest", RestoreStatus state = RestoreStatus.Planned,
        long? startedUnix = null)
    {
        var ct = TestContext.Current.CancellationToken;
        var op = new RestoreOperationState(id, state, backupId,
            source.Length == 0 ? $"{cluster}/{shard}" : source, target, "shard1a",
            TimeProvider.System.GetUtcNow().ToUnixTimeSeconds(), "operator",
            StartedUnix: startedUnix);
        await fixture.Gateway.PutAsync(fixture.Endpoint,
            BackupNames.RestoreKey(cluster, shard, id), RestoreStatusJson.Serialize(op), null, ct);
        return op;
    }

    // Чтение статусов restore шарда из etcd (истина — ключ, не параметр тика).
    private async Task<IReadOnlyList<RestoreOperationState>> ReadRestoresAsync(
        string cluster, string shard)
    {
        var ct = TestContext.Current.CancellationToken;
        var range = await fixture.Gateway.RangeAsync(
            fixture.Endpoint, $"/pgworker/backups/{cluster}/{shard}/restore/", ct);
        var parsed = BackupsParser.Parse(range.Value, out var errors);
        errors.Should().BeEmpty();
        return parsed.Value.Count == 0
            ? (IReadOnlyList<RestoreOperationState>)[]
            : parsed.Value[0].Shards.TryGetValue(shard, out var sb)
                ? sb.Restores
                : (IReadOnlyList<RestoreOperationState>)[];
    }

    // Бэкапы-параметр тика: парс префикса etcd (ReconcileLoop парсит тот же
    // префикс один раз на кластер и передаёт процессам).
    private async Task<IReadOnlyList<ClusterBackups>> BackupsFromEtcdAsync(string cluster)
    {
        var ct = TestContext.Current.CancellationToken;
        var range = await fixture.Gateway.RangeAsync(
            fixture.Endpoint, $"/pgworker/backups/{cluster}/", ct);
        var parsed = BackupsParser.Parse(range.Value, out var errors);
        errors.Should().BeEmpty();
        return parsed.Value;
    }

    // COMPLETED-полный в etcd (wal_start → сегмент 000000010000000000000002).
    private async Task SeedFullAsync(string cluster, string shard, string id)
    {
        var ct = TestContext.Current.CancellationToken;
        await fixture.Gateway.PutAsync(fixture.Endpoint,
            BackupNames.FullKey(cluster, shard, id),
            """{"state":"COMPLETED","node":"shard1a","role":"replica","started_unix":1757500000,"finished_unix":1757500300,"wal_start_segment":"000000010000000000000002"}""",
            null, ct);
    }

    // Непрерывная цепочка от 000000010000000000000002 (сегменты 2–4).
    private static void SeedChain(FakeBackupS3 s3, string cluster, string shard,
        string fullId, bool withManifest = true, string[]? segments = null)
    {
        var prefix = $"{cluster}/{shard}";
        s3.Texts[$"{prefix}/full/{fullId}/backup_label"] =
            "START WAL LOCATION: 0/2000028 (file 000000010000000000000002)\n";
        if (withManifest)
            s3.Texts[$"{prefix}/full/{fullId}/backup_manifest"] = "{}";
        foreach (var seg in segments ?? ["000000010000000000000002", "000000010000000000000003", "000000010000000000000004"])
            s3.Objects.Add((cluster, shard, seg));
    }

    // Секрет-стаб: пер-кластерные креды «уже есть» (прецедент AdoptionContractTests).
    private sealed class StubAppSecret : IClusterSecretEnsurer
    {
        public Task<Result<ClusterCredentials>> EnsureAsync(
            string cluster, ClusterConfig config, CancellationToken ct)
            => Task.FromResult(Result<ClusterCredentials>.Success(new ClusterCredentials(
                new AppCredentials("app", "pw"), "moverpw000000000000000000000000A",
                new AppCredentials("bucket_admin", "bapw"),
                "backuppw00000000000000000000000A")));
    }

    // ── PLANNED: happy-path ──

    [Fact]
    public async Task Валидация_проходит_фиксирует_backup_id_и_started_unix_переходя_в_RUNNING()
    {
        // Arrange — свой COMPLETED-полный в etcd + непрерывная цепочка + манифест
        const string fullId = "20260910120000Z";
        await SeedAsync("c1");
        await SeedFullAsync("c1", "shard1", fullId);
        var s3 = new FakeBackupS3();
        SeedChain(s3, "c1", "shard1", fullId);
        var driver = new StubScaleDriver();
        var process = BuildProcess(s3, driver);
        var op = await SeedRestoreAsync("c1", "shard1", "20260911120000Z", backupId: "");
        (await _claims.TryClaimClusterAsync("c1", TestContext.Current.CancellationToken))
            .Value.Should().BeTrue("клэйм — предусловие тика");

        // Act
        var result = await process.TickAsync(
            BuildSnap(), await BackupsFromEtcdAsync("c1"), TestContext.Current.CancellationToken);

        // Assert — ключ в etcd: RUNNING, backup_id зафиксирован, started_unix
        result.IsSuccess.Should().BeTrue(result.Error?.ToString());
        var restores = await ReadRestoresAsync("c1", "shard1");
        var running = restores.Single(r => r.Id == op.Id);
        running.State.Should().Be(RestoreStatus.Running);
        running.BackupId.Should().Be(fullId);
        running.StartedUnix.Should().BeGreaterThan(0);
        (await fixture.Gateway.GetAsync(fixture.Endpoint, "/pgworker/work/c1",
            TestContext.Current.CancellationToken)).Value!.Value
            .Should().Contain($"validated/shard1/{op.Id}");
    }

    // ── PLANNED: permanent-отказы валидации ──

    [Fact]
    public async Task Валидация_полный_без_backup_manifest_permanent_FAILED()
    {
        // Arrange — префикс полного есть (etcd-статус + backup_label), манифеста
        // НЕТ: имитация упавшего на середине upload t02
        const string fullId = "20260910120000Z";
        await SeedAsync("c1");
        await SeedFullAsync("c1", "shard1", fullId);
        var s3 = new FakeBackupS3();
        SeedChain(s3, "c1", "shard1", fullId, withManifest: false);
        var process = BuildProcess(s3, new StubScaleDriver());
        var op = await SeedRestoreAsync("c1", "shard1", "20260911120001Z");
        (await _claims.TryClaimClusterAsync("c1", TestContext.Current.CancellationToken)).Value.Should().BeTrue();

        // Act
        (await process.TickAsync(BuildSnap(), await BackupsFromEtcdAsync("c1"),
            TestContext.Current.CancellationToken)).IsSuccess.Should().BeTrue();

        // Assert — permanent FAILED с причиной
        var failed = (await ReadRestoresAsync("c1", "shard1")).Single(r => r.Id == op.Id);
        failed.State.Should().Be(RestoreStatus.Failed);
        failed.Error.Should().Contain("без backup_manifest");
        failed.FinishedUnix.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task Валидация_дубль_заявки_младшие_permanent_FAILED_старейшая_исполняется()
    {
        // Arrange — два PLANNED одного шарда; валидные данные под старейшую
        const string fullId = "20260910120000Z";
        await SeedAsync("c1");
        await SeedFullAsync("c1", "shard1", fullId);
        var s3 = new FakeBackupS3();
        SeedChain(s3, "c1", "shard1", fullId);
        var process = BuildProcess(s3, new StubScaleDriver());
        var oldest = await SeedRestoreAsync("c1", "shard1", "20260911120000Z");
        var younger = await SeedRestoreAsync("c1", "shard1", "20260911120100Z");
        (await _claims.TryClaimClusterAsync("c1", TestContext.Current.CancellationToken)).Value.Should().BeTrue();

        // Act — один тик с обеими заявками (старейшая по Id исполняется)
        (await process.TickAsync(BuildSnap(),
                await BackupsFromEtcdAsync("c1"),
                TestContext.Current.CancellationToken)).IsSuccess.Should().BeTrue();

        // Assert — младший FAILED «дубль заявки», старейший дошёл до RUNNING
        var restores = await ReadRestoresAsync("c1", "shard1");
        restores.Single(r => r.Id == younger.Id).State.Should().Be(RestoreStatus.Failed);
        restores.Single(r => r.Id == younger.Id).Error.Should().Contain("дубль заявки");
        restores.Single(r => r.Id == oldest.Id).State.Should().Be(RestoreStatus.Running);
    }

    [Fact]
    public async Task Валидация_усыновлённый_шард_permanent_FAILED()
    {
        // Arrange — portalloc с object-нодами (усвоенный стендовый шард)
        await SeedAsync("c1", alloc: new Dictionary<string, NodeAddress>
        {
            ["shard1/shard1a"] = new("local", new NodePorts(15433, 18011, 0), Object: "as-shard1a"),
        });
        var s3 = new FakeBackupS3();
        var process = BuildProcess(s3, new StubScaleDriver());
        var op = await SeedRestoreAsync("c1", "shard1", "20260911120002Z");
        (await _claims.TryClaimClusterAsync("c1", TestContext.Current.CancellationToken)).Value.Should().BeTrue();

        // Act
        (await process.TickAsync(BuildSnap(), await BackupsFromEtcdAsync("c1"),
            TestContext.Current.CancellationToken)).IsSuccess.Should().BeTrue();

        // Assert
        var failed = (await ReadRestoresAsync("c1", "shard1")).Single(r => r.Id == op.Id);
        failed.State.Should().Be(RestoreStatus.Failed);
        failed.Error.Should().Contain("усыновлённых шардов");
    }

    [Fact]
    public async Task Валидация_дыра_цепочки_permanent_FAILED_с_границами()
    {
        // Arrange — сегмент 3 пропущен: 2, 4 (ожидается 000000010000000000000003)
        const string fullId = "20260910120000Z";
        await SeedAsync("c1");
        await SeedFullAsync("c1", "shard1", fullId);
        var s3 = new FakeBackupS3();
        SeedChain(s3, "c1", "shard1", fullId,
            segments: ["000000010000000000000002", "000000010000000000000004"]);
        var process = BuildProcess(s3, new StubScaleDriver());
        var op = await SeedRestoreAsync("c1", "shard1", "20260911120003Z");
        (await _claims.TryClaimClusterAsync("c1", TestContext.Current.CancellationToken)).Value.Should().BeTrue();

        // Act
        (await process.TickAsync(BuildSnap(), await BackupsFromEtcdAsync("c1"),
            TestContext.Current.CancellationToken)).IsSuccess.Should().BeTrue();

        // Assert — границы дыры в error
        var failed = (await ReadRestoresAsync("c1", "shard1")).Single(r => r.Id == op.Id);
        failed.State.Should().Be(RestoreStatus.Failed);
        failed.Error.Should().Contain("дыра WAL-цепочки: ожидался");
    }

    [Fact]
    public async Task Валидация_полных_нет_в_S3_permanent_FAILED()
    {
        // Arrange — DR-ветка: source-override на пустой префикс
        await SeedAsync("c1");
        var s3 = new FakeBackupS3(); // Fulls пусты
        var process = BuildProcess(s3, new StubScaleDriver());
        var op = await SeedRestoreAsync("c1", "shard1", "20260911120004Z", source: "c9/shard1");
        (await _claims.TryClaimClusterAsync("c1", TestContext.Current.CancellationToken)).Value.Should().BeTrue();

        // Act
        (await process.TickAsync(BuildSnap(), await BackupsFromEtcdAsync("c1"),
            TestContext.Current.CancellationToken)).IsSuccess.Should().BeTrue();

        // Assert
        var failed = (await ReadRestoresAsync("c1", "shard1")).Single(r => r.Id == op.Id);
        failed.State.Should().Be(RestoreStatus.Failed);
        failed.Error.Should().Contain("полные в c9/shard1 не найдены");
    }

    // ── PLANNED: transient (статус не меняем, тик Ok) ──

    [Fact]
    public async Task Валидация_S3_недоступен_статус_не_меняется()
    {
        // Arrange — list S3 падает (FailList), заявка через source-override
        await SeedAsync("c1");
        var s3 = new FakeBackupS3 { FailList = true };
        var process = BuildProcess(s3, new StubScaleDriver());
        var op = await SeedRestoreAsync("c1", "shard1", "20260911120005Z", source: "c9/shard1");
        (await _claims.TryClaimClusterAsync("c1", TestContext.Current.CancellationToken)).Value.Should().BeTrue();

        // Act
        var result = await process.TickAsync(BuildSnap(),
            await BackupsFromEtcdAsync("c1"), TestContext.Current.CancellationToken);

        // Assert — тик Ok (InProgress), заявка осталась PLANNED, журнал-факт
        result.IsSuccess.Should().BeTrue(result.Error?.ToString());
        result.Value.Should().Be(ProcessOutcome.InProgress);
        (await ReadRestoresAsync("c1", "shard1")).Single(r => r.Id == op.Id)
            .State.Should().Be(RestoreStatus.Planned);
        (await fixture.Gateway.GetAsync(fixture.Endpoint, "/pgworker/work/c1",
            TestContext.Current.CancellationToken)).Value!.Value.Should().Contain("s3-unavailable");
    }

    // ── RUNNING: демонтаж + джоб + супервиз ──

    // Драйвер: нод-мутации считает StubScaleDriver, движок джоба — fake.
    private sealed class TestDriver(IClusterDriver inner, IDockerEngine engine) : IClusterDriver
    {
        public StubScaleDriver Inner => (StubScaleDriver)inner;
        public IDockerEngine? EngineFor(string host) => engine;
        public bool SupportsRunningInspection => inner.SupportsRunningInspection;
        public Task<Result<IReadOnlyList<HostInfo>>> GetHostsAsync(CancellationToken ct) => inner.GetHostsAsync(ct);
        public Task<Result<IReadOnlySet<(string Host, int Port)>>> GetBusyPortsAsync(CancellationToken ct) => inner.GetBusyPortsAsync(ct);
        public Task<Result> EnsureNodeAsync(ShardTopology t, string n, NodeAddress a, InstallSecrets s, EtcdEndpoints e, NodeResources? r, CancellationToken ct) => inner.EnsureNodeAsync(t, n, a, s, e, r, ct);
        public Task<Result> RemoveNodeAsync(string c, string sh, string node, CancellationToken ct) => inner.RemoveNodeAsync(c, sh, node, ct);
        public Task<Result> StopNodeAsync(string c, string sh, string node, CancellationToken ct) => inner.StopNodeAsync(c, sh, node, ct);
        public Task<Result<DataPresence>> NodeDataPresenceAsync(string c, string sh, string node, CancellationToken ct) => inner.NodeDataPresenceAsync(c, sh, node, ct);
        public Task<Result<string>> ExecNodeAsync(string c, string sh, string node, IReadOnlyList<string> cmd, CancellationToken ct) => inner.ExecNodeAsync(c, sh, node, cmd, ct);
        public Task<Result<string>> ExecContainerAsync(string container, IReadOnlyList<string> cmd, CancellationToken ct) => inner.ExecContainerAsync(container, cmd, ct);
        public Task<Result<IReadOnlyDictionary<string, DiscoveredNode>>> InspectNodesAsync(string c, IReadOnlyCollection<string> names, CancellationToken ct) => inner.InspectNodesAsync(c, names, ct);
        public Task<Result<IReadOnlyList<string>>> ListNodeObjectsAsync(string c, CancellationToken ct) => inner.ListNodeObjectsAsync(c, ct);
        public Task<Result> EnsureBackupAgentAsync(string c, string sh, ContainerSpec spec, string host, CancellationToken ct) => inner.EnsureBackupAgentAsync(c, sh, spec, host, ct);
        public Task<Result> RemoveBackupAgentsAsync(string c, string? sh, CancellationToken ct) => inner.RemoveBackupAgentsAsync(c, sh, ct);
        public Task<Result<IReadOnlyList<DockerContainer>>> ListBackupAgentsAsync(string c, CancellationToken ct) => inner.ListBackupAgentsAsync(c, ct);
        public Task<Result> RemoveBackupJobsAsync(string c, CancellationToken ct) => inner.RemoveBackupJobsAsync(c, ct);
        public Task<Result> RemoveRestoreJobsAsync(string c, string sh, CancellationToken ct) => inner.RemoveRestoreJobsAsync(c, sh, ct);
    }

    // Снапшот шарда с двумя нодами (демонтаж по всем).
    private static ClusterSnapshot BuildTwoNodeSnap(string cluster = "c1", string shard = "shard1") => new(
        new ClusterConfig(cluster, 2, cluster, null, ClusterState.Active),
        [new ShardSpec(shard, 2, $"host=shard1a dbname={cluster}", "shard1a:17001",
            [new NodeSpec(shard, "shard1a", NodeState.Running),
             new NodeSpec(shard, "shard1b", NodeState.Running)])],
        []);

    private async Task SeedTwoNodeAllocAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        await fixture.Gateway.PutAsync(fixture.Endpoint, "/pgworker/portalloc/c1",
            Portalloc.Serialize(new Dictionary<string, NodeAddress>
            {
                ["shard1/shard1a"] = new("h1", new NodePorts(16001, 18001, 17001)),
                ["shard1/shard1b"] = new("h1", new NodePorts(16002, 18002, 17002)),
            }), null, ct);
    }

    [Fact]
    public async Task Демонтаж_сносит_агента_ноды_и_чистит_ha_scope_ставит_REBUILDING()
    {
        // Arrange — RUNNING-заявка; живой агент, HA-scope и request_cpu в etcd
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("c1");
        await SeedTwoNodeAllocAsync();
        await fixture.Gateway.PutAsync(fixture.Endpoint, "/service/c1-shard1/leader", "shard1a", null, ct);
        await fixture.Gateway.PutAsync(fixture.Endpoint, "/service/c1-shard1/initialize", "pg 1", null, ct);
        await fixture.Gateway.PutAsync(fixture.Endpoint, "/service/c1-shard1/sync", "shard1a", null, ct);
        await fixture.Gateway.PutAsync(fixture.Endpoint, "/service/c1-shard1/optime/shard1a", "1", null, ct);
        await fixture.Gateway.PutAsync(fixture.Endpoint, "/service/c1-shard1/members/shard1a", "x", null, ct);
        await fixture.Gateway.PutAsync(fixture.Endpoint, "/service/c1-shard1/request_cpu", "2", null, ct);
        var inner = new StubScaleDriver();
        inner.BackupAgentObjects.Add(new DockerContainer("id-agent", ["/pgw-backup-wal-c1-shard1"], "running", "img"));
        var engine = new FakeBackupEngine();
        var driver = new TestDriver(inner, engine);
        var process = BuildProcess(new FakeBackupS3(), driver);
        var op = await SeedRestoreAsync("c1", "shard1", "20260911121000Z",
            backupId: "20260910120000Z", state: RestoreStatus.Running, startedUnix: 1);
        (await _claims.TryClaimClusterAsync("c1", ct)).Value.Should().BeTrue();

        // Act
        (await process.TickAsync(BuildTwoNodeSnap(), await BackupsFromEtcdAsync("c1"), ct)).IsSuccess.Should().BeTrue();

        // Assert — агент снесён, ноды удалены и в REBUILDING, scope чист,
        // request_cpu жив (заявка ресурсов — не HA-состояние)
        inner.RemovedBackupAgents.Should().Contain("pgw-backup-wal-c1-shard1");
        inner.RemovedNodes.Should().BeEquivalentTo(["shard1/shard1a", "shard1/shard1b"]);
        (await fixture.Gateway.GetAsync(fixture.Endpoint, "/clusters/c1/shards/shard1/nodes/shard1a/state", ct))
            .Value!.Value.Should().Be("REBUILDING");
        (await fixture.Gateway.GetAsync(fixture.Endpoint, "/clusters/c1/shards/shard1/nodes/shard1b/state", ct))
            .Value!.Value.Should().Be("REBUILDING");
        (await fixture.Gateway.GetAsync(fixture.Endpoint, "/service/c1-shard1/leader", ct)).Value.Should().BeNull();
        (await fixture.Gateway.GetAsync(fixture.Endpoint, "/service/c1-shard1/optime/shard1a", ct)).Value.Should().BeNull();
        (await fixture.Gateway.GetAsync(fixture.Endpoint, "/service/c1-shard1/members/shard1a", ct)).Value.Should().BeNull();
        (await fixture.Gateway.GetAsync(fixture.Endpoint, "/service/c1-shard1/request_cpu", ct))
            .Value!.Value.Should().Be("2");
        (await fixture.Gateway.GetAsync(fixture.Endpoint, "/pgworker/work/c1", ct)).Value!.Value
            .Should().Contain("demolished/shard1/");
    }

    [Fact]
    public async Task Джоб_exit0_ok_переход_REJOINING_с_lsn()
    {
        // Arrange — RUNNING + контейнер exited 0 с result-JSON ok
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("c1");
        var engine = new FakeBackupEngine();
        var driver = new TestDriver(new StubScaleDriver(), engine);
        var process = BuildProcess(new FakeBackupS3(), driver);
        var name = BackupNames.RestoreContainerName("c1", "shard1", "20260911121001Z");
        engine.Containers[name] = new FakeBackupEngine.ContainerRec(
            "cnt-restore", "exited", 0, "{\"ok\":true,\"restored_to_lsn\":\"0/42\"}\n");
        var op = await SeedRestoreAsync("c1", "shard1", "20260911121001Z",
            backupId: "20260910120000Z", state: RestoreStatus.Running, startedUnix: 1);
        (await _claims.TryClaimClusterAsync("c1", ct)).Value.Should().BeTrue();

        // Act
        (await process.TickAsync(BuildSnap(), await BackupsFromEtcdAsync("c1"), ct)).IsSuccess.Should().BeTrue();

        // Assert — REJOINING + lsn; контейнер прибран; volume не тронут
        var restored = (await ReadRestoresAsync("c1", "shard1")).Single(r => r.Id == op.Id);
        restored.State.Should().Be(RestoreStatus.Rejoining);
        restored.RestoredToLsn.Should().Be("0/42");
        engine.Removed.Should().Contain(name);
        engine.RemovedVolumes.Should().BeEmpty();
    }

    [Fact]
    public async Task Джоб_exit1_пишет_FAILED_с_error_и_чистит_контейнер()
    {
        // Arrange — exited 1 с error-JSON
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("c1");
        var engine = new FakeBackupEngine();
        var driver = new TestDriver(new StubScaleDriver(), engine);
        var process = BuildProcess(new FakeBackupS3(), driver);
        var name = BackupNames.RestoreContainerName("c1", "shard1", "20260911121002Z");
        engine.Containers[name] = new FakeBackupEngine.ContainerRec(
            "cnt-restore", "exited", 1, "{\"ok\":false,\"error\":\"boom\"}\n");
        var op = await SeedRestoreAsync("c1", "shard1", "20260911121002Z",
            backupId: "20260910120000Z", state: RestoreStatus.Running, startedUnix: 1);
        (await _claims.TryClaimClusterAsync("c1", ct)).Value.Should().BeTrue();

        // Act
        (await process.TickAsync(BuildSnap(), await BackupsFromEtcdAsync("c1"), ct)).IsSuccess.Should().BeTrue();

        // Assert — FAILED с причиной, контейнер и volume первой ноды чисты
        var failed = (await ReadRestoresAsync("c1", "shard1")).Single(r => r.Id == op.Id);
        failed.State.Should().Be(RestoreStatus.Failed);
        failed.Error.Should().Be("boom");
        engine.Removed.Should().Contain(name);
        engine.RemovedVolumes.Should().Contain("pgw-c1-shard1-shard1a-data");
    }

    [Fact]
    public async Task Тик_с_существующим_джобом_не_повторяет_демонтаж_и_доносит_итог()
    {
        // Arrange — RUNNING + exited-1 джоб: контейнер существует и держит
        // data-volume первой ноды (реальный docker — 409 «volume is in use»).
        // Регрессия Release-гейта t05: повторный демонтаж при живом джобе
        // падал 409 и зацикливал статус в RUNNING, итог джоба не доносился.
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("c1");
        var inner = new StubScaleDriver();
        var engine = new FakeBackupEngine();
        var driver = new TestDriver(inner, engine);
        var process = BuildProcess(new FakeBackupS3(), driver);
        var name = BackupNames.RestoreContainerName("c1", "shard1", "20260911121002Z");
        engine.Containers[name] = new FakeBackupEngine.ContainerRec(
            "cnt-restore", "exited", 1, "{\"ok\":false,\"error\":\"boom\"}\n");
        var op = await SeedRestoreAsync("c1", "shard1", "20260911121002Z",
            backupId: "20260910120000Z", state: RestoreStatus.Running, startedUnix: 1);
        (await _claims.TryClaimClusterAsync("c1", ct)).Value.Should().BeTrue();

        // Act
        (await process.TickAsync(BuildSnap(), await BackupsFromEtcdAsync("c1"), ct)).IsSuccess.Should().BeTrue();

        // Assert — демонтаж НЕ повторялся (контейнер существует ⇒ уже разобран),
        // итог джоба доведён до статуса, контейнер и volume прибраны
        inner.RemovedNodes.Should().BeEmpty("демонтаж не повторяется при существующем джобе");
        var failed = (await ReadRestoresAsync("c1", "shard1")).Single(r => r.Id == op.Id);
        failed.State.Should().Be(RestoreStatus.Failed);
        failed.Error.Should().Be("boom");
        engine.Removed.Should().Contain(name);
        engine.RemovedVolumes.Should().Contain("pgw-c1-shard1-shard1a-data");
    }

    [Fact]
    public async Task Джоб_сверх_бюджета_докилл_и_FAILED()
    {
        // Arrange — running-контейнер; started_unix глубже бюджета (1800+60)
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("c1");
        var engine = new FakeBackupEngine();
        var driver = new TestDriver(new StubScaleDriver(), engine);
        var process = BuildProcess(new FakeBackupS3(), driver);
        var name = BackupNames.RestoreContainerName("c1", "shard1", "20260911121003Z");
        engine.Containers[name] = new FakeBackupEngine.ContainerRec(
            "cnt-restore", "running", -1, "{\"phase\":\"recovering\"}\n");
        var op = await SeedRestoreAsync("c1", "shard1", "20260911121003Z",
            backupId: "20260910120000Z", state: RestoreStatus.Running,
            startedUnix: TimeProvider.System.GetUtcNow().ToUnixTimeSeconds() - 2000);
        (await _claims.TryClaimClusterAsync("c1", ct)).Value.Should().BeTrue();

        // Act
        (await process.TickAsync(BuildSnap(), await BackupsFromEtcdAsync("c1"), ct)).IsSuccess.Should().BeTrue();

        // Assert — докилл force + FAILED «бюджет»
        engine.Removed.Should().Contain(name);
        var failed = (await ReadRestoresAsync("c1", "shard1")).Single(r => r.Id == op.Id);
        failed.State.Should().Be(RestoreStatus.Failed);
        failed.Error.Should().Contain("бюджет");
    }

    [Fact]
    public async Task RUNNING_без_контейнера_перезапуск_идемпотентен()
    {
        // Arrange — RUNNING, джоба нет (после рестарта docker-хоста)
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("c1");
        var engine = new FakeBackupEngine();
        var driver = new TestDriver(new StubScaleDriver(), engine);
        var process = BuildProcess(new FakeBackupS3(), driver);
        var op = await SeedRestoreAsync("c1", "shard1", "20260911121004Z",
            backupId: "20260910120000Z", state: RestoreStatus.Running, startedUnix: 1);
        (await _claims.TryClaimClusterAsync("c1", ct)).Value.Should().BeTrue();

        // Act — два тика подряд
        (await process.TickAsync(BuildSnap(), await BackupsFromEtcdAsync("c1"), ct)).IsSuccess.Should().BeTrue();
        (await process.TickAsync(BuildSnap(), await BackupsFromEtcdAsync("c1"), ct)).IsSuccess.Should().BeTrue();

        // Assert — джоб создан и запущен; второй тик идемпотентен (created →
        // довыгон start, без нового create); env: source-префикс, пустая цель
        // (latest), volume первой ноды
        var (name, spec) = engine.Created.Should().ContainSingle().Subject;
        name.Should().Be(BackupNames.RestoreContainerName("c1", "shard1", op.Id));
        engine.Started.Count(s => s == name).Should().BeGreaterThanOrEqualTo(1);
        spec.Env["SRC_PREFIX"].Should().Be("c1/shard1");
        spec.Env["TARGET_TIME"].Should().Be("");
        spec.VolumeName.Should().Be("pgw-c1-shard1-shard1a-data");
        // BACKUP_ID — резолвнутый полный (op.BackupId), не id заявки:
        // джоб качает full/<backup_id>/ (регрессия E2E-гейта t05)
        spec.Env["BACKUP_ID"].Should().Be("20260910120000Z").And.NotBe(op.Id);
    }

    [Fact]
    public async Task Target_time_нормализуется_к_формату_PG()
    {
        // Arrange — RUNNING с target time:<RFC3339>: парсер recovery_target_time
        // PG не принимает «T»-сепаратор и суффикс «Z» (инцидент E2E-гейта t05),
        // в env джоба уходит PG-формат «+00:00»
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("c1");
        var engine = new FakeBackupEngine();
        var driver = new TestDriver(new StubScaleDriver(), engine);
        var process = BuildProcess(new FakeBackupS3(), driver);
        var op = await SeedRestoreAsync("c1", "shard1", "20260911121005Z",
            backupId: "20260910120000Z", target: "time:2026-09-11T10:00:00Z",
            state: RestoreStatus.Running, startedUnix: 1);
        (await _claims.TryClaimClusterAsync("c1", ct)).Value.Should().BeTrue();

        // Act
        (await process.TickAsync(BuildSnap(), await BackupsFromEtcdAsync("c1"), ct)).IsSuccess.Should().BeTrue();

        // Assert
        engine.Created.Should().ContainSingle().Subject.Spec.Env["TARGET_TIME"]
            .Should().Be("2026-09-11 10:00:00+00:00");
    }

    [Fact]
    public async Task Target_time_битое_permanent_FAILED()
    {
        // Arrange — RUNNING с time:не-RFC3339 (ручная запись мимо API)
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("c1");
        var engine = new FakeBackupEngine();
        var driver = new TestDriver(new StubScaleDriver(), engine);
        var process = BuildProcess(new FakeBackupS3(), driver);
        var op = await SeedRestoreAsync("c1", "shard1", "20260911121006Z",
            backupId: "20260910120000Z", target: "time:завтра-утром",
            state: RestoreStatus.Running, startedUnix: 1);
        (await _claims.TryClaimClusterAsync("c1", ct)).Value.Should().BeTrue();

        // Act
        (await process.TickAsync(BuildSnap(), await BackupsFromEtcdAsync("c1"), ct)).IsSuccess.Should().BeTrue();

        // Assert — permanent FAILED, джоб не создаётся
        var failed = (await ReadRestoresAsync("c1", "shard1")).Single(r => r.Id == op.Id);
        failed.State.Should().Be(RestoreStatus.Failed);
        failed.Error.Should().Contain("не RFC3339");
        engine.Created.Should().BeEmpty();
    }

    [Fact]
    public async Task Transient_docker_отказ_статус_не_меняется()
    {
        // Arrange — RUNNING, list docker падает
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("c1");
        var engine = new FakeBackupEngine { ListFails = true };
        var driver = new TestDriver(new StubScaleDriver(), engine);
        var process = BuildProcess(new FakeBackupS3(), driver);
        var op = await SeedRestoreAsync("c1", "shard1", "20260911121005Z",
            backupId: "20260910120000Z", state: RestoreStatus.Running, startedUnix: 1);
        (await _claims.TryClaimClusterAsync("c1", ct)).Value.Should().BeTrue();

        // Act
        var result = await process.TickAsync(BuildSnap(), await BackupsFromEtcdAsync("c1"), ct);

        // Assert — InProgress, статус RUNNING сохранён, журнал-факт docker-unavailable
        result.IsSuccess.Should().BeTrue(result.Error?.ToString());
        result.Value.Should().Be(ProcessOutcome.InProgress);
        (await ReadRestoresAsync("c1", "shard1")).Single(r => r.Id == op.Id)
            .State.Should().Be(RestoreStatus.Running);
        (await fixture.Gateway.GetAsync(fixture.Endpoint, "/pgworker/work/c1", ct)).Value!.Value
            .Should().Contain("docker-unavailable");
    }

    // ── REJOINING: ensure нод + пробы + COMPLETED ──

    [Fact]
    public async Task Rejoin_ensure_первой_ноды_и_ожидание_пробы()
    {
        // Arrange — REJOINING после успешного джоба; Patroni отвечает «start»
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("c1");
        var inner = new StubScaleDriver();
        var driver = new TestDriver(inner, new FakeBackupEngine());
        var patroni = new PatroniHandler { Ready = false };
        var process = BuildProcess(new FakeBackupS3(), driver, patroni: patroni);
        var op = await SeedRestoreAsync("c1", "shard1", "20260911122000Z",
            backupId: "20260910120000Z", state: RestoreStatus.Rejoining);
        (await _claims.TryClaimClusterAsync("c1", ct)).Value.Should().BeTrue();

        // Act
        var result = await process.TickAsync(BuildSnap(), await BackupsFromEtcdAsync("c1"), ct);

        // Assert — первая нода ensured, статус REJOINING (InProgress), реплика ещё не поднималась
        result.IsSuccess.Should().BeTrue(result.Error?.ToString());
        result.Value.Should().Be(ProcessOutcome.InProgress);
        inner.EnsuredNodes.Should().Contain("shard1/shard1a");
        inner.EnsuredNodes.Should().NotContain("shard1/shard1b");
        (await ReadRestoresAsync("c1", "shard1")).Single(r => r.Id == op.Id)
            .State.Should().Be(RestoreStatus.Rejoining);
    }

    [Fact]
    public async Task Rejoin_все_ноды_running_статусы_RUNNING_и_COMPLETED_wal_удалён()
    {
        // Arrange — REJOINING; Patroni: обе ноды running; wal-ключ и scope живы
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("c1");
        await SeedTwoNodeAllocAsync();
        await fixture.Gateway.PutAsync(fixture.Endpoint, "/pgworker/backups/c1/shard1/wal",
            """{"state":"ACTIVE","slot":"s","master_node":"shard1a","chain_start_segment":"000000010000000000000002","last_received_segment":"000000010000000000000004","last_uploaded_segment":"000000010000000000000004","last_uploaded_unix":1}""",
            null, ct);
        var inner = new StubScaleDriver();
        var driver = new TestDriver(inner, new FakeBackupEngine());
        var process = BuildProcess(new FakeBackupS3(), driver,
            patroni: new PatroniHandler { Ready = true });
        var op = await SeedRestoreAsync("c1", "shard1", "20260911122001Z",
            backupId: "20260910120000Z", state: RestoreStatus.Rejoining);
        op = op with { RestoredToLsn = "0/42" };
        await fixture.Gateway.PutAsync(fixture.Endpoint,
            BackupNames.RestoreKey("c1", "shard1", op.Id), RestoreStatusJson.Serialize(op), null, ct);
        (await _claims.TryClaimClusterAsync("c1", ct)).Value.Should().BeTrue();

        // Act
        (await process.TickAsync(BuildTwoNodeSnap(), await BackupsFromEtcdAsync("c1"), ct)).IsSuccess.Should().BeTrue();

        // Assert — ноды RUNNING, заявка COMPLETED (finished+lsn), wal-ключ удалён (AC4)
        inner.EnsuredNodes.Should().BeEquivalentTo(["shard1/shard1a", "shard1/shard1b"]);
        (await fixture.Gateway.GetAsync(fixture.Endpoint, "/clusters/c1/shards/shard1/nodes/shard1a/state", ct))
            .Value!.Value.Should().Be("RUNNING");
        (await fixture.Gateway.GetAsync(fixture.Endpoint, "/clusters/c1/shards/shard1/nodes/shard1b/state", ct))
            .Value!.Value.Should().Be("RUNNING");
        var completed = (await ReadRestoresAsync("c1", "shard1")).Single(r => r.Id == op.Id);
        completed.State.Should().Be(RestoreStatus.Completed);
        completed.FinishedUnix.Should().BeGreaterThan(0);
        completed.RestoredToLsn.Should().Be("0/42");
        (await fixture.Gateway.GetAsync(fixture.Endpoint, "/pgworker/backups/c1/shard1/wal", ct))
            .Value.Should().BeNull("сброс цепочки — переснятие полного планировщиком (AC4)");
        (await fixture.Gateway.GetAsync(fixture.Endpoint, "/pgworker/work/c1", ct)).Value!.Value
            .Should().Contain("done/shard1/");
    }

    [Fact]
    public async Task Rejoin_сверх_PatroniBootSec_permanent_FAILED()
    {
        // Arrange — часы фиксированные: первый тик фиксирует since, второй
        // (после +700 c > 600 бюджет) — бюджет исчерпан
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("c1");
        var clock = new MutableClock();
        var inner = new StubScaleDriver();
        var driver = new TestDriver(inner, new FakeBackupEngine());
        var process = BuildProcess(new FakeBackupS3(), driver, clock: clock,
            patroni: new PatroniHandler { Ready = false });
        var op = await SeedRestoreAsync("c1", "shard1", "20260911122002Z",
            backupId: "20260910120000Z", state: RestoreStatus.Rejoining);
        (await _claims.TryClaimClusterAsync("c1", ct)).Value.Should().BeTrue();

        // Act — тик 1: ожидание (since зафиксирован); тик 2 после бюджета
        (await process.TickAsync(BuildSnap(), await BackupsFromEtcdAsync("c1"), ct))
            .Value.Should().Be(ProcessOutcome.InProgress);
        clock.Now = clock.Now.AddSeconds(700);
        (await process.TickAsync(BuildSnap(), await BackupsFromEtcdAsync("c1"), ct)).IsSuccess.Should().BeTrue();

        // Assert — permanent FAILED «Patroni не поднялся»
        var failed = (await ReadRestoresAsync("c1", "shard1")).Single(r => r.Id == op.Id);
        failed.State.Should().Be(RestoreStatus.Failed);
        failed.Error.Should().Contain("Patroni не поднялся");
    }

    [Fact]
    public async Task Takeover_новый_инстанс_продолжает_по_статусу()
    {
        // Arrange — инстанс A начинает rejoin (probe не готов), «умирает»;
        // инстанс B (свежий, без in-memory) видит REJOINING в etcd
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("c1");
        await SeedTwoNodeAllocAsync();
        var innerA = new StubScaleDriver();
        var processA = BuildProcess(new FakeBackupS3(), new TestDriver(innerA, new FakeBackupEngine()),
            patroni: new PatroniHandler { Ready = false });
        await SeedRestoreAsync("c1", "shard1", "20260911122003Z",
            backupId: "20260910120000Z", state: RestoreStatus.Rejoining);
        (await _claims.TryClaimClusterAsync("c1", ct)).Value.Should().BeTrue();
        (await processA.TickAsync(BuildTwoNodeSnap(), await BackupsFromEtcdAsync("c1"), ct))
            .Value.Should().Be(ProcessOutcome.InProgress);

        // Act — инстанс B продолжает: probe готов → доводит до COMPLETED
        var innerB = new StubScaleDriver();
        var processB = BuildProcess(new FakeBackupS3(), new TestDriver(innerB, new FakeBackupEngine()),
            patroni: new PatroniHandler { Ready = true });
        (await processB.TickAsync(BuildTwoNodeSnap(), await BackupsFromEtcdAsync("c1"), ct)).IsSuccess.Should().BeTrue();

        // Assert — продолжение с REJOINING (ensure-вызовы были, статус финальный)
        innerB.EnsuredNodes.Should().NotBeEmpty("инстанс B продолжает ensure по etcd-статусу");
        (await ReadRestoresAsync("c1", "shard1")).Single()
            .State.Should().Be(RestoreStatus.Completed);
        (await fixture.Gateway.GetAsync(fixture.Endpoint, "/pgworker/backups/c1/shard1/wal", ct))
            .Value.Should().BeNull();
    }

    // ── Каркас тика ──

    [Fact]
    public async Task Тик_без_клэйма_и_без_активных_заявок_молчит()
    {
        // Arrange — клэйма нет, заявок нет
        await SeedAsync("c1");
        var process = BuildProcess(new FakeBackupS3(), new StubScaleDriver());

        // Act — тик без клэйма
        var refused = await process.TickAsync(BuildSnap(), [], TestContext.Current.CancellationToken);

        // Assert — отказ клэйм-гварда
        refused.IsSuccess.Should().BeFalse("мутации без клэйма запрещены");
        refused.Error!.Message.Should().Contain("клэйм не наш");

        // Act — клэйм есть, заявок нет
        (await _claims.TryClaimClusterAsync("c1", TestContext.Current.CancellationToken)).Value.Should().BeTrue();
        var done = await process.TickAsync(BuildSnap(), [], TestContext.Current.CancellationToken);

        // Assert — Done без мутаций
        done.IsSuccess.Should().BeTrue();
        done.Value.Should().Be(ProcessOutcome.Done);
    }

    [Fact]
    public async Task Тик_шард_убран_из_декларации_заявка_не_исполняется()
    {
        // Arrange — заявка на shard9, которого в снапшоте нет
        await SeedAsync("c1");
        var s3 = new FakeBackupS3();
        var process = BuildProcess(s3, new StubScaleDriver());
        var op = await SeedRestoreAsync("c1", "shard9", "20260911120006Z");
        (await _claims.TryClaimClusterAsync("c1", TestContext.Current.CancellationToken)).Value.Should().BeTrue();

        // Act
        var result = await process.TickAsync(BuildSnap(),
            await BackupsFromEtcdAsync("c1"), TestContext.Current.CancellationToken);

        // Assert — Done, статус не изменился (закроет remove-shard ветка)
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be(ProcessOutcome.Done);
        (await ReadRestoresAsync("c1", "shard9")).Single(r => r.Id == op.Id)
            .State.Should().Be(RestoreStatus.Planned);
    }
}
