using System.Text.Json;
using FluentAssertions;
using S3Worker.Core.Model;
using S3Worker.Docker.Drivers;
using S3Worker.Provisioning;
using S3Worker.Provisioning.Processes;
using Shared.Etcd.Client;
using Xunit;

namespace S3Worker.UnitTests.Provisioning;

// ProvisioningProcess P0–P5 (arch/24 §5 A): клэйм/journal, перечитка config
// (гонка TO_REMOVE), placement+portalloc, ensure/health/bucket/endpoints/config.
public class ProvisioningProcessTests
{
    private const string Prefix = "/s3worker";
    private const string Profile = "backups-main";
    private const string Bucket = "pgworker-backups";

    private static readonly IReadOnlyDictionary<string, string?> FullProfileEnv = new Dictionary<string, string?>
    {
        [$"S3W_NODE_SECRETS__{Profile}__ROOT_USER"] = "root",
        [$"S3W_NODE_SECRETS__{Profile}__ROOT_PASSWORD"] = "pw",
        [$"S3W_NODE_SECRETS__{Profile}__ACCESS_KEYS__0__ACCESSKEY"] = "ak",
        [$"S3W_NODE_SECRETS__{Profile}__ACCESS_KEYS__0__SECRETKEY"] = "sk",
        [$"S3W_NODE_SECRETS__{Profile}__ACCESS_KEYS__0__POLICY"] = "read-write",
    };

    private sealed class Rig
    {
        public Fakes.FakeEtcd Etcd = new();
        public Fakes.FakeS3Driver Driver = new();
        public Fakes.FakeNodeClient NodeClient = new();
        public FixedTimeProvider Clock = new();
        public SecretProfileStore Secrets = new(
            name => name is not null && FullProfileEnv.TryGetValue(name, out var v) ? v : null,
            FullProfileEnv.Keys);
        public ClaimStore Claims = null!;
        public BootWaitBudget BootWait = null!;
        public ProvisioningProcess Process = null!;

        public static Rig New(int portFrom = 18000, string? advertised = null)
        {
            var rig = new Rig();
            rig.Claims = new ClaimStore(Prefix, ["http://etcd:2379"], rig.Etcd, TimeProvider.System);
            rig.Driver.Hosts =
            [
                new S3HostAttributes("h1", 0, new Dictionary<string, string>()),
                new S3HostAttributes("h2", 1, new Dictionary<string, string>()),
            ];
            rig.BootWait = new BootWaitBudget(rig.Clock);
            rig.Process = new ProvisioningProcess(
                rig.Etcd, ["http://etcd:2379"], rig.Driver, rig.Claims,
                new WorkJournal(Prefix, rig.Etcd, ["http://etcd:2379"]),
                new PortAllocLock(Prefix, ["http://etcd:2379"], rig.Etcd, TimeProvider.System, "inst-1"),
                new PortAllocIndex(rig.Etcd, ["http://etcd:2379"],
                    Microsoft.Extensions.Logging.Abstractions.NullLogger<PortAllocIndex>.Instance),
                rig.Secrets, NewPki(rig), new InstanceEnvComposer(),
                NewMesh(rig), rig.NodeClient,
                new S3ProvisioningOptions(portFrom, 18999, 100, 90, 15, advertised, "owns3:dev", Bucket),
                rig.BootWait);
            return rig;
        }
    }

    // PKI-стор и процесс G на тех же фейках (rig-зависимые).
    private static PkiStore NewPki(Rig rig)
        => new(rig.Etcd, ["http://etcd:2379"], TimeProvider.System);

    private static MeshGovernanceProcess NewMesh(Rig rig)
    {
        var meter = new System.Diagnostics.Metrics.Meter("S3Worker-tests");
        return new MeshGovernanceProcess(
            rig.Etcd, ["http://etcd:2379"], rig.Claims,
            new WorkJournal(Prefix, rig.Etcd, ["http://etcd:2379"]),
            rig.Secrets, NewPki(rig), rig.NodeClient,
            new S3ProvisioningOptions(18000, 18999, 100, 90, 15, null, "owns3:dev", Bucket),
            meter);
    }

    // Заявка: NOT_INITIALIZED, 2 ноды, plain-селекторы h1/h2, ресурсы.
    private static S3ClusterSnapshot Snap(
        string cluster = "demo", string state = S3NodeStates.NotInitialized,
        string? nodeState = null) => new(
        cluster,
        new S3ClusterConfig("owns3", 2, null, 1760156800, state),
        [
            new S3NodeSnapshot("node1", nodeState, null,
                new S3NodePlacement("h1", null, null),
                new S3NodeResources("1", "1Gi", "5Gi"), null, null),
            new S3NodeSnapshot("node2", nodeState, null,
                new S3NodePlacement("h2", null, null),
                new S3NodeResources("1", "1Gi", "5Gi"), null, null),
        ],
        null, null);

    // AAA: P2 — PKI-выпуск (pki/ca, pki/orchestrator, nodes/<I>/pki в FakeEtcd)
    // и env контейнера с репликационным материалом (spec §3.6, §8.1.3–8.1.4).
    [Fact]
    public async Task P2_PkiKeysInEtcd_EnvWithReplication()
    {
        // Arrange: заявка N=2, здоровье обеих нод живое.
        var rig = Rig.New();
        await SeedDeclarationAsync(rig.Etcd, "demo", S3NodeStates.NotInitialized);
        await rig.Claims.TryClaimClusterAsync("demo", TestContext.Current.CancellationToken);
        rig.NodeClient.Health = true;

        // Act
        var result = await rig.Process.RunAsync(Snap(), TestContext.Current.CancellationToken);

        // Assert: ключи PKI в etcd; env обеспеченной ноды несёт OWNS3_REPLICATION_*
        // (значениями, не путями).
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Etcd.Store.Keys.Should().Contain("/s3/clusters/demo/pki/ca");
        rig.Etcd.Store.Keys.Should().Contain("/s3/clusters/demo/pki/orchestrator");
        rig.Etcd.Store.Keys.Should().Contain("/s3/clusters/demo/nodes/node1/pki");
        rig.Etcd.Store.Keys.Should().Contain("/s3/clusters/demo/nodes/node2/pki");
        var env = rig.Driver.Ensured.Should().NotBeEmpty().And.Subject.First().Env;
        env["OWNS3_REPLICATION_CERT"].Should().NotBeEmpty();
        env["OWNS3_REPLICATION_PRIVATE_KEY"].Should().Contain("BEGIN PRIVATE KEY");
        env["OWNS3_REPLICATION_TRUST_CA"].Should().Contain("publicKeyPem");
        env["OWNS3_REPLICATION_ADVERTISED_ENDPOINT"].Should().StartWith("http://").And.Contain(":");
    }

    // AAA: повторный provisioning — PKI-материал тот же (идемпотентность if-absent).
    [Fact]
    public async Task RepeatProvisioning_PkiMaterialUnchanged()
    {
        // Arrange: HappyPath-тиком pki выпущен.
        var rig = Rig.New();
        await SeedDeclarationAsync(rig.Etcd, "demo", S3NodeStates.NotInitialized);
        await rig.Claims.TryClaimClusterAsync("demo", TestContext.Current.CancellationToken);
        await rig.Process.RunAsync(Snap(), TestContext.Current.CancellationToken);
        var caBefore = rig.Etcd.Store["/s3/clusters/demo/pki/ca"].Value;
        var nodePkiBefore = rig.Etcd.Store["/s3/clusters/demo/nodes/node1/pki"].Value;

        // Act: повторный тик (заявка снова NOT_INITIALIZED — эмуляция).
        await SeedDeclarationAsync(rig.Etcd, "demo", S3NodeStates.NotInitialized);
        var second = await rig.Process.RunAsync(Snap(), TestContext.Current.CancellationToken);

        // Assert: материал не перевыпущен (значения идентичны).
        second.IsSuccess.Should().BeTrue(second.Error?.Message);
        rig.Etcd.Store["/s3/clusters/demo/pki/ca"].Value.Should().Be(caBefore);
        rig.Etcd.Store["/s3/clusters/demo/nodes/node1/pki"].Value.Should().Be(nodePkiBefore);
    }

    // AAA: 0-инстансовый provisioning — P1–P4 без docker-мутаций, endpoints НЕ
    // пишется, config nodes=0 без state (arch/23 §2, arch/24 §5 A).
    [Fact]
    public async Task ProvisioningZeroInstances_EndpointsNotWritten()
    {
        // Arrange: заявка с nodes=0 и без инстансных ключей.
        var rig = Rig.New();
        rig.Etcd.Seed("/s3/clusters/empty/config",
            "{\"platform\":\"owns3\",\"nodes\":0,\"secret_profile\":\"" + Profile + "\",\"created_unix\":1760156800,\"state\":\"NOT_INITIALIZED\"}");
        await rig.Claims.TryClaimClusterAsync("empty", TestContext.Current.CancellationToken);
        var snap = new S3ClusterSnapshot("empty",
            new S3ClusterConfig("owns3", 0, null, 1760156800, S3NodeStates.NotInitialized),
            [], null);

        // Act
        var result = await rig.Process.RunAsync(snap, TestContext.Current.CancellationToken);

        // Assert: Active без docker-мутаций; endpoints отсутствует; config без state.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Driver.Ensured.Should().BeEmpty();
        rig.Etcd.Store.Keys.Should().NotContain("/s3/clusters/empty/endpoints");
        var config = rig.Etcd.Store["/s3/clusters/empty/config"].Value;
        config.Should().Contain("\"nodes\":0").And.NotContain("state");
    }

    private static async Task SeedDeclarationAsync(Fakes.FakeEtcd etcd, string cluster, string state)
    {
        etcd.Seed($"/s3/clusters/{cluster}/config",
            $$"""{"platform":"owns3","nodes":2,"secret_profile":"{{Profile}}","created_unix":1760156800,"state":"{{state}}"}""");
        etcd.Seed($"/s3/clusters/{cluster}/nodes/node1/placement", """{"docker_host":"h1"}""");
        etcd.Seed($"/s3/clusters/{cluster}/nodes/node2/placement", """{"docker_host":"h2"}""");
        etcd.Seed($"/s3/clusters/{cluster}/nodes/node1/resources", """{"cpu":"1","mem":"1Gi","disk":"5Gi"}""");
        etcd.Seed($"/s3/clusters/{cluster}/nodes/node2/resources", """{"cpu":"1","mem":"1Gi","disk":"5Gi"}""");
        await Task.CompletedTask;
    }

    // AAA: P0 — клэйм не взяли → никаких записей.
    [Fact]
    public async Task P0_NoClaim_NoWrites()
    {
        // Arrange: процесс без клэйма кластера.
        var rig = Rig.New();

        // Act
        var result = await rig.Process.RunAsync(Snap(), TestContext.Current.CancellationToken);

        // Assert: Failed-грань, journal/work и docker не тронуты.
        result.IsSuccess.Should().BeFalse();
        rig.Etcd.Store.Keys.Should().NotContain("/s3worker/work/demo");
        rig.Driver.Ensured.Should().BeEmpty();
    }

    // AAA: P0 — клэйм взяли → journal started.
    [Fact]
    public async Task P0_WithClaim_JournalStarted()
    {
        // Arrange: клэйм захвачен заранее.
        var rig = Rig.New();
        await SeedDeclarationAsync(rig.Etcd, "demo", S3NodeStates.NotInitialized);
        await rig.Claims.TryClaimClusterAsync("demo", TestContext.Current.CancellationToken);

        // Act
        await rig.Process.RunAsync(Snap(), TestContext.Current.CancellationToken);

        // Assert: journal started есть (happy-путь доходит до done, started — первый).
        var journal = await new WorkJournal(Prefix, rig.Etcd, ["http://etcd:2379"])
            .ReadAsync("demo", TestContext.Current.CancellationToken);
        journal.Value!.Op.Should().Be("provision");
        journal.Value.Phase.Should().Be("done");
    }

    // AAA: гонка TO_REMOVE посреди — перечитка config → безопасная остановка
    // без docker-мутаций.
    [Fact]
    public async Task ToRemoveRace_SafeShutdown()
    {
        // Arrange: клэйм + config уже TO_REMOVE.
        var rig = Rig.New();
        await SeedDeclarationAsync(rig.Etcd, "demo", S3NodeStates.ToRemove);
        await rig.Claims.TryClaimClusterAsync("demo", TestContext.Current.CancellationToken);

        // Act
        var result = await rig.Process.RunAsync(Snap(state: S3NodeStates.ToRemove), TestContext.Current.CancellationToken);

        // Assert: journal aborted-state-changed; docker не тронут.
        result.IsSuccess.Should().BeTrue();
        var journal = await new WorkJournal(Prefix, rig.Etcd, ["http://etcd:2379"])
            .ReadAsync("demo", TestContext.Current.CancellationToken);
        journal.Value!.Phase.Should().Be("aborted-state-changed");
        rig.Driver.Ensured.Should().BeEmpty();
    }

    // AAA: P1 — ноль кандидатов placement → transient Failed, journal с пустым
    // перечнем кандидатов.
    [Fact]
    public async Task P1_ZeroCandidates_TransientFailed()
    {
        // Arrange: клэйм; docker-хостов нет (пустой список).
        var rig = Rig.New();
        await SeedDeclarationAsync(rig.Etcd, "demo", S3NodeStates.NotInitialized);
        await rig.Claims.TryClaimClusterAsync("demo", TestContext.Current.CancellationToken);
        rig.Driver.Hosts = [];

        // Act
        var result = await rig.Process.RunAsync(Snap(), TestContext.Current.CancellationToken);

        // Assert: Failed, фаза planning; docker-мутаций нет.
        result.IsSuccess.Should().BeFalse();
        var journal = await new WorkJournal(Prefix, rig.Etcd, ["http://etcd:2379"])
            .ReadAsync("demo", TestContext.Current.CancellationToken);
        journal.Value!.Phase.Should().Be("planning");
        journal.Value.LastError.Should().Contain("zero candidates");
        rig.Driver.Ensured.Should().BeEmpty();
    }

    // AAA: P1 — порт-клэйм занят → journal waiting-portalloc-lock, без аллокаций.
    [Fact]
    public async Task P1_PortallocLockTaken_Waiting()
    {
        // Arrange: чужой держатель portalloc-клэйма.
        var rig = Rig.New();
        await SeedDeclarationAsync(rig.Etcd, "demo", S3NodeStates.NotInitialized);
        await rig.Claims.TryClaimClusterAsync("demo", TestContext.Current.CancellationToken);
        rig.Etcd.Seed($"{Prefix}/locks/portalloc",
            """{"instance":"other"}""");

        // Act
        var result = await rig.Process.RunAsync(Snap(), TestContext.Current.CancellationToken);

        // Assert: InProgress-грань (journal waiting-portalloc-lock), docker чист.
        result.IsSuccess.Should().BeTrue();
        var journal = await new WorkJournal(Prefix, rig.Etcd, ["http://etcd:2379"])
            .ReadAsync("demo", TestContext.Current.CancellationToken);
        journal.Value!.Phase.Should().Be("waiting-portalloc-lock");
        rig.Driver.Ensured.Should().BeEmpty();
    }

    // AAA: P1 — неполный профиль → permanent Failed c диагностикой без значений.
    [Fact]
    public async Task P1_IncompleteProfile_FailedWithoutValues()
    {
        // Arrange: профиль без ROOT_PASSWORD.
        var env = new Dictionary<string, string?>
        {
            [$"S3W_NODE_SECRETS__{Profile}__ROOT_USER"] = "root",
        };
        var rig = Rig.New();
        rig.Secrets = new SecretProfileStore(
            name => name is not null && env.TryGetValue(name, out var v) ? v : null, env.Keys);
        rig.Process = new ProvisioningProcess(
            rig.Etcd, ["http://etcd:2379"], rig.Driver, rig.Claims,
            new WorkJournal(Prefix, rig.Etcd, ["http://etcd:2379"]),
            new PortAllocLock(Prefix, ["http://etcd:2379"], rig.Etcd, TimeProvider.System, "inst-1"),
            new PortAllocIndex(rig.Etcd, ["http://etcd:2379"],
                Microsoft.Extensions.Logging.Abstractions.NullLogger<PortAllocIndex>.Instance),
            rig.Secrets, NewPki(rig), new InstanceEnvComposer(),
            NewMesh(rig), rig.NodeClient,
            new S3ProvisioningOptions(18000, 18999, 100, 90, 15, null, "owns3:dev", Bucket),
            rig.BootWait);
        await SeedDeclarationAsync(rig.Etcd, "demo", S3NodeStates.NotInitialized);
        await rig.Claims.TryClaimClusterAsync("demo", TestContext.Current.CancellationToken);

        // Act
        var result = await rig.Process.RunAsync(Snap(), TestContext.Current.CancellationToken);

        // Assert: Failed, диагностика «неполон», значений нет; docker чист.
        result.IsSuccess.Should().BeFalse();
        var journal = await new WorkJournal(Prefix, rig.Etcd, ["http://etcd:2379"])
            .ReadAsync("demo", TestContext.Current.CancellationToken);
        journal.Value!.Phase.Should().Be("invalid-secret-profile");
        journal.Value.LastError.Should().Contain("incomplete").And.NotContain("pw");
        rig.Driver.Ensured.Should().BeEmpty();
    }

    // AAA: happy-путь P1–P5: plan → ensure → health → bucket → endpoints/config.
    [Fact]
    public async Task HappyPath_RequestToActive()
    {
        // Arrange: клэйм, живой health, 2 хоста.
        var rig = Rig.New(advertised: "host.docker.internal");
        await SeedDeclarationAsync(rig.Etcd, "demo", S3NodeStates.NotInitialized);
        await rig.Claims.TryClaimClusterAsync("demo", TestContext.Current.CancellationToken);
        rig.Driver.Hosts =
        [
            new S3HostAttributes("h1", 0, new Dictionary<string, string>()),
            new S3HostAttributes("h2", 1, new Dictionary<string, string>()),
        ];

        // Act
        var result = await rig.Process.RunAsync(Snap(), TestContext.Current.CancellationToken);

        // Assert — итог: config без state (канон Active), endpoints, portalloc,
        // placed_on, states RUNNING, 2 ноды с volume-спекой, бакеты созданы.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        var config = rig.Etcd.Store[$"/s3/clusters/demo/config"].Value;
        config.Should().Be("""{"platform":"owns3","nodes":2,"secret_profile":"backups-main","created_unix":1760156800}""");
        rig.Etcd.Store[$"/s3/clusters/demo/endpoints"].Value
            .Should().Be("host.docker.internal:18000,host.docker.internal:18001");
        var portalloc = S3ProcessCommon.ParsePortAlloc(rig.Etcd.Store[$"{Prefix}/portalloc/demo"].Value);
        portalloc.Should().ContainKey("node1").WhoseValue.Host.Should().Be("h1");
        portalloc.Should().ContainKey("node2").WhoseValue.Host.Should().Be("h2");
        rig.Etcd.Store[$"/s3/clusters/demo/nodes/node1/placed_on"].Value.Should().Be("h1");
        rig.Etcd.Store[$"/s3/clusters/demo/nodes/node1/state"].Value.Should().Be(S3NodeStates.Running);
        rig.Etcd.Store[$"/s3/clusters/demo/nodes/node2/state"].Value.Should().Be(S3NodeStates.Running);
        rig.Driver.Ensured.Should().HaveCount(2);
        var spec = rig.Driver.Ensured[0];
        spec.NodeName.Should().Be("node1");
        spec.Image.Should().Be("owns3:dev");
        spec.Env!["OWNS3_ROOT_USER"].Should().Be("root");
        spec.Env["OWNS3_DATA_DIR"].Should().Be("/data");
        spec.CpuCores.Should().Be(1m);
        spec.MemoryBytes.Should().Be(1024L * 1024 * 1024);
        rig.NodeClient.Buckets.Should().HaveCount(2);
        var journal = await new WorkJournal(Prefix, rig.Etcd, ["http://etcd:2379"])
            .ReadAsync("demo", TestContext.Current.CancellationToken);
        journal.Value!.Phase.Should().Be("done");
    }

    // AAA: идемпотентность повторного прогона — порты стабильны.
    [Fact]
    public async Task RepeatRun_PortsStable()
    {
        // Arrange: первый прогон прошёл (active), второй — тот же процесс.
        var rig = Rig.New(advertised: "host.docker.internal");
        await SeedDeclarationAsync(rig.Etcd, "demo", S3NodeStates.NotInitialized);
        await rig.Claims.TryClaimClusterAsync("demo", TestContext.Current.CancellationToken);
        rig.Driver.Hosts =
        [
            new S3HostAttributes("h1", 0, new Dictionary<string, string>()),
            new S3HostAttributes("h2", 1, new Dictionary<string, string>()),
        ];
        var first = await rig.Process.RunAsync(Snap(), TestContext.Current.CancellationToken);
        first.IsSuccess.Should().BeTrue();
        var portallocBefore = rig.Etcd.Store[$"{Prefix}/portalloc/demo"].Value;
        var modRevisionBefore = rig.Etcd.Store[$"/s3/clusters/demo/config"].ModRevision;

        // Act — второй прогон (ноды уже RUNNING, config уже канонический).
        var second = await rig.Process.RunAsync(
            Snap(nodeState: S3NodeStates.Running), TestContext.Current.CancellationToken);

        // Assert: Success; portalloc и config не переписаны (mod_revision тот же).
        second.IsSuccess.Should().BeTrue(second.Error?.Message);
        rig.Etcd.Store[$"{Prefix}/portalloc/demo"].Value.Should().Be(portallocBefore);
        rig.Etcd.Store[$"/s3/clusters/demo/config"].ModRevision.Should().Be(modRevisionBefore);
    }

    // AAA: P2 — health не отвечает в бюджет → InProgress (waiting-nodes),
    // state=PROVISIONING записан, docker-нода создана.
    [Fact]
    public async Task HealthSilent_InProgressWithoutBlocking()
    {
        // Arrange: клэйм; health всегда false.
        var rig = Rig.New();
        await SeedDeclarationAsync(rig.Etcd, "demo", S3NodeStates.NotInitialized);
        await rig.Claims.TryClaimClusterAsync("demo", TestContext.Current.CancellationToken);
        rig.Driver.Hosts =
        [
            new S3HostAttributes("h1", 0, new Dictionary<string, string>()),
            new S3HostAttributes("h2", 1, new Dictionary<string, string>()),
        ];
        rig.NodeClient.Health = false;

        // Act
        var result = await rig.Process.RunAsync(Snap(), TestContext.Current.CancellationToken);

        // Assert: Success-грань процесса (InProgress-семантика journal), нода
        // создана, state=PROVISIONING, endpoints/config не пишутся.
        result.IsSuccess.Should().BeTrue();
        var journal = await new WorkJournal(Prefix, rig.Etcd, ["http://etcd:2379"])
            .ReadAsync("demo", TestContext.Current.CancellationToken);
        journal.Value!.Phase.Should().Be("waiting-nodes");
        rig.Etcd.Store[$"/s3/clusters/demo/nodes/node1/state"].Value.Should().Be(S3NodeStates.Provisioning);
        rig.Etcd.Store.Keys.Should().NotContain("/s3/clusters/demo/endpoints");
    }

    // AAA: provisioning → deprovision → повторный provisioning того же имени —
    // бюджет boot-ожидания с нуля (нет мгновенного permanent-фейла по старому
    // since того же процесса воркера).
    [Fact]
    public async Task DeprovisionResetsBootWaitBudget()
    {
        // Arrange: health молчит; общий бюджет у provisioning/deprovision;
        // клэйм и живые docker-хосты.
        var rig = Rig.New();
        var deprovision = new DeprovisioningProcess(
            rig.Etcd, ["http://etcd:2379"], rig.Driver, rig.Claims,
            new WorkJournal(Prefix, rig.Etcd, ["http://etcd:2379"]), rig.BootWait);
        await SeedDeclarationAsync(rig.Etcd, "demo", S3NodeStates.NotInitialized);
        await rig.Claims.TryClaimClusterAsync("demo", TestContext.Current.CancellationToken);
        rig.Driver.Hosts =
        [
            new S3HostAttributes("h1", 0, new Dictionary<string, string>()),
            new S3HostAttributes("h2", 1, new Dictionary<string, string>()),
        ];
        rig.NodeClient.Health = false;

        // Act 1: первый provisioning — InProgress (waiting-nodes), since зафиксирован.
        var first = await rig.Process.RunAsync(Snap(), TestContext.Current.CancellationToken);
        // Время ушло за бюджет: следующий прогон — мгновенный permanent-фейл
        // (проверка предусловия «старый since фейлит»).
        rig.Clock.Utc = rig.Clock.Utc.AddSeconds(101);
        var stale = await rig.Process.RunAsync(Snap(), TestContext.Current.CancellationToken);
        // Act 2: демонтаж той же заявки → done (сброс бюджета, снятие клэйма).
        await SeedDeclarationAsync(rig.Etcd, "demo", S3NodeStates.ToRemove);
        var deprov = await deprovision.TickAsync(
            Snap(state: S3NodeStates.ToRemove), TestContext.Current.CancellationToken);
        // Act 3: то же имя снова NOT_INITIALIZED, клэйм заново, health молчит.
        await SeedDeclarationAsync(rig.Etcd, "demo", S3NodeStates.NotInitialized);
        await rig.Claims.TryClaimClusterAsync("demo", TestContext.Current.CancellationToken);
        var again = await rig.Process.RunAsync(Snap(), TestContext.Current.CancellationToken);

        // Assert: первый — InProgress; «протухший» — permanent-фейл; после
        // демонтажа повторный provisioning — снова InProgress без last_error
        // (бюджет пошёл заново, а не мгновенный фейл).
        first.IsSuccess.Should().BeTrue();
        stale.IsSuccess.Should().BeFalse();
        deprov.IsSuccess.Should().BeTrue(deprov.Error?.Message);
        again.IsSuccess.Should().BeTrue(again.Error?.Message);
        var journal = await new WorkJournal(Prefix, rig.Etcd, ["http://etcd:2379"])
            .ReadAsync("demo", TestContext.Current.CancellationToken);
        journal.Value!.Phase.Should().Be("waiting-nodes");
        journal.Value.LastError.Should().BeNull();
    }
}
