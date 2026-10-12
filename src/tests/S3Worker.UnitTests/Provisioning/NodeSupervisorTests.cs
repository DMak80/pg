using FluentAssertions;
using S3Worker.Core.Model;
using S3Worker.Docker.Drivers;
using S3Worker.Provisioning;
using S3Worker.Provisioning.Processes;
using Xunit;

namespace S3Worker.UnitTests.Provisioning;

// Надзор C (arch/24 §5 C): пересоздание снесённого (S7, одно за тик),
// PROVISIONING → RUNNING по health, UNREACHABLE по NodeDeadSec (без деструктива),
// слепой inspect — никаких действий, usage-сбор (snake_case, HealthIntervalSec),
// rolling-ресурсы, чужие states не трогаются.
public class NodeSupervisorTests
{
    private const string Prefix = "/s3worker";

    private sealed class Rig
    {
        public Fakes.FakeEtcd Etcd = new();
        public Fakes.FakeS3Driver Driver = new()
        {
            Hosts = [new S3HostAttributes("h1", 0, new Dictionary<string, string>())],
        };
        public Fakes.FakeNodeClient NodeClient = new();
        public FixedTimeProvider Clock = new();
        public ClaimStore Claims = null!;
        public NodeSupervisor Supervisor = null!;

        public static Rig New(int nodeDeadSec = 90, int healthIntervalSec = 15)
        {
            var rig = new Rig();
            rig.Claims = new ClaimStore(Prefix, ["http://etcd:2379"], rig.Etcd, rig.Clock);
            var secrets = new SecretProfileStore(
                name => name == "S3W_NODE_SECRETS__backups-main__ROOT_USER" ? "root"
                    : name == "S3W_NODE_SECRETS__backups-main__ROOT_PASSWORD" ? "pw" : null,
                ["S3W_NODE_SECRETS__backups-main__ROOT_USER", "S3W_NODE_SECRETS__backups-main__ROOT_PASSWORD"]);
            rig.Supervisor = new NodeSupervisor(
                rig.Etcd, ["http://etcd:2379"], rig.Driver, rig.Claims,
                new WorkJournal(Prefix, rig.Etcd, ["http://etcd:2379"]),
                rig.NodeClient, secrets,
                new S3ProvisioningOptions(18000, 18999, 100, nodeDeadSec, healthIntervalSec, null, "owns3:dev", "pgworker-backups"),
                new PortAllocHealer(
                    rig.Etcd, ["http://etcd:2379"], rig.Driver, rig.Claims,
                    new WorkJournal(Prefix, rig.Etcd, ["http://etcd:2379"]),
                    new PortAllocLock(Prefix, ["http://etcd:2379"], rig.Etcd, rig.Clock, "inst-1"),
                    new PortAllocIndex(rig.Etcd, ["http://etcd:2379"],
                        Microsoft.Extensions.Logging.Abstractions.NullLogger<PortAllocIndex>.Instance),
                    new S3ProvisioningOptions(18000, 18999, 100, nodeDeadSec, healthIntervalSec, null, "owns3:dev", "pgworker-backups")),
                rig.Clock);
            return rig;
        }

        // Active-кластер N=1: node1 RUNNING на h1:18000 (portalloc + placed_on);
        // docker-факт лимитов = декларации (расхождение — отдельный кейс).
        public async Task SeedActiveAsync(string state = S3NodeStates.Running)
        {
            Etcd.Seed("/s3/clusters/demo/config",
                """{"platform":"owns3","nodes":1,"secret_profile":"backups-main","created_unix":1760156800}""");
            Etcd.Seed("/s3/clusters/demo/nodes/node1/state", state);
            Etcd.Seed("/s3/clusters/demo/nodes/node1/placed_on", "h1");
            Etcd.Seed("/s3/clusters/demo/nodes/node1/resources", """{"cpu":"1","mem":"1Gi","disk":"5Gi"}""");
            Etcd.Seed($"{Prefix}/portalloc/demo", """{"node1":{"host":"h1","s3":18000}}""");
            Driver.Nodes["demo/node1"] = new Fakes.FakeS3Driver.NodeFact("h1", 18000);
            Driver.Limits = new Shared.Docker.NodeLimits(1_000_000_000, 1024L * 1024 * 1024);
            await Claims.TryClaimClusterAsync("demo", TestContext.Current.CancellationToken);
        }

        public S3ClusterSnapshot Snap() => new(
            "demo",
            new S3ClusterConfig("owns3", 1, "backups-main", 1760156800, null),
            [new S3NodeSnapshot("node1", S3NodeStates.Running, "h1",
                new S3NodePlacement("h1", null, null),
                new S3NodeResources("1", "1Gi", "5Gi"), null, null)],
            null, null);
    }

    // AAA: контейнер снесён (inspect → null) → пересоздание на том же placed_on,
    // state=PROVISIONING; volume не тронут (removeVolume=false).
    [Fact]
    public async Task КонтейнерСнесён_ПересозданиеНаТомЖеХосте()
    {
        // Arrange: объект docker отсутствует (положительное свидетельство).
        var rig = Rig.New();
        await rig.SeedActiveAsync();
        rig.Driver.Nodes.Clear();

        // Act
        var result = await rig.Supervisor.TickAsync(rig.Snap(), TestContext.Current.CancellationToken);

        // Assert: снос без тома + ensure на h1; state=PROVISIONING.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Driver.Removed.Should().Contain("demo/node1");
        rig.Driver.Ensured.Should().ContainSingle()
            .Which.Host.Should().Be("h1");
        rig.Etcd.Store["/s3/clusters/demo/nodes/node1/state"].Value.Should().Be(S3NodeStates.Provisioning);
    }

    // AAA: контейнер остановлен (inspect → Running=false; restart-политика
    // unless-stopped его не поднимет) → пересоздание (S7: не-running по
    // инспекции — положительное свидетельство), а не вечный UNREACHABLE.
    [Fact]
    public async Task КонтейнерОстановлен_Пересоздание()
    {
        // Arrange: docker-объект есть, но не running.
        var rig = Rig.New();
        await rig.SeedActiveAsync();
        rig.Driver.Nodes["demo/node1"] = new Fakes.FakeS3Driver.NodeFact("h1", 18000, Running: false);

        // Act
        var result = await rig.Supervisor.TickAsync(rig.Snap(), TestContext.Current.CancellationToken);

        // Assert: снос без тома + ensure на том же placed_on; state=PROVISIONING.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Driver.Removed.Should().Contain("demo/node1");
        rig.Driver.Ensured.Should().ContainSingle()
            .Which.Host.Should().Be("h1");
        rig.Etcd.Store["/s3/clusters/demo/nodes/node1/state"].Value.Should().Be(S3NodeStates.Provisioning);
    }

    // AAA: одно пересоздание за тик — вторая снесённая нода следующим тиком.
    [Fact]
    public async Task ОдноПересозданиеЗаТик()
    {
        // Arrange: две ноды, обе снесены.
        var rig = Rig.New();
        rig.Etcd.Seed("/s3/clusters/demo/config",
            """{"platform":"owns3","nodes":2,"secret_profile":"backups-main","created_unix":1760156800}""");
        rig.Etcd.Seed("/s3/clusters/demo/nodes/node1/state", S3NodeStates.Running);
        rig.Etcd.Seed("/s3/clusters/demo/nodes/node2/state", S3NodeStates.Running);
        rig.Etcd.Seed("/s3/clusters/demo/nodes/node1/placed_on", "h1");
        rig.Etcd.Seed("/s3/clusters/demo/nodes/node2/placed_on", "h1");
        rig.Etcd.Seed($"{Prefix}/portalloc/demo", """{"node1":{"host":"h1","s3":18000},"node2":{"host":"h1","s3":18001}}""");
        rig.Driver.Nodes.Clear(); // обе ноды снесены
        await rig.Claims.TryClaimClusterAsync("demo", TestContext.Current.CancellationToken);
        var snap = new S3ClusterSnapshot("demo",
            new S3ClusterConfig("owns3", 2, "backups-main", 1760156800, null),
            [
                new S3NodeSnapshot("node1", S3NodeStates.Running, "h1", new S3NodePlacement("h1", null, null),
                    new S3NodeResources("1", "1Gi", "5Gi"), null, null),
                new S3NodeSnapshot("node2", S3NodeStates.Running, "h1", new S3NodePlacement("h1", null, null),
                    new S3NodeResources("1", "1Gi", "5Gi"), null, null),
            ],
            null, null);

        // Act
        var result = await rig.Supervisor.TickAsync(snap, TestContext.Current.CancellationToken);

        // Assert: ensure ровно одна (node1 Ordinal-первая).
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Driver.Ensured.Should().ContainSingle()
            .Which.NodeName.Should().Be("node1");
    }

    // AAA: health ок + state=PROVISIONING → RUNNING.
    [Fact]
    public async Task ПровижинингПрошёл_ПробаЖива_Running()
    {
        // Arrange: нода PROVISIONING, docker-объект и health живы.
        var rig = Rig.New();
        await rig.SeedActiveAsync(S3NodeStates.Provisioning);

        // Act
        var result = await rig.Supervisor.TickAsync(rig.Snap(), TestContext.Current.CancellationToken);

        // Assert
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Etcd.Store["/s3/clusters/demo/nodes/node1/state"].Value.Should().Be(S3NodeStates.Running);
    }

    // AAA: health молчит дольше NodeDeadSec → UNREACHABLE; деструктива нет.
    [Fact]
    public async Task МолчаниеДольшеПорога_UnreachableБезДеструктива()
    {
        // Arrange: порог 5 с; первый тик при молчании ставит first_seen = T0.
        var rig = Rig.New(nodeDeadSec: 5);
        await rig.SeedActiveAsync();
        rig.Clock.Utc = new DateTimeOffset(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);
        rig.NodeClient.Health = false;
        await rig.Supervisor.TickAsync(rig.Snap(), TestContext.Current.CancellationToken); // first_seen=T0
        rig.Clock.Utc = new DateTimeOffset(2026, 1, 1, 12, 0, 6, TimeSpan.Zero);

        // Act: второй тик — за порогом.
        var result = await rig.Supervisor.TickAsync(rig.Snap(), TestContext.Current.CancellationToken);

        // Assert: UNREACHABLE; контейнер не пересоздан (деструктива нет).
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Etcd.Store["/s3/clusters/demo/nodes/node1/state"].Value.Should().Be(S3NodeStates.Unreachable);
        rig.Driver.Ensured.Should().BeEmpty();
        rig.Driver.Removed.Should().BeEmpty();
    }

    // AAA: слепой inspect (драйвер Failed) → никаких действий (ошибка тика).
    [Fact]
    public async Task СлепойInspect_НикакихДействий()
    {
        // Arrange: docker-хост молчит.
        var rig = Rig.New();
        await rig.SeedActiveAsync();
        rig.Driver.InspectFault = true;

        // Act
        var result = await rig.Supervisor.TickAsync(rig.Snap(), TestContext.Current.CancellationToken);

        // Assert: Failed-тик; docker/etcd-мутаций нет.
        result.IsSuccess.Should().BeFalse();
        rig.Driver.Ensured.Should().BeEmpty();
        rig.Driver.Removed.Should().BeEmpty();
    }

    // AAA: usage-сбор — ключа нет → проба грани → put snake_case; ошибка грани
    // — транзиент (тик Success).
    [Fact]
    public async Task UsageСбор_SnakeCase_Ошибка_Транзиент()
    {
        // Arrange: usage-ключа нет; грань жива.
        var rig = Rig.New();
        await rig.SeedActiveAsync();
        rig.NodeClient.Usage = new S3Worker.Core.OwnS3.OwnS3NodeUsage(7, 9);

        // Act
        var result = await rig.Supervisor.TickAsync(rig.Snap(), TestContext.Current.CancellationToken);

        // Assert: usage записан snake_case.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        var usage = rig.Etcd.Store["/s3/clusters/demo/nodes/node1/usage"].Value;
        usage.Should().Contain("\"used_bytes\":7").And.Contain("\"total_bytes\":9");

        // Act 2: грань сломалась, usage свежий — тик Success (транзиент).
        rig.NodeClient.UsageFault = true;
        var second = await rig.Supervisor.TickAsync(rig.Snap(), TestContext.Current.CancellationToken);
        second.IsSuccess.Should().BeTrue();
    }

    // AAA: лимиты контейнера ≠ декларации → пересоздание с новыми лимитами
    // (rolling; в бюджете «одно пересоздание за тик»).
    [Fact]
    public async Task РесурсыРасходятся_ПересозданиеСНовымиЛимитами()
    {
        // Arrange: docker-факт 2 ядра против декларации 1.
        var rig = Rig.New();
        await rig.SeedActiveAsync();
        rig.Driver.Limits = new Shared.Docker.NodeLimits(2_000_000_000, 1024L * 1024 * 1024);

        // Act
        var result = await rig.Supervisor.TickAsync(rig.Snap(), TestContext.Current.CancellationToken);

        // Assert: пересоздание; спека несёт декларированные лимиты; state=PROVISIONING.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        var spec = rig.Driver.Ensured.Should().ContainSingle().Subject;
        spec.CpuCores.Should().Be(1m);
        spec.MemoryBytes.Should().Be(1024L * 1024 * 1024);
        rig.Etcd.Store["/s3/clusters/demo/nodes/node1/state"].Value.Should().Be(S3NodeStates.Provisioning);
    }

    // AAA: ноды NOT_INITIALIZED надзор не трогает.
    [Fact]
    public async Task НодыЧужихПроцессов_НеТрогаются()
    {
        // Arrange: нода NOT_INITIALIZED (заявка), docker-объекта нет.
        var rig = Rig.New();
        await rig.SeedActiveAsync(S3NodeStates.NotInitialized);
        rig.Driver.Nodes.Clear();

        // Act
        var result = await rig.Supervisor.TickAsync(rig.Snap(), TestContext.Current.CancellationToken);

        // Assert: тик Success, docker не тронут.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Driver.Ensured.Should().BeEmpty();
        rig.Driver.Removed.Should().BeEmpty();
    }
}
