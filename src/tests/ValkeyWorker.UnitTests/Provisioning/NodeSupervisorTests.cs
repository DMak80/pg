using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Shared.Etcd.Client;
using ValkeyWorker.Core.Model;
using ValkeyWorker.Etcd.Parsing;
using ValkeyWorker.UnitTests.Provisioning;
using Xunit;

namespace ValkeyWorker.UnitTests.Provisioning;

// NodeSupervisor (arch/21 §5 C): снос/пересоздание, автоконверге лимитов,
// слепой docker-инспект (S7), UNREACHABLE по NodeDeadSec (таймаут и refused —
// молчание при живом docker-факте), E9, endpoints-RMW.
public class NodeSupervisorTests
{
    private static readonly FixedTimeProvider Clock = new();

    private sealed class Rig
    {
        public Fakes.FakeEtcd Etcd = new();
        public Fakes.FakeDriver Driver = new();
        public Fakes.FakeValkeyConnection Valkey = new() { TrustAnyPassword = true };
        public ClaimStore Claims = null!;
        public ValkeyWorker.Provisioning.Processes.NodeSupervisor Supervisor = null!;

        public static Rig Create()
        {
            var rig = new Rig();
            rig.Claims = new ClaimStore("/valkeyworker", ["http://etcd:2379"], rig.Etcd, TimeProvider.System);
            var healer = new ValkeyWorker.Provisioning.Processes.PortAllocHealer(
                rig.Etcd, ["http://etcd:2379"], rig.Driver, rig.Claims,
                new WorkJournal("/valkeyworker", rig.Etcd, ["http://etcd:2379"]),
                new PortAllocLock("/valkeyworker", ["http://etcd:2379"], rig.Etcd, TimeProvider.System, "inst-1"),
                new ValkeyWorker.Provisioning.Processes.PortAllocIndex(
                    rig.Etcd, ["http://etcd:2379"],
                    NullLogger<ValkeyWorker.Provisioning.Processes.PortAllocIndex>.Instance),
                new ValkeyWorker.Provisioning.Processes.ValkeyProvisioningOptions(
                    17000, 17999, 100, 90, "localhost", "valkey/valkey:9.1.2"));
            rig.Supervisor = new ValkeyWorker.Provisioning.Processes.NodeSupervisor(
                rig.Etcd, ["http://etcd:2379"], rig.Driver, rig.Claims,
                new WorkJournal("/valkeyworker", rig.Etcd, ["http://etcd:2379"]),
                rig.Valkey,
                new ValkeyWorker.Provisioning.Processes.ValkeyProvisioningOptions(
                    17000, 17999, 100, 90, "localhost", "valkey/valkey:9.1.2"),
                healer, Clock);
            return rig;
        }

        // Active-кластер: config без state, креды, portalloc, живой контейнер
        // + клэйм прогона (надзор мутирует только под своим клэймом).
        public void SeedActive(string cluster, int port = 17001, string resources = """{"cpu":"2","mem":"1Gi","disk":"10Gi"}""")
        {
            Claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken).GetAwaiter().GetResult();
            Etcd.Seed($"/valkey/clusters/{cluster}/config",
                """{"nodes":1,"maxmemory_bytes":536870912,"maxmemory_policy":"allkeys-lru","created_unix":1756500000}""");
            Etcd.Seed($"/valkey/clusters/{cluster}/nodes/node1/state", "RUNNING");
            Etcd.Seed($"/valkey/clusters/{cluster}/nodes/node1/resources", resources);
            Etcd.Seed($"/valkey/clusters/{cluster}/endpoints", $"localhost:{port}");
            Etcd.Seed($"/valkey/clusters/{cluster}/app_user", "app");
            Etcd.Seed($"/valkey/clusters/{cluster}/app_password", "AppPassword0123456789abcdef12345");
            Etcd.Seed($"/valkey/clusters/{cluster}/admin_user", "admin");
            Etcd.Seed($"/valkey/clusters/{cluster}/admin_password", "AdminPassword0123456789abcdef12345");
            Etcd.Seed($"/valkeyworker/portalloc/{cluster}", "{\"node1\":{\"host\":\"h1\",\"client\":" + port + "}}");
            Driver.Containers[$"vwk-{cluster}-node1"] =
                new Fakes.FakeDriver.ContainerFact("h1", port, 2m, 1024L * 1024 * 1024,
                    ["valkey-server", "--appendonly", "no"], "valkey/valkey:9.1.2", "id1");
        }

        public ValkeyClusterSnapshot Snapshot(string cluster)
        {
            var range = Etcd.RangeAsync("", "/valkey/clusters/", TestContext.Current.CancellationToken)
                .GetAwaiter().GetResult();
            return ValkeySnapshotParser.Parse(range.Value).Value.Clusters.First(c => c.Cluster == cluster);
        }
    }

    [Fact]
    public async Task СносКонтейнера_ПересозданиеТемиЖеКредамиИПортом()
    {
        // Arrange: контейнер снесён (объекта нет), portalloc на 17001.
        const string cluster = "gone";
        var rig = Rig.Create();
        rig.SeedActive(cluster);
        rig.Driver.Containers.Clear();

        // Act: тик надзора.
        var result = await rig.Supervisor.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: пересоздан (Ensure с args из etcd-кредов), state=PROVISIONING;
        // следующий тик (PING ok) → RUNNING.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        var ensured = rig.Driver.Ensured.Should().ContainSingle().Subject;
        ensured.NodeName.Should().Be("node1");
        ensured.ClientHostPort.Should().Be(17001);
        ensured.Args.Should().Contain("AdminPassword0123456789abcdef12345".Insert(0, ">"));
        rig.Etcd.Store["/valkey/clusters/gone/nodes/node1/state"].Value.Should().Be("PROVISIONING");

        // следующий тик: PING → RUNNING
        var second = await rig.Supervisor.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);
        second.IsSuccess.Should().BeTrue(second.Error?.Message);
        rig.Etcd.Store["/valkey/clusters/gone/nodes/node1/state"].Value.Should().Be("RUNNING");
    }

    [Fact]
    public async Task АвтоконвергеЛимитов_ПересозданиеОдноЗаТик()
    {
        // Arrange: декларация cpu 2, контейнер cpu 1 (дрейф).
        const string cluster = "drift";
        var rig = Rig.Create();
        rig.SeedActive(cluster);
        rig.Driver.Containers["vwk-drift-node1"] =
            new Fakes.FakeDriver.ContainerFact("h1", 17001, 1m, 1024L * 1024 * 1024,
                ["valkey-server", "--appendonly", "no"], "valkey/valkey:9.1.2", "id1");

        // Act
        var result = await rig.Supervisor.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: пересоздание с лимитами декларации (cpu=2), ОДНО за тик.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Driver.Ensured.Should().ContainSingle();
        rig.Driver.Ensured[0].CpuCores.Should().Be(2m);
        rig.Etcd.Store["/valkey/clusters/drift/nodes/node1/state"].Value.Should().Be("PROVISIONING");

        // Совпадающие лимиты → пусто (второй прогон после ensure — уже 2 ядра).
        var second = await rig.Supervisor.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);
        second.IsSuccess.Should().BeTrue(second.Error?.Message);
        rig.Driver.Ensured.Should().HaveCount(1);
    }

    [Fact]
    public async Task СлепойInspect_FailedКонтейнерЖивStateНеМеняется()
    {
        // Arrange: docker-хост молчит на инспектах.
        const string cluster = "blind";
        var rig = Rig.Create();
        rig.SeedActive(cluster);
        rig.Driver.ResourcesFault = true;
        rig.Driver.EndpointFault = true;

        // Act
        var result = await rig.Supervisor.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: ошибка тика, пересозданий нет, state без изменений.
        result.IsSuccess.Should().BeFalse();
        rig.Driver.Ensured.Should().BeEmpty();
        rig.Etcd.Store["/valkey/clusters/blind/nodes/node1/state"].Value.Should().Be("RUNNING");
    }

    [Fact]
    public async Task Unreachable_ПоПорогуNodeDead_ПересозданиеЗатемRunning()
    {
        // Arrange: нода молчит; трек first_seen уже старше NodeDeadSec+1
        // (молчание копилось прошлыми тиками).
        const string cluster = "dead";
        var rig = Rig.Create();
        rig.SeedActive(cluster);
        rig.Valkey.Silent = true;
        var journal = new WorkJournal("/valkeyworker", rig.Etcd, ["http://etcd:2379"]);
        var stale = Clock.GetUtcNow().AddSeconds(-91).ToUnixTimeSeconds();
        await journal.WriteSupervisionAsync(
            "dead", "inst-1", new Dictionary<string, long> { ["node1"] = stale }, null,
            TestContext.Current.CancellationToken);

        // Act: тик с молчащей нодой (порог исчерпан).
        var result = await rig.Supervisor.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: UNREACHABLE + пересоздание; трек ноды сброшен (счётчик заново).
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Etcd.Store["/valkey/clusters/dead/nodes/node1/state"].Value.Should().Be("PROVISIONING");
        rig.Driver.Ensured.Should().ContainSingle();
        var track = await journal.ReadUnreachableAsync("dead", TestContext.Current.CancellationToken);
        track.Value.Should().NotContainKey("node1");

        // Затем: зрячая проба отвечает → RUNNING (PROVISIONING переводится надзором).
        rig.Valkey.Silent = false;
        var second = await rig.Supervisor.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);
        second.IsSuccess.Should().BeTrue(second.Error?.Message);
        rig.Etcd.Store["/valkey/clusters/dead/nodes/node1/state"].Value.Should().Be("RUNNING");
    }

    [Fact]
    public async Task СлепойDocker_ТикFailedТрекЗамороженБезДействий()
    {
        // Arrange: docker-хост молчит на инспектах (собственная слепота воркера,
        // S7); трек first_seen уже есть — слепой тик не смеет его двигать.
        const string cluster = "s7";
        var rig = Rig.Create();
        rig.SeedActive(cluster);
        var journal = new WorkJournal("/valkeyworker", rig.Etcd, ["http://etcd:2379"]);
        await journal.WriteSupervisionAsync(
            "s7", "inst-1", new Dictionary<string, long> { ["node1"] = 1000 }, null,
            TestContext.Current.CancellationToken);
        rig.Driver.EndpointFault = true;

        // Act
        var result = await rig.Supervisor.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: ошибка тика (слепой inspect — не проба), пересозданий нет,
        // state не менялся, трек не перезаписан.
        result.IsSuccess.Should().BeFalse();
        rig.Driver.Ensured.Should().BeEmpty();
        rig.Etcd.Store["/valkey/clusters/s7/nodes/node1/state"].Value.Should().Be("RUNNING");
        var track = await journal.ReadUnreachableAsync("s7", TestContext.Current.CancellationToken);
        track.Value!["node1"].Should().Be(1000, "слепой тик не двигает трек");
    }

    [Fact]
    public async Task ОстановленныйКонтейнер_RefusedМолчание_ПорогПересоздание()
    {
        // Arrange: docker stop (объект жив, Running=false), PING — connection
        // refused (не таймаут); трек молчания исчерпан прошлыми тиками.
        const string cluster = "stopped";
        var rig = Rig.Create();
        rig.SeedActive(cluster);
        rig.Driver.Stopped.Add("vwk-stopped-node1");
        rig.Valkey.ConnectionFault = true;
        var journal = new WorkJournal("/valkeyworker", rig.Etcd, ["http://etcd:2379"]);
        var stale = Clock.GetUtcNow().AddSeconds(-91).ToUnixTimeSeconds();
        await journal.WriteSupervisionAsync(
            "stopped", "inst-1", new Dictionary<string, long> { ["node1"] = stale }, null,
            TestContext.Current.CancellationToken);

        // Act: тик надзора.
        var result = await rig.Supervisor.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: refused при живом docker-факте — молчание (не слепая проба):
        // UNREACHABLE-путь → пересоздание, state=PROVISIONING.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Etcd.Store["/valkey/clusters/stopped/nodes/node1/state"].Value.Should().Be("PROVISIONING");
        rig.Driver.Ensured.Should().ContainSingle();

        // Затем: контейнер поднят (Running), проба отвечает → RUNNING.
        rig.Valkey.ConnectionFault = false;
        var second = await rig.Supervisor.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);
        second.IsSuccess.Should().BeTrue(second.Error?.Message);
        rig.Etcd.Store["/valkey/clusters/stopped/nodes/node1/state"].Value.Should().Be("RUNNING");
    }

    [Fact]
    public async Task E9_PortallocУтерян_РеконструкцияБезДеструктива()
    {
        // Arrange: portalloc-ключ удалён, контейнер жив на 17001.
        const string cluster = "e9";
        var rig = Rig.Create();
        rig.SeedActive(cluster);
        rig.Etcd.Store.Remove("/valkeyworker/portalloc/e9");

        // Act
        var result = await rig.Supervisor.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: реконструкция из inspect (ключ вернулся), пересоздания не было.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Etcd.Store.Should().ContainKey("/valkeyworker/portalloc/e9");
        rig.Etcd.Store["/valkeyworker/portalloc/e9"].Value.Should().Contain("17001");
        rig.Driver.Ensured.Should().BeEmpty();
        rig.Driver.Removed.Should().BeEmpty();
    }

    [Fact]
    public async Task EndpointsРасхождение_RmwККанону()
    {
        // Arrange: endpoints указывают не на portalloc-порт.
        const string cluster = "ep";
        var rig = Rig.Create();
        rig.SeedActive(cluster, port: 17001);
        rig.Etcd.Store["/valkey/clusters/ep/endpoints"] =
            new Fakes.FakeEtcd.Entry("localhost:19999", rig.Etcd.Store["/valkey/clusters/ep/endpoints"].ModRevision, 2);

        // Act
        var result = await rig.Supervisor.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: endpoints сошлись к portalloc-канону.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Etcd.Store["/valkey/clusters/ep/endpoints"].Value.Should().Be("localhost:17001");
    }
}
