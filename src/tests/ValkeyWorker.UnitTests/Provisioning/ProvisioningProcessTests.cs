using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Shared.Etcd.Client;
using ValkeyWorker.Core.Model;
using ValkeyWorker.Etcd.Parsing;
using ValkeyWorker.UnitTests.Provisioning;
using Xunit;

namespace ValkeyWorker.UnitTests.Provisioning;

// ProvisioningProcess V0–V5 (arch/21 §5 A): полный прогон, идемпотентность
// re-run, сверка V3 (args), ожидание portalloc-клэйма, гонка TO_REMOVE,
// бюджет V4 (boot-timeout), чужой клэйм.
public class ProvisioningProcessTests
{
    private static readonly ValkeyWorker.Provisioning.Processes.ValkeyProvisioningOptions Options =
        new(17000, 17999, 100, 90, "localhost", "valkey/valkey:9.1.2");

    private static readonly FixedTimeProvider Clock = new();

    // Образ ноды (константа рига — как ValkeyProvisioningOptions).
    private const string Image = "valkey/valkey:9.1.2";

    private sealed class Rig
    {
        // Единый журнал порядка операций etcd+docker (тест секции portalloc).
        public List<string> OpsLog = [];
        public Fakes.FakeEtcd Etcd = null!;
        public Fakes.FakeDriver Driver = null!;
        public Fakes.FakeValkeyConnection Valkey = new() { TrustAnyPassword = true };
        public ClaimStore Claims = null!;
        public List<string> Snapshots = [];
        public ValkeyWorker.Provisioning.Processes.ProvisioningProcess Process = null!;

        public static Rig Create(bool withSnapshot = true)
        {
            var rig = new Rig();
            rig.Etcd = new Fakes.FakeEtcd(rig.OpsLog);
            rig.Driver = new Fakes.FakeDriver { SharedOps = rig.OpsLog };
            rig.Claims = new ClaimStore("/valkeyworker", ["http://etcd:2379"], rig.Etcd, TimeProvider.System);
            rig.Process = new ValkeyWorker.Provisioning.Processes.ProvisioningProcess(
                rig.Etcd, ["http://etcd:2379"], rig.Driver, rig.Claims,
                new WorkJournal("/valkeyworker", rig.Etcd, ["http://etcd:2379"]),
                new PortAllocLock("/valkeyworker", ["http://etcd:2379"], rig.Etcd, TimeProvider.System, "inst-1"),
                new ValkeyWorker.Provisioning.Processes.PortAllocIndex(
                    rig.Etcd, ["http://etcd:2379"],
                    NullLogger<ValkeyWorker.Provisioning.Processes.PortAllocIndex>.Instance),
                new ValkeyWorker.Provisioning.Processes.ClusterSecretEnsurer(rig.Etcd, ["http://etcd:2379"]),
                new ValkeyWorker.Provisioning.Processes.NodeTlsProvisioner(rig.Driver, Image, Clock),
                rig.Valkey, Options,
                withSnapshot
                    ? async _ =>
                    {
                        rig.Snapshots.Add("shot");
                        return Result.Success();
                    }
                    : null,
                Clock);
            return rig;
        }

        // Сид заявки NOT_INITIALIZED (config + node1/state + resources).
        public void SeedCluster(string cluster, int port = 0)
        {
            Etcd.Seed($"/valkey/clusters/{cluster}/config",
                """{"nodes":1,"maxmemory_bytes":536870912,"maxmemory_policy":"allkeys-lru","created_unix":1756500000,"state":"NOT_INITIALIZED"}""");
            Etcd.Seed($"/valkey/clusters/{cluster}/nodes/node1/state", "NOT_INITIALIZED");
            Etcd.Seed($"/valkey/clusters/{cluster}/nodes/node1/resources",
                """{"cpu":"2","mem":"1Gi","disk":"10Gi"}""");
        }

        public ValkeyClusterSnapshot Snapshot(string cluster)
        {
            var range = Etcd.RangeAsync("", "/valkey/clusters/", TestContext.Current.CancellationToken)
                .GetAwaiter().GetResult();
            return ValkeySnapshotParser.Parse(range.Value).Value.Clusters.First(c => c.Cluster == cluster);
        }
    }

    [Fact]
    public async Task ПолныйПрогон_V0V5_КонтейнерИДискавери()
    {
        // Arrange: заявка NOT_INITIALIZED, пустой docker.
        const string cluster = "full";
        var rig = Rig.Create();
        rig.SeedCluster(cluster);
        var claimed = await rig.Claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken);
        claimed.Value.Should().BeTrue();

        // Act: один тик процесса.
        var result = await rig.Process.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: контейнер создан с каноническими параметрами; дискавери в etcd.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        var ensured = rig.Driver.Ensured.Should().ContainSingle().Subject;
        ensured.NodeName.Should().Be("node1");
        ensured.Image.Should().Be("valkey/valkey:9.1.2");
        ensured.Args.Should().Contain("--appendonly", "no");
        ensured.CpuCores.Should().Be(2m);
        ensured.MemoryBytes.Should().Be(1024L * 1024 * 1024);

        // порт из окна, не занят, portalloc закреплён
        var portAlloc = rig.Etcd.Store["/valkeyworker/portalloc/full"].Value;
        portAlloc.Should().Contain("client");
        var port = System.Text.Json.JsonDocument.Parse(portAlloc)
            .RootElement.GetProperty("node1").GetProperty("client").GetInt32();
        port.Should().BeGreaterThanOrEqualTo(17000).And.BeLessThan(17999);

        // state=RUNNING; endpoints записаны; config без state; journal done
        rig.Etcd.Store["/valkey/clusters/full/nodes/node1/state"].Value.Should().Be("RUNNING");
        var endpoints = rig.Etcd.Store["/valkey/clusters/full/endpoints"].Value;
        endpoints.Should().Be($"localhost:{port}");
        var config = rig.Etcd.Store["/valkey/clusters/full/config"].Value;
        config.Should().NotContain("state");
        rig.Etcd.Store["/valkeyworker/work/full"].Value.Should().Contain("done");

        // креды: 32 симв
        rig.Etcd.Store["/valkey/clusters/full/admin_password"].Value.Should().HaveLength(32);

        // снапшот «до» и «после»
        rig.Snapshots.Should().HaveCount(2);
    }

    [Fact]
    public async Task ИдемпотентныйReRun_БезИзменений()
    {
        // Arrange: первый тик довёл кластер до RUNNING.
        const string cluster = "rerun";
        var rig = Rig.Create(withSnapshot: false);
        rig.SeedCluster(cluster);
        await rig.Claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken);
        (await rig.Process.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken))
            .IsSuccess.Should().BeTrue();
        var ensuredAfterFirst = rig.Driver.Ensured.Count;

        // Act: второй тик на готовом кластере.
        var result = await rig.Process.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: контейнер совпадает по image/args/порту/лимитам — без пересоздания.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Driver.Ensured.Count.Should().Be(ensuredAfterFirst);
        rig.Driver.Removed.Should().BeEmpty();
    }

    [Fact]
    public async Task СверкаВ3_ИныеАргс_ПересозданиеСКаноническими()
    {
        // Arrange: после первого тика подменяем args живого контейнера.
        const string cluster = "drift";
        var rig = Rig.Create(withSnapshot: false);
        rig.SeedCluster(cluster);
        await rig.Claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken);
        (await rig.Process.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken))
            .IsSuccess.Should().BeTrue();

        var liveArgs = rig.Driver.Containers["vwk-drift-node1"].Args.ToArray();
        liveArgs[Array.IndexOf(liveArgs, "--maxmemory") + 1] = "999";
        rig.Driver.Containers["vwk-drift-node1"] =
            new Fakes.FakeDriver.ContainerFact("h1", rig.Driver.Containers["vwk-drift-node1"].HostPort,
                null, null, liveArgs, "valkey/valkey:9.1.2", "drifted");

        // Act: тик над «дрейфовавшим» контейнером.
        var result = await rig.Process.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: пересоздан с каноническими args (Ensure второй, Remove зафиксирован).
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Driver.Ensured.Should().HaveCount(2);
        rig.Driver.Removed.Should().Contain("vwk-drift-node1");
        rig.Driver.Containers["vwk-drift-node1"].Args.Should().Equal(
            rig.Driver.Ensured[0].Args);
    }

    [Fact]
    public async Task PortAllocLockЗанят_ЖдущийТикБезКонтейнера()
    {
        // Arrange: чужой держатель locks/portalloc (живой leased-ключ).
        const string cluster = "waitlock";
        var rig = Rig.Create(withSnapshot: false);
        rig.SeedCluster(cluster);
        await rig.Claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken);
        var grant = await rig.Etcd.LeaseGrantAsync("", 60, TestContext.Current.CancellationToken);
        await rig.Etcd.PutAsync("", "/valkeyworker/locks/portalloc", """{"instance":"other"}""", grant.Value,
            TestContext.Current.CancellationToken);

        // Act
        var result = await rig.Process.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: Result успех (InProgress), journal waiting-portalloc-lock, контейнера нет.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Etcd.Store["/valkeyworker/work/waitlock"].Value.Should().Contain("waiting-portalloc-lock");
        rig.Driver.Ensured.Should().BeEmpty();
    }

    [Fact]
    public async Task ГонкаToRemoveМеждуV1иV3_ПроцессПрекращён()
    {
        // Arrange: панель пишет TO_REMOVE сразу после portalloc-пут (V1).
        const string cluster = "race";
        var rig = Rig.Create(withSnapshot: false);
        rig.SeedCluster(cluster);
        await rig.Claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken);
        rig.Etcd.OnPut = key =>
        {
            if (key.Contains("portalloc/race"))
            {
                rig.Etcd.Store["/valkey/clusters/race/config"] =
                    new Fakes.FakeEtcd.Entry(
                        """{"nodes":1,"maxmemory_bytes":536870912,"maxmemory_policy":"allkeys-lru","created_unix":1756500000,"state":"TO_REMOVE"}""",
                        rig.Etcd.Store["/valkey/clusters/race/config"].ModRevision + 1, 2);
            }
        };

        // Act
        var result = await rig.Process.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: процесс прекращён до контейнера; journal aborted-state-changed.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Etcd.Store["/valkeyworker/work/race"].Value.Should().Contain("aborted-state-changed");
        rig.Driver.Ensured.Should().BeEmpty();
    }

    [Fact]
    public async Task V4БюджетИсчерпан_BootTimeout_StateProvisioning()
    {
        // Arrange: нода молчит; каждый вызов PING двигает FixedTimeProvider за бюджет.
        const string cluster = "boot";
        var rig = Rig.Create(withSnapshot: false);
        rig.SeedCluster(cluster);
        await rig.Claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken);
        rig.Valkey.Silent = true;
        rig.Valkey.OnCommand = () => Clock.Utc = Clock.Utc.AddSeconds(101);

        // Act
        var result = await rig.Process.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: Result.Failed, journal boot-timeout, state остаётся PROVISIONING,
        // контейнер создан (V3 прошёл).
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeAssignableTo<Exception>();
        result.Error!.Message.Should().Contain("не отвечает");
        rig.Etcd.Store["/valkeyworker/work/boot"].Value.Should().Contain("boot-timeout");
        rig.Etcd.Store["/valkey/clusters/boot/nodes/node1/state"].Value.Should().Be("PROVISIONING");
        rig.Driver.Ensured.Should().ContainSingle();
    }

    [Fact]
    public async Task ЧужойКлэйм_НикакихЗаписей()
    {
        // Arrange: кластер под клэймом другого инстанса.
        const string cluster = "foreign";
        var rig = Rig.Create(withSnapshot: false);
        rig.SeedCluster(cluster);
        var other = new ClaimStore("/valkeyworker", ["http://etcd:2379"], rig.Etcd, TimeProvider.System);
        (await other.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken)).Value.Should().BeTrue();

        // Act
        var result = await rig.Process.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: отказ без мутаций etcd домена и docker.
        result.IsSuccess.Should().BeFalse();
        rig.Driver.Ensured.Should().BeEmpty();
        rig.Etcd.Store.Should().NotContainKey("/valkey/clusters/foreign/endpoints");
        rig.Etcd.Store["/valkey/clusters/foreign/nodes/node1/state"].Value.Should().Be("NOT_INITIALIZED");
    }

    [Fact]
    public async Task ДовыделениеПортов_КлэймДоЧтенияЗанятости()
    {
        // Arrange: порт не закреплён — требуется секция довыделения (arch/20 §3).
        const string cluster = "order";
        var rig = Rig.Create(withSnapshot: false);
        rig.SeedCluster(cluster);
        await rig.Claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken);

        // Act: тик provisioning.
        var result = await rig.Process.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);
        result.IsSuccess.Should().BeTrue(result.Error?.Message);

        // Assert: захват locks/portalloc зафиксирован в etcd РАНЬШЕ чтений
        // занятости — секция «чтение занятости → выбор портов → запись»
        // целиком под клэймом (иначе гонка MaxClusters>1 выбирает один порт).
        var lockIndex = rig.OpsLog.FindIndex(o => o.Contains("/valkeyworker/locks/portalloc"));
        var hostsIndex = rig.OpsLog.IndexOf("docker:hosts");
        lockIndex.Should().BeGreaterThanOrEqualTo(0, "клэйм portalloc захвачен");
        hostsIndex.Should().BeGreaterThanOrEqualTo(0, "занятость docker читалась");
        lockIndex.Should().BeLessThan(hostsIndex, "чтение занятости — ВНУТРИ клэйма portalloc");
        rig.OpsLog.IndexOf("docker:busy-ports").Should().BeGreaterThan(lockIndex);
    }

    [Fact]
    public async Task ВсёЗакреплено_РаннийВыходБезКлэйма()
    {
        // Arrange: portalloc уже закреплён за кластером (re-run).
        const string cluster = "pinned";
        var rig = Rig.Create(withSnapshot: false);
        rig.SeedCluster(cluster);
        rig.Etcd.Seed("/valkeyworker/portalloc/pinned",
            """{"node1":{"host":"h1","client":17555}}""");
        await rig.Claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken);

        // Act
        var result = await rig.Process.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: ранний выход — глобальный клэйм не брался, порт переиспользован.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.OpsLog.Should().NotContain(o => o.Contains("/valkeyworker/locks/portalloc"));
        rig.Etcd.Store["/valkey/clusters/pinned/endpoints"].Value.Should().Be("localhost:17555");
    }

    // t06: V3 пишет TLS-volume и поднимает контейнер с TLS-args и TlsVolume.
    [Fact]
    public async Task Provision_V3_WritesTlsVolumeAndStartsContainer()
    {
        // Arrange: заявка NOT_INITIALIZED.
        const string cluster = "tlsprov";
        var rig = Rig.Create(withSnapshot: false);
        rig.SeedCluster(cluster);
        await rig.Claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken);

        // Act: тик provisioning.
        var result = await rig.Process.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: volume записан (3 файла), spec.TlsVolume = vwk-<C>-tls,
        // args содержат --tls-port, ca_pem/ca_key появились в etcd (V2).
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        TarArchive.Read(rig.Driver.TlsVolumes[(cluster, "h1")])
            .Keys.Should().BeEquivalentTo("node.crt", "node.key", "ca.pem");
        var ensured = rig.Driver.Ensured.Should().ContainSingle().Subject;
        ensured.TlsVolume.Should().Be($"vwk-{cluster}-tls");
        ensured.Args.Should().Contain("--tls-port");
        rig.Etcd.Store[$"/valkey/clusters/{cluster}/ca_pem"].Value.Should().NotBeNullOrEmpty();
        rig.Etcd.Store[$"/valkey/clusters/{cluster}/ca_key"].Value.Should().NotBeNullOrEmpty();
    }
}
