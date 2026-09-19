using FluentAssertions;
using Shared.Etcd.Client;
using ValkeyWorker.Core.Model;
using ValkeyWorker.Core.Valkey;
using ValkeyWorker.Etcd.Parsing;
using ValkeyWorker.UnitTests.Provisioning;
using Xunit;

namespace ValkeyWorker.UnitTests.Provisioning;

// TlsMigrator (t06, arch/21 §5 T): детект (ключи CA/args живого контейнера),
// фазы T0–T3 (journal started → ensured-ca → recreated → done), идемпотентность
// re-run (канонический кластер — NotNeeded, docker не тронут), гонка
// TO_REMOVE посреди миграции — abort.
public class TlsMigratorTests
{
    private static readonly ValkeyWorker.Provisioning.Processes.ValkeyProvisioningOptions Options =
        new(17000, 17999, 100, 90, "localhost", "valkey/valkey:9.1.2");

    private static readonly FixedTimeProvider Clock = new();

    private static readonly IReadOnlyList<string> TlsArgs =
        ["valkey-server", "--tls-port", "6379", "--port", "0"];
    private static readonly IReadOnlyList<string> PlainArgs =
        ["valkey-server", "--appendonly", "no"];

    private sealed class Rig
    {
        public Fakes.FakeEtcd Etcd = new();
        public Fakes.FakeDriver Driver = new();
        public Fakes.FakeValkeyConnection Valkey = new() { TrustAnyPassword = true };
        public ClaimStore Claims = null!;
        public List<string> Snapshots = [];
        public ValkeyWorker.Provisioning.Processes.TlsMigrator Migrator = null!;

        public static Rig Create()
        {
            var rig = new Rig();
            rig.Claims = new ClaimStore("/valkeyworker", ["http://etcd:2379"], rig.Etcd, TimeProvider.System);
            rig.Migrator = new ValkeyWorker.Provisioning.Processes.TlsMigrator(
                rig.Etcd, ["http://etcd:2379"], rig.Driver, rig.Claims,
                new WorkJournal("/valkeyworker", rig.Etcd, ["http://etcd:2379"]),
                new ValkeyWorker.Provisioning.Processes.ClusterSecretEnsurer(rig.Etcd, ["http://etcd:2379"]),
                new ValkeyWorker.Provisioning.Processes.NodeTlsProvisioner(rig.Driver, Clock),
                rig.Valkey, Options,
                async _ =>
                {
                    rig.Snapshots.Add("shot");
                    return Result.Success();
                },
                Clock);
            return rig;
        }

        // Премиграционный Active-кластер: канон без ca-ключей, plain-контейнер
        // (args без TLS), portalloc закреплён, клэйм наш.
        public void SeedPlain(string cluster, int port = 17001)
        {
            Claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken).GetAwaiter().GetResult();
            Etcd.Seed($"/valkey/clusters/{cluster}/config",
                """{"nodes":1,"maxmemory_bytes":536870912,"maxmemory_policy":"allkeys-lru","created_unix":1756500000}""");
            Etcd.Seed($"/valkey/clusters/{cluster}/nodes/node1/state", "RUNNING");
            Etcd.Seed($"/valkey/clusters/{cluster}/endpoints", $"localhost:{port}");
            Etcd.Seed($"/valkey/clusters/{cluster}/app_user", "app");
            Etcd.Seed($"/valkey/clusters/{cluster}/app_password", "AppPassword0123456789abcdef12345");
            Etcd.Seed($"/valkey/clusters/{cluster}/admin_user", "admin");
            Etcd.Seed($"/valkey/clusters/{cluster}/admin_password", "AdminPassword0123456789abcdef12345");
            Etcd.Seed($"/valkeyworker/portalloc/{cluster}", "{\"node1\":{\"host\":\"h1\",\"client\":" + port + "}}");
            Driver.Containers[$"vwk-{cluster}-node1"] =
                new Fakes.FakeDriver.ContainerFact("h1", port, 2m, 1024L * 1024 * 1024,
                    PlainArgs, "valkey/valkey:9.1.2", "id-plain");
        }

        public ValkeyClusterSnapshot Snapshot(string cluster)
        {
            var range = Etcd.RangeAsync("", "/valkey/clusters/", TestContext.Current.CancellationToken)
                .GetAwaiter().GetResult();
            return ValkeySnapshotParser.Parse(range.Value).Value.Clusters.First(c => c.Cluster == cluster);
        }
    }

    [Fact]
    public void NeedsMigration_MissingCaKeys_True()
    {
        // Arrange — Active-кластер без ca_pem/ca_key (премиграционный)
        var snap = ValkeySnapshotParser.Parse(
        [
            new Kv("/valkey/clusters/c1/config", """{"nodes":1,"maxmemory_bytes":1,"maxmemory_policy":"allkeys-lru","created_unix":1}""", 1),
        ]).Value.Clusters.Single();

        // Act / Assert — CA-ключей нет: миграция нужна (контейнер уже TLS — не важно)
        ValkeyWorker.Provisioning.Processes.TlsMigrator
            .NeedsMigration(snap, TlsArgs).Should().BeTrue();
    }

    [Fact]
    public void NeedsMigration_PlainContainerArgs_True()
    {
        // Arrange — CA в etcd есть, контейнер без --tls-port (старый канон)
        var snap = ValkeySnapshotParser.Parse(
        [
            new Kv("/valkey/clusters/c1/config", """{"nodes":1,"maxmemory_bytes":1,"maxmemory_policy":"allkeys-lru","created_unix":1}""", 1),
            new Kv("/valkey/clusters/c1/ca_pem", "-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----", 1),
            new Kv("/valkey/clusters/c1/ca_key", "-----BEGIN PRIVATE KEY-----\n-----END PRIVATE KEY-----", 1),
        ]).Value.Clusters.Single();

        // Act / Assert — args живого контейнера без TLS: миграция нужна
        ValkeyWorker.Provisioning.Processes.TlsMigrator
            .NeedsMigration(snap, PlainArgs).Should().BeTrue();
    }

    [Fact]
    public void NeedsMigration_CanonicalCluster_False()
    {
        // Arrange — CA есть, args TLS, PEM-валидность не влияет на детект? Влияет:
        // парсер nullит битый PEM — для чистого детекта используем валидные ключи.
        var (caPem, caKeyPem) = ValkeyPki.GenerateCa("c1");
        var snap = ValkeySnapshotParser.Parse(
        [
            new Kv("/valkey/clusters/c1/config", """{"nodes":1,"maxmemory_bytes":1,"maxmemory_policy":"allkeys-lru","created_unix":1}""", 1),
            new Kv("/valkey/clusters/c1/ca_pem", caPem, 1),
            new Kv("/valkey/clusters/c1/ca_key", caKeyPem, 1),
        ]).Value.Clusters.Single();

        // Act / Assert — канон: миграция не нужна
        ValkeyWorker.Provisioning.Processes.TlsMigrator
            .NeedsMigration(snap, TlsArgs).Should().BeFalse();
    }

    [Fact]
    public async Task Run_MigratesPlainCluster_PhasesT0T3()
    {
        // Arrange — plain-контейнер жив (args без TLS), ca-ключей нет.
        const string cluster = "mig";
        var rig = Rig.Create();
        rig.SeedPlain(cluster);
        var snap = rig.Snapshot(cluster);

        // Act
        var outcome = await rig.Migrator.RunAsync(snap, TestContext.Current.CancellationToken);

        // Assert — InProgress; контейнер пересоздан с TLS-args ТЕМ ЖЕ портом;
        // volume записан; ca_pem/ca_key появились; journal доведён до done;
        // PING-мок позван; снапшоты «до»/«после» сняты.
        outcome.IsSuccess.Should().BeTrue(outcome.Error?.Message);
        outcome.Value.Should().Be(ValkeyWorker.Provisioning.Processes.TlsMigrator.MigrationOutcome.InProgress);
        var ensured = rig.Driver.Ensured.Should().ContainSingle().Subject;
        ensured.ClientHostPort.Should().Be(17001, "portalloc не меняется");
        ensured.Args.Should().Contain("--tls-port").And.Contain("--port");
        rig.Driver.TlsVolumes[(cluster, "h1")].Should().NotBeEmpty();
        rig.Etcd.Store[$"/valkey/clusters/{cluster}/ca_pem"].Value.Should().NotBeNullOrEmpty();
        rig.Etcd.Store[$"/valkey/clusters/{cluster}/ca_key"].Value.Should().NotBeNullOrEmpty();
        var journal = rig.Etcd.Store[$"/valkeyworker/work/{cluster}"].Value;
        journal.Should().Contain("\"done\"");
        rig.Etcd.Store[$"/valkey/clusters/{cluster}/nodes/node1/state"].Value.Should().Be("RUNNING");
        rig.Snapshots.Should().HaveCount(2);
    }

    [Fact]
    public async Task Run_RerunOnMigratedCluster_Noop()
    {
        // Arrange — миграция доведена до done (первый прогон).
        const string cluster = "rerun";
        var rig = Rig.Create();
        rig.SeedPlain(cluster);
        (await rig.Migrator.RunAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken))
            .IsSuccess.Should().BeTrue();
        var ensuredAfterFirst = rig.Driver.Ensured.Count;
        var tarBefore = rig.Driver.TlsVolumes[(cluster, "h1")];

        // Act — повторный тик по каноническому кластеру.
        var outcome = await rig.Migrator.RunAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert — NotNeeded; docker не тронут (ни новых Ensured, ни переписи volume).
        outcome.IsSuccess.Should().BeTrue(outcome.Error?.Message);
        outcome.Value.Should().Be(ValkeyWorker.Provisioning.Processes.TlsMigrator.MigrationOutcome.NotNeeded);
        rig.Driver.Ensured.Count.Should().Be(ensuredAfterFirst);
        rig.Driver.TlsVolumes[(cluster, "h1")].Should().BeSameAs(tarBefore);
    }

    [Fact]
    public async Task Run_ClusterRemovedMidMigration_Aborts()
    {
        // Arrange — после T1 (put ca_pem в txn ensure) панель пишет TO_REMOVE.
        const string cluster = "abort";
        var rig = Rig.Create();
        rig.SeedPlain(cluster);
        rig.Etcd.OnPut = key =>
        {
            if (key.EndsWith("/ca_pem"))
            {
                rig.Etcd.Seed($"/valkey/clusters/{cluster}/config",
                    """{"nodes":1,"maxmemory_bytes":536870912,"maxmemory_policy":"allkeys-lru","created_unix":1756500000,"state":"TO_REMOVE"}""");
            }
        };

        // Act
        var outcome = await rig.Migrator.RunAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert — T2 не выполняется (контейнер не пересоздан); journal aborted;
        // клэйм жив (наш).
        outcome.IsSuccess.Should().BeTrue();
        rig.Driver.Ensured.Should().BeEmpty();
        rig.Driver.TlsVolumes.Should().BeEmpty();
        var journal = rig.Etcd.Store[$"/valkeyworker/work/{cluster}"].Value;
        journal.Should().Contain("aborted-state-changed");
        rig.Claims.IsMine(cluster).Should().BeTrue();
    }
}
