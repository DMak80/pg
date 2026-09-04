using FluentAssertions;
using KafkaWorker.Core;
using KafkaWorker.Core.Templates;
using KafkaWorker.Etcd.Parsing;
using KafkaWorker.Core.Model;
using KafkaWorker.Etcd.Coordination;
using KafkaWorker.Provisioning.Kafka;
using KafkaWorker.Provisioning.Processes;
using Xunit;

namespace KafkaWorker.UnitTests.Provisioning;

// SecurityMigrator (t03 Ф6, arch/16 §5 M): чистый детект премиграционного
// кластера; M0-гварды живых операций; M2 пересоздаёт всех брокеров разом
// (том жив); M3 waiting до готовности; идемпотентность канонического кластера.
public class SecurityMigratorTests
{
    private const string Ep = "http://etcd:2379";

    private sealed record Rig(
        Fakes.FakeEtcd Etcd,
        Fakes.FakeKafkaDriver Driver,
        FakeKafkaAdminClient Admin,
        ClaimStore Claims,
        WorkJournal Journal,
        SecurityMigrator Migrator);

    // Премиграционный кластер: только app-креды + endpoints + portalloc.
    private static void SeedLegacy(Fakes.FakeEtcd etcd, int brokers = 1)
    {
        etcd.Seed("/kafka/clusters/events/config",
            $$"""{"brokers":{{brokers}},"replication_factor":1,"min_insync_replicas":1,"default_partitions":12,"default_retention_ms":604800000,"created_unix":1756500000}""");
        for (var k = 1; k <= brokers; k++)
        {
            etcd.Seed($"/kafka/clusters/events/brokers/broker{k}/state", "RUNNING");
            etcd.Seed($"/kafka/clusters/events/brokers/broker{k}/role", "controller");
        }

        etcd.Seed("/kafka/clusters/events/endpoints", "h1:16000,h1:16001");
        etcd.Seed("/kafka/clusters/events/app_user", "app");
        etcd.Seed("/kafka/clusters/events/app_password", "OldPassword0123456789abcdef");
        etcd.Seed("/kafkaworker/portalloc/events",
            """{"broker1":{"host":"h1","client":16000},"broker2":{"host":"h1","client":16001}}""");
    }

    private static async Task<KafkaClusterSnapshot> Snapshot(Fakes.FakeEtcd etcd)
    {
        var range = await etcd.RangeAsync(Ep, "/kafka/clusters/", CancellationToken.None);
        return KafkaSnapshotParser.Parse(range.Value).Value.Single(c => c.Cluster == "events");
    }

    private static async Task<Rig> NewRig(int brokers = 1)
    {
        var etcd = new Fakes.FakeEtcd();
        SeedLegacy(etcd, brokers);
        var claims = new ClaimStore([Ep], etcd, TimeProvider.System);
        await claims.TryClaimClusterAsync("events", CancellationToken.None);
        var journal = new WorkJournal(etcd, [Ep]);
        var driver = new Fakes.FakeKafkaDriver();
        var admin = new FakeKafkaAdminClient();
        var migrator = new SecurityMigrator(
            etcd, [Ep], driver, claims, journal,
            new ClusterSecretEnsurer(etcd, [Ep]),
            new FakeAdminFactory(admin),
            new FakeConverger(),
            ProvisioningOptions.Default,
            new BrokerCertificateCache());
        return new Rig(etcd, driver, admin, claims, journal, migrator);
    }

    private sealed class FakeAdminFactory(FakeKafkaAdminClient client) : IKafkaAdminClientFactory
    {
        public IKafkaAdminClient Create(string bootstrap, string user, string password, string? caPem) => client;
    }

    private sealed class FakeConverger : IClusterConfigConverger
    {
        public int Calls;

        public Task<Result> ApplyAsync(
            string cluster, string bootstrap, string user, string password, string? caPem,
            KafkaClusterConfig config, CancellationToken ct)
        {
            Calls++;
            return Task.FromResult(Result.Success());
        }
    }

    // ===== Чистый детект (NeedsMigration) =====

    [Fact]
    public async Task NeedsMigration_NoCaInSnapshot_True()
    {
        // Arrange: Active-снапшот без CA/admin-полей (премиграционный).
        var etcd = new Fakes.FakeEtcd();
        SeedLegacy(etcd);
        var snap = await Snapshot(etcd);
        var envs = new Dictionary<string, IReadOnlyDictionary<string, string>>();

        // Act / Assert: детект по etcd-полям (16 §5 M).
        SecurityMigrator.NeedsMigration(snap, envs).Should().BeTrue();
    }

    [Fact]
    public void NeedsMigration_KeysPresentButPlainContainerEnv_True()
    {
        // Arrange: ключи ensure уже положил (M1 частично), контейнер жив на
        // SASL_PLAINTEXT (нет KAFKA_SSL_TRUSTSTORE_TYPE).
        var snap = new KafkaClusterSnapshot(
            "events", new KafkaClusterConfig(1, 1, 1, 12, 604800000, null, null), [], [], [], 0,
            AdminUser: "admin", AdminPassword: "AdminPassword0123456789abcdef",
            CaPem: "PEM", CaKey: "KEY");
        var envs = new Dictionary<string, IReadOnlyDictionary<string, string>>
        {
            ["broker1"] = new Dictionary<string, string>
            {
                ["KAFKA_LISTENER_SECURITY_PROTOCOL_MAP"] =
                    "CONTROLLER:PLAINTEXT,INTERNAL:SASL_PLAINTEXT,CLIENT:SASL_PLAINTEXT",
            },
        };

        // Act / Assert: детект по env контейнеров.
        SecurityMigrator.NeedsMigration(snap, envs).Should().BeTrue();
    }

    [Fact]
    public void NeedsMigration_CanonicalCluster_False()
    {
        // Arrange: ключи есть + env SSL.
        var snap = new KafkaClusterSnapshot(
            "events", new KafkaClusterConfig(1, 1, 1, 12, 604800000, null, null), [], [], [], 0,
            AdminUser: "admin", AdminPassword: "AdminPassword0123456789abcdef",
            CaPem: "PEM", CaKey: "KEY");
        var envs = new Dictionary<string, IReadOnlyDictionary<string, string>>
        {
            ["broker1"] = new Dictionary<string, string> { ["KAFKA_SSL_TRUSTSTORE_TYPE"] = "PEM" },
        };

        // Act / Assert: повторный проход M — no-op (идемпотентность).
        SecurityMigrator.NeedsMigration(snap, envs).Should().BeFalse();
    }

    // ===== RunAsync (M0–M4 на фейках) =====

    [Fact]
    public async Task RunAsync_M0_LiveTicket_JournalWaiting_NoMutations()
    {
        // Arrange: премиграционный кластер + живая заявка app-ротации (M0-гвард).
        var rig = await NewRig();
        rig.Etcd.Seed("/kafkaworker/rotations/events",
            """{"requested_unix":1750000200,"requested_by":"t"}""");

        // Act
        var result = await rig.Migrator.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: InProgress, брокеры не тронуты, journal waiting-rotation.
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be(SecurityMigrator.MigrationOutcome.InProgress);
        rig.Driver.Removed.Should().BeEmpty();
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.Phase.Should().Be("waiting-rotation");
    }

    [Fact]
    public async Task RunAsync_M2_RecreatesAllLiveBrokers_ThenDone()
    {
        // Arrange: премиграционный кластер на ДВУХ живых брокерах, оба отвечают
        // после recreate (DescribeCluster видит обоих + контроллер).
        var rig = await NewRig(brokers: 2);
        rig.Admin.ClusterView = new KafkaClusterView(
            [new KafkaBrokerView(1, "broker1"), new KafkaBrokerView(2, "broker2")], ControllerId: 1);

        // Act
        var result = await rig.Migrator.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: InProgress (journal done — закрывающий тик), КАЖДЫЙ живой
        // брокер пересоздан с сохранением тома, env нового канона (SSL).
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be(SecurityMigrator.MigrationOutcome.InProgress);
        rig.Driver.Removed.Should().HaveCount(2)
            .And.OnlyContain(r => !r.RemoveVolume);
        rig.Driver.AllEnsured.Should().HaveCount(2);
        rig.Driver.AllEnsured.Should().OnlyContain(e =>
            e.Env["KAFKA_LISTENER_SECURITY_PROTOCOL_MAP"].Contains("INTERNAL:SASL_SSL")
            && e.Env["KAFKA_SSL_TRUSTSTORE_TYPE"] == "PEM");
        // M1: ensure дописал admin/CA-ключи etcd.
        rig.Etcd.Store["/kafka/clusters/events/admin_user"].Value.Should().Be("admin");
        rig.Etcd.Store["/kafka/clusters/events/ca_pem"].Value.Should().NotBeNull();
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.Phase.Should().Be("done");
    }

    [Fact]
    public async Task RunAsync_M3_BrokersNotReady_WaitingTick()
    {
        // Arrange: брокер не готов после recreate (DescribeCluster падает).
        var rig = await NewRig();
        rig.Admin.ClusterError = new ApplicationException("not ready");

        // Act
        var result = await rig.Migrator.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: тик успешен, InProgress, journal waiting-brokers — следующий
        // тик продолжит M3 с того же места (env реального контейнера уже SSL —
        // M2 не перезакатывает; в этом fake-драйвере env не фиксируется,
        // поэтому проверяем только фазу и отсутствие падения тика).
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be(SecurityMigrator.MigrationOutcome.InProgress);
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.Phase.Should().Be("waiting-brokers");
    }

    [Fact]
    public async Task RunAsync_M3_Ready_PutsRunningStateForRecreatedBrokers()
    {
        // Arrange: broker1 в снапшоте не RUNNING (перезакатан), кластер готов.
        var rig = await NewRig();
        rig.Etcd.Seed("/kafka/clusters/events/brokers/broker1/state", "PROVISIONING");
        rig.Admin.ClusterView = new KafkaClusterView(
            [new KafkaBrokerView(1, "broker1")], ControllerId: 1);

        // Act
        var result = await rig.Migrator.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: брокер возвращён в RUNNING и journal done.
        result.IsSuccess.Should().BeTrue();
        rig.Etcd.Store["/kafka/clusters/events/brokers/broker1/state"].Value.Should().Be("RUNNING");
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.Phase.Should().Be("done");
    }

    [Fact]
    public async Task RunAsync_CanonicalCluster_NotNeeded()
    {
        // Arrange: ensure уже выполнился, контейнеры в новом каноне.
        var rig = await NewRig();
        rig.Etcd.SeedSecurity("events");
        rig.Driver.NodeEnvs = new Dictionary<string, IReadOnlyDictionary<string, string>>
        {
            ["broker1"] = new Dictionary<string, string> { ["KAFKA_SSL_TRUSTSTORE_TYPE"] = "PEM" },
        };

        // Act
        var result = await rig.Migrator.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: NotNeeded мгновенно — ничего не тронуто.
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be(SecurityMigrator.MigrationOutcome.NotNeeded);
        rig.Driver.Removed.Should().BeEmpty();
    }
}
