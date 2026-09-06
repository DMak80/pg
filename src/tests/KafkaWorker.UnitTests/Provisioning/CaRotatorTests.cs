using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using KafkaWorker.Core.Templates;
using KafkaWorker.Provisioning.Kafka;
using KafkaWorker.Provisioning.Processes;
using KafkaWorker.Etcd.Coordination;
using Xunit;
using static KafkaWorker.UnitTests.Provisioning.Fakes;

namespace KafkaWorker.UnitTests.Provisioning;

// CaRotator (t07, arch/16 §5 K): фазы P→D→R→C окна двойного доверия — staging
// стабилен, bundle до замены сертов, rolling по одному брокеру, атомарный коммит.
public class CaRotatorTests
{
    private const string Ep = "http://etcd:2379";
    private const string Cluster = "events";

    private static readonly ProvisioningOptions Options =
        new(16000, 16999, BrokerBootSec: 100, NodeDeadSec: 90, null, "apache/kafka:4.0.0");

    private static CaRotator Sut(FakeEtcd etcd, FakeKafkaDriver driver, FakeKafkaAdminClient admin)
        => new(etcd, [Ep], driver, Claims(etcd), new WorkJournal(etcd, [Ep]),
            new FakeAdminFactory(admin), Options, new BrokerCertificateCache(), snapshot: null);

    private static ClaimStore Claims(FakeEtcd etcd)
    {
        var claims = new ClaimStore([Ep], etcd, TimeProvider.System);
        claims.TryClaimClusterAsync(Cluster, CancellationToken.None).GetAwaiter().GetResult();
        etcd.Txns.Clear(); // отсечь claim-txn: ассерты — про txn ротации
        return claims;
    }

    private static KafkaClusterSnapshot Snapshot(FakeEtcd etcd, string caPem, string caKey) => new(
        Cluster,
        new KafkaClusterConfig(2, 2, 1, 3, 604800000, 1756500000, null),
        [
            new KafkaBrokerDecl("broker1", "RUNNING", "controller", null),
            new KafkaBrokerDecl("broker2", "RUNNING", "broker", null),
        ],
        [], [], 0,
        Endpoints: "h1:16001,h1:16002",
        AppUser: "app", AppPassword: "app-pw",
        AdminUser: "admin", AdminPassword: "admin-pw",
        CaPem: caPem, CaKey: caKey);

    private static FakeKafkaAdminClient ReadyAdmin(int brokers = 2) => new()
    {
        ClusterView = new KafkaClusterView(
            Enumerable.Range(1, brokers).Select(i => new KafkaBrokerView(i, $"broker{i}")).ToList(),
            ControllerId: 1),
    };

    private static FakeEtcd SeedEtcd()
    {
        var etcd = new FakeEtcd();
        etcd.SeedSecurity(Cluster); // admin/app/CA — канон t03
        etcd.Seed($"/kafkaworker/portalloc/{Cluster}",
            """{"broker1":{"host":"h1","client":16001},"broker2":{"host":"h1","client":16002}}""");
        etcd.Seed($"/kafkaworker/ca_rotations/{Cluster}",
            """{"requested_unix":1756900100,"requested_by":"admin"}""");
        return etcd;
    }

    [Fact]
    public async Task Tick_FullCycle_CommitsNewCaAndClearsStaging()
    {
        // Arrange — заявка стоит, кластер канонический, admin отвечает 2 брокерами
        var etcd = SeedEtcd();
        var driver = new FakeKafkaDriver();
        var oldPem = etcd.Store[$"/kafka/clusters/{Cluster}/ca_pem"].Value;
        var admin = ReadyAdmin();

        // Act — один тик доводит P→D→R→C→финал (фейки мгновенны)
        var result = await Sut(etcd, driver, admin).RunAsync(Snapshot(etcd, oldPem,
            etcd.Store[$"/kafka/clusters/{Cluster}/ca_key"].Value), CancellationToken.None);

        // Assert — NEW в каноне, staging удалён, заявка снята, журнал done
        result.IsSuccess.Should().BeTrue();
        var newPem = etcd.Store[$"/kafka/clusters/{Cluster}/ca_pem"].Value;
        var newKey = etcd.Store[$"/kafka/clusters/{Cluster}/ca_key"].Value;
        newPem.Should().NotBe(oldPem);
        newKey.Should().NotBeEmpty();
        etcd.Store.Should().NotContainKey($"/kafka/clusters/{Cluster}/ca_next_key");
        etcd.Store.Should().NotContainKey($"/kafka/clusters/{Cluster}/ca_next_pem");
        etcd.Store.Should().NotContainKey($"/kafkaworker/ca_rotations/{Cluster}");
        // Канон ca_pem после коммита — ТОЛЬКО NEW (bundle свёрнут)
        newPem.Should().NotContain(oldPem, "после коммита доверие OLD снято");
        (await new WorkJournal(etcd, [Ep]).ReadAsync(Cluster, CancellationToken.None)).Value!.Op
            .Should().Be("rotate-ca");
        (await new WorkJournal(etcd, [Ep]).ReadAsync(Cluster, CancellationToken.None)).Value!.Phase
            .Should().Be("done");
        // Rolling: оба брокера пересозданы с томом (removeVolume=false)
        driver.Removed.Should().HaveCount(2)
            .And.OnlyContain(r => r.RemoveVolume == false);
        driver.AllEnsured.Should().HaveCount(2);
        // Env брокеров: truststore был bundle OLD+NEW в окне (фаза R предшествует C)
        var ensuredEnv = driver.AllEnsured[0].Env;
        ensuredEnv["KAFKA_SSL_TRUSTSTORE_CERTIFICATES"]
            .Should().Contain(oldPem.Replace("\n", "\\n"));
    }

    [Fact]
    public async Task Tick_StagingStableAcrossTicks_NoCaRegeneration()
    {
        // Arrange — docker-remove падает один раз: тик прервётся внутри R (после P/D);
        // клэйм общий для обоих тиков (один держатель)
        var etcd = SeedEtcd();
        var driver = new FakeKafkaDriver { RemoveFailsOnce = true };
        var admin = ReadyAdmin();
        var oldPem = etcd.Store[$"/kafka/clusters/{Cluster}/ca_pem"].Value;
        var claims = Claims(etcd);
        var sut = new CaRotator(etcd, [Ep], driver, claims, new WorkJournal(etcd, [Ep]),
            new FakeAdminFactory(admin), Options, new BrokerCertificateCache(), snapshot: null);

        // Act 1 — сбойный тик (P/D пройдены, R упал)
        var first = await sut.RunAsync(Snapshot(etcd, oldPem,
            etcd.Store[$"/kafka/clusters/{Cluster}/ca_key"].Value), CancellationToken.None);
        first.IsSuccess.Should().BeFalse();
        var stagingKey = etcd.Store[$"/kafka/clusters/{Cluster}/ca_next_key"].Value;

        // Act 2 — повторный тик (снапшот перечитан: ca_pem уже bundle после фазы D)
        var second = await sut.RunAsync(Snapshot(etcd,
            etcd.Store[$"/kafka/clusters/{Cluster}/ca_pem"].Value,
            etcd.Store[$"/kafka/clusters/{Cluster}/ca_key"].Value), CancellationToken.None);

        // Assert — staging тот же (в etcd, не перегенерирован после сбоя)
        second.IsSuccess.Should().BeTrue();
        etcd.Store[$"/kafka/clusters/{Cluster}/ca_next_key"].Value.Should().Be(stagingKey);
    }

    [Fact]
    public async Task Tick_NoTicket_NoMutations()
    {
        // Arrange — заявок нет
        var etcd = new FakeEtcd();
        etcd.SeedSecurity(Cluster);
        var driver = new FakeKafkaDriver();
        var admin = ReadyAdmin();

        // Act
        var result = await Sut(etcd, driver, admin).RunAsync(Snapshot(etcd,
            etcd.Store[$"/kafka/clusters/{Cluster}/ca_pem"].Value,
            etcd.Store[$"/kafka/clusters/{Cluster}/ca_key"].Value), CancellationToken.None);

        // Assert — no-op: ноль мутаций
        result.IsSuccess.Should().BeTrue();
        driver.Removed.Should().BeEmpty();
        etcd.Txns.Should().BeEmpty();
    }

    [Fact]
    public async Task Tick_ClaimNotMine_MutationsForbidden()
    {
        // Arrange — заявка есть, клэйм не взят
        var etcd = SeedEtcd();
        var driver = new FakeKafkaDriver();
        var admin = ReadyAdmin();

        // Act — CaRotator с НЕ взявшим клэйм ClaimStore
        var sut = new CaRotator(etcd, [Ep], driver,
            new ClaimStore([Ep], etcd, TimeProvider.System), new WorkJournal(etcd, [Ep]),
            new FakeAdminFactory(admin), Options, new BrokerCertificateCache(), snapshot: null);
        var result = await sut.RunAsync(Snapshot(etcd,
            etcd.Store[$"/kafka/clusters/{Cluster}/ca_pem"].Value,
            etcd.Store[$"/kafka/clusters/{Cluster}/ca_key"].Value), CancellationToken.None);

        // Assert — отказ до любых мутаций
        result.IsSuccess.Should().BeFalse();
        driver.Removed.Should().BeEmpty();
        etcd.Txns.Should().BeEmpty();
    }

    [Fact]
    public async Task Tick_PremigrationCluster_WaitsWithoutMutations()
    {
        // Arrange — нет CA/админов (премиграционный): только заявка
        var etcd = new FakeEtcd();
        etcd.Seed($"/kafkaworker/ca_rotations/{Cluster}",
            """{"requested_unix":1756900100,"requested_by":"admin"}""");
        var driver = new FakeKafkaDriver();
        var admin = ReadyAdmin();

        // Act
        var snap = Snapshot(etcd, caPem: null!, caKey: null!) with { CaPem = null, CaKey = null };
        var result = await Sut(etcd, driver, admin).RunAsync(snap, CancellationToken.None);

        // Assert — waiting-cluster, брокеры не тронуты
        result.IsSuccess.Should().BeTrue();
        driver.Removed.Should().BeEmpty();
        (await new WorkJournal(etcd, [Ep]).ReadAsync(Cluster, CancellationToken.None)).Value!.Phase
            .Should().Be("waiting-cluster");
    }

    private sealed class FakeAdminFactory(FakeKafkaAdminClient client) : IKafkaAdminClientFactory
    {
        public IKafkaAdminClient Create(string bootstrap, string user, string password, string? caPem) => client;
    }
}
