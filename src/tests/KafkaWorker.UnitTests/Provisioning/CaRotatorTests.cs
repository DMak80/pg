using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using KafkaWorker.Core.Templates;
using KafkaWorker.Provisioning.Kafka;
using KafkaWorker.Provisioning.Processes;
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
        => new(etcd, [Ep], driver, Claims(etcd), new WorkJournal("/kafkaworker", etcd, [Ep]),
            new FakeAdminFactory(admin), Options, new BrokerCertificateCache(), snapshot: null);

    private static ClaimStore Claims(FakeEtcd etcd)
    {
        var claims = new ClaimStore("/kafkaworker", [Ep], etcd, TimeProvider.System);
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
        (await new WorkJournal("/kafkaworker", etcd, [Ep]).ReadAsync(Cluster, CancellationToken.None)).Value!.Op
            .Should().Be("rotate-ca");
        (await new WorkJournal("/kafkaworker", etcd, [Ep]).ReadAsync(Cluster, CancellationToken.None)).Value!.Phase
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
        // Arrange — docker-remove падает один раз: первый тик прервётся внутри R
        // (P/D пройдены, staging в etcd); клэйм общий для обоих тиков
        var etcd = SeedEtcd();
        var driver = new FakeKafkaDriver { RemoveFailsOnce = true };
        var admin = ReadyAdmin();
        var oldPem = etcd.Store[$"/kafka/clusters/{Cluster}/ca_pem"].Value;
        var claims = Claims(etcd);
        var sut = new CaRotator(etcd, [Ep], driver, claims, new WorkJournal("/kafkaworker", etcd, [Ep]),
            new FakeAdminFactory(admin), Options, new BrokerCertificateCache(), snapshot: null);

        // Act 1 — сбойный тик (P/D пройдены, R упал на первом RemoveNode)
        var first = await sut.RunAsync(Snapshot(etcd, oldPem,
            etcd.Store[$"/kafka/clusters/{Cluster}/ca_key"].Value), CancellationToken.None);
        first.IsSuccess.Should().BeFalse($"err={first.Error?.Message}");
        etcd.Store.TryGetValue($"/kafka/clusters/{Cluster}/ca_next_key", out var stagingEntry);
        stagingEntry.Should().NotBeNull($"staging создан в P (err={first.Error?.Message})");
        var stagingKey = stagingEntry!.Value;

        // Act 2 — повторный тик на несошедшемся кластере (view: 1 брокер): R ждёт,
        // коммита нет — staging обязан остаться ТЕМ ЖЕ (не перегенерирован)
        var converging = new CaRotator(etcd, [Ep], driver, claims, new WorkJournal("/kafkaworker", etcd, [Ep]),
            new FakeAdminFactory(ReadyAdmin(1)), Options, new BrokerCertificateCache(), snapshot: null);
        var second = await converging.RunAsync(Snapshot(etcd,
            etcd.Store[$"/kafka/clusters/{Cluster}/ca_pem"].Value,
            etcd.Store[$"/kafka/clusters/{Cluster}/ca_key"].Value), CancellationToken.None);

        // Assert — тик успешен (waiting-режим), staging стабилен, заявка жива
        second.IsSuccess.Should().BeTrue($"err={second.Error?.Message}");
        etcd.Store.TryGetValue($"/kafka/clusters/{Cluster}/ca_next_key", out var afterSecond);
        afterSecond.Should().NotBeNull("ротация не завершена — staging жив");
        afterSecond!.Value.Should().Be(stagingKey, "стейджинг в etcd переживает тики");
        etcd.Store.Should().ContainKey($"/kafka/clusters/{Cluster}/ca_next_pem");
        etcd.Store.Should().ContainKey($"/kafkaworker/ca_rotations/{Cluster}");
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
            new ClaimStore("/kafkaworker", [Ep], etcd, TimeProvider.System), new WorkJournal("/kafkaworker", etcd, [Ep]),
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
            $$"""{"requested_unix":{{DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 10}},"requested_by":"admin"}"""); // свежая: старую K0.5-точка сняла бы экспирацией (t10)
        var driver = new FakeKafkaDriver();
        var admin = ReadyAdmin();

        // Act
        var snap = Snapshot(etcd, caPem: null!, caKey: null!) with { CaPem = null, CaKey = null };
        var result = await Sut(etcd, driver, admin).RunAsync(snap, CancellationToken.None);

        // Assert — waiting-cluster, брокеры не тронуты
        result.IsSuccess.Should().BeTrue();
        driver.Removed.Should().BeEmpty();
        (await new WorkJournal("/kafkaworker", etcd, [Ep]).ReadAsync(Cluster, CancellationToken.None)).Value!.Phase
            .Should().Be("waiting-cluster");
    }

    private sealed class FakeAdminFactory(FakeKafkaAdminClient client) : IKafkaAdminClientFactory
    {
        public IKafkaAdminClient Create(string bootstrap, string user, string password, string? caPem) => client;
    }

    // ===== t10: экспирация дооконных waiting-точек; окно уводит в доигрывание =====

    [Fact]
    public async Task Run_ClusterDown_OldCaTicket_Expired()
    {
        // Arrange — дооконная ветка: endpoints нет (кластер не поднят); заявка старая.
        var etcd = SeedEtcd();
        var snap = Snapshot(etcd, etcd.Store[$"/kafka/clusters/{Cluster}/ca_pem"].Value,
            etcd.Store[$"/kafka/clusters/{Cluster}/ca_key"].Value) with { Endpoints = null };

        // Act — тик CaRotator.
        var result = await Sut(etcd, new FakeKafkaDriver(), ReadyAdmin()).RunAsync(snap, CancellationToken.None);

        // Assert — заявка снята; исход expired kind=ca reason=waiting-cluster; Success.
        result.IsSuccess.Should().BeTrue();
        etcd.Store.Should().NotContainKey($"/kafkaworker/ca_rotations/{Cluster}");
        var outcome = etcd.Store[$"/kafkaworker/ticket_outcomes/{Cluster}"].Value;
        outcome.Should().Contain("\"kind\":\"ca\"")
            .And.Contain("\"outcome\":\"expired\"")
            .And.Contain("\"reason\":\"waiting-cluster\"");
        (await new WorkJournal("/kafkaworker", etcd, [Ep]).ReadAsync(Cluster, CancellationToken.None)).Value!.Phase
            .Should().Be("expired");
    }

    [Fact]
    public async Task Run_PasswordRotationAlive_OldCaTicket_Expired()
    {
        // Arrange — дооконная ветка: живая пароль-ротация (заявка rotations/<C>).
        var etcd = SeedEtcd();
        etcd.Seed($"/kafkaworker/rotations/{Cluster}",
            """{"requested_unix":1756900200,"requested_by":"it"}""");

        // Act — тик CaRotator.
        var result = await Sut(etcd, new FakeKafkaDriver(), ReadyAdmin())
            .RunAsync(Snapshot(etcd, etcd.Store[$"/kafka/clusters/{Cluster}/ca_pem"].Value,
                etcd.Store[$"/kafka/clusters/{Cluster}/ca_key"].Value), CancellationToken.None);

        // Assert — expired reason=waiting-password-rotation.
        result.IsSuccess.Should().BeTrue();
        etcd.Store.Should().NotContainKey($"/kafkaworker/ca_rotations/{Cluster}");
        var outcome = etcd.Store[$"/kafkaworker/ticket_outcomes/{Cluster}"].Value;
        outcome.Should().Contain("\"reason\":\"waiting-password-rotation\"");
    }

    [Fact]
    public async Task Run_RotateJournalExpired_NoPasswordTickets_PlaysThrough()
    {
        // Arrange — журнал rotate в ТЕРМИНАЛЬНОЙ фазе expired (пароль-заявку
        // сняла экспирация), парольных заявок нет; ca-заявка свежая. Expired
        // пишется только вне мутаций — гвард «живая пароль-ротация» обязан
        // пропустить терминальную фазу (иначе свежая ca-заявка крутилась бы
        // в waiting-password-rotation до собственного таймаута).
        var etcd = SeedEtcd();
        etcd.Seed($"/kafkaworker/ca_rotations/{Cluster}",
            $$"""{"requested_unix":{{DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 10}},"requested_by":"it"}""");
        await new WorkJournal("/kafkaworker", etcd, [Ep]).WritePhaseAsync(
            Cluster, "rotate", "expired", "inst1", null, CancellationToken.None);
        var driver = new FakeKafkaDriver();

        // Act — тик CaRotator.
        var result = await Sut(etcd, driver, ReadyAdmin())
            .RunAsync(Snapshot(etcd, etcd.Store[$"/kafka/clusters/{Cluster}/ca_pem"].Value,
                etcd.Store[$"/kafka/clusters/{Cluster}/ca_key"].Value), CancellationToken.None);

        // Assert — НЕ waiting-password-rotation: K идёт дальше по ветке и
        // доигрывает ротацию до конца (done + исход done).
        result.IsSuccess.Should().BeTrue();
        etcd.Store.Should().NotContainKey($"/kafkaworker/ca_rotations/{Cluster}");
        etcd.Store[$"/kafkaworker/ticket_outcomes/{Cluster}"].Value
            .Should().Contain("\"outcome\":\"done\"");
        (await new WorkJournal("/kafkaworker", etcd, [Ep]).ReadAsync(Cluster, CancellationToken.None)).Value!.Phase
            .Should().Be("done");
        driver.Removed.Should().HaveCount(2);
    }

    [Theory]
    [InlineData("phase-a")]
    [InlineData("rotated-commit")]
    [InlineData("phase-c")]
    [InlineData("admin:phase-a")]
    public async Task Run_RotateJournalMutationPhase_WaitsPasswordRotation(string phase)
    {
        // Arrange — журнал rotate в МУТАЦИОННОЙ фазе роли (брокеры несут JAAS
        // [OLD, NEW] либо окно C не закрыто; admin:-префикс — та же роль);
        // парольных заявок нет; ca-заявка свежая.
        var etcd = SeedEtcd();
        etcd.Seed($"/kafkaworker/ca_rotations/{Cluster}",
            $$"""{"requested_unix":{{DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 10}},"requested_by":"it"}""");
        await new WorkJournal("/kafkaworker", etcd, [Ep]).WritePhaseAsync(
            Cluster, "rotate", phase, "inst1", null, CancellationToken.None);

        // Act — тик CaRotator.
        var result = await Sut(etcd, new FakeKafkaDriver(), ReadyAdmin())
            .RunAsync(Snapshot(etcd, etcd.Store[$"/kafka/clusters/{Cluster}/ca_pem"].Value,
                etcd.Store[$"/kafka/clusters/{Cluster}/ca_key"].Value), CancellationToken.None);

        // Assert — waiting-password-rotation; ca-заявка жива; исхода нет.
        result.IsSuccess.Should().BeTrue();
        (await new WorkJournal("/kafkaworker", etcd, [Ep]).ReadAsync(Cluster, CancellationToken.None)).Value!.Phase
            .Should().Be("waiting-password-rotation");
        etcd.Store.Should().ContainKey($"/kafkaworker/ca_rotations/{Cluster}");
        etcd.Store.Should().NotContainKey($"/kafkaworker/ticket_outcomes/{Cluster}");
    }

    [Fact]
    public async Task Run_ReassignmentAlive_OldCaTicket_Expired()
    {
        // Arrange — дооконная ветка: живой reassignments-прогресс (режим balance).
        var etcd = SeedEtcd();
        etcd.Seed($"/kafkaworker/reassignments/{Cluster}", """{"mode":"balance"}""");

        // Act — тик CaRotator.
        var result = await Sut(etcd, new FakeKafkaDriver(), ReadyAdmin())
            .RunAsync(Snapshot(etcd, etcd.Store[$"/kafka/clusters/{Cluster}/ca_pem"].Value,
                etcd.Store[$"/kafka/clusters/{Cluster}/ca_key"].Value), CancellationToken.None);

        // Assert — expired reason=waiting-reassignment.
        result.IsSuccess.Should().BeTrue();
        etcd.Store.Should().NotContainKey($"/kafkaworker/ca_rotations/{Cluster}");
        var outcome = etcd.Store[$"/kafkaworker/ticket_outcomes/{Cluster}"].Value;
        outcome.Should().Contain("\"reason\":\"waiting-reassignment\"");
    }

    [Fact]
    public async Task Run_BlindPrecheck_OldCaTicket_Expired()
    {
        // Arrange — дооконная ветка: поля живы, DescribeCluster слепой (без ClusterView).
        var etcd = SeedEtcd();
        var admin = new FakeKafkaAdminClient(); // DescribeCluster → Failed

        // Act — тик CaRotator.
        var result = await Sut(etcd, new FakeKafkaDriver(), admin)
            .RunAsync(Snapshot(etcd, etcd.Store[$"/kafka/clusters/{Cluster}/ca_pem"].Value,
                etcd.Store[$"/kafka/clusters/{Cluster}/ca_key"].Value), CancellationToken.None);

        // Assert — expired reason=waiting-cluster.
        result.IsSuccess.Should().BeTrue();
        etcd.Store.Should().NotContainKey($"/kafkaworker/ca_rotations/{Cluster}");
        var outcome = etcd.Store[$"/kafkaworker/ticket_outcomes/{Cluster}"].Value;
        outcome.Should().Contain("\"reason\":\"waiting-cluster\"");
    }

    [Fact]
    public async Task Run_StagingAliveNoEndpoints_OldCaTicket_NotExpired()
    {
        // Arrange — AC3a: staging жив ∧ endpoints НЕТ ∧ заявка старая.
        var etcd = SeedEtcd();
        etcd.Seed($"/kafka/clusters/{Cluster}/ca_next_key", UnitCa.CaKey); // валидный PEM: окно доигрывается с R-пересозданием
        etcd.Seed($"/kafka/clusters/{Cluster}/ca_next_pem", UnitCa.CaPem);
        var snap = Snapshot(etcd, etcd.Store[$"/kafka/clusters/{Cluster}/ca_pem"].Value,
            etcd.Store[$"/kafka/clusters/{Cluster}/ca_key"].Value) with { Endpoints = null };

        // Act — тик CaRotator.
        var result = await Sut(etcd, new FakeKafkaDriver(), ReadyAdmin()).RunAsync(snap, CancellationToken.None);

        // Assert — window-open ветка: заявка НЕ снята (доигрывание/передержка,
        // мимо экспирационных точек), ticket_outcomes нет, journal ≠ expired.
        result.IsSuccess.Should().BeTrue();
        etcd.Store.Should().ContainKey($"/kafkaworker/ca_rotations/{Cluster}");
        etcd.Store.Should().NotContainKey($"/kafkaworker/ticket_outcomes/{Cluster}");
        (await new WorkJournal("/kafkaworker", etcd, [Ep]).ReadAsync(Cluster, CancellationToken.None)).Value!.Phase
            .Should().NotBe("expired");
    }

    [Fact]
    public async Task Run_StagingAlivePasswordTicket_OldCaTicket_NotExpired()
    {
        // Arrange — AC3b: staging жив ∧ живая пароль-заявка ∧ ca-заявка старая.
        var etcd = SeedEtcd();
        etcd.Seed($"/kafka/clusters/{Cluster}/ca_next_key", UnitCa.CaKey); // валидный PEM: окно доигрывается с R-пересозданием
        etcd.Seed($"/kafka/clusters/{Cluster}/ca_next_pem", UnitCa.CaPem);
        etcd.Seed($"/kafkaworker/rotations/{Cluster}",
            """{"requested_unix":1756900200,"requested_by":"it"}""");
        // R не сходится (view 1 брокер из 2) — окно остаётся открытым на передержке.
        var admin = ReadyAdmin(1);

        // Act — тик CaRotator.
        var result = await Sut(etcd, new FakeKafkaDriver(), admin)
            .RunAsync(Snapshot(etcd, etcd.Store[$"/kafka/clusters/{Cluster}/ca_pem"].Value,
                etcd.Store[$"/kafka/clusters/{Cluster}/ca_key"].Value), CancellationToken.None);

        // Assert — window-open ветка: ca-заявка НЕ снята, ticket_outcomes нет.
        result.IsSuccess.Should().BeTrue();
        etcd.Store.Should().ContainKey($"/kafkaworker/ca_rotations/{Cluster}");
        etcd.Store.Should().NotContainKey($"/kafkaworker/ticket_outcomes/{Cluster}");
    }

    [Fact]
    public async Task Run_FreshCaTicket_WaitingCluster()
    {
        // Arrange — дооконная ветка: endpoints нет; возраст < порога.
        var etcd = SeedEtcd();
        etcd.Seed($"/kafkaworker/ca_rotations/{Cluster}",
            $$"""{"requested_unix":{{DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 10}},"requested_by":"it"}""");
        var snap = Snapshot(etcd, etcd.Store[$"/kafka/clusters/{Cluster}/ca_pem"].Value,
            etcd.Store[$"/kafka/clusters/{Cluster}/ca_key"].Value) with { Endpoints = null };

        // Act — тик CaRotator.
        var result = await Sut(etcd, new FakeKafkaDriver(), ReadyAdmin()).RunAsync(snap, CancellationToken.None);

        // Assert — journal waiting-cluster, заявка жива.
        result.IsSuccess.Should().BeTrue();
        etcd.Store.Should().ContainKey($"/kafkaworker/ca_rotations/{Cluster}");
        (await new WorkJournal("/kafkaworker", etcd, [Ep]).ReadAsync(Cluster, CancellationToken.None)).Value!.Phase
            .Should().Be("waiting-cluster");
        etcd.Store.Should().NotContainKey($"/kafkaworker/ticket_outcomes/{Cluster}");
    }

    [Fact]
    public async Task Run_FullRotation_WritesDoneOutcome()
    {
        // Arrange — живая заявка, кластер канонический.
        var etcd = SeedEtcd();
        var driver = new FakeKafkaDriver();
        var oldPem = etcd.Store[$"/kafka/clusters/{Cluster}/ca_pem"].Value;

        // Act — один тик доводит P→D→R→C→финал (фейки мгновенны).
        var result = await Sut(etcd, driver, ReadyAdmin()).RunAsync(Snapshot(etcd, oldPem,
            etcd.Store[$"/kafka/clusters/{Cluster}/ca_key"].Value), CancellationToken.None);

        // Assert — ticket_outcomes outcome=done kind=ca.
        result.IsSuccess.Should().BeTrue();
        var outcome = etcd.Store[$"/kafkaworker/ticket_outcomes/{Cluster}"].Value;
        outcome.Should().Contain("\"kind\":\"ca\"").And.Contain("\"outcome\":\"done\"");
    }
}
