using FluentAssertions;
using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using KafkaWorker.Etcd.Coordination;
using KafkaWorker.Etcd.Parsing;
using KafkaWorker.Provisioning.Kafka;
using KafkaWorker.Provisioning.Processes;

namespace KafkaWorker.UnitTests.Provisioning;

// AppPasswordRotator (arch/16 §5 H): фазы A/B/C без окна недоступности, отказ
// между фазами продолжает повтор, снапшоты P12 «до/после», нет заявки → no-op.

public class AppPasswordRotatorTests
{
    private const string Ep = "http://etcd:2379";
    private const string OldPassword = "OldPassword0123456789abcdef";

    private sealed record Rig(
        Fakes.FakeEtcd Etcd,
        Fakes.FakeKafkaDriver Driver,
        FakeKafkaAdminClient Admin,
        ClaimStore Claims,
        WorkJournal Journal,
        AppPasswordRotator Process,
        List<string> SnapshotPoints);

    private static void SeedActive(Fakes.FakeEtcd etcd, int brokers = 2)
    {
        etcd.Seed("/kafka/clusters/events/config",
            $$"""{"brokers":{{brokers}},"replication_factor":1,"min_insync_replicas":1,"default_partitions":12,"default_retention_ms":604800000,"created_unix":1756500000}""");
        for (var k = 1; k <= brokers; k++)
        {
            etcd.Seed($"/kafka/clusters/events/brokers/broker{k}/state", "RUNNING");
            etcd.Seed($"/kafka/clusters/events/brokers/broker{k}/role", k <= Math.Min(3, brokers) ? "controller" : "broker");
        }

        etcd.Seed("/kafka/clusters/events/endpoints", "h1:16000,h1:16001");
        etcd.Seed("/kafka/clusters/events/app_user", "app");
        etcd.Seed("/kafka/clusters/events/app_password", OldPassword);
        etcd.Seed("/kafkaworker/portalloc/events",
            """{"broker1":{"host":"h1","client":16000},"broker2":{"host":"h1","client":16001}}""");
    }

    private static async Task<KafkaClusterSnapshot> Snapshot(Fakes.FakeEtcd etcd)
    {
        var range = await etcd.RangeAsync(Ep, "/kafka/clusters/", CancellationToken.None);
        return KafkaSnapshotParser.Parse(range.Value).Value.Single(c => c.Cluster == "events");
    }

    private static async Task<Rig> NewRig(int brokers = 2)
    {
        var etcd = new Fakes.FakeEtcd();
        SeedActive(etcd, brokers);
        var claims = new ClaimStore([Ep], etcd, TimeProvider.System);
        await claims.TryClaimClusterAsync("events", CancellationToken.None);
        var journal = new WorkJournal(etcd, [Ep]);
        var driver = new Fakes.FakeKafkaDriver();
        driver.NodeObjects.AddRange(Enumerable.Range(1, brokers).Select(k => $"kfw-events-broker{k}"));
        var admin = new FakeKafkaAdminClient();
        var snapshotPoints = new List<string>();
        var process = new AppPasswordRotator(
            etcd, [Ep], driver, claims, journal, new FakeAdminFactory(admin),
            ProvisioningOptions.Default,
            snapshot: ct =>
            {
                snapshotPoints.Add($"n{snapshotPoints.Count}");
                return Task.FromResult(Result.Success());
            });
        return new Rig(etcd, driver, admin, claims, journal, process, snapshotPoints);
    }

    private sealed class FakeAdminFactory(FakeKafkaAdminClient client) : IKafkaAdminClientFactory
    {
        public IKafkaAdminClient Create(string bootstrap, string user, string password, string? caPem) => client;
    }

    private static void ReadyCluster(FakeKafkaAdminClient admin, int brokers)
        => admin.ClusterView = new KafkaClusterView(
            Enumerable.Range(1, brokers).Select(i => new KafkaBrokerView(i, $"broker{i}")).ToList(),
            ControllerId: 1);

    private static void SeedRotation(Fakes.FakeEtcd etcd)
        => etcd.Seed("/kafkaworker/rotations/events",
            """{"requested_unix":1750000200,"requested_by":"admin"}""");

    [Fact]
    public async Task Run_NoTicket_NoOp()
    {
        // Arrange: заявки ротации нет.
        var rig = await NewRig();
        ReadyCluster(rig.Admin, 2);

        // Act
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: ничего не делаем — ни docker, ни etcd, ни снапшотов.
        result.IsSuccess.Should().BeTrue();
        rig.Driver.Removed.Should().BeEmpty();
        rig.SnapshotPoints.Should().BeEmpty();
        rig.Etcd.Store["/kafka/clusters/events/app_password"].Value.Should().Be(OldPassword);
    }

    [Fact]
    public async Task Run_FullRotation_PhasesABCWithSnapshots()
    {
        // Arrange: живая заявка; кластер готов.
        var rig = await NewRig();
        SeedRotation(rig.Etcd);
        ReadyCluster(rig.Admin, 2);

        // Act
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: фазы A→B→C за прогон; каждый брокер пересоздан ДВАЖДЫ
        // (A: JAAS old+new; C: JAAS new); пароль в etcd заменён; заявка удалена;
        // снапшоты «до» (старт) и «после» (финал).
        result.IsSuccess.Should().BeTrue();
        rig.Driver.Removed.Count(r => r.RemoveVolume == false).Should().Be(4); // 2 брокера × 2 фазы
        rig.Driver.Removed.Should().NotContain(r => r.RemoveVolume);

        // env пересозданий: A — два креда, C — один новый (AllEnsured: rolling
        // дедуплицирует Ensured по имени — фазы различны только в полном логе).
        var newPassword = rig.Etcd.Store["/kafka/clusters/events/app_password"].Value;
        newPassword.Should().HaveLength(32).And.NotBe(OldPassword);
        var jaas = rig.Driver.AllEnsured
            .Select(e => (e.NodeName, Jaas: e.Env["KAFKA_LISTENER_NAME_CLIENT_PLAIN_SASL_JAAS_CONFIG"]))
            .ToList();
        jaas.Should().HaveCount(4); // 2 брокера × 2 фазы
        jaas.Take(2).Should().OnlyContain(j => j.Jaas.Contains($"user_app=\"{OldPassword}\"")
                                               && j.Jaas.Contains($"user_app2=\"{newPassword}\""));
        jaas.Skip(2).Should().OnlyContain(j => j.Jaas.Contains($"user_app=\"{newPassword}\"")
                                               && !j.Jaas.Contains("user_app2"));

        rig.Etcd.Store.Should().NotContainKey("/kafkaworker/rotations/events");
        rig.SnapshotPoints.Should().HaveCount(2); // «до» и «после»
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.Phase.Should().Be("done");
    }

    [Fact]
    public async Task Run_FailureDuringPhaseA_RestartsSafely()
    {
        // Arrange: заявка есть; docker падает на пересоздании broker2 (фаза A).
        var rig = await NewRig();
        SeedRotation(rig.Etcd);
        ReadyCluster(rig.Admin, 2);
        var failNext = true;
        rig.Driver.EnsureResultByNode = node =>
            failNext && node == "broker2"
                ? Result.Failed(new ApplicationException("docker down"))
                : Result.Success();

        // Act: первый прогон падает посреди фазы A (broker1 уже пересоздан
        // с [OLD, NEW1]; NEW1 нигде не зафиксирован — генерация в памяти).
        var first = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);
        first.IsSuccess.Should().BeFalse();
        rig.Etcd.Store["/kafka/clusters/events/app_password"].Value.Should().Be(OldPassword);
        var firstRunEnsured = rig.Driver.AllEnsured.Count; // orphan-запись [OLD, NEW1]

        // Act-2: docker ожил — повтор доводит ротацию до конца.
        failNext = false;
        var second = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: пароль заменён, заявка снята, фазы завершены (окно A держало
        // оба креда — клиенты со старым паролем работали всё время до B).
        second.IsSuccess.Should().BeTrue();
        var finalPassword = rig.Etcd.Store["/kafka/clusters/events/app_password"].Value;
        finalPassword.Should().HaveLength(32);
        rig.Etcd.Store.Should().NotContainKey("/kafkaworker/rotations/events");

        // Assert-2 (специфика повторного прогона): ЕДИНЫЙ новый пароль на ВСЕХ
        // брокерах к моменту B. Повтор фазы A генерирует NEW2 и обязан
        // пересоздать ВСЕХ с [OLD, NEW2] (трек _rolled прошлой попытки
        // сбрасывается): иначе broker1 остался бы с [OLD, NEW1] и после
        // коммита NEW2 отбрасывал SASL-логины NEW2 до конца фазы C — окно
        // недоступности, невозможное по построению (spec §4.2 H, §9.4).
        var secondRunJaas = rig.Driver.AllEnsured
            .Skip(firstRunEnsured)
            .Select(e => e.Env["KAFKA_LISTENER_NAME_CLIENT_PLAIN_SASL_JAAS_CONFIG"])
            .ToList();
        secondRunJaas.Where(j => j.Contains("user_app2="))
            .Should().HaveCount(2, "повтор фазы A пересоздаёт ВСЕХ брокеров (трек сброшен)");
        secondRunJaas.Should().OnlyContain(
            j => !j.Contains("user_app2=") || j.Contains($"user_app2=\"{finalPassword}\""),
            "все пересоздания фазы A повтора несут закоммиченный в B пароль");
    }

    [Fact]
    public async Task Run_FailureBetweenBAndC_ContinuesByJournal()
    {
        // Arrange: B прошла (заявка удалена txn'ом), но C упала — docker-сбой
        // в фазе C того же прогона.
        var rig = await NewRig();
        SeedRotation(rig.Etcd);
        ReadyCluster(rig.Admin, 2);
        rig.Etcd.OnPut = key =>
        {
            if (key == "/kafka/clusters/events/app_password")
            {
                // Фаза B только что закоммитила NEW и удалила заявку: ломаем C.
                rig.Driver.EnsureResultByNode = _ => Result.Failed(new ApplicationException("docker down in C"));
                rig.Etcd.OnPut = null;
            }
        };

        // Act: прогон проходит A и B, падает в C.
        var first = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);
        first.IsSuccess.Should().BeFalse();
        var newPassword = rig.Etcd.Store["/kafka/clusters/events/app_password"].Value;
        newPassword.Should().NotBe(OldPassword);
        rig.Etcd.Store.Should().NotContainKey("/kafkaworker/rotations/events");

        // Act-2: docker ожил; заявки уже НЕТ — продолжение только по journal-фазе.
        rig.Driver.EnsureResultByNode = null;
        var second = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: фаза C доведена (снятие OLD-пользователя), финал done.
        second.IsSuccess.Should().BeTrue();
        var phaseC = rig.Driver.AllEnsured
            .Where(e => e.NodeName == "broker1").Last()
            .Env["KAFKA_LISTENER_NAME_CLIENT_PLAIN_SASL_JAAS_CONFIG"];
        phaseC.Should().Contain($"user_app=\"{newPassword}\"").And.NotContain("user_app2");
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.Phase.Should().Be("done");
    }

    [Fact]
    public async Task Run_ClusterNotReady_WaitsNextTick()
    {
        // Arrange: заявка есть, но кластер не отвечает DescribeCluster (поднятие).
        var rig = await NewRig();
        SeedRotation(rig.Etcd);
        rig.Admin.ClusterError = new ApplicationException("not ready");

        // Act
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: пароль не тронут (фаза B требует живого кластера), брокеры
        // не пересоздаются; успех (InProgress — следующий тик повторит).
        result.IsSuccess.Should().BeTrue();
        rig.Etcd.Store["/kafka/clusters/events/app_password"].Value.Should().Be(OldPassword);
        rig.Driver.Removed.Should().BeEmpty();
    }

    [Fact]
    public async Task Run_NotClaimed_Refuses()
    {
        // Arrange: клэйм чужой.
        var etcd = new Fakes.FakeEtcd();
        SeedActive(etcd);
        SeedRotation(etcd);
        var claims = new ClaimStore([Ep], etcd, TimeProvider.System);
        var process = new AppPasswordRotator(
            etcd, [Ep], new Fakes.FakeKafkaDriver(), claims, new WorkJournal(etcd, [Ep]),
            new FakeAdminFactory(new FakeKafkaAdminClient()), ProvisioningOptions.Default);

        // Act
        var result = await process.RunAsync(await Snapshot(etcd), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error!.Message.Should().Contain("клэйм не наш");
    }
}
