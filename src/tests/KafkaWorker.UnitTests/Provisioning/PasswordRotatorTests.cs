using FluentAssertions;
using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using KafkaWorker.Etcd.Parsing;
using KafkaWorker.Provisioning.Kafka;
using KafkaWorker.Provisioning.Processes;
using KafkaWorker.Core.Templates;

namespace KafkaWorker.UnitTests.Provisioning;

// PasswordRotator (arch/16 §5 H): фазы A/B/C без окна недоступности, отказ
// между фазами продолжает повтор, снапшоты P12 «до/после», нет заявки → no-op.

public class PasswordRotatorTests
{
    private const string Ep = "http://etcd:2379";
    private const string OldPassword = "OldPassword0123456789abcdef";

    private sealed record Rig(
        Fakes.FakeEtcd Etcd,
        Fakes.FakeKafkaDriver Driver,
        FakeKafkaAdminClient Admin,
        ClaimStore Claims,
        WorkJournal Journal,
        PasswordRotator Process,
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
        etcd.SeedSecurity("events");
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
        var claims = new ClaimStore("/kafkaworker", [Ep], etcd, TimeProvider.System);
        await claims.TryClaimClusterAsync("events", CancellationToken.None);
        var journal = new WorkJournal("/kafkaworker", etcd, [Ep]);
        var driver = new Fakes.FakeKafkaDriver();
        driver.NodeObjects.AddRange(Enumerable.Range(1, brokers).Select(k => $"kfw-events-broker{k}"));
        var admin = new FakeKafkaAdminClient();
        var snapshotPoints = new List<string>();
        var process = new PasswordRotator(
            etcd, [Ep], driver, claims, journal, new FakeAdminFactory(admin),
            ProvisioningOptions.Default, new BrokerCertificateCache(),
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
        // с [OLD, NEW1]; NEW1 стабилен на жизнь заявки — фиксирован в памяти
        // ротатора, t03-фикс: регенерация NEW каждым тиком перезкатывала бы
        // брокеров вечно, не давая кластеру собраться).
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

        // Assert-2 (специфика повторного прогона, t03-фикс): NEW пароль
        // СТАБИЛЕН на жизнь заявки — повтор фазы A НЕ пересоздаёт уже
        // перекатившегося broker1 (трек жив), а только добирает оставшихся;
        // после коммита B фаза C перекатывает всех с одиночным NEW — окно
        // «брокер не принимает закоммиченный пароль» невозможно (spec §4.2 H).
        var secondRunJaas = rig.Driver.AllEnsured
            .Skip(firstRunEnsured)
            .Select(e => (e.NodeName, Jaas: e.Env["KAFKA_LISTENER_NAME_CLIENT_PLAIN_SASL_JAAS_CONFIG"]))
            .ToList();
        // broker2 добран в фазе A с тем же NEW1 (user_app2 = финальный пароль),
        // broker1 в повторе не трогался.
        secondRunJaas.Should().ContainSingle(e => e.NodeName == "broker2"
            && e.Jaas.Contains($"user_app2=\"{finalPassword}\""));
        secondRunJaas.Where(e => e.NodeName == "broker1")
            .Should().OnlyContain(e => !e.Jaas.Contains("user_app2="));
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
        var claims = new ClaimStore("/kafkaworker", [Ep], etcd, TimeProvider.System);
        var process = new PasswordRotator(
            etcd, [Ep], new Fakes.FakeKafkaDriver(), claims, new WorkJournal("/kafkaworker", etcd, [Ep]),
            new FakeAdminFactory(new FakeKafkaAdminClient()), ProvisioningOptions.Default, new BrokerCertificateCache());

        // Act
        var result = await process.RunAsync(await Snapshot(etcd), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error!.Message.Should().Contain("клэйм не наш");
    }

    // ===== t03 Ф4: роль admin (заявка /kafkaworker/admin_rotations/<C>) =====

    [Fact]
    public async Task RunAsync_AdminTicket_RotatesAdminPasswordKeepsApp()
    {
        // Arrange: заявка /kafkaworker/admin_rotations/events; Active-кластер.
        var rig = await NewRig();
        rig.Etcd.Seed("/kafkaworker/admin_rotations/events",
            """{"requested_unix":1756500900,"requested_by":"test"}""");
        ReadyCluster(rig.Admin, 2);

        // Act: тик ротации.
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: admin_password заменён (фазы A/B/C), app_password не тронут;
        // env пересозданий несёт user_admin+user_admin2 и одиночный user_app.
        result.IsSuccess.Should().BeTrue();
        var newAdmin = rig.Etcd.Store["/kafka/clusters/events/admin_password"].Value;
        newAdmin.Should().HaveLength(32).And.NotBe("AdminPassword0123456789abcdef");
        rig.Etcd.Store["/kafka/clusters/events/app_password"].Value.Should().Be(OldPassword);
        rig.Etcd.Store.Should().NotContainKey("/kafkaworker/admin_rotations/events");
        var jaas = rig.Driver.AllEnsured
            .Select(e => e.Env["KAFKA_LISTENER_NAME_CLIENT_PLAIN_SASL_JAAS_CONFIG"])
            .ToList();
        // Фаза A: окно user_admin=OLD + user_admin2=NEW; фаза C: только NEW.
        jaas.Take(2).Should().OnlyContain(j =>
            j.Contains(@"user_admin=""AdminPassword0123456789abcdef""")
            && j.Contains($@"user_admin2=""{newAdmin}""")
            && j.Contains($@"user_app=""{OldPassword}"""));
        jaas.Skip(2).Should().OnlyContain(j =>
            j.Contains($@"user_admin=""{newAdmin}""")
            && !j.Contains("user_admin2"));
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.Phase.Should().Be("admin:done");
    }

    [Fact]
    public async Task RunAsync_AppTicketFirst_AdminTicketWaitsNextTick()
    {
        // Arrange: живы ОБЕ заявки.
        var rig = await NewRig();
        SeedRotation(rig.Etcd);
        rig.Etcd.Seed("/kafkaworker/admin_rotations/events",
            """{"requested_unix":1756500950,"requested_by":"test"}""");
        ReadyCluster(rig.Admin, 2);

        // Act: один тик.
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: исполнена ТОЛЬКО app (детерминированный порядок spec §5.2),
        // admin-заявка жива и ждёт следующего тика.
        result.IsSuccess.Should().BeTrue();
        rig.Etcd.Store.Should().NotContainKey("/kafkaworker/rotations/events");
        rig.Etcd.Store.Should().ContainKey("/kafkaworker/admin_rotations/events");
    }

    // ===== t10: приоритет H перед K + экспирация под гвардом =====

    [Fact]
    public async Task Run_CaTicketWithoutStaging_HExecutesOwnTicket()
    {
        // Arrange: живая ca-заявка (ca_rotations/events), staging ОТСУТСТВУЕТ,
        // живая заявка пароль-ротации; кластер готов.
        var rig = await NewRig();
        SeedRotation(rig.Etcd);
        rig.Etcd.Seed("/kafkaworker/ca_rotations/events",
            """{"requested_unix":1750000300,"requested_by":"it"}""");
        ReadyCluster(rig.Admin, 2);

        // Act: тик PasswordRotator.
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: H НЕ уходит в waiting — ротация доиграна: пароль NEW, заявка
        // rotations удалена, journal done; ca-заявка НЕ тронута (её снимет K).
        result.IsSuccess.Should().BeTrue();
        rig.Etcd.Store["/kafka/clusters/events/app_password"].Value
            .Should().HaveLength(32).And.NotBe(OldPassword);
        rig.Etcd.Store.Should().NotContainKey("/kafkaworker/rotations/events");
        rig.Etcd.Store.Should().ContainKey("/kafkaworker/ca_rotations/events");
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.Phase.Should().Be("done");
    }

    [Fact]
    public async Task Run_OpenCaWindow_WaitsCaWindow()
    {
        // Arrange: живые ca_next_key+ca_next_pem (окно открыто) + заявка ротации.
        var rig = await NewRig();
        SeedRotation(rig.Etcd);
        rig.Etcd.Seed("/kafka/clusters/events/ca_next_key", "next-key");
        rig.Etcd.Seed("/kafka/clusters/events/ca_next_pem", "next-pem");
        ReadyCluster(rig.Admin, 2);

        // Act: тик PasswordRotator.
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: journal-фаза waiting-ca-window; пароль OLD; брокеры не тронуты.
        result.IsSuccess.Should().BeTrue();
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.Phase.Should().Be("waiting-ca-window");
        rig.Etcd.Store["/kafka/clusters/events/app_password"].Value.Should().Be(OldPassword);
        rig.Driver.Removed.Should().BeEmpty();
    }

    [Fact]
    public async Task Run_OpenCaWindow_OldTicket_WaitsWithoutExpiry()
    {
        // Arrange: staging жив (окно K открыто) + заявка requested_unix = UtcNow-3700.
        var rig = await NewRig();
        var oldUnix = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 3700;
        rig.Etcd.Seed("/kafkaworker/rotations/events",
            $$"""{"requested_unix":{{oldUnix}},"requested_by":"it"}""");
        rig.Etcd.Seed("/kafka/clusters/events/ca_next_key", "next-key");
        rig.Etcd.Seed("/kafka/clusters/events/ca_next_pem", "next-pem");
        ReadyCluster(rig.Admin, 2);

        // Act: тик PasswordRotator.
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: точка waiting-ca-window НЕ экспирационная (тройной гвард §3.1:
        // предикат 2 ложен — staging жив) — journal waiting-ca-window, заявка ЖИВА,
        // ticket_outcomes не создаётся. Окно K доиграется, H продолжит; вечное
        // окно — зона kafka-ca-rotation-stale панели.
        result.IsSuccess.Should().BeTrue();
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.Phase.Should().Be("waiting-ca-window");
        rig.Etcd.Store.Should().ContainKey("/kafkaworker/rotations/events");
        rig.Etcd.Store.Should().NotContainKey(TicketOutcomes.Key("/kafkaworker", "events"));
    }

    [Fact]
    public async Task Run_ClusterDown_TicketOlderThanTimeout_Expired()
    {
        // Arrange: staging отсутствует (дооконная ветка); endpoints удалены из etcd
        // (кластер «не поднят»); журнал роли не в мутационной фазе; заявка старая.
        var rig = await NewRig();
        var oldUnix = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 3700;
        rig.Etcd.Seed("/kafkaworker/rotations/events",
            $$"""{"requested_unix":{{oldUnix}},"requested_by":"it"}""");
        rig.Etcd.Store.Remove("/kafka/clusters/events/endpoints");
        ReadyCluster(rig.Admin, 2);

        // Act: тик PasswordRotator.
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: Success; заявка снята; journal-фаза expired; исход
        // {"kind":"password-app","outcome":"expired","reason":"waiting-cluster",...}.
        result.IsSuccess.Should().BeTrue();
        rig.Etcd.Store.Should().NotContainKey("/kafkaworker/rotations/events");
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.Phase.Should().Be("expired");
        state.Value.LastError.Should().StartWith("ticket age=");
        var outcome = rig.Etcd.Store[TicketOutcomes.Key("/kafkaworker", "events")].Value;
        outcome.Should().Contain("\"kind\":\"password-app\"");
        outcome.Should().Contain("\"outcome\":\"expired\"");
        outcome.Should().Contain("\"reason\":\"waiting-cluster\"");
    }

    [Fact]
    public async Task Run_PhaseAInProgress_BlindCluster_OldTicket_WaitsWithoutExpiry()
    {
        // Arrange (AC3c): журнал роли phase-a (WritePhaseAsync руками), endpoints жив,
        // DescribeCluster слепой (ClusterView не задан/пуст — преф-чек не отвечает),
        // заявка старая (requested_unix = UtcNow-3700).
        var rig = await NewRig();
        var oldUnix = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 3700;
        rig.Etcd.Seed("/kafkaworker/rotations/events",
            $$"""{"requested_unix":{{oldUnix}},"requested_by":"it"}""");
        rig.Admin.ClusterError = new ApplicationException("blind");
        (await rig.Journal.WritePhaseAsync(
            "events", "rotate", "phase-a", rig.Claims.InstanceId, null, CancellationToken.None))
            .IsSuccess.Should().BeTrue();

        // Act: тик PasswordRotator.
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: третий предикат гварда ложен (мутационная фаза роли жива —
        // брокеры несут JAAS [OLD, NEW]) — заявка ЖИВА, исход тика waiting-cluster
        // БЕЗ экспирации (слепота в середине A — передержка до сходимости).
        result.IsSuccess.Should().BeTrue();
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.Phase.Should().Be("waiting-cluster");
        rig.Etcd.Store.Should().ContainKey("/kafkaworker/rotations/events");
        rig.Etcd.Store.Should().NotContainKey(TicketOutcomes.Key("/kafkaworker", "events"));
    }

    [Fact]
    public async Task Run_AfterCommitTailWithoutEndpoints_NoOpWithoutExpiry()
    {
        // Arrange: afterCommit-хвост — заявки НЕТ (del фазой B), journal
        // rotate/phase-c; endpoints удалены (кластер «не поднят»).
        var rig = await NewRig();
        (await rig.Journal.WritePhaseAsync(
            "events", "rotate", "phase-c", rig.Claims.InstanceId, null, CancellationToken.None))
            .IsSuccess.Should().BeTrue();
        rig.Etcd.Store.Remove("/kafka/clusters/events/endpoints");

        // Act: тик PasswordRotator.
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: Success, роль НЕ обрабатывалась (исход функции как раньше —
        // передержка хвоста); НИ journal-записи, НИ экспирации, НИ NRE;
        // исхода ticket_outcomes нет.
        result.IsSuccess.Should().BeTrue();
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.Phase.Should().Be("phase-c");
        rig.Etcd.Store.Should().NotContainKey(TicketOutcomes.Key("/kafkaworker", "events"));
    }

    [Fact]
    public async Task Run_AfterCommitTailAndOpenCaWindow_WaitingWithoutExpiry()
    {
        // Arrange: afterCommit-хвост (заявки нет) + живой staging ca_next_*.
        var rig = await NewRig();
        (await rig.Journal.WritePhaseAsync(
            "events", "rotate", "phase-c", rig.Claims.InstanceId, null, CancellationToken.None))
            .IsSuccess.Should().BeTrue();
        rig.Etcd.Seed("/kafka/clusters/events/ca_next_key", "next-key");
        rig.Etcd.Seed("/kafka/clusters/events/ca_next_pem", "next-pem");
        ReadyCluster(rig.Admin, 2);

        // Act: тик PasswordRotator.
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: journal-фаза waiting-ca-window (Kv?-ветка — экспирации нет);
        // ticket_outcomes не создаётся.
        result.IsSuccess.Should().BeTrue();
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.Phase.Should().Be("waiting-ca-window");
        rig.Etcd.Store.Should().NotContainKey(TicketOutcomes.Key("/kafkaworker", "events"));
    }

    [Fact]
    public async Task Run_ClusterDownFreshTicket_WaitingCluster()
    {
        // Arrange: staging нет; endpoints нет; заявка свежая (age < порога).
        var rig = await NewRig();
        var freshUnix = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 10;
        rig.Etcd.Seed("/kafkaworker/rotations/events",
            $$"""{"requested_unix":{{freshUnix}},"requested_by":"it"}""");
        rig.Etcd.Store.Remove("/kafka/clusters/events/endpoints");

        // Act: тик PasswordRotator.
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: journal waiting-cluster; заявка жива; исхода нет.
        result.IsSuccess.Should().BeTrue();
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.Phase.Should().Be("waiting-cluster");
        rig.Etcd.Store.Should().ContainKey("/kafkaworker/rotations/events");
        rig.Etcd.Store.Should().NotContainKey(TicketOutcomes.Key("/kafkaworker", "events"));
    }

    [Fact]
    public async Task Run_ClusterDownBrokenPayload_NoExpiry()
    {
        // Arrange: staging нет; endpoints нет; payload заявки {"requested_by":"x"}
        // (без requested_unix) — возраст «неизвестен».
        var rig = await NewRig();
        rig.Etcd.Seed("/kafkaworker/rotations/events", """{"requested_by":"x"}""");
        rig.Etcd.Store.Remove("/kafka/clusters/events/endpoints");

        // Act: тик PasswordRotator.
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: waiting-cluster; заявка жива (параноидальный отказ).
        result.IsSuccess.Should().BeTrue();
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.Phase.Should().Be("waiting-cluster");
        rig.Etcd.Store.Should().ContainKey("/kafkaworker/rotations/events");
        rig.Etcd.Store.Should().NotContainKey(TicketOutcomes.Key("/kafkaworker", "events"));
    }

    [Fact]
    public async Task Run_FullRotation_WritesDoneOutcome()
    {
        // Arrange: живой заявкой (существующий кейс FullRotation) + чтение исхода.
        var rig = await NewRig();
        SeedRotation(rig.Etcd);
        ReadyCluster(rig.Admin, 2);

        // Act: тик PasswordRotator (полная ротация A/B/C).
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: после done в /kafkaworker/ticket_outcomes/events исход
        // {"kind":"password-app","outcome":"done","requested_unix":1750000200,...}.
        result.IsSuccess.Should().BeTrue();
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.Phase.Should().Be("done");
        var outcome = rig.Etcd.Store[TicketOutcomes.Key("/kafkaworker", "events")].Value;
        outcome.Should().Contain("\"kind\":\"password-app\"");
        outcome.Should().Contain("\"outcome\":\"done\"");
        outcome.Should().Contain("\"requested_unix\":1750000200");
    }
}
