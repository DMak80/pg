using System.Text.Json;
using FluentAssertions;
using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using KafkaWorker.Etcd.Coordination;
using KafkaWorker.Etcd.Parsing;
using KafkaWorker.Provisioning.Kafka;
using KafkaWorker.Provisioning.Processes;

namespace KafkaWorker.UnitTests.Provisioning;

// PartitionReassignerProcess (arch/16 §5 I; spec t02 §5.1–§5.4): drain
// TO_REMOVE-брокера (батчи/дедуп/завершение по факту+USR), balance по заявке
// панели, слепая проба без подач, отмена, клэйм-guard.

public class PartitionReassignerProcessTests
{
    private const string Ep = "http://etcd:2379";
    private const string ProgressKey = "/kafkaworker/reassignments/events";
    private const string TicketKey = "/kafkaworker/rebalances/events";

    private sealed record Rig(
        Fakes.FakeEtcd Etcd,
        Fakes.FakeKafkaDriver Driver,
        FakeKafkaAdminClient Admin,
        ClaimStore Claims,
        WorkJournal Journal,
        PartitionReassignerProcess Process,
        FixedTimeProvider Time);

    // Active-кластер events: broker1..3 controller + broker4 broker-only.
    private static void SeedActive(Fakes.FakeEtcd etcd)
    {
        etcd.Seed("/kafka/clusters/events/config",
            """{"brokers":4,"replication_factor":3,"min_insync_replicas":2,"default_partitions":12,"default_retention_ms":604800000,"created_unix":1756500000}""");
        for (var k = 1; k <= 4; k++)
        {
            etcd.Seed($"/kafka/clusters/events/brokers/broker{k}/state", "RUNNING");
            etcd.Seed($"/kafka/clusters/events/brokers/broker{k}/role", k <= 3 ? "controller" : "broker");
        }

        etcd.Seed("/kafka/clusters/events/endpoints", "h1:16000,h1:16001,h1:16002,h1:16003");
        etcd.Seed("/kafka/clusters/events/app_user", "app");
        etcd.Seed("/kafka/clusters/events/app_password", "OldPassword0123456789abcdef");
        etcd.SeedSecurity("events");
    }

    private static async Task<KafkaClusterSnapshot> Snapshot(Fakes.FakeEtcd etcd)
    {
        var range = await etcd.RangeAsync(Ep, "/kafka/clusters/", CancellationToken.None);
        return KafkaSnapshotParser.Parse(range.Value).Value.Single(c => c.Cluster == "events");
    }

    // Риг: IntervalSec=0 — троттл выключен (последовательные RunAsync в одном
    // «времени»); дедуп переподачи изолируется движением FixedTimeProvider.
    private static async Task<Rig> NewRig(int retrySubmitSec = 120)
    {
        var etcd = new Fakes.FakeEtcd();
        SeedActive(etcd);
        var claims = new ClaimStore([Ep], etcd, TimeProvider.System);
        await claims.TryClaimClusterAsync("events", CancellationToken.None);
        var journal = new WorkJournal(etcd, [Ep]);
        var driver = new Fakes.FakeKafkaDriver();
        driver.NodeObjects.AddRange(Enumerable.Range(1, 4).Select(k => $"kfw-events-broker{k}"));
        var admin = new FakeKafkaAdminClient();
        var time = new FixedTimeProvider();
        var process = new PartitionReassignerProcess(
            etcd, [Ep], driver, claims, journal, new FakeAdminFactory(admin),
            new ReassignOptions(IntervalSec: 0, BatchPartitions: 10, ExecSec: 180, RetrySubmitSec: retrySubmitSec),
            time);
        return new Rig(etcd, driver, admin, claims, journal, process, time);
    }

    private sealed class FakeAdminFactory(FakeKafkaAdminClient client) : IKafkaAdminClientFactory
    {
        public IKafkaAdminClient Create(string bootstrap, string user, string password, string? caPem) => client;
    }

    private static ReassignProgress ReadProgress(Fakes.FakeEtcd etcd)
        => JsonSerializer.Deserialize<ReassignProgress>(
            etcd.Store[ProgressKey].Value,
            new JsonSerializerOptions { PropertyNameCaseInsensitive = true })!;

    [Fact]
    public async Task Drain_подаёт_батч_и_пишет_прогресс()
    {
        // Arrange: broker4 TO_REMOVE, реплики юзер-топика на нём; exec успешен.
        var rig = await NewRig();
        rig.Etcd.Seed("/kafka/clusters/events/brokers/broker4/state", "TO_REMOVE");
        rig.Admin.Topics = [new KafkaTopicView("orders", 2, [[1, 2, 4], [2, 4, 1]])];

        // Act
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: один exec с --execute; прогресс-ключ жив с mode=drain и
        // остатком партиций.
        result.IsSuccess.Should().BeTrue();
        var exec = Assert.Single(rig.Driver.Execs);
        string.Join(' ', exec.Cmd).Should().Contain("--execute");
        rig.Etcd.Store.Keys.Should().Contain(ProgressKey);
        var progress = ReadProgress(rig.Etcd);
        progress.Mode.Should().Be("drain");
        progress.DrainBroker.Should().Be("broker4");
        progress.PartitionsRemaining.Should().Be(2);
        progress.PartitionsTotal.Should().Be(2);
    }

    [Fact]
    public async Task Drain_завершение_очищает_прогресс()
    {
        // Arrange: broker4 TO_REMOVE, но реплик его уже нет (drain дошёл),
        // ISR == Replicas.
        var rig = await NewRig();
        rig.Etcd.Seed("/kafka/clusters/events/brokers/broker4/state", "TO_REMOVE");
        rig.Admin.Topics = [new KafkaTopicView("orders", 2, [[1, 2], [2, 1]], [[1, 2], [2, 1]])];
        rig.Etcd.Seed(ProgressKey,
            """{"mode":"drain","drain_broker":"broker4","partitions_total":2,"partitions_remaining":1,"submitted_unix":1,"updated_unix":2,"instance":"x"}""");

        // Act
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: exec нет, прогресс удалён, journal done — завершение внутри
        // drain-ветки (кандидат отобран по state, не по факту).
        result.IsSuccess.Should().BeTrue();
        rig.Driver.Execs.Should().BeEmpty();
        rig.Etcd.Store.Keys.Should().NotContain(ProgressKey);
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.Op.Should().Be("reassign");
        state.Value.Phase.Should().Be("done");
    }

    [Fact]
    public async Task Drain_minISR_отказ()
    {
        // Arrange: broker4 TO_REMOVE; юзер-топик с minISR=2, но живых целей
        // меньше — план недостижим.
        var rig = await NewRig();
        rig.Etcd.Seed("/kafka/clusters/events/brokers/broker4/state", "TO_REMOVE");
        rig.Etcd.Seed("/kafka/clusters/events/brokers/broker3/state", "REMOVING");
        rig.Etcd.Seed("/kafka/clusters/events/brokers/broker2/state", "REMOVING");
        rig.Admin.Topics = [new KafkaTopicView("orders", 1, [[1, 2, 4]])];

        // Act
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: подач нет; прогресс жив с человекочитаемой причиной;
        // тик успешен (перманентное ожидание, не ошибка).
        result.IsSuccess.Should().BeTrue();
        rig.Driver.Execs.Should().BeEmpty();
        var progress = ReadProgress(rig.Etcd);
        progress.LastError.Should().Contain("min.insync.replicas");
    }

    [Fact]
    public async Task Drain_USR_держит_завершение()
    {
        // Arrange: реплик drain-брокера нет, но одна партиция under-replicated.
        var rig = await NewRig();
        rig.Etcd.Seed("/kafka/clusters/events/brokers/broker4/state", "TO_REMOVE");
        rig.Admin.Topics = [new KafkaTopicView("orders", 1, [[1, 2]], [[1]])];

        // Act
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: демонтажной семантики нет — ждём полной синхронизации.
        result.IsSuccess.Should().BeTrue();
        rig.Driver.Execs.Should().BeEmpty();
        rig.Etcd.Store.Keys.Should().Contain(ProgressKey);
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.Phase.Should().Be("waiting-sync");
    }

    [Fact]
    public async Task Drain_дедуп_переподачи()
    {
        // Arrange: реплики на drain; время статично (троттл=0).
        var rig = await NewRig();
        rig.Etcd.Seed("/kafka/clusters/events/brokers/broker4/state", "TO_REMOVE");
        rig.Admin.Topics = [new KafkaTopicView("orders", 2, [[1, 2, 4], [2, 4, 1]])];

        // Act: два тика подряд без движения факта — повторная подача
        // подавлена дедупом RetrySubmitSec.
        (await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None))
            .IsSuccess.Should().BeTrue();
        (await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None))
            .IsSuccess.Should().BeTrue();
        rig.Driver.Execs.Should().HaveCount(1);

        // Assert: после RetrySubmitSec третья подача разрешена.
        rig.Time.Utc += TimeSpan.FromSeconds(120);
        (await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None))
            .IsSuccess.Should().BeTrue();
        rig.Driver.Execs.Should().HaveCount(2);
    }

    [Fact]
    public async Task Drain_exec_в_drain_контейнер_упал_переподача_через_RUNNING()
    {
        // Arrange: broker4 TO_REMOVE с репликами; контейнер drain-брокера
        // мёртв (docker-хост умер/выведен, снесён) — exec в него Failed,
        // живые узлы отвечают (code-review Фазы 7: надзор TO_REMOVE не
        // восстанавливает, fallback обязателен — иначе вечный Failed тика).
        var rig = await NewRig();
        rig.Etcd.Seed("/kafka/clusters/events/brokers/broker4/state", "TO_REMOVE");
        rig.Admin.Topics = [new KafkaTopicView("orders", 2, [[1, 2, 4], [2, 4, 1]])];
        rig.Driver.ExecHandler = (node, _) => node == "broker4"
            ? Result<string>.Failed(new ApplicationException("нет running-контейнера kfw-events-broker4"))
            : Result<string>.Success("");

        // Act
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: тик успешен — после отказа в drain-контейнер тот же батч
        // переподан через первый RUNNING-узел (broker1 по имени), прогресс
        // записан: drain не застревает на мёртвой exec-цели.
        result.IsSuccess.Should().BeTrue();
        rig.Driver.Execs.Should().HaveCount(2);
        rig.Driver.Execs[0].Node.Should().Be("broker4");
        rig.Driver.Execs[1].Node.Should().Be("broker1");
        string.Join(' ', rig.Driver.Execs[1].Cmd).Should().Contain("--execute");
        var progress = ReadProgress(rig.Etcd);
        progress.Mode.Should().Be("drain");
        progress.PartitionsRemaining.Should().Be(2);
    }

    [Fact]
    public async Task Drain_сходится_без_контейнера_drain_брокера()
    {
        // Arrange: broker4 TO_REMOVE, его контейнера нет вовсе (exec в broker4
        // всегда Failed); поданный батч Kafka применяется между тиками.
        var rig = await NewRig();
        rig.Etcd.Seed("/kafka/clusters/events/brokers/broker4/state", "TO_REMOVE");
        rig.Admin.Topics = [new KafkaTopicView("orders", 2, [[1, 2, 4], [2, 4, 1]])];
        rig.Driver.ExecHandler = (node, _) => node == "broker4"
            ? Result<string>.Failed(new ApplicationException("контейнер не найден"))
            : Result<string>.Success("");

        // Act: тик 1 — отказ в drain-контейнер, переподача через RUNNING;
        // факт «доиграл» поданный батч (реплик broker4 нет, ISR полный).
        (await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None))
            .IsSuccess.Should().BeTrue();
        rig.Admin.Topics = [new KafkaTopicView("orders", 2, [[1, 2], [2, 1]], [[1, 2], [2, 1]])];
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: drain завершён по факту — прогресс удалён, journal done;
        // попыток exec ровно две (тик 1: broker4 → fallback broker1), тик 2
        // без подач — сходимость не зависит от drain-контейнера.
        result.IsSuccess.Should().BeTrue();
        rig.Driver.Execs.Should().HaveCount(2);
        rig.Etcd.Store.Keys.Should().NotContain(ProgressKey);
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.Op.Should().Be("reassign");
        state.Value!.Phase.Should().Be("done");
    }

    [Fact]
    public async Task Balance_исполняет_и_снимает_заявку()
    {
        // Arrange: TO_REMOVE нет; заявка жива; факт RF=2 при трёх живых.
        var rig = await NewRig();
        rig.Admin.Topics = [new KafkaTopicView("orders", 2, [[1, 2], [2, 1]])];
        rig.Etcd.Seed(TicketKey, """{"requested_unix":1756500123,"requested_by":"ops"}""");

        // Act: первый тик подаёт батч добора.
        (await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None))
            .IsSuccess.Should().BeTrue();
        rig.Driver.Execs.Should().HaveCount(1);

        // Кластер «доехал» до плана — второй тик снимает заявку и прогресс.
        rig.Admin.Topics = [new KafkaTopicView("orders", 2, [[1, 2, 3], [2, 1, 3]])];
        (await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None))
            .IsSuccess.Should().BeTrue();

        // Assert: заявка и прогресс-ключ удалены, journal done.
        rig.Etcd.Store.Keys.Should().NotContain(TicketKey).And.NotContain(ProgressKey);
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.Phase.Should().Be("done");
    }

    [Fact]
    public async Task Balance_ждёт_drain()
    {
        // Arrange: заявка жива, но есть TO_REMOVE-брокер с репликами — drain
        // приоритетнее баланса.
        var rig = await NewRig();
        rig.Etcd.Seed("/kafka/clusters/events/brokers/broker4/state", "TO_REMOVE");
        rig.Admin.Topics = [new KafkaTopicView("orders", 2, [[1, 2, 4], [1, 2]])];
        rig.Etcd.Seed(TicketKey, """{"requested_unix":1756500123,"requested_by":"ops"}""");

        // Act
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: подача только drain (контейнер broker4), заявка ждёт.
        result.IsSuccess.Should().BeTrue();
        var exec = Assert.Single(rig.Driver.Execs);
        exec.Node.Should().Be("broker4");
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.Phase.Should().Be("waiting-drain");
        rig.Etcd.Store.Keys.Should().Contain(TicketKey);
    }

    [Fact]
    public async Task Balance_отмена_заявки()
    {
        // Arrange: заявки нет, drain-кандидатов нет, но прогресс-ключ жив —
        // мусор оборванного баланса/отмены.
        var rig = await NewRig();
        rig.Admin.Topics = [new KafkaTopicView("orders", 1, [[1, 2, 3]])];
        rig.Etcd.Seed(ProgressKey,
            """{"mode":"balance","partitions_total":1,"partitions_remaining":1,"submitted_unix":1,"updated_unix":2,"instance":"x"}""");

        // Act
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: прогресс удалён, подач нет (in-flight Kafka доиграет сам).
        result.IsSuccess.Should().BeTrue();
        rig.Driver.Execs.Should().BeEmpty();
        rig.Etcd.Store.Keys.Should().NotContain(ProgressKey);
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.Phase.Should().Be("cancelled");
    }

    [Fact]
    public async Task Слепая_проба()
    {
        // Arrange: describe падает (кластер слеп); прогресс-ключ жив с
        // известным значением — его трогать нельзя.
        var rig = await NewRig();
        rig.Admin.TopicsError = new ApplicationException("kafka: timeout");
        rig.Etcd.Seed("/kafka/clusters/events/brokers/broker4/state", "TO_REMOVE");
        var seeded = """{"mode":"drain","drain_broker":"broker4","partitions_total":9,"partitions_remaining":4,"submitted_unix":1,"updated_unix":2,"instance":"x"}""";
        rig.Etcd.Seed(ProgressKey, seeded);

        // Act
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: ноль подач, прошлый прогресс сохранён, тик успешен.
        result.IsSuccess.Should().BeTrue();
        rig.Driver.Execs.Should().BeEmpty();
        rig.Etcd.Store[ProgressKey].Value.Should().Be(seeded);
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.Phase.Should().Be("waiting-cluster");
    }

    [Fact]
    public async Task Клэйм_не_наш()
    {
        // Arrange: клэйм кластера чужой (риг без TryClaim).
        var etcd = new Fakes.FakeEtcd();
        SeedActive(etcd);
        var claims = new ClaimStore([Ep], etcd, TimeProvider.System);
        var process = new PartitionReassignerProcess(
            etcd, [Ep], new Fakes.FakeKafkaDriver(), claims, new WorkJournal(etcd, [Ep]),
            new FakeAdminFactory(new FakeKafkaAdminClient()), ReassignOptions.Default, new FixedTimeProvider());

        // Act
        var result = await process.RunAsync(await Snapshot(etcd), CancellationToken.None);

        // Assert: мутации запрещены.
        result.IsSuccess.Should().BeFalse();
        result.Error!.Message.Should().Contain("клэйм");
    }
}
