using System.Text.Json;
using FluentAssertions;
using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using KafkaWorker.Etcd.Client;
using KafkaWorker.Etcd.Coordination;
using KafkaWorker.Etcd.Parsing;
using KafkaWorker.Provisioning.Kafka;
using KafkaWorker.Provisioning.Processes;
using Xunit;

namespace KafkaWorker.UnitTests.Provisioning;

// TopicSyncProcess (план C1-шаги 2–3; канон arch/15 §3, arch/16 §5 D):
// автосинк факта + исполнение desired-заявок на fake gateway/adminclient.
// Покрыто: RMW по mod_revision (обе стороны гонки с панелью), apply-порядок
// (конфиги до partitions), идемпотентность повтора, missing-ветка, перманентный
// отказ уменьшения, транзиент-ретрай, троттлинг интервала.
public class TopicSyncProcessTests
{
    private const string Ep = "http://etcd:2379";
    private const string Key = "/kafka/clusters/events/topics/orders";

    private sealed record Rig(
        Fakes.FakeEtcd Etcd,
        FakeKafkaAdminClient Admin,
        ClaimStore Claims,
        WorkJournal Journal,
        TopicSyncProcess Process,
        Func<Task<KafkaClusterSnapshot>> Snapshot);

    private sealed class FakeAdminFactory(FakeKafkaAdminClient client) : IKafkaAdminClientFactory
    {
        public IKafkaAdminClient Create(string bootstrap, string user, string password, string? caPem) => client;
    }

    private static async Task<Rig> NewRigAsync(int intervalSec = 0)
    {
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/kafka/clusters/events/config",
            """{"brokers":2,"replication_factor":1,"min_insync_replicas":1,"default_partitions":12,"default_retention_ms":604800000,"created_unix":1756500000}""");
        etcd.Seed("/kafka/clusters/events/brokers/broker1/state", "RUNNING");
        etcd.Seed("/kafka/clusters/events/endpoints", "h1:16000,h1:16001");
        etcd.Seed("/kafka/clusters/events/app_user", "app");
        etcd.Seed("/kafka/clusters/events/app_password", "p");
        var claims = new ClaimStore([Ep], etcd, TimeProvider.System);
        await claims.TryClaimClusterAsync("events", CancellationToken.None);
        var journal = new WorkJournal(etcd, [Ep]);
        var admin = new FakeKafkaAdminClient
        {
            ClusterView = new KafkaClusterView([new KafkaBrokerView(1, "broker1")], 1),
        };
        var process = new TopicSyncProcess(
            etcd, [Ep], claims, journal, new FakeAdminFactory(admin), TimeProvider.System, intervalSec);
        return new Rig(etcd, admin, claims, journal, process, Snapshot);

        async Task<KafkaClusterSnapshot> Snapshot()
        {
            var range = await etcd.RangeAsync(Ep, "/kafka/clusters/", CancellationToken.None);
            return KafkaSnapshotParser.Parse(range.Value).Value.Single(c => c.Cluster == "events");
        }
    }

    // Факт в fake admin: топик + управляемые конфиги.
    private static void SeedTopicFact(FakeKafkaAdminClient admin, string topic = "orders", int partitions = 3)
    {
        admin.Topics = [new KafkaTopicView(topic, partitions, [[1]])];
        admin.TopicConfigs = new Dictionary<string, IReadOnlyDictionary<string, string>>
        {
            [topic] = new Dictionary<string, string> { ["retention.ms"] = "604800000" },
        };
    }

    // Ключ реестра: факт + опциональная desired-заявка.
    private static void SeedRegistry(
        Fakes.FakeEtcd etcd,
        string? desiredJson = null,
        bool missing = false,
        int partitions = 3)
    {
        var raw = "{\"partitions\":" + partitions
            + ",\"replication_factor\":1"
            + ",\"configs\":{\"retention.ms\":\"604800000\"}"
            + (desiredJson is null ? "" : "," + desiredJson)
            + ",\"synced_unix\":1756500900"
            + ",\"missing\":" + (missing ? "true" : "false") + "}";
        etcd.Seed(Key, raw);
    }

    private static async Task<JsonElement> RegistryValue(Fakes.FakeEtcd etcd)
    {
        var kv = await etcd.GetAsync(Ep, Key, CancellationToken.None);
        using var doc = JsonDocument.Parse(kv.Value!.Value);
        return doc.RootElement.Clone();
    }

    [Fact]
    public async Task NewTopic_PutsRegistryKeyWithFact()
    {
        // Arrange: топик создан на стороне Kafka, ключа в etcd нет.
        var rig = await NewRigAsync();
        SeedTopicFact(rig.Admin);

        // Act
        var result = await rig.Process.RunAsync(await rig.Snapshot(), CancellationToken.None);

        // Assert: ключ с фактом, заявки нет, missing=false.
        result.IsSuccess.Should().BeTrue();
        var value = await RegistryValue(rig.Etcd);
        value.GetProperty("partitions").GetInt32().Should().Be(3);
        value.GetProperty("replication_factor").GetInt32().Should().Be(1);
        value.GetProperty("configs").GetProperty("retention.ms").GetString().Should().Be("604800000");
        value.GetProperty("missing").GetBoolean().Should().BeFalse();
        value.TryGetProperty("desired", out _).Should().BeFalse();
        value.GetProperty("synced_unix").GetInt64().Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task DesiredConfigs_AppliedAndCleared()
    {
        // Arrange: живая заявка retention 1 день при факте 7 дней.
        var rig = await NewRigAsync();
        SeedTopicFact(rig.Admin);
        SeedRegistry(rig.Etcd, "\"desired\":{\"configs\":{\"retention.ms\":\"86400000\"}},\"desired_unix\":1756500950,\"desired_by\":\"admin\"");

        // Act
        var result = await rig.Process.RunAsync(await rig.Snapshot(), CancellationToken.None);

        // Assert: alter выполнен, заявка снята, факт = заявке.
        result.IsSuccess.Should().BeTrue();
        rig.Admin.AlterTopicCalls.Should().ContainSingle()
            .Which.Configs.Should().ContainKey("retention.ms").WhoseValue.Should().Be("86400000");
        var value = await RegistryValue(rig.Etcd);
        value.TryGetProperty("desired", out _).Should().BeFalse("заявка исполнена и снята");
        value.GetProperty("configs").GetProperty("retention.ms").GetString().Should().Be("86400000");
    }

    [Fact]
    public async Task DesiredPartitionsUp_ConfigsBeforePartitions()
    {
        // Arrange: заявка partitions 3→6 + retention.
        var rig = await NewRigAsync();
        SeedTopicFact(rig.Admin);
        SeedRegistry(rig.Etcd, "\"desired\":{\"partitions\":6,\"configs\":{\"retention.ms\":\"86400000\"}},\"desired_unix\":1756500950,\"desired_by\":\"admin\"");

        // Act
        var result = await rig.Process.RunAsync(await rig.Snapshot(), CancellationToken.None);

        // Assert: alter-конфиги ДО CreatePartitions (план C1: apply-порядок),
        // факт записан с новыми partitions.
        result.IsSuccess.Should().BeTrue();
        rig.Admin.CallLog.Should().Equal("describe-topics", "alter-topic:orders", "create-partitions:orders:6");
        var value = await RegistryValue(rig.Etcd);
        value.GetProperty("partitions").GetInt32().Should().Be(6);
        value.GetProperty("configs").GetProperty("retention.ms").GetString().Should().Be("86400000");
    }

    [Fact]
    public async Task RmwRace_PanelWroteDesiredAfterSnapshot_FreshDesiredPreserved()
    {
        // Arrange: снапшот ещё БЕЗ desired, но панель успела поставить заявку
        // до act (ключ перезаписан поверх снапшота) — факт дрейфнул (partitions↑).
        var rig = await NewRigAsync();
        SeedTopicFact(rig.Admin);
        SeedRegistry(rig.Etcd);
        SeedTopicFact(rig.Admin, partitions: 6);
        // Панель поставила заявку ПОСЛЕ сборки снапшота (снапшот ниже соберётся
        // после перезаписи ключа — поэтому фиксируем снапшот ДО).
        var snapshot = await rig.Snapshot();
        rig.Etcd.Seed(Key,
            """{"partitions":3,"replication_factor":1,"configs":{"retention.ms":"604800000"},"desired":{"configs":{"retention.ms":"3600000"}},"desired_unix":1756500990,"desired_by":"panel","synced_unix":1756500900,"missing":false}""");

        // Act: процесс решает по старому снапшоту (автосинк факта), но act
        // читает свежий ключ — заявка панели обязана уцелеть.
        var result = await rig.Process.RunAsync(snapshot, CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        var value = await RegistryValue(rig.Etcd);
        value.GetProperty("partitions").GetInt32().Should().Be(6, "факт синхронизирован");
        value.TryGetProperty("desired", out var desired).Should().BeTrue("свежая заявка панели не затёрта");
        desired.GetProperty("configs").GetProperty("retention.ms").GetString().Should().Be("3600000");
        value.GetProperty("desired_by").GetString().Should().Be("panel");
    }

    [Fact]
    public async Task RmwRace_PanelWritesDuringTxn_CompareLostValueIntact()
    {
        // Arrange: панель перезаписывает ключ МЕЖДУ read и txn процесса —
        // compare mod_revision проигрывает, значение панели не портится.
        var rig = await NewRigAsync();
        SeedTopicFact(rig.Admin);
        SeedRegistry(rig.Etcd, "\"desired\":{\"partitions\":9},\"desired_unix\":1756500900,\"desired_by\":\"admin\"");
        var panelValue =
            """{"partitions":3,"replication_factor":1,"configs":{"retention.ms":"604800000"},"desired":{"partitions":9},"desired_unix":1756500990,"desired_by":"panel","synced_unix":1756500900,"missing":false}""";
        var fired = false;
        rig.Etcd.OnTxnBeforeCompare = _ =>
        {
            if (fired)
                return;
            fired = true;
            rig.Etcd.Seed(Key, panelValue); // ревизия ушла — compare не сойдётся
        };

        // Act
        var result = await rig.Process.RunAsync(await rig.Snapshot(), CancellationToken.None);

        // Assert: тик успешен (не ошибка), значение осталось панельским —
        // следующий тик разберёт свежую заявку.
        result.IsSuccess.Should().BeTrue();
        (await rig.Etcd.GetAsync(Ep, Key, CancellationToken.None)).Value!.Value
            .Should().Be(panelValue, "проигрыш compare не портит запись панели");
    }

    [Fact]
    public async Task TopicDeletedWithoutDesired_KeyRemoved()
    {
        // Arrange: топик был в реестре, из Kafka исчез, заявки нет.
        var rig = await NewRigAsync();
        rig.Admin.Topics = [];
        rig.Admin.TopicConfigs = new Dictionary<string, IReadOnlyDictionary<string, string>>();
        SeedRegistry(rig.Etcd);

        // Act
        var result = await rig.Process.RunAsync(await rig.Snapshot(), CancellationToken.None);

        // Assert: реестр = факт → ключ удалён.
        result.IsSuccess.Should().BeTrue();
        (await rig.Etcd.GetAsync(Ep, Key, CancellationToken.None)).Value.Should().BeNull();
    }

    [Fact]
    public async Task TopicDeletedWithDesired_MarkedMissing()
    {
        // Arrange: топик исчез при живой заявке.
        var rig = await NewRigAsync();
        rig.Admin.Topics = [];
        rig.Admin.TopicConfigs = new Dictionary<string, IReadOnlyDictionary<string, string>>();
        SeedRegistry(rig.Etcd, "\"desired\":{\"configs\":{\"retention.ms\":\"86400000\"}},\"desired_unix\":1756500950,\"desired_by\":\"admin\"");

        // Act
        var result = await rig.Process.RunAsync(await rig.Snapshot(), CancellationToken.None);

        // Assert: ключ жив с missing=true и сохранённой заявкой.
        result.IsSuccess.Should().BeTrue();
        var value = await RegistryValue(rig.Etcd);
        value.GetProperty("missing").GetBoolean().Should().BeTrue();
        value.GetProperty("desired").GetProperty("configs").GetProperty("retention.ms").GetString()
            .Should().Be("86400000");
    }

    [Fact]
    public async Task DesiredCancelled_KeyRemoved()
    {
        // Arrange: missing-топик, заявку отменили из панели (desired убран).
        var rig = await NewRigAsync();
        rig.Admin.Topics = [];
        rig.Admin.TopicConfigs = new Dictionary<string, IReadOnlyDictionary<string, string>>();
        SeedRegistry(rig.Etcd, missing: true);

        // Act
        var result = await rig.Process.RunAsync(await rig.Snapshot(), CancellationToken.None);

        // Assert: нет топика и нет заявки → ключ удалён (arch/15 §3).
        result.IsSuccess.Should().BeTrue();
        (await rig.Etcd.GetAsync(Ep, Key, CancellationToken.None)).Value.Should().BeNull();
    }

    [Fact]
    public async Task DesiredPartitionsDown_PermanentReject_JournalAndClear()
    {
        // Arrange: etcd-мусор — desired partitions меньше факта.
        var rig = await NewRigAsync();
        SeedTopicFact(rig.Admin, partitions: 6);
        SeedRegistry(rig.Etcd, "\"desired\":{\"partitions\":3},\"desired_unix\":1756500950,\"desired_by\":\"admin\"", partitions: 6);

        // Act
        var result = await rig.Process.RunAsync(await rig.Snapshot(), CancellationToken.None);

        // Assert: перманентный отказ в журнале, заявка снята, факт записан,
        // к Kafka применений не было.
        result.IsSuccess.Should().BeTrue();
        var work = (await rig.Etcd.GetAsync(Ep, "/kafkaworker/work/events", CancellationToken.None))
            .Value!.Value;
        work.Should().Contain("topicsync").And.Contain("rejected").And.Contain("partitions 3");
        rig.Admin.AlterTopicCalls.Should().BeEmpty();
        rig.Admin.CreatePartitionsCalls.Should().BeEmpty();
        var value = await RegistryValue(rig.Etcd);
        value.TryGetProperty("desired", out _).Should().BeFalse("заявка снята после отказа");
        value.GetProperty("partitions").GetInt32().Should().Be(6);
    }

    [Fact]
    public async Task AlterTransientFails_DesiredSurvives_RetrySucceeds()
    {
        // Arrange: alter падает дольше jitter-ретраев одного тика (3 попытки),
        // но не дольше следующего тика — заявка остаётся, повтор доводит дело.
        var rig = await NewRigAsync();
        SeedTopicFact(rig.Admin);
        SeedRegistry(rig.Etcd, "\"desired\":{\"configs\":{\"retention.ms\":\"86400000\"}},\"desired_unix\":1756500950,\"desired_by\":\"admin\"");
        rig.Admin.AlterTopicFailCount = 3;

        // Act
        var first = await rig.Process.RunAsync(await rig.Snapshot(), CancellationToken.None);
        var second = await rig.Process.RunAsync(await rig.Snapshot(), CancellationToken.None);

        // Assert
        first.IsSuccess.Should().BeFalse("транзиент перекрыл ретраи тика — ошибка, desired жив");
        second.IsSuccess.Should().BeTrue();
        rig.Admin.CallLog.Count(l => l.StartsWith("alter-topic", StringComparison.Ordinal)).Should().Be(4,
            "3 транзиента тика + 1 успешный повтор");
        rig.Admin.AlterTopicCalls.Should().HaveCount(1, "применение состоялось один раз");
        var value = await RegistryValue(rig.Etcd);
        value.TryGetProperty("desired", out _).Should().BeFalse();
        value.GetProperty("configs").GetProperty("retention.ms").GetString().Should().Be("86400000");
    }

    [Fact]
    public async Task IdempotentSecondRun_NoNewWrites()
    {
        // Arrange: стабильное состояние после первого прогона.
        var rig = await NewRigAsync();
        SeedTopicFact(rig.Admin);
        await rig.Process.RunAsync(await rig.Snapshot(), CancellationToken.None);
        var txns = rig.Etcd.Txns.Count;
        var log = rig.Admin.CallLog.Count;

        // Act: повтор при том же факте.
        var result = await rig.Process.RunAsync(await rig.Snapshot(), CancellationToken.None);

        // Assert: no-op — ни txn, ни admin-вызовов (кроме describe).
        result.IsSuccess.Should().BeTrue();
        rig.Etcd.Txns.Count.Should().Be(txns);
        rig.Admin.CallLog.Count.Should().Be(log + 1, "только describe-шаг");
    }

    [Fact]
    public async Task IntervalThrottle_SecondImmediateRunSkipped()
    {
        // Arrange: интервал 15 c — подряд идущие тики не должны дёргать Kafka.
        var rig = await NewRigAsync(intervalSec: 15);
        SeedTopicFact(rig.Admin);
        await rig.Process.RunAsync(await rig.Snapshot(), CancellationToken.None);
        var log = rig.Admin.CallLog.Count;

        // Act
        var result = await rig.Process.RunAsync(await rig.Snapshot(), CancellationToken.None);

        // Assert: троттлинг — describe не выполнялся.
        result.IsSuccess.Should().BeTrue();
        rig.Admin.CallLog.Count.Should().Be(log);
    }

    [Fact]
    public async Task NoClaim_MutationsForbidden()
    {
        // Arrange: клэйм держит другой инстанс (rig), процесс — со своим
        // ClaimStore без клэйма events.
        var rig = await NewRigAsync();
        var stranger = new ClaimStore([Ep], rig.Etcd, TimeProvider.System);
        var process = new TopicSyncProcess(
            rig.Etcd, [Ep], stranger, rig.Journal, new FakeAdminFactory(rig.Admin), TimeProvider.System, 0);
        await stranger.DisposeAsync();

        // Act
        var result = await process.RunAsync(await rig.Snapshot(), CancellationToken.None);

        // Assert: отказ до любых мутаций.
        result.IsSuccess.Should().BeFalse();
        result.Error!.Message.Should().Contain("клэйм");
        rig.Admin.CallLog.Should().BeEmpty();
    }

    [Fact]
    public async Task LifecycleCreateTicket_CreatesTopicAndRemovesTicket()
    {
        // Arrange: create-заявка на несуществующий топик; fact-ключа нет.
        var rig = await NewRigAsync();
        rig.Etcd.Seed("/kafka/clusters/events/topics/audit/desired.create",
            """{"partitions":12,"replication_factor":1,"configs":{"retention.ms":"86400000"},"requested_unix":1750000000,"requested_by":"admin"}""");

        // Act
        var result = await rig.Process.RunAsync(await rig.Snapshot(), CancellationToken.None);

        // Assert: CreateTopics вызван с параметрами заявки; заявка удалена;
        // факт-ключ появится следующим автосинк-тиком (топик теперь в fake).
        result.IsSuccess.Should().BeTrue();
        var created = rig.Admin.CreatedTopics.Should().ContainSingle().Which;
        created.Topic.Should().Be("audit");
        created.Partitions.Should().Be(12);
        created.ReplicationFactor.Should().Be((short)1);
        created.Configs.Should().ContainKey("retention.ms").WhoseValue.Should().Be("86400000");
        (await rig.Etcd.GetAsync(Ep, "/kafka/clusters/events/topics/audit/desired.create", CancellationToken.None))
            .Value.Should().BeNull();
    }

    [Fact]
    public async Task LifecycleCreateTicket_TopicAlreadyExists_CleansTicketWithoutCreate()
    {
        // Arrange: топик уже в факте Kafka (создан параллельно CLI) + живая create-заявка.
        var rig = await NewRigAsync();
        SeedTopicFact(rig.Admin, "orders");
        rig.Etcd.Seed("/kafka/clusters/events/topics/orders/desired.create",
            """{"partitions":6,"replication_factor":1,"requested_unix":1750000000,"requested_by":"admin"}""");

        // Act
        var result = await rig.Process.RunAsync(await rig.Snapshot(), CancellationToken.None);

        // Assert: повторный create НЕ вызван (идемпотентность AlreadyExists решена на нашей стороне).
        result.IsSuccess.Should().BeTrue();
        rig.Admin.CreatedTopics.Should().BeEmpty();
        (await rig.Etcd.GetAsync(Ep, "/kafka/clusters/events/topics/orders/desired.create", CancellationToken.None))
            .Value.Should().BeNull();
    }

    [Fact]
    public async Task LifecycleDeleteTicket_DeletesTopicRegistryKeyAndTicket()
    {
        // Arrange: факт-ключ + delete-заявка на живой топик.
        var rig = await NewRigAsync();
        SeedTopicFact(rig.Admin, "orders");
        SeedRegistry(rig.Etcd);
        rig.Etcd.Seed("/kafka/clusters/events/topics/orders/desired.delete",
            """{"requested_unix":1750000100,"requested_by":"admin"}""");

        // Act
        var result = await rig.Process.RunAsync(await rig.Snapshot(), CancellationToken.None);

        // Assert: DeleteTopics вызван; одной txn снесены факт-ключ и заявка.
        result.IsSuccess.Should().BeTrue();
        rig.Admin.DeletedTopics.Should().Contain("orders");
        (await rig.Etcd.GetAsync(Ep, "/kafka/clusters/events/topics/orders", CancellationToken.None)).Value.Should().BeNull();
        (await rig.Etcd.GetAsync(Ep, "/kafka/clusters/events/topics/orders/desired.delete", CancellationToken.None)).Value.Should().BeNull();
    }

    [Fact]
    public async Task LifecycleDeleteTicket_TransientFailure_RetriedNextTick()
    {
        // Arrange: DeleteTopics падает дольше jitter-ретраев одного тика
        // (3 попытки — образец AlterTopicFailCount в существующих тестах).
        var rig = await NewRigAsync();
        SeedTopicFact(rig.Admin, "orders");
        SeedRegistry(rig.Etcd);
        rig.Etcd.Seed("/kafka/clusters/events/topics/orders/desired.delete",
            """{"requested_unix":1,"requested_by":"u"}""");
        rig.Admin.DeleteTopicFailCount = 3;

        // Act
        var first = await rig.Process.RunAsync(await rig.Snapshot(), CancellationToken.None);
        var second = await rig.Process.RunAsync(await rig.Snapshot(), CancellationToken.None);

        // Assert: первый тик неуспешен (заявка жива), второй доводит.
        first.IsSuccess.Should().BeFalse();
        second.IsSuccess.Should().BeTrue();
        rig.Admin.DeletedTopics.Should().Contain("orders");
    }

    [Fact]
    public async Task LifecycleCreate_TopicAppearsBetweenDescribeAndAct_AlreadyExistsTicketRemoved()
    {
        // Arrange: describe топика не видел, но к моменту create-мутации топик
        // уже создан CLI (рассинхрон окна describe→act; ревью Фазы 4 r2).
        // Точка врезки: journal-write — после decide, до CreateTopicsAsync.
        var rig = await NewRigAsync();
        rig.Etcd.Seed("/kafka/clusters/events/topics/audit/desired.create",
            """{"partitions":12,"replication_factor":1,"requested_unix":1750000000,"requested_by":"admin"}""");
        var fired = false;
        rig.Etcd.OnPut = key =>
        {
            if (fired || key != "/kafkaworker/work/events")
                return;
            fired = true;
            SeedTopicFact(rig.Admin, "audit"); // топик «появился» между describe и act
        };

        // Act
        var result = await rig.Process.RunAsync(await rig.Snapshot(), CancellationToken.None);

        // Assert: адаптер вернул AlreadyExists — тик успешен, заявка снята
        // (исходы адаптера — не ошибки; сходимость следующего тика).
        result.IsSuccess.Should().BeTrue();
        rig.Admin.CreatedTopics.Should().BeEmpty("повторный CreateTopics не выполнялся");
        (await rig.Etcd.GetAsync(Ep, "/kafka/clusters/events/topics/audit/desired.create", CancellationToken.None))
            .Value.Should().BeNull();
    }

    [Fact]
    public async Task LifecycleDelete_TopicVanishesBetweenDescribeAndAct_NotFoundKeysRemoved()
    {
        // Arrange: describe видел топик (delete-заявка решена), но к моменту
        // DeleteTopics топик уже удалён CLI (ревью Фазы 4 r2).
        var rig = await NewRigAsync();
        SeedTopicFact(rig.Admin, "orders");
        SeedRegistry(rig.Etcd);
        rig.Etcd.Seed("/kafka/clusters/events/topics/orders/desired.delete",
            """{"requested_unix":1750000100,"requested_by":"admin"}""");
        var fired = false;
        rig.Etcd.OnPut = key =>
        {
            if (fired || key != "/kafkaworker/work/events")
                return;
            fired = true;
            rig.Admin.Topics = []; // топик «исчез» между describe и act
            rig.Admin.TopicConfigs = new Dictionary<string, IReadOnlyDictionary<string, string>>();
        };

        // Act
        var result = await rig.Process.RunAsync(await rig.Snapshot(), CancellationToken.None);

        // Assert: адаптер вернул NotFound = исполнено — тик успешен, оба ключа
        // удалены (факт-ключ снесён cleanup-веткой txn).
        result.IsSuccess.Should().BeTrue();
        rig.Admin.DeletedTopics.Should().BeEmpty("DeleteTopics не нашёл топик — NotFound");
        (await rig.Etcd.GetAsync(Ep, "/kafka/clusters/events/topics/orders", CancellationToken.None)).Value.Should().BeNull();
        (await rig.Etcd.GetAsync(Ep, "/kafka/clusters/events/topics/orders/desired.delete", CancellationToken.None)).Value.Should().BeNull();
    }
}
