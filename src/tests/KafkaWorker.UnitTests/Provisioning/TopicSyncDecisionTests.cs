using FluentAssertions;
using KafkaWorker.Core.Model;
using KafkaWorker.Provisioning.Processes;
using Xunit;

namespace KafkaWorker.UnitTests.Provisioning;

// Чистые decision-функции автосинка топиков (план C1-шаг 1; канон arch/15 §3,
// arch/16 §5 D): (факт-набор Kafka, реестр etcd) → план действий. Тесты
// таблицей покрывают все ветки протокола: автосинк факта, исчезновение
// (с/без desired), desired-применение, перманентный отказ уменьшения.
public class TopicSyncDecisionTests
{
    private static TopicFact Fact(
        string topic,
        int partitions = 3,
        int rf = 1,
        string? retention = "604800000",
        string? minIsr = "1") => new(
        topic,
        partitions,
        (short?)rf,
        new Dictionary<string, string?>
        {
            ["retention.ms"] = retention,
            ["min.insync.replicas"] = minIsr,
        }.Where(p => p.Value is not null).ToDictionary(p => p.Key, p => p.Value!));

    private static KafkaTopicReg Reg(
        string topic,
        int partitions = 3,
        short? rf = 1,
        string? retention = "604800000",
        string? minIsr = "1",
        TopicDesired? desired = null,
        long? desiredUnix = null,
        string? desiredBy = null,
        bool missing = false) => new(
        topic,
        partitions,
        rf,
        retention is null && minIsr is null
            ? null
            : new Dictionary<string, string?>
            {
                ["retention.ms"] = retention,
                ["min.insync.replicas"] = minIsr,
            }.Where(p => p.Value is not null).ToDictionary(p => p.Key, p => p.Value!),
        desired,
        desiredUnix,
        desiredBy,
        SyncedUnix: 1_750_000_000,
        Missing: missing);

    private static TopicDesired Desired(
        int? partitions = null,
        string? retention = null,
        string? minIsr = null) => new(
        partitions,
        new Dictionary<string, string?>
        {
            ["retention.ms"] = retention,
            ["min.insync.replicas"] = minIsr,
        }.Where(p => p.Value is not null).ToDictionary(p => p.Key, p => p.Value!));

    public static TheoryData<string, IReadOnlyList<TopicFact>, IReadOnlyList<KafkaTopicReg>, TopicSyncAction>
        Cases => new()
    {
        // 1. Новый факт-топик (создан CLI/клиентом) → put ключа с фактом, desired нет.
        {
            "new-fact-topic",
            [Fact("orders")],
            [],
            new TopicSyncAction.Sync("orders", 3, 1,
                new Dictionary<string, string> { ["retention.ms"] = "604800000", ["min.insync.replicas"] = "1" },
                Desired: null, DesiredUnix: null, DesiredBy: null)
        },

        // 2. Факт совпадает с реестром (desired нет) → no-op.
        { "fact-equals-registry", [Fact("orders")], [Reg("orders")], new TopicSyncAction.Skip("orders") },

        // 3. Факт изменился извне (partitions вырос CLI) → обновить ключ фактом.
        {
            "fact-drifted",
            [Fact("orders", partitions: 6)],
            [Reg("orders", partitions: 3)],
            new TopicSyncAction.Sync("orders", 6, 1,
                new Dictionary<string, string> { ["retention.ms"] = "604800000", ["min.insync.replicas"] = "1" },
                Desired: null, DesiredUnix: null, DesiredBy: null)
        },

        // 4. Топик исчез из Kafka, desired нет → удалить ключ (реестр = факт).
        { "gone-no-desired", [], [Reg("orders")], new TopicSyncAction.Forget("orders") },

        // 5. Топик исчез при живом desired → missing=true (заявка не исполнима).
        {
            "gone-with-desired",
            [],
            [Reg("orders", desired: Desired(retention: "86400000"), desiredUnix: 1_750_090_000, desiredBy: "admin")],
            new TopicSyncAction.MarkMissing("orders")
        },

        // 6. Уже missing=true → no-op (не переписывать ключ каждый тик).
        {
            "already-missing",
            [],
            [Reg("orders", desired: Desired(retention: "86400000"), missing: true)],
            new TopicSyncAction.Skip("orders")
        },

        // 7. Пропавший топик появился снова → missing=false, desired сохраняется.
        {
            "reappeared",
            [Fact("orders")],
            [Reg("orders", desired: Desired(retention: "86400000"), desiredUnix: 1_750_090_000, desiredBy: "admin", missing: true)],
            new TopicSyncAction.Sync("orders", 3, 1,
                new Dictionary<string, string> { ["retention.ms"] = "604800000", ["min.insync.replicas"] = "1" },
                Desired: Desired(retention: "86400000"), DesiredUnix: 1_750_090_000, DesiredBy: "admin")
        },

        // 8. desired-конфиг отличается → Apply (только конфиги).
        {
            "desired-retention",
            [Fact("orders")],
            [Reg("orders", desired: Desired(retention: "86400000"))],
            new TopicSyncAction.Apply(
                "orders",
                new Dictionary<string, string> { ["retention.ms"] = "86400000" },
                TotalPartitions: null)
        },

        // 9. desired partitions↑ (без конфигов) → Apply (CreatePartitions).
        {
            "desired-partitions-up",
            [Fact("orders", partitions: 3)],
            [Reg("orders", partitions: 3, desired: Desired(partitions: 6))],
            new TopicSyncAction.Apply("orders", new Dictionary<string, string>(), TotalPartitions: 6)
        },

        // 10. desired partitions↑ + конфиг → Apply обоими (порядок — акт процесса).
        {
            "desired-both",
            [Fact("orders", partitions: 3)],
            [Reg("orders", partitions: 3, desired: Desired(partitions: 6, retention: "86400000"))],
            new TopicSyncAction.Apply(
                "orders",
                new Dictionary<string, string> { ["retention.ms"] = "86400000" },
                TotalPartitions: 6)
        },

        // 11. desired partitions < факт → перманентный отказ журнала (spec §4.2 D).
        {
            "desired-partitions-down",
            [Fact("orders", partitions: 6)],
            [Reg("orders", partitions: 6, desired: Desired(partitions: 3))],
            new TopicSyncAction.Reject("orders", "partitions 3 < факт 6: уменьшение партиций Kafka не поддерживает")
        },

        // 12. desired partitions == факт и конфиги равны → заявка исполнена → снять.
        {
            "desired-already-applied",
            [Fact("orders", partitions: 6)],
            [Reg("orders", partitions: 6, desired: Desired(partitions: 6))],
            new TopicSyncAction.Sync("orders", 6, 1,
                new Dictionary<string, string> { ["retention.ms"] = "604800000", ["min.insync.replicas"] = "1" },
                Desired: null, DesiredUnix: null, DesiredBy: null)
        },

        // 13. desired-конфиг == факту → снять заявку без применения.
        {
            "desired-config-equals-fact",
            [Fact("orders", retention: "86400000")],
            [Reg("orders", retention: "604800000", desired: Desired(retention: "86400000"))],
            new TopicSyncAction.Sync("orders", 3, 1,
                new Dictionary<string, string> { ["retention.ms"] = "86400000", ["min.insync.replicas"] = "1" },
                Desired: null, DesiredUnix: null, DesiredBy: null)
        },

        // 14. Internal-топик __consumer_offsets → в реестр не попадает.
        { "internal-topic-skipped", [Fact("__consumer_offsets")], [], new TopicSyncAction.Skip("__consumer_offsets") },

        // 15. Мусор в desired-конфигах (неуправляемый ключ) → не применяется.
        {
            "desired-unknown-config-ignored",
            [Fact("orders")],
            [Reg("orders", desired: new TopicDesired(
                null,
                new Dictionary<string, string> { ["cleanup.policy"] = "compact" }))],
            new TopicSyncAction.Sync("orders", 3, 1,
                new Dictionary<string, string> { ["retention.ms"] = "604800000", ["min.insync.replicas"] = "1" },
                Desired: null, DesiredUnix: null, DesiredBy: null)
        },
    };

    [Theory]
    [MemberData(nameof(Cases))]
    public void Decide_ReturnsExpectedAction(
        string _, IReadOnlyList<TopicFact> facts, IReadOnlyList<KafkaTopicReg> registry, TopicSyncAction expected)
    {
        // Act
        var actions = TopicSyncDecision.Decide(facts, registry);

        // Assert
        // BeEquivalentTo по runtime-типу (compile-тип — абстрактная база без
        // членов) и структурно: record-Equals не подходит — словари Configs
        // сравниваются по ссылке.
        actions.Should().ContainSingle().Which
            .Should().BeEquivalentTo(expected, o => o.RespectingRuntimeTypes());
    }

    [Fact]
    public void Decide_MultipleTopics_MixedPlan()
    {
        // Arrange: новый топик + исчезнувший + инвариантный — один decide-проход.
        var facts = new List<TopicFact> { Fact("kept"), Fact("new") };
        var registry = new List<KafkaTopicReg> { Reg("kept"), Reg("gone") };

        // Act
        var actions = TopicSyncDecision.Decide(facts, registry);

        // Assert
        actions.Should().HaveCount(3);
        actions.OfType<TopicSyncAction.Sync>().Should().ContainSingle().Which.Topic.Should().Be("new");
        actions.OfType<TopicSyncAction.Forget>().Should().ContainSingle().Which.Topic.Should().Be("gone");
        actions.OfType<TopicSyncAction.Skip>().Should().ContainSingle().Which.Topic.Should().Be("kept");
    }

    [Fact]
    public void DecideLifecycle_DeleteWithTopicInFacts_ProducesLifecycleDelete()
    {
        // Arrange: delete-заявка на топик, который есть в факте Kafka.
        var tickets = new[] { new TopicLifecycleTicket("orders", TopicLifecycleOps.Delete, 0, null, null, 1, "u") };
        var facts = new[] { new TopicFact("orders", 3, 1, null) };
        var registry = new[] { new KafkaTopicReg("orders", 3, 1, null, null, null, null, 1, false) };

        // Act
        var actions = TopicSyncDecision.DecideLifecycle(tickets, facts, registry);

        // Assert
        actions.Should().ContainSingle().Which.Should().BeOfType<TopicSyncAction.LifecycleDelete>()
            .Which.Topic.Should().Be("orders");
    }

    [Fact]
    public void DecideLifecycle_CreateWithoutTopic_ProducesLifecycleCreate()
    {
        // Arrange: create-заявка, топика нет ни в факте, ни в реестре.
        var tickets = new[] { new TopicLifecycleTicket("audit", TopicLifecycleOps.Create, 12, 3,
            new Dictionary<string, string> { ["retention.ms"] = "86400000" }, 1, "u") };

        // Act
        var actions = TopicSyncDecision.DecideLifecycle(tickets, [], []);

        // Assert
        actions.Should().ContainSingle().Which.Should().BeOfType<TopicSyncAction.LifecycleCreate>()
            .Which.Should().BeEquivalentTo(new { Topic = "audit", Partitions = 12, ReplicationFactor = (short)3 });
    }

    [Fact]
    public void DecideLifecycle_CreateWithMissingRegistryKey_ProducesLifecycleCreate()
    {
        // Arrange: create-заявка при висящем missing-ключе реестра (топика нет в
        // факте — «пересоздание», arch/15 §3.1): desired у ключа уже отменён.
        var tickets = new[] { new TopicLifecycleTicket("ghost", TopicLifecycleOps.Create, 3, 1, null, 1, "u") };
        var registry = new[] { new KafkaTopicReg("ghost", 3, 1, null, null, null, null, 1, true) };

        // Act
        var actions = TopicSyncDecision.DecideLifecycle(tickets, [], registry);

        // Assert: топика нет в факте — создаём (missing-ветка автосинка снимет
        // missing следующим тиком по появившемуся факту).
        actions.Should().ContainSingle().Which.Should().BeOfType<TopicSyncAction.LifecycleCreate>()
            .Which.Topic.Should().Be("ghost");
    }

    [Fact]
    public void DecideLifecycle_CreateWithTopicInFacts_CleanupAsAlreadyExists()
    {
        // Arrange: create-заявка при живом топике (панель обошла проверку — мусор).
        var tickets = new[] { new TopicLifecycleTicket("orders", TopicLifecycleOps.Create, 6, 1, null, 1, "u") };
        var facts = new[] { new TopicFact("orders", 3, 1, null) };

        // Act
        var actions = TopicSyncDecision.DecideLifecycle(tickets, facts, []);

        // Assert: cleanup create-заявки («уже существует, параметры не применены»).
        actions.Should().ContainSingle().Which.Should().BeOfType<TopicSyncAction.LifecycleCleanup>()
            .Which.Should().BeEquivalentTo(new { Topic = "orders", Op = TopicLifecycleOps.Create });
    }

    [Fact]
    public void DecideLifecycle_BothTickets_CleanupCreateThenDelete()
    {
        // Arrange: обе заявки живы (etcd-мусор).
        var tickets = new[]
        {
            new TopicLifecycleTicket("orders", TopicLifecycleOps.Create, 6, 1, null, 1, "u"),
            new TopicLifecycleTicket("orders", TopicLifecycleOps.Delete, 0, null, null, 2, "u"),
        };
        var facts = new[] { new TopicFact("orders", 3, 1, null) };

        // Act
        var actions = TopicSyncDecision.DecideLifecycle(tickets, facts, []);

        // Assert: сначала чистка create (арх/15 §3.1 — ДО исполнения delete),
        // затем delete; create не исполняется.
        actions.Should().HaveCount(2);
        actions[0].Should().BeOfType<TopicSyncAction.LifecycleCleanup>().Which.Op.Should().Be(TopicLifecycleOps.Create);
        actions[1].Should().BeOfType<TopicSyncAction.LifecycleDelete>();
    }

    [Fact]
    public void DecideLifecycle_DeleteWithoutTopic_CleanupExternal()
    {
        // Arrange: delete-заявка, топика нет в факте (удалён CLI раньше).
        var tickets = new[] { new TopicLifecycleTicket("orders", TopicLifecycleOps.Delete, 0, null, null, 1, "u") };
        var registry = new[] { new KafkaTopicReg("orders", 3, 1, null, null, null, null, 1, true) }; // missing-ключ висит

        // Act
        var actions = TopicSyncDecision.DecideLifecycle(tickets, [], registry);

        // Assert: cleanup delete-заявки; act-ветка снесёт и missing-ключ.
        actions.Should().ContainSingle().Which.Should().BeOfType<TopicSyncAction.LifecycleCleanup>()
            .Which.Op.Should().Be(TopicLifecycleOps.Delete);
    }

    [Fact]
    public void DecideLifecycle_InternalTopicTicket_CleanupWithoutExecution()
    {
        // Arrange: заявка на __-имя — панель такие не ставит, мусор.
        var tickets = new[] { new TopicLifecycleTicket("__consumer_offsets", TopicLifecycleOps.Create, 1, 1, null, 1, "u") };

        // Act
        var actions = TopicSyncDecision.DecideLifecycle(tickets, [], []);

        // Assert
        actions.Should().ContainSingle().Which.Should().BeOfType<TopicSyncAction.LifecycleCleanup>();
    }

    [Fact]
    public void DecideLifecycle_InternalTopicDeleteTicket_CleanupWithoutExecution()
    {
        // Arrange: delete-заявка на __-имя — симметричный guard (не полагаемся
        // на неявный фильтр DescribeFactsAsync; ревью Фазы 4 r1).
        var tickets = new[] { new TopicLifecycleTicket("__consumer_offsets", TopicLifecycleOps.Delete, 0, null, null, 1, "u") };

        // Act
        var actions = TopicSyncDecision.DecideLifecycle(tickets, [], []);

        // Assert: cleanup без DeleteTopics — даже если бы факт содержал __-имя.
        actions.Should().ContainSingle().Which.Should().BeOfType<TopicSyncAction.LifecycleCleanup>()
            .Which.Op.Should().Be(TopicLifecycleOps.Delete);
    }
}
