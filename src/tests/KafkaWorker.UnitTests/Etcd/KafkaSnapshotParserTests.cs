using System.Text.Json;
using FluentAssertions;
using KafkaWorker.Core.Model;
using KafkaWorker.Core.Templates;
using KafkaWorker.Etcd.Client;
using KafkaWorker.Etcd.Parsing;

namespace KafkaWorker.UnitTests.Etcd;

// Тесты парсера контроль-плейна /kafka/clusters/ (arch/15 §2–3): фикстуры —
// канонические примеры значений из arch-канона; толерантность к битому JSON
// и неизвестным ключам обязательна (parseError-запись, не исключение).

public class KafkaSnapshotParserTests
{
    // Валидные PEM-ключи CA для кейсов безопасности t03 (генерация один раз).
    private static readonly (string CaPem, string CaKeyPem) ValidCa = ClusterPki.GenerateCa("test");
    private static string ValidCaPem => ValidCa.CaPem;
    private static string ValidCaKeyPem => ValidCa.CaKeyPem;

    private static IReadOnlyList<Kv> LoadFixture(string name)
    {
        var path = Path.Combine(AppContext.BaseDirectory, "EtcdFixtures", "Kafka", name);
        var items = JsonSerializer.Deserialize<List<FixtureKv>>(File.ReadAllText(path), Json) ?? [];
        return items.Select(i => new Kv(i.Key, i.Value, i.ModRevision)).ToList();
    }

    private static readonly JsonSerializerOptions Json = new() { PropertyNameCaseInsensitive = true };

    private sealed record FixtureKv(string Key, string Value, ulong ModRevision);

    // Хелпер инлайновых Kv для lifecycle-кейсов (mod_revision непринципиален).
    private static Kv Kv(string key, string value) => new(key, value, 1);

    [Fact]
    public void Parse_FullPrefix_TwoClustersWithConfigBrokersTopics()
    {
        // Arrange: полный префикс — Active-кластер events + заявка pending (arch/15 §2.1).
        var kvs = LoadFixture("kafka-full.json");

        // Act: разбор префикса /kafka/clusters/.
        var result = KafkaSnapshotParser.Parse(kvs);

        // Assert: два кластера; events — Active с полным набором, pending — NOT_INITIALIZED.
        result.IsSuccess.Should().BeTrue();
        var clusters = result.Value;
        clusters.Should().HaveCount(2);
        var events = clusters.Single(c => c.Cluster == "events");
        events.Config.State.Should().BeNull(); // отсутствие state = Active
        events.Config.Brokers.Should().Be(3);
        events.Config.ReplicationFactor.Should().Be(3);
        events.Config.MinInSyncReplicas.Should().Be(2);
        events.Config.DefaultPartitions.Should().Be(12);
        events.Config.DefaultRetentionMs.Should().Be(604800000L);
        events.Config.CreatedUnix.Should().Be(1756500000L);
        events.Brokers.Should().HaveCount(3);
        events.Brokers[0].Name.Should().Be("broker1");
        events.Brokers[0].State.Should().Be("RUNNING");
        events.Brokers[0].Role.Should().Be("controller");
        events.Brokers[0].Resources.Should().NotBeNull();
        events.Brokers[0].Resources.Should().Be(new BrokerResources(2m, 4, 40));
        events.Topics.Should().HaveCount(3);
        events.Endpoints.Should().Be("host.docker.internal:16001,host.docker.internal:16002,host.docker.internal:16003");
        events.AppUser.Should().Be("app");
        events.AppPassword.Should().Be("AbCdEf0123456789AbCdEf0123456789");
        events.ParseErrors.Should().BeEmpty();

        var pending = clusters.Single(c => c.Cluster == "pending");
        pending.Config.State.Should().Be("NOT_INITIALIZED");
        pending.Brokers.Should().OnlyContain(b => b.State == "NOT_INITIALIZED");
        pending.Brokers.Should().OnlyContain(b => b.Role == null); // роль фиксирует воркер
        pending.Endpoints.Should().BeNull();
    }

    [Fact]
    public void Parse_OrdersTopic_DesiredPresent()
    {
        // Arrange: топик orders с живой desired-заявкой (arch/15 §3).
        var kvs = LoadFixture("kafka-full.json");

        // Act: разбор и выбор топика.
        var orders = KafkaSnapshotParser.Parse(kvs).Value
            .Single(c => c.Cluster == "events").Topics.Single(t => t.Topic == "orders");

        // Assert: факт + заявка читаются полностью.
        orders.Partitions.Should().Be(12);
        orders.ReplicationFactor.Should().Be(3);
        orders.Configs!["retention.ms"].Should().Be("604800000");
        orders.Desired.Should().NotBeNull();
        orders.Desired!.Partitions.Should().Be(16);
        orders.Desired.Configs!["retention.ms"].Should().Be("86400000");
        orders.DesiredUnix.Should().Be(1750000000L);
        orders.DesiredBy.Should().Be("admin");
        orders.SyncedUnix.Should().Be(1750000100L);
        orders.Missing.Should().BeFalse();
    }

    [Fact]
    public void Parse_PaymentsTopic_NoDesired_GhostMissing()
    {
        // Arrange: payments — без заявки; ghost — missing (топик исчез при живой заявке).
        var kvs = LoadFixture("kafka-full.json");

        // Act: разбор и выбор топиков.
        var topics = KafkaSnapshotParser.Parse(kvs).Value
            .Single(c => c.Cluster == "events").Topics;

        // Assert: desired=null у payments; missing=true у ghost.
        var payments = topics.Single(t => t.Topic == "payments");
        payments.Desired.Should().BeNull();
        payments.DesiredUnix.Should().BeNull();
        payments.Missing.Should().BeFalse();

        var ghost = topics.Single(t => t.Topic == "ghost");
        ghost.Missing.Should().BeTrue();
        ghost.Desired.Should().NotBeNull();
    }

    [Fact]
    public void Parse_BrokenConfig_ParseErrorWithoutException()
    {
        // Arrange: битый JSON config + битый topic + неизвестный ключ (arch/15 §6).
        var kvs = LoadFixture("kafka-degenerate.json");

        // Act: разбор вырожденного префикса.
        var result = KafkaSnapshotParser.Parse(kvs);

        // Assert: исключения нет; parseError-записи по config и topics/<bad>;
        // кластер broken всё равно построен (brokers видны), unknown-ключ не валит.
        result.IsSuccess.Should().BeTrue();
        var broken = result.Value.Single(c => c.Cluster == "broken");
        broken.ParseErrors.Should().Contain(e => e.Contains("config"));
        broken.ParseErrors.Should().Contain(e => e.Contains("topics/bad"));
        broken.Brokers.Should().HaveCount(1); // брокер жив, несмотря на битый config
        broken.ParseErrors.Should().Contain(e => e.Contains("resources")); // cpu="много" не разобрался
        broken.UnknownKeys.Should().Be(1); // /kafka/clusters/broken/surprise
    }

    [Fact]
    public void Parse_ToRemoveState_ReadAsString()
    {
        // Arrange: кластер dying с state=TO_REMOVE (arch/15 §2.1).
        var kvs = LoadFixture("kafka-degenerate.json");

        // Act: разбор.
        var dying = KafkaSnapshotParser.Parse(kvs).Value.Single(c => c.Cluster == "dying");

        // Assert: state — строка-значение (толерантно к будущим значениям).
        dying.Config.State.Should().Be("TO_REMOVE");
    }

    [Fact]
    public void Parse_UnknownBrokerState_KeptAsRawString()
    {
        // Arrange: брокер с незнакомым state (система развивается, arch/15 §6).
        var kvs = LoadFixture("kafka-degenerate.json");

        // Act: разбор.
        var result = KafkaSnapshotParser.Parse(kvs);

        // Assert: state — как есть строкой; ключи pg-домена игнорируются
        // (3 кластера kafka: broken/dying/legacy, /pgworker/ не считается).
        var legacy = result.Value.Single(c => c.Cluster == "legacy");
        legacy.Brokers.Single().State.Should().Be("WEIRD_STATE");
        legacy.Endpoints.Should().Be("host.docker.internal:16010");
        result.Value.Should().HaveCount(3);
    }

    [Fact]
    public void Parse_EmptyPrefix_NoClusters()
    {
        // Arrange: пустой префикс (кластеров нет).
        var kvs = Array.Empty<Kv>();

        // Act: разбор пустого набора.
        var result = KafkaSnapshotParser.Parse(kvs);

        // Assert: успех, пустой список, нет ошибок.
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().BeEmpty();
    }

    [Fact]
    public void Parse_LifecycleCreateTicket_FillsLifecycleTickets()
    {
        // Arrange: leaf-ключ заявки создания рядом с факт-ключом топика.
        var kvs = new List<Kv>
        {
            Kv("/kafka/clusters/events/config", """{"brokers":3,"replication_factor":3,"min_insync_replicas":2,"default_partitions":12,"default_retention_ms":604800000,"created_unix":1}"""),
            Kv("/kafka/clusters/events/topics/audit/desired.create",
                """{"partitions":12,"replication_factor":3,"configs":{"retention.ms":"86400000"},"requested_unix":1750000000,"requested_by":"admin"}"""),
        };

        // Act
        var result = KafkaSnapshotParser.Parse(kvs);

        // Assert: один тикет create с полными полями; факт-топиков нет.
        var cluster = result.Value.Single(c => c.Cluster == "events");
        cluster.LifecycleTickets.Should().ContainSingle().Which.Should().BeEquivalentTo(new TopicLifecycleTicket(
            "audit", "create", 12, 3,
            new Dictionary<string, string> { ["retention.ms"] = "86400000" },
            1750000000L, "admin"));
        cluster.Topics.Should().BeEmpty();
    }

    [Fact]
    public void Parse_LifecycleDeleteTicket_AndMalformedTicket()
    {
        // Arrange: заявка удаления + битый JSON второй заявки.
        var kvs = new List<Kv>
        {
            Kv("/kafka/clusters/events/config", """{"brokers":3,"replication_factor":3,"min_insync_replicas":2,"default_partitions":12,"default_retention_ms":604800000}"""),
            Kv("/kafka/clusters/events/topics/orders/desired.delete",
                """{"requested_unix":1750000100,"requested_by":"admin"}"""),
            Kv("/kafka/clusters/events/topics/bad/desired.create", """{oops"""),
        };

        // Act
        var result = KafkaSnapshotParser.Parse(kvs);

        // Assert: валидный delete-тикет; битый — parseError, не исключение.
        var cluster = result.Value.Single(c => c.Cluster == "events");
        cluster.LifecycleTickets.Should().ContainSingle(t => t.Topic == "orders" && t.Op == "delete");
        cluster.ParseErrors.Should().Contain(e => e.Contains("topics/bad"));
    }

    [Fact]
    public void Parse_TicketWithoutRequestedUnix_IsParseError()
    {
        // Arrange: JSON валиден, но аудита requested_unix нет — заявка битая
        // (панель пишет аудит всегда; образец — ParseRotations панели).
        var kvs = new List<Kv>
        {
            Kv("/kafka/clusters/events/config", """{"brokers":1,"replication_factor":1,"min_insync_replicas":1,"default_partitions":1,"default_retention_ms":1}"""),
            Kv("/kafka/clusters/events/topics/x/desired.delete", """{"requested_by":"u"}"""),
        };

        // Act
        var result = KafkaSnapshotParser.Parse(kvs);

        // Assert: parseError, тикет не создан.
        var cluster = result.Value.Single();
        cluster.LifecycleTickets.Should().BeEmpty();
        cluster.ParseErrors.Should().Contain(e => e.Contains("topics/x"));
    }

    [Fact]
    public void Parse_UnknownTopicsLeaf_CountsUnknownKey()
    {
        // Arrange: неизвестный leaf под topics/<T>/ — не ошибка, счётчик.
        var kvs = new List<Kv>
        {
            Kv("/kafka/clusters/events/config", """{"brokers":1,"replication_factor":1,"min_insync_replicas":1,"default_partitions":1,"default_retention_ms":1}"""),
            Kv("/kafka/clusters/events/topics/x/desired.pause", "{}"),
        };

        // Act
        var result = KafkaSnapshotParser.Parse(kvs);

        // Assert
        result.Value.Single().UnknownKeys.Should().Be(1);
    }

    // Канонический минимальный набор ключей кластера (config + хвост — extra).
    private static List<Kv> ClusterKvs(string cluster, List<Kv>? extra = null)
    {
        var kvs = new List<Kv>
        {
            Kv($"/kafka/clusters/{cluster}/config", """{"brokers":1,"replication_factor":1,"min_insync_replicas":1,"default_partitions":1,"default_retention_ms":1}"""),
        };
        if (extra is not null)
            kvs.AddRange(extra);
        return kvs;
    }

    [Fact]
    public void Parse_AdminAndCaKeys_FilledIntoSnapshot()
    {
        // Arrange: полный набор ключей кластера (вкл. admin_user/admin_password/ca_pem/ca_key).
        var kvs = ClusterKvs("events", extra:
        [
            Kv("/kafka/clusters/events/admin_user", "admin"),
            Kv("/kafka/clusters/events/admin_password", "AdminSecret0123456789AAAAAAA"),
            Kv("/kafka/clusters/events/ca_pem", ValidCaPem),
            Kv("/kafka/clusters/events/ca_key", ValidCaKeyPem),
        ]);

        // Act: разбор.
        var snap = KafkaSnapshotParser.Parse(kvs).Value.Single();

        // Assert: поля дискавери/секретов заполнены, unknownKeys их не считает.
        snap.AdminUser.Should().Be("admin");
        snap.AdminPassword.Should().Be("AdminSecret0123456789AAAAAAA");
        snap.CaPem.Should().Be(ValidCaPem);
        snap.CaKey.Should().Be(ValidCaKeyPem);
        snap.UnknownKeys.Should().Be(0);
    }

    [Fact]
    public void Parse_MalformedCaPem_ParseErrorAndNullField()
    {
        // Arrange: ca_pem — мусор (15 §6: битый PEM → parseError, ключ пропускается).
        var kvs = ClusterKvs("events", extra: [Kv("/kafka/clusters/events/ca_pem", "garbage")]);

        // Act: разбор.
        var snap = KafkaSnapshotParser.Parse(kvs).Value.Single();

        // Assert: поле null + запись parseErrors (не исключение).
        snap.CaPem.Should().BeNull();
        snap.ParseErrors.Should().Contain(e => e.Contains("ca_pem", StringComparison.Ordinal));
    }

    [Fact]
    public void Parse_MissingSecurityKeys_NullFields_NoErrors()
    {
        // Arrange: премиграционный кластер — только app-креды.
        var kvs = ClusterKvs("old", extra:
        [
            Kv("/kafka/clusters/old/app_user", "app"),
            Kv("/kafka/clusters/old/app_password", "AppSecret0123456789AAAAAAAA"),
        ]);

        // Act: разбор.
        var snap = KafkaSnapshotParser.Parse(kvs).Value.Single();

        // Assert: admin/CA null, ошибок нет — детект премиграционного кластера (M).
        snap.AdminUser.Should().BeNull();
        snap.CaPem.Should().BeNull();
        snap.ParseErrors.Should().BeEmpty();
    }
}
