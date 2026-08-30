using FluentAssertions;
using KafkaWorker.Core.Model;
using KafkaWorker.Core.Templates;

namespace KafkaWorker.UnitTests.Templates;

// Тесты генератора env брокера (arch/16 §2.2): детерминированный набор KAFKA_*
// из заявки + ролей KRaft + кредов; advertised CLIENT — по правилу arch/16 §2.1
// (AdvertisedClient передан вычисленным: AdvertisedClientHost ?? docker-хост).

public class NodeEnvBuilderTests
{
    private static KafkaClusterConfig Config(int brokers, int rf = 3, int minIsr = 2) =>
        new(brokers, rf, minIsr, 12, 604800000L, 1756500000L, null);

    private static NodeEnvSpec Spec(
        int nodeId = 1,
        string nodeName = "broker1",
        bool isController = true,
        int brokerCount = 3,
        string advertisedClient = "host.docker.internal:16001",
        string[]? voters = null,
        string[]? passwords = null) => new(
        "events", nodeId, nodeName, advertisedClient, isController,
        voters ?? ["1@broker1:9093", "2@broker2:9093", "3@broker3:9093"],
        "app", passwords ?? ["OldPassword0123456789AbCdEf01"],
        Config(brokerCount), brokerCount, "/var/lib/kafka/data");

    [Fact]
    public void Build_ControllerNode_CombinedRolesAndQuorum()
    {
        // Arrange: 3-брокерный кластер, нода 1 — controller (m=min(3,3)).
        var spec = Spec(nodeId: 1, isController: true, brokerCount: 3);

        // Act: генерация env.
        var env = NodeEnvBuilder.Build(spec);

        // Assert: combined-роль, кворум всех controller-нод, слушатели и карта протоколов.
        env["KAFKA_PROCESS_ROLES"].Should().Be("broker,controller");
        env["KAFKA_NODE_ID"].Should().Be("1");
        env["KAFKA_CONTROLLER_QUORUM_VOTERS"].Should().Be("1@broker1:9093,2@broker2:9093,3@broker3:9093");
        env["KAFKA_LISTENERS"].Should().Be("CONTROLLER://:9093,INTERNAL://:9092,CLIENT://:9094");
        env["KAFKA_LISTENER_SECURITY_PROTOCOL_MAP"]
            .Should().Be("CONTROLLER:PLAINTEXT,INTERNAL:SASL_PLAINTEXT,CLIENT:SASL_PLAINTEXT");
        env["KAFKA_CONTROLLER_LISTENER_NAMES"].Should().Be("CONTROLLER");
        env["KAFKA_INTER_BROKER_LISTENER_NAME"].Should().Be("INTERNAL");
        env["KAFKA_SASL_ENABLED_MECHANISMS"].Should().Be("PLAIN");
    }

    [Fact]
    public void Build_BrokerOnlyNode_NoControllerListenerAndVoterSlot()
    {
        // Arrange: добавляемая нода broker4 — broker-only (кворум не меняется).
        var spec = Spec(nodeId: 4, nodeName: "broker4", isController: false, brokerCount: 4,
            voters: ["1@broker1:9093", "2@broker2:9093", "3@broker3:9093"]);

        // Act: генерация env.
        var env = NodeEnvBuilder.Build(spec);

        // Assert: роль только broker; CONTROLLER-листенер отсутствует;
        // voters — прежний кворум (нода в него не входит).
        env["KAFKA_PROCESS_ROLES"].Should().Be("broker");
        env["KAFKA_LISTENERS"].Should().Be("INTERNAL://:9092,CLIENT://:9094");
        env["KAFKA_CONTROLLER_QUORUM_VOTERS"].Should().NotContain("4@");
        env["KAFKA_ADVERTISED_LISTENERS"].Should().NotContain("CONTROLLER");
    }

    [Fact]
    public void Build_SingleBrokerCluster_InternalTopicsRfOne()
    {
        // Arrange: 1-брокерный стенд — формулы min(3,B)/min(2,B) дают 1.
        var spec = Spec(nodeId: 1, isController: true, brokerCount: 1,
            voters: ["1@broker1:9093"]);

        // Act: генерация env.
        var env = NodeEnvBuilder.Build(spec);

        // Assert: служебные топики стартуют на одном брокере.
        env["KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR"].Should().Be("1");
        env["KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR"].Should().Be("1");
        env["KAFKA_TRANSACTION_STATE_LOG_MIN_ISR"].Should().Be("1");
    }

    [Fact]
    public void Build_AdvertisedListeners_ClientFromSpec_InternalDnsName()
    {
        // Arrange: advertised CLIENT уже вычислен по правилу arch/16 §2.1.
        var spec = Spec(advertisedClient: "host.docker.internal:16002", nodeName: "broker2", nodeId: 2);

        // Act: генерация env.
        var env = NodeEnvBuilder.Build(spec);

        // Assert: CLIENT — advertised-хост:клиентский порт; INTERNAL — docker-DNS имя ноды.
        env["KAFKA_ADVERTISED_LISTENERS"].Should().Be("INTERNAL://broker2:9092,CLIENT://host.docker.internal:16002");
    }

    [Fact]
    public void Build_Jaas_SingleAndDualUsers()
    {
        // Arrange: обычный режим — один кред; окно ротации — два (arch/16 §5 H).
        var single = Spec(passwords: ["OldPassword0123456789AbCdEf01"]);
        var dual = Spec(passwords: ["OldPassword0123456789AbCdEf01", "NewPassword0123456789AbCdEf0"]);

        // Act: генерация env обоих вариантов.
        var singleEnv = NodeEnvBuilder.Build(single);
        var dualEnv = NodeEnvBuilder.Build(dual);

        // Assert: JAAS содержит user_<name>="<password>"; в окне ротации — оба
        // пользователя (фаза A: OLD рабочий, NEW уже валиден).
        singleEnv["KAFKA_LISTENER_NAME_CLIENT_PLAIN_SASL_JAAS_CONFIG"]
            .Should().Contain("user_app=\"OldPassword0123456789AbCdEf01\"")
            .And.NotContain("user_app2");
        dualEnv["KAFKA_LISTENER_NAME_CLIENT_PLAIN_SASL_JAAS_CONFIG"]
            .Should().Contain("user_app=\"OldPassword0123456789AbCdEf01\"")
            .And.Contain("user_app2=\"NewPassword0123456789AbCdEf0\"");
        dualEnv["KAFKA_LISTENER_NAME_INTERNAL_PLAIN_SASL_JAAS_CONFIG"]
            .Should().Contain("user_app=\"OldPassword0123456789AbCdEf01\"");

        // Inter-broker-креды (фикс 3-брокерного e2e C5): INTERNAL-JAAS несёт
        // username/password клиента (broker-as-client) + user_inter; CLIENT —
        // нет (внешние клиенты не inter). Пароль inter детерминирован и НЕ
        // ротируется (app-окно его не трогает).
        var inter = NodeEnvBuilder.InterBrokerPassword(dual.Cluster);
        dualEnv["KAFKA_LISTENER_NAME_INTERNAL_PLAIN_SASL_JAAS_CONFIG"]
            .Should().Contain($"username=\"inter\" password=\"{inter}\"")
            .And.Contain($"user_inter=\"{inter}\"");
        singleEnv["KAFKA_LISTENER_NAME_INTERNAL_PLAIN_SASL_JAAS_CONFIG"]
            .Should().Contain("username=\"inter\"");
        singleEnv["KAFKA_LISTENER_NAME_CLIENT_PLAIN_SASL_JAAS_CONFIG"]
            .Should().NotContain("username=");
    }

    [Fact]
    public void Build_Jaas_PasswordsQuoted_EvenDigitLeading()
    {
        // Arrange: пароль начинается с ЦИФРЫ (алфавит [A-Za-z0-9] — ~16%
        // генераций): Java JAAS-парсер не принимает незакавыченное значение,
        // начинающееся с цифры («Value not specified for key …» — брокер
        // падает на старте, вскрыто flaky-падением интеграционного теста).
        var digitLeading = Spec(passwords: ["0epSfWoy7q5SJu9RhhK8F8eHxzHuvx1A"]);

        // Act: генерация env.
        var env = NodeEnvBuilder.Build(digitLeading);

        // Assert: все пароли user_* — в двойных кавычках (валидны для
        // Java-парсера при любом первом символе; согласовано с
        // username/password/user_inter в INTERNAL-JAAS).
        env["KAFKA_LISTENER_NAME_CLIENT_PLAIN_SASL_JAAS_CONFIG"]
            .Should().Contain("user_app=\"0epSfWoy7q5SJu9RhhK8F8eHxzHuvx1A\"");
        env["KAFKA_LISTENER_NAME_INTERNAL_PLAIN_SASL_JAAS_CONFIG"]
            .Should().Contain("user_app=\"0epSfWoy7q5SJu9RhhK8F8eHxzHuvx1A\"");
        env.Values.OfType<string>().Where(v => v.Contains("user_app"))
            .Should().NotContain(v => v.Contains("user_app=0epSfWoy"));
    }

    [Fact]
    public void InterBrokerPassword_DeterministicAndStable()
    {
        // Arrange / Act
        var first = NodeEnvBuilder.InterBrokerPassword("events");
        var second = NodeEnvBuilder.InterBrokerPassword("events");
        var other = NodeEnvBuilder.InterBrokerPassword("payments");

        // Assert: стабилен, уникален per-cluster, 32 симв [A-Za-z0-9].
        first.Should().Be(second);
        first.Should().NotBe(other);
        first.Should().HaveLength(32).And.MatchRegex("^[A-Za-z0-9]{32}$");
    }

    [Fact]
    public void Build_DefaultConfigsFromRequestAndFixedKeys()
    {
        // Arrange: заявка B=3/R=3/M=2/P=12/X=7д.
        var spec = Spec(brokerCount: 3);

        // Act: генерация env.
        var env = NodeEnvBuilder.Build(spec);

        // Assert: default-конфиги заявки + фиксированные значения + том + CLUSTER_ID.
        env["KAFKA_DEFAULT_REPLICATION_FACTOR"].Should().Be("3");
        env["KAFKA_MIN_INSYNC_REPLICAS"].Should().Be("2");
        env["KAFKA_NUM_PARTITIONS"].Should().Be("12");
        env["KAFKA_LOG_RETENTION_MS"].Should().Be("604800000");
        env["KAFKA_AUTO_CREATE_TOPICS_ENABLE"].Should().Be("false");
        env["KAFKA_LOG_DIRS"].Should().Be("/var/lib/kafka/data");
        env["CLUSTER_ID"].Should().Be(NodeEnvBuilder.ClusterId("events"));
        // Один и тот же кластер — один и тот же CLUSTER_ID (переживает пересоздание).
        NodeEnvBuilder.ClusterId("events").Should().Be(NodeEnvBuilder.ClusterId("events"));
        NodeEnvBuilder.ClusterId("events").Should().HaveLength(22);
        NodeEnvBuilder.ClusterId("other").Should().NotBe(NodeEnvBuilder.ClusterId("events"));
    }
}
