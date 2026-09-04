using Confluent.Kafka;
using Confluent.Kafka.Admin;
using FluentAssertions;
using KafkaWorker.Core;
using KafkaWorker.Etcd.Coordination;
using KafkaWorker.Docker.Drivers;
using KafkaWorker.Etcd.Parsing;
using KafkaWorker.Provisioning.Processes;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace KafkaWorker.IntegrationTests.Kafka;

// Converge-миграция PLAINTEXT→SASL_SSL (t03 Ф6, spec §8.2): «премиграционный»
// кластер (старый env — локальная копия прежней таблицы NodeEnvBuilder; старого
// кода в src больше нет, единственное допустимое место SASL_PLAINTEXT) → тики
// SecurityMigrator до NotNeeded → endpoints/данные живы, admin+ACL в новом
// каноне, app производит/потребляет по SASL_SSL.
[Collection(KafkaCollection.Name)]
public class SecurityMigrationTests(KafkaClusterFixture fixture)
{
    [Fact]
    public async Task Migration_PlainToTls_EndpointsDataLive_AppWorksAfter()
    {
        var cluster = fixture.Cluster("mig");
        var ct = TestContext.Current.CancellationToken;

        // Arrange 1: премиграционный кластер — сид + ТОЛЬКО app-креды (ensure
        // admin/CA не выполнялся).
        await fixture.SeedClusterAsync(cluster, brokers: 1);
        // Роль ноды (фиксируется provisioning'ом в реальном контуре; в тесте — сид).
        await fixture.Gateway.PutAsync(fixture.Endpoint, $"/kafka/clusters/{cluster}/brokers/broker1/role",
            "controller", null, ct);
        await fixture.Gateway.PutAsync(fixture.Endpoint, $"/kafka/clusters/{cluster}/app_user", "app", null, ct);
        await fixture.Gateway.PutAsync(fixture.Endpoint, $"/kafka/clusters/{cluster}/app_password",
            "LegacyAppPassword0123456789", null, ct);
        var claims = new ClaimStore([fixture.Endpoint], fixture.Gateway, TimeProvider.System);
        await claims.TryClaimClusterAsync(cluster, ct);
        var journal = new WorkJournal(fixture.Gateway, [fixture.Endpoint]);

        // host — ИМЯ docker-хоста из таблицы драйвера ("local"), не адрес.
        var portAlloc = """{"broker1":{"host":"local","client":PORT}}""".Replace("PORT", fixture.PortFrom.ToString());
        await fixture.Gateway.PutAsync(fixture.Endpoint, $"/kafkaworker/portalloc/{cluster}",
            portAlloc, null, ct);
        // Дискавери-факт legacy-кластера (писался воркером по факту DescribeCluster).
        await fixture.Gateway.PutAsync(fixture.Endpoint, $"/kafka/clusters/{cluster}/endpoints",
            $"localhost:{fixture.PortFrom}", null, ct);

        // Старый env (SASL_PLAINTEXT-канон, до t03) + локальный порт публикации.
        var plainEnv = LegacyPlainEnv(cluster, "broker1", fixture.PortFrom);
        var spec = new KafkaNodeSpec(
            cluster, "broker1", "local", fixture.PortFrom, fixture.Options.NodeImage, plainEnv,
            CpuCores: 1, MemoryBytes: 512L * 1024 * 1024);
        var ensuredNode = await fixture.Driver.EnsureNodeAsync(spec, ct);
        ensuredNode.IsSuccess.Should().BeTrue(
            "контейнер legacy-брокера обязан подняться: {0}", ensuredNode.Error?.Message);

        // Arrange 2: готовность PLAINTEXT-кластера + сообщение в топике legacy.
        var legacyBootstrap = $"localhost:{fixture.PortFrom}";
        var plainConfig = new AdminClientConfig
        {
            BootstrapServers = legacyBootstrap,
            SecurityProtocol = SecurityProtocol.SaslPlaintext,
            SaslMechanism = SaslMechanism.Plain,
            SaslUsername = "app",
            SaslPassword = "LegacyAppPassword0123456789",
        };
        var deadline = DateTimeOffset.UtcNow.AddSeconds(120);
        using (var plainAdmin = new AdminClientBuilder(plainConfig).Build())
        {
            while (DateTimeOffset.UtcNow < deadline)
            {
                try
                {
                    if (plainAdmin.GetMetadata(TimeSpan.FromSeconds(5)).Brokers.Count >= 1)
                        break;
                }
                catch (KafkaException)
                {
                    // брокер ещё поднимается
                }

                await Task.Delay(2000, ct);
            }
        }

        var topic = $"legacy-{fixture.RunTag}";
        using (var plainAdmin = new AdminClientBuilder(plainConfig).Build())
            await plainAdmin.CreateTopicsAsync(
                [new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }],
                new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(15) });
        var legacyMessage = $"legacy-{Guid.NewGuid():N}";
        var legacyProducerConfig = new ProducerConfig
        {
            BootstrapServers = legacyBootstrap,
            SecurityProtocol = SecurityProtocol.SaslPlaintext,
            SaslMechanism = SaslMechanism.Plain,
            SaslUsername = "app",
            SaslPassword = "LegacyAppPassword0123456789",
        };
        using (var producer = new ProducerBuilder<Null, string>(legacyProducerConfig).Build())
            (await producer.ProduceAsync(topic, new Message<Null, string> { Value = legacyMessage }, ct)).Status
                .Should().Be(PersistenceStatus.Persisted);

        var endpointsBefore = await fixture.GetAsync($"/kafka/clusters/{cluster}/endpoints");

        // Act: тики SecurityMigrator до NotNeeded (поллинг ≤ 200 с).
        var migrator = new SecurityMigrator(
            fixture.Gateway, [fixture.Endpoint], fixture.Driver, claims, journal,
            new ClusterSecretEnsurer(fixture.Gateway, [fixture.Endpoint]),
            fixture.AdminFactory, new ClusterConfigConverger(fixture.AdminFactory),
            fixture.Options, fixture.Certificates);
        var migrateDeadline = DateTimeOffset.UtcNow.AddSeconds(200);
        var outcome = SecurityMigrator.MigrationOutcome.InProgress;
        while (DateTimeOffset.UtcNow < migrateDeadline)
        {
            var snap = await fixture.SnapshotAsync(cluster);
            var result = await migrator.RunAsync(snap!, ct);
            result.IsSuccess.Should().BeTrue($"тик миграции не должен падать: {result.Error?.Message}");
            outcome = result.Value;
            if (outcome == SecurityMigrator.MigrationOutcome.NotNeeded)
                break;
            await Task.Delay(3000, ct);
        }

        // Assert 1: миграция завершена; новые ключи etcd появились.
        var migJournal = await journal.ReadAsync(cluster, ct);
        outcome.Should().Be(SecurityMigrator.MigrationOutcome.NotNeeded,
            "journal={0}", migJournal.Value?.Phase ?? "<нет>");
        (await fixture.GetAsync($"/kafka/clusters/{cluster}/admin_user")).Should().Be("admin");
        (await fixture.GetAsync($"/kafka/clusters/{cluster}/admin_password"))
            .Should().HaveLength(32).And.MatchRegex("^[A-Za-z0-9]{32}$");
        (await fixture.GetAsync($"/kafka/clusters/{cluster}/ca_pem"))
            .Should().StartWith("-----BEGIN CERTIFICATE-----");
        (await fixture.GetAsync($"/kafka/clusters/{cluster}/ca_key"))
            .Should().StartWith("-----BEGIN PRIVATE KEY-----");

        // Assert 2: endpoints НЕ изменились (хосты/порты не менялись).
        var endpointsAfter = await fixture.GetAsync($"/kafka/clusters/{cluster}/endpoints");
        endpointsAfter.Should().Be(endpointsBefore);

        // Assert 3: env контейнера в новом каноне.
        var env = await fixture.Driver.NodeEnvAsync(cluster, "broker1", ct);
        env.IsSuccess.Should().BeTrue();
        env.Value.Should().NotBeNull();
        env.Value!["KAFKA_SSL_TRUSTSTORE_TYPE"].Should().Be("PEM");

        // Assert 4: SASL_SSL-consumer (admin + ca_pem) читает legacy-сообщение —
        // данные пережили миграцию.
        var caPem = await fixture.GetAsync($"/kafka/clusters/{cluster}/ca_pem");
        var adminPassword = await fixture.GetAsync($"/kafka/clusters/{cluster}/admin_password");
        var tlsBootstrap = endpointsAfter!.Replace("host.docker.internal", "localhost", StringComparison.Ordinal);
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = tlsBootstrap,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.Plain,
            SaslUsername = "admin",
            SaslPassword = adminPassword!,
            GroupId = $"mig-{fixture.RunTag}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
        };
        consumerConfig.Set("ssl.ca.pem", caPem!);
        using (var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build())
        {
            consumer.Subscribe(topic);
            var consumed = consumer.Consume(TimeSpan.FromSeconds(30));
            consumed.Should().NotBeNull("legacy-сообщение обязано быть прочитано по SASL_SSL");
            consumed!.Message.Value.Should().Be(legacyMessage);
        }

        // Assert 5: app-кред по SASL_SSL производит (ACL роли app после M4).
        var appPassword = await fixture.GetAsync($"/kafka/clusters/{cluster}/app_password");
        var appProducerConfig = new ProducerConfig
        {
            BootstrapServers = tlsBootstrap,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.Plain,
            SaslUsername = "app",
            SaslPassword = appPassword!,
        };
        appProducerConfig.Set("ssl.ca.pem", caPem);
        using (var producer = new ProducerBuilder<Null, string>(appProducerConfig).Build())
            (await producer.ProduceAsync(topic, new Message<Null, string> { Value = "post-migration" }, ct)).Status
                .Should().Be(PersistenceStatus.Persisted);

        // Assert 6: повторный тик — NotNeeded сразу (идемпотентность).
        var repeatSnap = await fixture.SnapshotAsync(cluster);
        var repeat = await migrator.RunAsync(repeatSnap!, ct);
        repeat.IsSuccess.Should().BeTrue();
        repeat.Value.Should().Be(SecurityMigrator.MigrationOutcome.NotNeeded);

        await claims.DisposeAsync();
    }

    // Локальная копия прежней таблицы NodeEnvBuilder (SASL_PLAINTEXT-канон,
    // до t03): старый код в src больше не существует — тест фиксирует «как было».
    private static IReadOnlyDictionary<string, string> LegacyPlainEnv(
        string cluster, string broker, int clientPort)
    {
        var interPassword = "LegacyInterPassword0123456789AB";
        return new Dictionary<string, string>
        {
            ["CLUSTER_ID"] = KafkaWorker.Core.Templates.NodeEnvBuilder.ClusterId(cluster),
            ["KAFKA_NODE_ID"] = "1",
            ["KAFKA_PROCESS_ROLES"] = "broker,controller",
            ["KAFKA_CONTROLLER_QUORUM_VOTERS"] = "1@broker1:9093",
            ["KAFKA_LISTENERS"] = "CONTROLLER://:9093,INTERNAL://:9092,CLIENT://:9094",
            ["KAFKA_ADVERTISED_LISTENERS"] = $"INTERNAL://broker1:9092,CLIENT://localhost:{clientPort}",
            ["KAFKA_LISTENER_SECURITY_PROTOCOL_MAP"] =
                "CONTROLLER:PLAINTEXT,INTERNAL:SASL_PLAINTEXT,CLIENT:SASL_PLAINTEXT",
            ["KAFKA_CONTROLLER_LISTENER_NAMES"] = "CONTROLLER",
            ["KAFKA_INTER_BROKER_LISTENER_NAME"] = "INTERNAL",
            ["KAFKA_SASL_ENABLED_MECHANISMS"] = "PLAIN",
            ["KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL"] = "PLAIN",
            ["KAFKA_LISTENER_NAME_INTERNAL_PLAIN_SASL_JAAS_CONFIG"] =
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                + $"username=\"inter\" password=\"{interPassword}\" user_inter=\"{interPassword}\" user_app=\"LegacyAppPassword0123456789\";",
            ["KAFKA_LISTENER_NAME_CLIENT_PLAIN_SASL_JAAS_CONFIG"] =
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                + "user_app=\"LegacyAppPassword0123456789\";",
            ["KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR"] = "1",
            ["KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR"] = "1",
            ["KAFKA_TRANSACTION_STATE_LOG_MIN_ISR"] = "1",
            ["KAFKA_DEFAULT_REPLICATION_FACTOR"] = "1",
            ["KAFKA_MIN_INSYNC_REPLICAS"] = "1",
            ["KAFKA_NUM_PARTITIONS"] = "3",
            ["KAFKA_AUTO_CREATE_TOPICS_ENABLE"] = "false",
            ["KAFKA_LOG_DIRS"] = "/var/lib/kafka/data",
        };
    }
}
