using Confluent.Kafka;
using Confluent.Kafka.Admin;
using FluentAssertions;
using KafkaWorker.Core;
using KafkaWorker.Etcd.Coordination;
using KafkaWorker.Provisioning.Processes;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace KafkaWorker.IntegrationTests.Kafka;

// Маркер-кейс мерж-гейта t03 (spec §8.3): поднятие TLS-кластера канона t03 —
// provisioning → SASL_SSL endpoints, приложение производит/потребляет через
// ca_pem + app-кред, ACL: app отказ на админ-операции, admin выполняет.
[Collection(KafkaCollection.Name)]
public class TlsClusterTests(KafkaClusterFixture fixture)
{
    [Fact]
    public async Task Provisioning_TlsClusterUp()
    {
        var cluster = fixture.Cluster("tls");
        var ct = TestContext.Current.CancellationToken;

        // Arrange: заявка 1-брокерного кластера + provisioning-цикл (поллинг, как FullLifecycle).
        await fixture.SeedClusterAsync(cluster, brokers: 1);
        var claims = new ClaimStore([fixture.Endpoint], fixture.Gateway, TimeProvider.System);
        await claims.TryClaimClusterAsync(cluster, ct);
        var journal = new WorkJournal(fixture.Gateway, [fixture.Endpoint]);
        var provision = new ProvisioningProcess(
            fixture.Gateway, [fixture.Endpoint], fixture.Driver, claims, journal,
            new PortAllocLock([fixture.Endpoint], fixture.Gateway, TimeProvider.System, claims.InstanceId),
            new PortAllocIndex(fixture.Gateway, [fixture.Endpoint], NullLogger<PortAllocIndex>.Instance),
            new ClusterSecretEnsurer(fixture.Gateway, [fixture.Endpoint]),
            fixture.AdminFactory, new ClusterConfigConverger(fixture.AdminFactory),
            fixture.Options, fixture.Certificates,
            snapshot: null);
        var deadline = DateTimeOffset.UtcNow.AddSeconds(200);
        while (DateTimeOffset.UtcNow < deadline)
        {
            var snap = await fixture.SnapshotAsync(cluster);
            if (snap!.Config.State is null)
                break; // provisioning завершён (config без state)
            var tick = await provision.RunAsync(snap, ct);
            tick.IsSuccess.Should().BeTrue(
                $"тик provisioning не должен падать (waiting-brokers — успех): {tick.Error?.Message}");
            await Task.Delay(3000, ct);
        }

        // Act 1: приложение (app-кред + ca_pem из etcd) производит и потребляет.
        var (bootstrap, caPem, appUser, appPassword) = await fixture.DiscoveryPartsAsync(cluster);
        var topic = $"orders-{fixture.RunTag}";
        using (var admin = (await fixture.DiscoveryAdminBuilderAsync(cluster, "admin")).Build())
            await admin.CreateTopicsAsync(
                [new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }],
                new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(15) });
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = bootstrap,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.Plain,
            SaslUsername = appUser,
            SaslPassword = appPassword,
        };
        producerConfig.Set("ssl.ca.pem", caPem);
        using (var producer = new ProducerBuilder<Null, string>(producerConfig).Build())
            (await producer.ProduceAsync(topic, new Message<Null, string> { Value = "hello-tls" }, ct)).Status
                .Should().Be(PersistenceStatus.Persisted);
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrap,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.Plain,
            SaslUsername = appUser,
            SaslPassword = appPassword,
            GroupId = $"g-{fixture.RunTag}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
        };
        consumerConfig.Set("ssl.ca.pem", caPem);
        using (var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build())
        {
            consumer.Subscribe(topic);
            var consumed = consumer.Consume(TimeSpan.FromSeconds(30));
            consumed.Message.Value.Should().Be("hello-tls");
        }

        // Act 2 / Assert 2: ACL deny-by-default — app-креду отказ в админ-операциях.
        using (var appAdmin = (await fixture.DiscoveryAdminBuilderAsync(cluster, "app")).Build())
        {
            var act = () => appAdmin.CreateTopicsAsync(
                [new TopicSpecification { Name = "forbidden-" + fixture.RunTag, NumPartitions = 1, ReplicationFactor = 1 }],
                new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(15) });
            await act.Should().ThrowAsync<CreateTopicsException>(
                "принципал User:app не имеет Create-ACL (16 §2.3), deny-by-default");
        }

        // Assert 3: admin-кред — super.user (CreateTopics прошёл выше); финальная
        // DescribeCluster admin-кредом успешна.
        using (var admin = (await fixture.DiscoveryAdminBuilderAsync(cluster, "admin")).Build())
            admin.GetMetadata(TimeSpan.FromSeconds(15)).Brokers.Should().HaveCount(1);

        await claims.DisposeAsync();
    }
}
