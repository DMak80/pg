using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using FluentAssertions;
using KafkaWorker.Core;
using KafkaWorker.Provisioning.Processes;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace KafkaWorker.IntegrationTests.Kafka;

// TopicSync против реального Kafka (план C1-шаг 4, критерий spec §4.3/§9.5):
// автосинк факта → desired-конфиг применяется и снимается → partitions↑ →
// исчезновение топика без/с desired (missing-ветка) → отмена заявки удаляет
// ключ. Фикстура — общая с волной A (etcd + 1-брокерный кластер через
// ProvisioningProcess); все операции чтения ключей — только etcd.
// Per-class коллекция (/AGENTS.base §13; spec §6.3.2): свой инстанс
// KafkaClusterFixture — свой etcd-контур, свои кластеры/сети (RunTag),
// своё окно портов и AdminFactory; классы параллельны (потолок —
// xunit.runner.json), факты класса последовательны.
[CollectionDefinition(Name)]
public sealed class TopicSyncCollection : ICollectionFixture<KafkaClusterFixture>
{
    public const string Name = "kfw-kafka-topicsync";
}

[Collection(TopicSyncCollection.Name)]
public class TopicSyncTests(KafkaClusterFixture fixture)
{
    [Fact]
    public async Task FullTopicSyncLifecycle_FactDesiredPartitionsMissing()
    {
        const string cluster = "itsync";
        const string topic = "sync1";
        var ct = TestContext.Current.CancellationToken;

        // Arrange: 1-брокерный кластер через provisioning-процесс (≤ 120 с).
        await fixture.SeedClusterAsync(cluster, brokers: 1);
        var claims = new ClaimStore("/kafkaworker", [fixture.Endpoint], fixture.Gateway, TimeProvider.System);
        await claims.TryClaimClusterAsync(cluster, ct);
        var journal = new WorkJournal("/kafkaworker", fixture.Gateway, [fixture.Endpoint]);
        var provision = new ProvisioningProcess(
            fixture.Gateway, [fixture.Endpoint], fixture.Driver, claims, journal,
            new PortAllocLock("/kafkaworker", [fixture.Endpoint], fixture.Gateway, TimeProvider.System, claims.InstanceId),
            new PortAllocIndex(fixture.Gateway, [fixture.Endpoint], NullLogger<PortAllocIndex>.Instance),
            new ClusterSecretEnsurer(fixture.Gateway, [fixture.Endpoint]),
            fixture.AdminFactory, new ClusterConfigConverger(fixture.AdminFactory),
            fixture.Options, fixture.Certificates,
            snapshot: null);
        var sync = new TopicSyncProcess(
            fixture.Gateway, [fixture.Endpoint], claims, journal,
            fixture.AdminFactory, TimeProvider.System, intervalSec: 0);

        var deadline = DateTimeOffset.UtcNow.AddSeconds(120);
        while (DateTimeOffset.UtcNow < deadline)
        {
            var snap = await fixture.SnapshotAsync(cluster);
            if (snap!.Config.State is null)
                break;

            await provision.RunAsync(snap, ct);
            await Task.Delay(3000, ct);
        }

        (await fixture.GetAsync($"/kafka/clusters/{cluster}/config")).Should().NotContain("state",
            "cluster is up — the verification phase below is meaningless without it");

        // Act 1: топик создан CLI/клиентом (3 партиции) → автосинк кладёт ключ.
        var builder = await fixture.DiscoveryAdminBuilderAsync(cluster, "admin");
        using (var admin = builder.Build())
        {
            await admin.CreateTopicsAsync([new TopicSpecification
            {
                Name = topic,
                NumPartitions = 3,
                ReplicationFactor = 1,
            }], new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(15) });
        }

        var snap1 = await fixture.SnapshotAsync(cluster);
        await sync.RunAsync(snap1!, ct);

        // Assert 1: ключ реестра = факт.
        var raw1 = await fixture.GetAsync($"/kafka/clusters/{cluster}/topics/{topic}");
        raw1.Should().NotBeNull("autosync writes the key for the new fact topic");
        using (var doc1 = JsonDocument.Parse(raw1!))
        {
            doc1.RootElement.GetProperty("partitions").GetInt32().Should().Be(3);
            doc1.RootElement.GetProperty("replication_factor").GetInt32().Should().Be(1);
            doc1.RootElement.TryGetProperty("desired", out _).Should().BeFalse();
            doc1.RootElement.GetProperty("missing").GetBoolean().Should().BeFalse();
        }

        // Act 2: desired-заявка retention 1 день (прямо в etcd, как панель RMW)
        // → автосинк применяет и снимает.
        var withDesired = raw1!
            .Replace("\"synced_unix\"", "\"desired\":{\"configs\":{\"retention.ms\":\"86400000\"}},\"desired_unix\":1756500950,\"desired_by\":\"test\",\"synced_unix\"");
        await fixture.Gateway.PutAsync(fixture.Endpoint, $"/kafka/clusters/{cluster}/topics/{topic}",
            withDesired, lease: null, ct);
        var snap2 = await fixture.SnapshotAsync(cluster);
        await sync.RunAsync(snap2!, ct);

        // Assert 2: DescribeTopicConfigs = заявке; desired снят; факт = заявке.
        using (var verify = builder.Build())
        {
            var configs = await verify.DescribeConfigsAsync(
                [new ConfigResource { Type = ResourceType.Topic, Name = topic }],
                new DescribeConfigsOptions { RequestTimeout = TimeSpan.FromSeconds(15) });
            configs.Single().Entries["retention.ms"].Value.Should().Be("86400000");
        }

        var raw2 = await fixture.GetAsync($"/kafka/clusters/{cluster}/topics/{topic}");
        using (var doc2 = JsonDocument.Parse(raw2!))
        {
            doc2.RootElement.TryGetProperty("desired", out _).Should().BeFalse("ticket executed and removed");
            doc2.RootElement.GetProperty("configs").GetProperty("retention.ms").GetString()
                .Should().Be("86400000");
        }

        // Act 3: desired partitions 3→6 → автосинк увеличивает.
        var withPartitions = raw2!
            .Replace("\"synced_unix\"", "\"desired\":{\"partitions\":6},\"desired_unix\":1756500951,\"desired_by\":\"test\",\"synced_unix\"");
        await fixture.Gateway.PutAsync(fixture.Endpoint, $"/kafka/clusters/{cluster}/topics/{topic}",
            withPartitions, lease: null, ct);
        var snap3 = await fixture.SnapshotAsync(cluster);
        await sync.RunAsync(snap3!, ct);

        // Assert 3: партиции выросли, ключ = факт.
        using (var verify3 = builder.Build())
        {
            var metadata = verify3.GetMetadata(TimeSpan.FromSeconds(15));
            metadata.Topics.Single(t => t.Topic == topic).Partitions.Should().HaveCount(6);
        }

        var raw3 = await fixture.GetAsync($"/kafka/clusters/{cluster}/topics/{topic}");
        using (var doc3 = JsonDocument.Parse(raw3!))
        {
            doc3.RootElement.GetProperty("partitions").GetInt32().Should().Be(6);
            doc3.RootElement.TryGetProperty("desired", out _).Should().BeFalse();
        }

        // Act 4: удаление топика БЕЗ desired → ключ удаляется (реестр = факт).
        using (var killer = builder.Build())
        {
            await killer.DeleteTopicsAsync([topic], new DeleteTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(15) });
        }

        var snap4 = await fixture.SnapshotAsync(cluster);
        await sync.RunAsync(snap4!, ct);
        (await fixture.GetAsync($"/kafka/clusters/{cluster}/topics/{topic}")).Should().BeNull(
            "topic disappeared without a ticket — key removed");

        // Act 5: missing-ветка: валидная заявка ДО удаления → missing=true;
        // отмена заявки → ключ удалён (arch/15 §3).
        using (var creator = builder.Build())
        {
            await creator.CreateTopicsAsync([new TopicSpecification
            {
                Name = topic,
                NumPartitions = 3,
                ReplicationFactor = 1,
            }], new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(15) });
        }

        var snap5 = await fixture.SnapshotAsync(cluster);
        await sync.RunAsync(snap5!, ct); // ключ восстановлен фактом
        var restored = await fixture.GetAsync($"/kafka/clusters/{cluster}/topics/{topic}");
        var withMissingDesired = restored!
            .Replace("\"synced_unix\"", "\"desired\":{\"configs\":{\"retention.ms\":\"432000000\"}},\"desired_unix\":1756500952,\"desired_by\":\"test\",\"synced_unix\"");
        await fixture.Gateway.PutAsync(fixture.Endpoint, $"/kafka/clusters/{cluster}/topics/{topic}",
            withMissingDesired, lease: null, ct);
        using (var killer2 = builder.Build())
        {
            await killer2.DeleteTopicsAsync([topic], new DeleteTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(15) });
        }

        var snap6 = await fixture.SnapshotAsync(cluster);
        await sync.RunAsync(snap6!, ct);

        // Assert 5: missing=true с сохранённой заявкой.
        var raw6 = await fixture.GetAsync($"/kafka/clusters/{cluster}/topics/{topic}");
        raw6.Should().NotBeNull("ticket is alive — key not removed");
        using (var doc6 = JsonDocument.Parse(raw6!))
        {
            doc6.RootElement.GetProperty("missing").GetBoolean().Should().BeTrue();
            doc6.RootElement.GetProperty("desired").GetProperty("configs").GetProperty("retention.ms").GetString()
                .Should().Be("432000000");
        }

        // Act 6: отмена заявки (desired=null в ключе) → автосинк удаляет ключ.
        var cancelled = "{\"partitions\":3,\"replication_factor\":1,\"configs\":{\"retention.ms\":\"604800000\"},\"synced_unix\":1756500900,\"missing\":true}";
        await fixture.Gateway.PutAsync(fixture.Endpoint, $"/kafka/clusters/{cluster}/topics/{topic}",
            cancelled, lease: null, ct);
        var snap7 = await fixture.SnapshotAsync(cluster);
        await sync.RunAsync(snap7!, ct);

        (await fixture.GetAsync($"/kafka/clusters/{cluster}/topics/{topic}")).Should().BeNull(
            "after ticket cancellation autosync removes the key of the missing topic");

        // Финал: демонтаж кластера (не оставлять контейнеры коллегам по коллекции).
        var rawConfig = await fixture.GetAsync($"/kafka/clusters/{cluster}/config");
        await fixture.Gateway.PutAsync(fixture.Endpoint, $"/kafka/clusters/{cluster}/config",
            rawConfig!.Replace("}", ",\"state\":\"TO_REMOVE\"}", StringComparison.Ordinal), lease: null, ct);
        var dying = await fixture.SnapshotAsync(cluster);
        await new DeprovisioningProcess(
            fixture.Gateway, [fixture.Endpoint], fixture.Driver, claims, journal, snapshot: null)
            .RunAsync(cluster, dying!.Brokers.Select(b => b.Name).ToList(), ct);
        await claims.DisposeAsync();
    }
}
