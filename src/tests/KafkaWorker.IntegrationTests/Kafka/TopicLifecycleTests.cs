using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using FluentAssertions;
using KafkaWorker.Core;
using KafkaWorker.Etcd.Coordination;
using KafkaWorker.Provisioning.Processes;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace KafkaWorker.IntegrationTests.Kafka;

// Lifecycle-заявки против реального Kafka (t01, spec §4.2/§9.2/9.3/9.5):
// create → топик с заявленными параметрами, заявка снята, факт-ключ следующим
// тиком; delete → топик и оба ключа исчезли; сходимость обеих веток после
// «отказа между мутацией и del» (топик появился/исчез внешне при живой заявке).
// Фикстура — общая с волной A (etcd + 1-брокерный кластер, как TopicSyncTests).
[Collection(KafkaCollection.Name)]
public class TopicLifecycleTests(KafkaClusterFixture fixture)
{
    // Подъём кластера + готовый TopicSyncProcess под клэймом (образец TopicSyncTests).
    private static async Task<TopicSyncProcess> UpAsync(
        KafkaClusterFixture fixture, string cluster, CancellationToken ct)
    {
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
            fixture.Options, snapshot: null);
        var sync = new TopicSyncProcess(
            fixture.Gateway, [fixture.Endpoint], claims, journal,
            fixture.AdminFactory, TimeProvider.System, intervalSec: 0);

        // Бюджет 200 с — потолок с запасом над воркерным BrokerBootSec=100
        // (AGENTS.md: тестовый BrokerBootSec <= 100; стенд на этом же хосте).
        var deadline = DateTimeOffset.UtcNow.AddSeconds(200);
        while (DateTimeOffset.UtcNow < deadline)
        {
            var snap = await fixture.SnapshotAsync(cluster);
            if (snap!.Config.State is null)
                break;

            await provision.RunAsync(snap, ct);
            await Task.Delay(3000, ct);
        }

        (await fixture.GetAsync($"/kafka/clusters/{cluster}/config")).Should().NotContain("state",
            "кластер поднялся — lifecycle-проверки ниже бессмысленны без него");
        return sync;
    }

    // Демонтаж кластера (не оставлять контейнеры коллегам по коллекции).
    private static async Task DownAsync(
        KafkaClusterFixture fixture, string cluster, CancellationToken ct)
    {
        var claims = new ClaimStore([fixture.Endpoint], fixture.Gateway, TimeProvider.System);
        await claims.TryClaimClusterAsync(cluster, ct);
        var journal = new WorkJournal(fixture.Gateway, [fixture.Endpoint]);
        var rawConfig = await fixture.GetAsync($"/kafka/clusters/{cluster}/config");
        await fixture.Gateway.PutAsync(fixture.Endpoint, $"/kafka/clusters/{cluster}/config",
            rawConfig!.Replace("}", ",\"state\":\"TO_REMOVE\"}", StringComparison.Ordinal), lease: null, ct);
        var dying = await fixture.SnapshotAsync(cluster);
        await new DeprovisioningProcess(
            fixture.Gateway, [fixture.Endpoint], fixture.Driver, claims, journal, snapshot: null)
            .RunAsync(cluster, dying!.Brokers.Select(b => b.Name).ToList(), ct);
        await claims.DisposeAsync();
    }

    private static async Task<bool> TopicExistsAsync(
        KafkaClusterFixture fixture, string cluster, string topic, CancellationToken ct)
    {
        var builder = await fixture.DiscoveryAdminBuilderAsync(cluster);
        using var admin = builder.Build();
        var metadata = admin.GetMetadata(TimeSpan.FromSeconds(15));
        return metadata.Topics.Any(t => t.Topic == topic);
    }

    private static async Task<(int Partitions, string? Retention)> DescribeTopicAsync(
        KafkaClusterFixture fixture, string cluster, string topic, CancellationToken ct)
    {
        var builder = await fixture.DiscoveryAdminBuilderAsync(cluster);
        using var admin = builder.Build();
        var metadata = admin.GetMetadata(TimeSpan.FromSeconds(15));
        var partitions = metadata.Topics.Single(t => t.Topic == topic).Partitions.Count;
        var configs = await admin.DescribeConfigsAsync(
            [new ConfigResource { Type = ResourceType.Topic, Name = topic }],
            new DescribeConfigsOptions { RequestTimeout = TimeSpan.FromSeconds(15) });
        return (partitions, configs.Single().Entries["retention.ms"].Value);
    }

    [Fact]
    public async Task CreateTicket_ExecutesAgainstRealKafka()
    {
        // Arrange: create-заявка (2 партиции, RF 1, retention 1д) сидом, как панель.
        var cluster = fixture.Cluster("itlifecycle1");
        const string topic = "lc-create";
        var ct = TestContext.Current.CancellationToken;
        var sync = await UpAsync(fixture, cluster, ct);
        try
        {
            await fixture.Gateway.PutAsync(fixture.Endpoint,
                $"/kafka/clusters/{cluster}/topics/{topic}/desired.create",
                """{"partitions":2,"replication_factor":1,"configs":{"retention.ms":"86400000"},"requested_unix":1750000000,"requested_by":"test"}""",
                lease: null, ct);

            // Act: тик воркера исполняет заявку.
            var result = await sync.RunAsync((await fixture.SnapshotAsync(cluster))!, ct);
            result.IsSuccess.Should().BeTrue();

            // Assert: топик в Kafka ровно с заявленными параметрами; заявка снята.
            (await TopicExistsAsync(fixture, cluster, topic, ct)).Should().BeTrue();
            var described = await DescribeTopicAsync(fixture, cluster, topic, ct);
            described.Partitions.Should().Be(2);
            described.Retention.Should().Be("86400000");
            (await fixture.GetAsync($"/kafka/clusters/{cluster}/topics/{topic}/desired.create"))
                .Should().BeNull("заявка исполнена — воркер снял ключ");

            // Повторный тик: автосинк кладёт факт-ключ (partitions 2, без заявок).
            await sync.RunAsync((await fixture.SnapshotAsync(cluster))!, ct);
            var raw = await fixture.GetAsync($"/kafka/clusters/{cluster}/topics/{topic}");
            raw.Should().NotBeNull("факт-ключ кладёт следующий автосинк-тик");
            using var doc = JsonDocument.Parse(raw!);
            doc.RootElement.GetProperty("partitions").GetInt32().Should().Be(2);
            doc.RootElement.GetProperty("replication_factor").GetInt32().Should().Be(1);
            doc.RootElement.GetProperty("configs").GetProperty("retention.ms").GetString()
                .Should().Be("86400000");
            doc.RootElement.TryGetProperty("desired", out _).Should().BeFalse();
        }
        finally
        {
            await DownAsync(fixture, cluster, ct);
        }
    }

    [Fact]
    public async Task DeleteTicket_RemovesTopicAndKeys()
    {
        // Arrange: живой топик (создан CLI) + факт-ключ + delete-заявка сидом.
        var cluster = fixture.Cluster("itlifecycle2");
        const string topic = "lc-delete";
        var ct = TestContext.Current.CancellationToken;
        var sync = await UpAsync(fixture, cluster, ct);
        try
        {
            var builder = await fixture.DiscoveryAdminBuilderAsync(cluster);
            using (var admin = builder.Build())
            {
                await admin.CreateTopicsAsync([new TopicSpecification
                {
                    Name = topic,
                    NumPartitions = 3,
                    ReplicationFactor = 1,
                }], new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(15) });
            }

            await sync.RunAsync((await fixture.SnapshotAsync(cluster))!, ct); // факт-ключ
            (await fixture.GetAsync($"/kafka/clusters/{cluster}/topics/{topic}")).Should().NotBeNull();

            await fixture.Gateway.PutAsync(fixture.Endpoint,
                $"/kafka/clusters/{cluster}/topics/{topic}/desired.delete",
                """{"requested_unix":1750000100,"requested_by":"test"}""",
                lease: null, ct);

            // Act: тик воркера исполняет delete-заявку.
            var result = await sync.RunAsync((await fixture.SnapshotAsync(cluster))!, ct);

            // Assert: топика нет в Kafka (метаданные), оба ключа etcd удалены.
            result.IsSuccess.Should().BeTrue();
            (await TopicExistsAsync(fixture, cluster, topic, ct)).Should().BeFalse();
            (await fixture.GetAsync($"/kafka/clusters/{cluster}/topics/{topic}"))
                .Should().BeNull("факт-ключ снесён одной txn с заявкой");
            (await fixture.GetAsync($"/kafka/clusters/{cluster}/topics/{topic}/desired.delete"))
                .Should().BeNull();
        }
        finally
        {
            await DownAsync(fixture, cluster, ct);
        }
    }

    [Fact]
    public async Task CreateTicket_TopicCreatedByCliConcurrently_CleansTicket()
    {
        // Arrange: топик создан AdminClient напрямую (3 партиции), затем
        // create-заявка с ДРУГИМИ параметрами — имитация «create прошёл, del
        // заявки не успел» (сходимость create-ветки, spec §4.2).
        var cluster = fixture.Cluster("itlifecycle3");
        const string topic = "lc-race-create";
        var ct = TestContext.Current.CancellationToken;
        var sync = await UpAsync(fixture, cluster, ct);
        try
        {
            var builder = await fixture.DiscoveryAdminBuilderAsync(cluster);
            using (var admin = builder.Build())
            {
                await admin.CreateTopicsAsync([new TopicSpecification
                {
                    Name = topic,
                    NumPartitions = 3,
                    ReplicationFactor = 1,
                }], new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(15) });
            }

            await fixture.Gateway.PutAsync(fixture.Endpoint,
                $"/kafka/clusters/{cluster}/topics/{topic}/desired.create",
                """{"partitions":6,"replication_factor":1,"requested_unix":1750000200,"requested_by":"test"}""",
                lease: null, ct);

            // Act: тик видит «топик есть + живая create-заявка» → чистка.
            var result = await sync.RunAsync((await fixture.SnapshotAsync(cluster))!, ct);

            // Assert: топик НЕ пересоздан (параметры исходные), заявка снята.
            result.IsSuccess.Should().BeTrue();
            var described = await DescribeTopicAsync(fixture, cluster, topic, ct);
            described.Partitions.Should().Be(3, "параметры заявки к живому топику не применяются");
            (await fixture.GetAsync($"/kafka/clusters/{cluster}/topics/{topic}/desired.create"))
                .Should().BeNull("заявка снята как исполненная внешне");
        }
        finally
        {
            await DownAsync(fixture, cluster, ct);
        }
    }

    [Fact]
    public async Task DeleteTicket_TopicDeletedExternally_CleansTicketWithoutError()
    {
        // Arrange: факт-ключ + delete-заявка; топик удаляем напрямую
        // AdminClient'ом — имитация «DeleteTopics прошёл, del заявки не успел»
        // (сходимость delete-ветки, spec §4.2).
        var cluster = fixture.Cluster("itlifecycle4");
        const string topic = "lc-race-delete";
        var ct = TestContext.Current.CancellationToken;
        var sync = await UpAsync(fixture, cluster, ct);
        try
        {
            var builder = await fixture.DiscoveryAdminBuilderAsync(cluster);
            using (var admin = builder.Build())
            {
                await admin.CreateTopicsAsync([new TopicSpecification
                {
                    Name = topic,
                    NumPartitions = 3,
                    ReplicationFactor = 1,
                }], new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(15) });
            }

            await sync.RunAsync((await fixture.SnapshotAsync(cluster))!, ct); // факт-ключ
            await fixture.Gateway.PutAsync(fixture.Endpoint,
                $"/kafka/clusters/{cluster}/topics/{topic}/desired.delete",
                """{"requested_unix":1750000300,"requested_by":"test"}""",
                lease: null, ct);

            using (var killer = builder.Build())
            {
                await killer.DeleteTopicsAsync([topic],
                    new DeleteTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(15) });
            }

            // Act: тик видит «топика нет + живая delete-заявка» → cleanup.
            var result = await sync.RunAsync((await fixture.SnapshotAsync(cluster))!, ct);

            // Assert: успех (NotFound = исполнено), заявка и факт-ключ удалены.
            result.IsSuccess.Should().BeTrue();
            (await fixture.GetAsync($"/kafka/clusters/{cluster}/topics/{topic}"))
                .Should().BeNull("missing-ключ снесён вместе с заявкой");
            (await fixture.GetAsync($"/kafka/clusters/{cluster}/topics/{topic}/desired.delete"))
                .Should().BeNull();
        }
        finally
        {
            await DownAsync(fixture, cluster, ct);
        }
    }
}
