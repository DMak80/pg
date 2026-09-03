using System.Net;
using System.Net.Http.Json;
using KafkaWorker.App.Api.Operations;
using KafkaWorker.Core.Writing;
using Xunit;

namespace KafkaWorker.IntegrationTests.Api;

// Мутация №15 (t06, spec §4.2): guard'ы/канонизация/идемпотентность на
// настоящем etcd (loops выключены — применяется NodeRegenerator'ом отдельно).
[Collection(KafkaApiCollection.Name)]
public class UpdateBrokerResourcesApiTests(KafkaApiFixture fixture)
{
    private static async Task<string?> GetValueAsync(KafkaApiFixture fixture, string key)
    {
        var ct = TestContext.Current.CancellationToken;
        var kv = await fixture.Etcd.Gateway.GetAsync(fixture.Etcd.Endpoint, key, ct);
        return kv.Value?.Value;
    }

    private static string Unique() => $"res{Guid.NewGuid():N}"[..8];

    [Fact]
    public async Task Update_PartialCpu_WritesCanonicalAndReturnsEffective()
    {
        // Arrange — сид: broker1 {"cpu":"2","mem":"4Gi","disk":"40Gi"}
        var cluster = Unique();
        await KafkaApiTestSeed.SeedActiveClusterAsync(fixture.Etcd, cluster);
        var client = fixture.Factory.CreateClient();

        // Act — меняем только cpu
        var response = await client.PutAsJsonAsync(
            $"/api/kafka/clusters/{cluster}/brokers/broker1/resources",
            new KafkaResourcesUpdateRequest(4m, null, null), TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var dto = await response.Content.ReadFromJsonAsync<KafkaBrokerResourcesDto>(TestContext.Current.CancellationToken);
        dto!.Cpu.Should().Be("4");
        dto.MemGi.Should().Be("4Gi"); // унаследовано
        (await GetValueAsync(fixture, $"/kafka/clusters/{cluster}/brokers/broker1/resources"))
            .Should().Be("""{"cpu":"4","mem":"4Gi","disk":"40Gi"}""");
    }

    [Fact]
    public async Task Update_IdempotentRepeat_SameKeyAnd200()
    {
        // Arrange
        var cluster = Unique();
        await KafkaApiTestSeed.SeedActiveClusterAsync(fixture.Etcd, cluster);
        var client = fixture.Factory.CreateClient();

        // Act
        await client.PutAsJsonAsync(
            $"/api/kafka/clusters/{cluster}/brokers/broker1/resources",
            new KafkaResourcesUpdateRequest(null, 8, null), TestContext.Current.CancellationToken);
        var second = await client.PutAsJsonAsync(
            $"/api/kafka/clusters/{cluster}/brokers/broker1/resources",
            new KafkaResourcesUpdateRequest(null, 8, null), TestContext.Current.CancellationToken);

        // Assert
        second.StatusCode.Should().Be(HttpStatusCode.OK);
        (await GetValueAsync(fixture, $"/kafka/clusters/{cluster}/brokers/broker1/resources"))
            .Should().Be("""{"cpu":"2","mem":"8Gi","disk":"40Gi"}""");
    }

    [Fact]
    public async Task Update_OutOfBoundsCpu_400WithErrors()
    {
        // Arrange
        var cluster = Unique();
        await KafkaApiTestSeed.SeedActiveClusterAsync(fixture.Etcd, cluster);
        var client = fixture.Factory.CreateClient();

        // Act
        var response = await client.PutAsJsonAsync(
            $"/api/kafka/clusters/{cluster}/brokers/broker1/resources",
            new KafkaResourcesUpdateRequest(100m, null, null), TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).Should().Contain("cpu");
    }

    [Fact]
    public async Task Update_EmptyBody_400()
    {
        // Arrange
        var cluster = Unique();
        await KafkaApiTestSeed.SeedActiveClusterAsync(fixture.Etcd, cluster);
        var client = fixture.Factory.CreateClient();

        // Act
        var response = await client.PutAsJsonAsync(
            $"/api/kafka/clusters/{cluster}/brokers/broker1/resources",
            new KafkaResourcesUpdateRequest(null, null, null), TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task Update_UnknownCluster_404()
    {
        // Arrange
        var client = fixture.Factory.CreateClient();

        // Act
        var response = await client.PutAsJsonAsync(
            "/api/kafka/clusters/ghost/brokers/broker1/resources",
            new KafkaResourcesUpdateRequest(4m, null, null), TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Update_UnknownBroker_404()
    {
        // Arrange — broker9 отсутствует в сиде (кластер из 3 брокеров)
        var cluster = Unique();
        await KafkaApiTestSeed.SeedActiveClusterAsync(fixture.Etcd, cluster);
        var client = fixture.Factory.CreateClient();

        // Act
        var response = await client.PutAsJsonAsync(
            $"/api/kafka/clusters/{cluster}/brokers/broker9/resources",
            new KafkaResourcesUpdateRequest(4m, null, null), TestContext.Current.CancellationToken);

        // Assert — 404 «брокер»: нет ключа brokers/broker9/resources
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Update_BrokerInRemoval_409()
    {
        // Arrange — брокер заявлен к демонтажу
        var cluster = Unique();
        await KafkaApiTestSeed.SeedActiveClusterAsync(fixture.Etcd, cluster);
        var ct = TestContext.Current.CancellationToken;
        await fixture.Etcd.Gateway.PutAsync(fixture.Etcd.Endpoint,
            $"/kafka/clusters/{cluster}/brokers/broker3/state", "TO_REMOVE", null, ct);
        var client = fixture.Factory.CreateClient();

        // Act
        var response = await client.PutAsJsonAsync(
            $"/api/kafka/clusters/{cluster}/brokers/broker3/resources",
            new KafkaResourcesUpdateRequest(4m, null, null), TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    [Fact]
    public async Task Update_NotActiveCluster_409()
    {
        // Arrange — config с state=TO_REMOVE
        var cluster = Unique();
        await KafkaApiTestSeed.SeedActiveClusterAsync(fixture.Etcd, cluster);
        var ct = TestContext.Current.CancellationToken;
        await fixture.Etcd.Gateway.PutAsync(fixture.Etcd.Endpoint,
            $"/kafka/clusters/{cluster}/config",
            """{"brokers":3,"replication_factor":3,"min_insync_replicas":2,"default_partitions":12,"default_retention_ms":604800000,"created_unix":1756500000,"state":"TO_REMOVE"}""",
            null, ct);
        var client = fixture.Factory.CreateClient();

        // Act
        var response = await client.PutAsJsonAsync(
            $"/api/kafka/clusters/{cluster}/brokers/broker1/resources",
            new KafkaResourcesUpdateRequest(4m, null, null), TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }
}
