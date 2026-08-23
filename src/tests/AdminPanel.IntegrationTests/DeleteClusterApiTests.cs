using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using AdminPanel.Core;
using AdminPanel.Etcd;
using FluentAssertions;
using Xunit;

namespace AdminPanel.IntegrationTests;

// DELETE /api/clusters/{name} против реального etcd: перевод config.state в
// TO_REMOVE (arch/02 §9.4), идемпотентность и коды отказов (arch/03 §1.2).
[Collection("api")]
public class DeleteClusterApiTests(AuthWebFactory factory, EtcdContainerFixture fixture)
    : IClassFixture<EtcdContainerFixture>
{
    private readonly AuthWebFactory _factory = factory;

    // Снапшот «живого etcd»: единственный endpoint = контейнер (паттерн CreateClusterApiTests).
    private void SetLiveSnapshot()
    {
        var etcd = new EtcdStatus(
            true,
            [new EtcdEndpoint(fixture.Endpoint, true, 1, "3.5.21", null, null, null, null, [])],
            [], [], fixture.Endpoint, false, _factory.Time.GetUtcNow(), 0);
        _factory.Snapshot = InspectionSnapshots.Fixture(_factory.Time.GetUtcNow()) with { Etcd = etcd };
    }

    // Кластер для удаления: создать через POST — как это делает UI.
    private async Task<HttpClient> CreateClusterAsync(string name)
    {
        SetLiveSnapshot();
        var client = await ApiTestLogin.LoginAsync(_factory);
        using var created = await client.PostAsJsonAsync(
            "/api/clusters",
            new { name, buckets = 4, shards = 2, replicas = 2, requestCpu = 0.5m, requestMem = 8, requestDisk = 100 },
            TestContext.Current.CancellationToken);
        created.StatusCode.Should().Be(HttpStatusCode.Created);
        return client;
    }

    [Fact]
    public async Task Delete_WithoutCookie_Returns401()
    {
        // Arrange
        using var client = _factory.CreateClient();

        // Act
        using var response = await client.DeleteAsync(
            "/api/clusters/shop", TestContext.Current.CancellationToken);

        // Assert: default-deny закрывает мутацию как все /api/*
        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task Delete_ExistingCluster_Returns204AndWritesToRemoveConfig()
    {
        // Arrange
        using var client = await CreateClusterAsync("shop");

        // Act
        using var response = await client.DeleteAsync(
            "/api/clusters/shop", TestContext.Current.CancellationToken);

        // Assert: 204; config в etcd — state=TO_REMOVE, константы сохранены (§9.4 п.5)
        response.StatusCode.Should().Be(HttpStatusCode.NoContent);
        var gateway = EtcdTestHarness.NewGateway();
        var range = await gateway.RangeAsync(fixture.Endpoint, "/clusters/shop/config", TestContext.Current.CancellationToken);
        var config = range.Value.Single(kv => kv.Key == "/clusters/shop/config").Value;
        using var doc = JsonDocument.Parse(config);
        doc.RootElement.GetProperty("state").GetString().Should().Be("TO_REMOVE");
        doc.RootElement.GetProperty("buckets").GetInt32().Should().Be(4);
        doc.RootElement.GetProperty("dbname").GetString().Should().Be("shop");
        doc.RootElement.TryGetProperty("created_unix", out _).Should().BeTrue();

        // Остальные ключи кластера не тронуты — панель их не удаляет (§9.4)
        var prefix = await gateway.RangeAsync(fixture.Endpoint, "/clusters/shop/", TestContext.Current.CancellationToken);
        prefix.Value.Should().HaveCount(15); // config + 2×(replicas+2 nodes) + 4 routing + 4 status

        // Читающий путь: refresher-тик распознаёт TO_REMOVE (parser → ClusterState.ToRemove)
        var store = new SnapshotStore();
        var refresher = EtcdTestHarness.NewRefresher(store, fixture.Endpoint);
        (await refresher.RefreshOnceAsync(CancellationToken.None)).IsSuccess.Should().BeTrue();
        store.Current!.Clusters.Single(c => c.Name == "shop").State.Should().Be(ClusterState.ToRemove);
    }

    [Fact]
    public async Task Delete_Twice_SecondIsIdempotent204()
    {
        // Arrange
        using var client = await CreateClusterAsync("dup");

        // Act
        using var first = await client.DeleteAsync("/api/clusters/dup", TestContext.Current.CancellationToken);
        using var second = await client.DeleteAsync("/api/clusters/dup", TestContext.Current.CancellationToken);

        // Assert: идемпотентность — повтор к TO_REMOVE-кластеру тоже 204 (§9.4 п.4)
        first.StatusCode.Should().Be(HttpStatusCode.NoContent);
        second.StatusCode.Should().Be(HttpStatusCode.NoContent);
    }

    [Fact]
    public async Task Delete_UnknownCluster_Returns404()
    {
        // Arrange
        SetLiveSnapshot();
        using var client = await ApiTestLogin.LoginAsync(_factory);

        // Act
        using var response = await client.DeleteAsync(
            "/api/clusters/ghost", TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
        var problem = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        problem.GetProperty("title").GetString().Should().Be("Cluster not found");
    }

    [Fact]
    public async Task Delete_InvalidName_Returns404()
    {
        // Arrange: неканоническое имя панель создать не могла (§9.3)
        SetLiveSnapshot();
        using var client = await ApiTestLogin.LoginAsync(_factory);

        // Act
        using var response = await client.DeleteAsync(
            "/api/clusters/Bad-Name", TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Delete_NoSnapshot_Returns503()
    {
        // Arrange
        _factory.Snapshot = null;
        using var client = await ApiTestLogin.LoginAsync(_factory);

        // Act
        using var response = await client.DeleteAsync(
            "/api/clusters/shop", TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
    }
}
