using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using AdminPanel.Etcd.Workers;
using FluentAssertions;
using Xunit;

namespace AdminPanel.IntegrationTests;

// DELETE /api/clusters/{name} — прокси в API PgWorker (task etcd-via-worker-api):
// стаб-воркер; 204 идемпотентен, 404/503 прежние тела, панель не пишет в etcd.
[Collection("api")]
public class DeleteClusterApiTests(AuthWebFactory factory)
{
    private readonly AuthWebFactory _factory = factory;

    private void SetLiveSnapshot()
        => _factory.Snapshot = InspectionSnapshots.Fixture(_factory.Time.GetUtcNow());

    [Fact]
    public async Task Delete_WithoutCookie_Returns401()
    {
        // Arrange
        _factory.WorkerApi.Reset();
        using var client = _factory.CreateClient();

        // Act
        using var response = await client.DeleteAsync(
            "/api/clusters/shop", TestContext.Current.CancellationToken);

        // Assert: default-deny закрывает мутацию как все /api/*
        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
        _factory.WorkerApi.Calls.Should().BeEmpty();
    }

    [Fact]
    public async Task Delete_ExistingCluster_Returns204()
    {
        // Arrange
        SetLiveSnapshot();
        _factory.WorkerApi.Reset();
        _factory.WorkerApi.Respond = _ => new WorkerApiResult(204, null);
        using var client = await ApiTestLogin.LoginAsync(_factory);

        // Act
        using var response = await client.DeleteAsync(
            "/api/clusters/shop", TestContext.Current.CancellationToken);

        // Assert: 204; прокси-вызов — DELETE в воркер без оператора
        response.StatusCode.Should().Be(HttpStatusCode.NoContent);
        _factory.WorkerApi.Calls.Should().ContainSingle().Which.Should().Match<TestWorkerApi.Call>(c =>
            c.Worker == "pgworker" && c.Method == HttpMethod.Delete && c.Path == "/api/clusters/shop"
            && c.RequestedBy == null);
        _factory.EtcdStub.WriteCalls.Should().Be(0); // панель не пишет в etcd (spec §9.1)
    }

    [Fact]
    public async Task Delete_Twice_Both204()
    {
        // Arrange: идемпотентность повторов — прежняя семантика §9.4 п.4
        SetLiveSnapshot();
        _factory.WorkerApi.Reset();
        _factory.WorkerApi.Respond = _ => new WorkerApiResult(204, null);
        using var client = await ApiTestLogin.LoginAsync(_factory);

        // Act
        using var first = await client.DeleteAsync("/api/clusters/dup", TestContext.Current.CancellationToken);
        using var second = await client.DeleteAsync("/api/clusters/dup", TestContext.Current.CancellationToken);

        // Assert
        first.StatusCode.Should().Be(HttpStatusCode.NoContent);
        second.StatusCode.Should().Be(HttpStatusCode.NoContent);
    }

    [Fact]
    public async Task Delete_UnknownCluster_Returns404WithWorkerBody()
    {
        // Arrange
        SetLiveSnapshot();
        _factory.WorkerApi.Reset();
        _factory.WorkerApi.Respond = _ => new WorkerApiResult(404,
            """{"title":"Cluster not found","status":404,"detail":"кластер ghost не найден (config-ключ отсутствует)"}""");
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
    public async Task Delete_WorkerApiUnavailable_Returns503()
    {
        // Arrange: живых ключей нет → 503 панели
        _factory.Snapshot = null;
        _factory.WorkerApi.Reset();
        _factory.WorkerApi.Throw = new WorkerApiUnavailableException("pgworker");
        using var client = await ApiTestLogin.LoginAsync(_factory);

        // Act
        using var response = await client.DeleteAsync(
            "/api/clusters/shop", TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
        var problem = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        problem.GetProperty("title").GetString().Should().Be("API воркера недоступен");
    }
}
