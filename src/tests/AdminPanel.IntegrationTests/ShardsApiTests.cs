using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using AdminPanel.Etcd.Workers;
using FluentAssertions;
using Xunit;

namespace AdminPanel.IntegrationTests;

// POST/DELETE /api/clusters/{c}/shards… — прокси в API PgWorker (task
// etcd-via-worker-api): стаб-воркер; коды/тела 1:1 прежней матрице
// (arch/03 §1.3/§1.4), панель не пишет в etcd.
[Collection("api")]
public class ShardsApiTests(AuthWebFactory factory)
{
    private readonly AuthWebFactory _factory = factory;

    private void SetLiveSnapshot()
        => _factory.Snapshot = InspectionSnapshots.Fixture(_factory.Time.GetUtcNow());

    private async Task<HttpClient> LoginAsync()
        => await ApiTestLogin.LoginAsync(_factory);

    [Fact]
    public async Task AddShard_WithoutCookie_Returns401()
    {
        // Arrange
        _factory.WorkerApi.Reset();
        using var client = _factory.CreateClient();

        // Act
        using var response = await client.PostAsJsonAsync(
            "/api/clusters/shop/shards",
            new { replicas = 2, requestCpu = 0.5m, requestMem = 8, requestDisk = 100 },
            TestContext.Current.CancellationToken);

        // Assert: default-deny закрывает мутацию как все /api/*
        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
        _factory.WorkerApi.Calls.Should().BeEmpty();
    }

    [Fact]
    public async Task AddShard_ActiveCluster_Returns201WithDto()
    {
        // Arrange: стаб-воркер отвечает DTO прежнего формата (§1.3)
        SetLiveSnapshot();
        _factory.WorkerApi.Reset();
        _factory.WorkerApi.Respond = _ => new WorkerApiResult(201,
            """{"cluster":"addshop","name":"shard3","replicas":2,"requestCpu":"0.5","requestMem":"8Gi","requestDisk":"100Gi","state":"NOT_INITIALIZED"}""");
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsJsonAsync(
            "/api/clusters/addshop/shards",
            new { replicas = 2, requestCpu = 0.5m, requestMem = 8, requestDisk = 100 },
            TestContext.Current.CancellationToken);

        // Assert: 201 + DTO канона; Location прежний
        response.StatusCode.Should().Be(HttpStatusCode.Created);
        response.Headers.Location!.ToString().Should().Be("/api/clusters/addshop/shards/shard3");
        var dto = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        dto.GetProperty("name").GetString().Should().Be("shard3");
        dto.GetProperty("cluster").GetString().Should().Be("addshop");
        dto.GetProperty("state").GetString().Should().Be("NOT_INITIALIZED");
        dto.GetProperty("requestCpu").GetString().Should().Be("0.5");
        dto.GetProperty("requestMem").GetString().Should().Be("8Gi");
        dto.GetProperty("requestDisk").GetString().Should().Be("100Gi");

        // Прокси-вызов: тело + путь; оператора нет
        _factory.WorkerApi.Calls.Should().ContainSingle().Which.Should().Match<TestWorkerApi.Call>(c =>
            c.Worker == "pgworker" && c.Method == HttpMethod.Post && c.Path == "/api/clusters/addshop/shards"
            && c.RequestedBy == null);
        _factory.EtcdStub.WriteCalls.Should().Be(0); // панель не пишет в etcd
    }

    [Fact]
    public async Task AddShard_ClusterNotInitialized_Returns409WithWorkerBody()
    {
        // Arrange
        SetLiveSnapshot();
        _factory.WorkerApi.Reset();
        _factory.WorkerApi.Respond = _ => new WorkerApiResult(409,
            """{"title":"Shard add rejected","status":409,"detail":"кластер fresh ещё инициализируется (NOT_INITIALIZED) — дождитесь инициализации"}""");
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsJsonAsync(
            "/api/clusters/fresh/shards",
            new { replicas = 2, requestCpu = 1m, requestMem = 4, requestDisk = 50 },
            TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
        var problem = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        problem.GetProperty("title").GetString().Should().Be("Shard add rejected");
        problem.GetProperty("detail").GetString().Should().Contain("дождитесь инициализации");
    }

    [Fact]
    public async Task AddShard_ClusterNotFound_Returns404()
    {
        // Arrange
        SetLiveSnapshot();
        _factory.WorkerApi.Reset();
        _factory.WorkerApi.Respond = _ => new WorkerApiResult(404,
            """{"title":"Cluster not found","status":404,"detail":"кластер ghost не найден"}""");
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsJsonAsync(
            "/api/clusters/ghost/shards",
            new { replicas = 2, requestCpu = 1m, requestMem = 4, requestDisk = 50 },
            TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task AddShard_InvalidBody_Returns400WithFieldErrors()
    {
        // Arrange: errors по границам §9.3 — от воркера
        SetLiveSnapshot();
        _factory.WorkerApi.Reset();
        _factory.WorkerApi.Respond = _ => new WorkerApiResult(400,
            """{"title":"Validation failed","status":400,"detail":"параметры добавления шарда некорректны","errors":{"replicas":["реплики: целое 1..26"],"requestCpu":["CPU (ядра): 0.01..64"]}}""");
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsJsonAsync(
            "/api/clusters/valid/shards",
            new { replicas = 27, requestCpu = 64.1m, requestMem = 8, requestDisk = 100 },
            TestContext.Current.CancellationToken);

        // Assert: ProblemDetails с errors по полям (канон RFC 9457)
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        var problem = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        problem.GetProperty("title").GetString().Should().Be("Validation failed");
        problem.GetProperty("errors").GetProperty("replicas").GetArrayLength().Should().Be(1);
        problem.GetProperty("errors").GetProperty("requestCpu").GetArrayLength().Should().Be(1);
    }

    [Fact]
    public async Task DeleteShard_WithoutCookie_Returns401()
    {
        // Arrange
        _factory.WorkerApi.Reset();
        using var client = _factory.CreateClient();

        // Act
        using var response = await client.DeleteAsync(
            "/api/clusters/shop/shards/shard1", TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task DeleteShard_EmptyShard_Returns204()
    {
        // Arrange
        SetLiveSnapshot();
        _factory.WorkerApi.Reset();
        _factory.WorkerApi.Respond = _ => new WorkerApiResult(204, null);
        using var client = await LoginAsync();

        // Act
        using var response = await client.DeleteAsync(
            "/api/clusters/remshop/shards/shard3", TestContext.Current.CancellationToken);

        // Assert: 204; прокси-вызов по каноническому пути
        response.StatusCode.Should().Be(HttpStatusCode.NoContent);
        _factory.WorkerApi.Calls.Should().ContainSingle().Which.Should().Match<TestWorkerApi.Call>(c =>
            c.Method == HttpMethod.Delete && c.Path == "/api/clusters/remshop/shards/shard3"
            && c.RequestedBy == null);
        _factory.EtcdStub.WriteCalls.Should().Be(0);
    }

    [Fact]
    public async Task DeleteShard_ShardWithBuckets_Returns409WithCount()
    {
        // Arrange: guard'ы прежние — теперь их выполняет воркер, тело то же
        SetLiveSnapshot();
        _factory.WorkerApi.Reset();
        _factory.WorkerApi.Respond = _ => new WorkerApiResult(409,
            """{"title":"Shard remove rejected","status":409,"detail":"на шарде 4 бакетов — сначала явно перевезти (UI переездов — t07)"}""");
        using var client = await LoginAsync();

        // Act
        using var response = await client.DeleteAsync(
            "/api/clusters/rem3/shards/shard1", TestContext.Current.CancellationToken);

        // Assert: 409 ProblemDetails с числом и подсказкой
        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
        var problem = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        problem.GetProperty("title").GetString().Should().Be("Shard remove rejected");
        problem.GetProperty("detail").GetString().Should().Contain("4").And.Contain("перевезти");
    }

    [Fact]
    public async Task DeleteShard_UnknownShard_Returns404()
    {
        // Arrange
        SetLiveSnapshot();
        _factory.WorkerApi.Reset();
        _factory.WorkerApi.Respond = _ => new WorkerApiResult(404,
            """{"title":"Not found","status":404,"detail":"шард rem4/ghost не найден (replicas-ключ отсутствует)"}""");
        using var client = await LoginAsync();

        // Act
        using var response = await client.DeleteAsync(
            "/api/clusters/rem4/shards/ghost", TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task DeleteShard_ClusterNotActive_Returns409()
    {
        // Arrange
        SetLiveSnapshot();
        _factory.WorkerApi.Reset();
        _factory.WorkerApi.Respond = _ => new WorkerApiResult(409,
            """{"title":"Shard remove rejected","status":409,"detail":"кластер dying удаляется (TO_REMOVE) — операция запрещена"}""");
        using var client = await LoginAsync();

        // Act
        using var response = await client.DeleteAsync(
            "/api/clusters/dying/shards/shard1", TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
        var problem = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        problem.GetProperty("detail").GetString().Should().Contain("удаляется");
    }
}
