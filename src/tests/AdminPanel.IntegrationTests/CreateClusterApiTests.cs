using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using AdminPanel.Etcd.Workers;
using FluentAssertions;
using Xunit;

namespace AdminPanel.IntegrationTests;

// POST /api/clusters — прокси в API PgWorker (task etcd-via-worker-api):
// стаб-воркер возвращает ответы прежнего контракта; проверяем 1:1 коды/тела
// панели, корректность прокси-вызова и что панель НЕ пишет в etcd (spec §9.1).
[Collection("api")]
public class CreateClusterApiTests(AuthWebFactory factory)
{
    private readonly AuthWebFactory _factory = factory;

    // Снапшот-основа: единственный endpoint жив (для read-путей; мутации идут
    // через стаб-воркер, снапшот им больше не нужен — кроме случая «ключей нет»).
    private void SetLiveSnapshot()
        => _factory.Snapshot = InspectionSnapshots.Fixture(_factory.Time.GetUtcNow());

    [Fact]
    public async Task Create_WithoutCookie_Returns401()
    {
        // Arrange
        _factory.WorkerApi.Reset();
        using var client = _factory.CreateClient();

        // Act
        using var response = await client.PostAsJsonAsync(
            "/api/clusters", new { name = "x", buckets = 1, shards = 1, replicas = 1, requestCpu = 1, requestMem = 1, requestDisk = 1 },
            TestContext.Current.CancellationToken);

        // Assert: default-deny guard закрывает мутацию как все /api/* — до воркера не дошло
        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
        _factory.WorkerApi.Calls.Should().BeEmpty();
    }

    [Fact]
    public async Task Create_Valid_ProxiesToWorkerAndReturns201()
    {
        // Arrange: стаб-воркер отвечает DTO прежнего формата (arch/03 §1.1)
        SetLiveSnapshot();
        _factory.WorkerApi.Reset();
        _factory.WorkerApi.Respond = _ => new WorkerApiResult(201,
            """{"name":"shop","dbName":"shop","sharded":true,"bucketsCount":4,"shardsTotal":2,"replicas":2,"requestCpu":"0.5","requestMem":"8Gi","requestDisk":"100Gi","state":"NOT_INITIALIZED"}""");
        using var client = await ApiTestLogin.LoginAsync(_factory);

        // Act
        using var response = await client.PostAsJsonAsync(
            "/api/clusters",
            new { name = "shop", buckets = 4, shards = 2, replicas = 2, requestCpu = 0.5m, requestMem = 8, requestDisk = 100 },
            TestContext.Current.CancellationToken);

        // Assert: 201 + Location + DTO канона — 1:1 прежнему контракту панели
        response.StatusCode.Should().Be(HttpStatusCode.Created);
        response.Headers.Location!.ToString().Should().Be("/api/clusters/shop");
        var dto = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        dto.GetProperty("state").GetString().Should().Be("NOT_INITIALIZED");
        dto.GetProperty("sharded").GetBoolean().Should().BeTrue();
        dto.GetProperty("requestCpu").GetString().Should().Be("0.5");
        dto.GetProperty("requestMem").GetString().Should().Be("8Gi");

        // Прокси-вызов: POST в воркер с телом запроса; оператора у create нет
        var call = _factory.WorkerApi.Calls.Should().ContainSingle().Subject;
        call.Worker.Should().Be("pgworker");
        call.Method.Should().Be(HttpMethod.Post);
        call.Path.Should().Be("/api/clusters");
        call.Body.Should().BeOfType<AdminPanel.Api.Operations.CreateClusterRequest>()
            .Which.Name.Should().Be("shop");
        call.RequestedBy.Should().BeNull();

        // Инвариант spec §9.1: панель не пишет в etcd
        _factory.EtcdStub.WriteCalls.Should().Be(0);
    }

    [Fact]
    public async Task Create_Duplicate_Returns409WithWorkerBody()
    {
        // Arrange
        SetLiveSnapshot();
        _factory.WorkerApi.Reset();
        _factory.WorkerApi.Respond = _ => new WorkerApiResult(409,
            """{"type":"https://tools.ietf.org/html/rfc9457","title":"Cluster already exists","status":409,"detail":"кластер dup уже существует (config-ключ присутствует)"}""");
        using var client = await ApiTestLogin.LoginAsync(_factory);

        // Act
        using var response = await client.PostAsJsonAsync(
            "/api/clusters",
            new { name = "dup", buckets = 1, shards = 1, replicas = 1, requestCpu = 1m, requestMem = 1, requestDisk = 1 },
            TestContext.Current.CancellationToken);

        // Assert: ProblemDetails воркера проксирован как есть (title/detail/status)
        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
        response.Content.Headers.ContentType!.MediaType.Should().Be("application/problem+json");
        var problem = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        problem.GetProperty("title").GetString().Should().Be("Cluster already exists");
        problem.GetProperty("status").GetInt32().Should().Be(409);
    }

    [Theory]
    [InlineData("name")]
    [InlineData("buckets")]
    public async Task Create_Invalid_Returns400WithFieldErrors(string field)
    {
        // Arrange: errors-массив приходит от воркера уже в каноническом виде (RFC 9457)
        SetLiveSnapshot();
        _factory.WorkerApi.Reset();
        _factory.WorkerApi.Respond = _ => new WorkerApiResult(400, $$"""
            {"type":"https://tools.ietf.org/html/rfc9457","title":"Validation failed","status":400,"detail":"параметры некорректны","errors":{"{{field}}":["поле некорректно"]}
            }
            """);
        using var client = await ApiTestLogin.LoginAsync(_factory);

        // Act
        using var response = await client.PostAsJsonAsync(
            "/api/clusters",
            new { name = "ok", buckets = 0, shards = 1, replicas = 1, requestCpu = 1m, requestMem = 1, requestDisk = 1 },
            TestContext.Current.CancellationToken);

        // Assert: панель проксирует errors-массив (читается как прежний GetArrayLength)
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        var problem = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        problem.GetProperty("errors").GetProperty(field).GetArrayLength().Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task Create_WorkerApiUnavailable_Returns503()
    {
        // Arrange: живых ключей api нет (снапшот пуст) — шлюз бросает, панель 503
        _factory.Snapshot = null;
        _factory.WorkerApi.Reset();
        _factory.WorkerApi.Throw = new WorkerApiUnavailableException("pgworker");
        using var client = await ApiTestLogin.LoginAsync(_factory);

        // Act
        using var response = await client.PostAsJsonAsync(
            "/api/clusters",
            new { name = "x", buckets = 1, shards = 1, replicas = 1, requestCpu = 1m, requestMem = 1, requestDisk = 1 },
            TestContext.Current.CancellationToken);

        // Assert: собственный 503 панели (не тело воркера)
        response.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
        var problem = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        problem.GetProperty("title").GetString().Should().Be("API воркера недоступен");
    }
}
