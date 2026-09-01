using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using AdminPanel.Etcd.Workers;
using FluentAssertions;
using Xunit;

namespace AdminPanel.IntegrationTests;

// POST /api/ha/{scope}/nodes/{node}/recreate — прокси в API PgWorker (task
// etcd-via-worker-api): стаб-воркер; 201-DTO/mode, идемпотентность, матрица
// 401/503/404/409/400 прежними телами; панель не пишет в etcd.
[Collection("api")]
public class RecreateNodeApiTests(AuthWebFactory factory)
{
    private readonly AuthWebFactory _factory = factory;

    private void SetLiveSnapshot()
        => _factory.Snapshot = InspectionSnapshots.Fixture(_factory.Time.GetUtcNow());

    private async Task<HttpClient> LoginAsync() => await ApiTestLogin.LoginAsync(_factory);

    [Fact]
    public async Task Recreate_WithoutCookie_Returns401()
    {
        // Arrange
        _factory.WorkerApi.Reset();
        using var client = _factory.CreateClient();

        // Act
        using var response = await client.PostAsync(
            "/api/ha/rc-s1/nodes/s1b/recreate", null, TestContext.Current.CancellationToken);

        // Assert: default-deny закрывает мутацию без auth
        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
        _factory.WorkerApi.Calls.Should().BeEmpty();
    }

    [Fact]
    public async Task Recreate_ValidNode_Returns201WithSoftMode()
    {
        // Arrange: стаб-воркер — прежний DTO (mode soft по умолчанию)
        SetLiveSnapshot();
        _factory.WorkerApi.Reset();
        _factory.WorkerApi.Respond = _ => new WorkerApiResult(201,
            """{"scope":"rcok-s1","node":"s1b","state":"TO_RECREATE","mode":"soft"}""");
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsync(
            "/api/ha/rcok-s1/nodes/s1b/recreate", null, TestContext.Current.CancellationToken);

        // Assert: 201 + DTO; тело не пересылалось (mode нет — воркер сам soft)
        response.StatusCode.Should().Be(HttpStatusCode.Created);
        var dto = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        dto.GetProperty("scope").GetString().Should().Be("rcok-s1");
        dto.GetProperty("node").GetString().Should().Be("s1b");
        dto.GetProperty("state").GetString().Should().Be("TO_RECREATE");
        dto.GetProperty("mode").GetString().Should().Be("soft");
        var call = _factory.WorkerApi.Calls.Should().ContainSingle().Subject;
        call.Worker.Should().Be("pgworker");
        call.Method.Should().Be(HttpMethod.Post);
        call.Path.Should().Be("/api/ha/rcok-s1/nodes/s1b/recreate");
        call.Body.Should().BeNull(); // тела нет — воркер сам подставит soft
        call.RequestedBy.Should().BeNull();
        _factory.EtcdStub.WriteCalls.Should().Be(0); // панель не пишет в etcd
    }

    [Fact]
    public async Task Recreate_HardMode_BodyForwarded()
    {
        // Arrange — оператор выбрал «грубо»: лидер сносится сразу, failover — Patroni
        SetLiveSnapshot();
        _factory.WorkerApi.Reset();
        _factory.WorkerApi.Respond = _ => new WorkerApiResult(201,
            """{"scope":"rchard-s1","node":"s1a","state":"TO_RECREATE","mode":"hard"}""");
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsJsonAsync(
            "/api/ha/rchard-s1/nodes/s1a/recreate", new { mode = "hard" }, TestContext.Current.CancellationToken);

        // Assert: 201; тело {mode:"hard"} переслано воркеру как есть
        response.StatusCode.Should().Be(HttpStatusCode.Created);
        var dto = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        dto.GetProperty("mode").GetString().Should().Be("hard");
        _factory.WorkerApi.Calls.Should().ContainSingle().Which.Body
            .Should().BeOfType<AdminPanel.Api.Operations.RecreateNodeRequest>()
            .Which.Mode.Should().Be("hard");
    }

    [Fact]
    public async Task Recreate_InvalidMode_Returns400()
    {
        // Arrange — режим обязан быть soft|hard (ошибка прежним текстом)
        SetLiveSnapshot();
        _factory.WorkerApi.Reset();
        _factory.WorkerApi.Respond = _ => new WorkerApiResult(400,
            """{"title":"Invalid mode","status":400,"detail":"режим пересоздания «sideways» недопустим: только soft или hard"}""");
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsJsonAsync(
            "/api/ha/rcbad-s1/nodes/s1b/recreate", new { mode = "sideways" }, TestContext.Current.CancellationToken);

        // Assert: 400 от воркера (панель не дублирует валидацию)
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        var body = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        body.GetProperty("title").GetString().Should().Be("Invalid mode");
    }

    [Fact]
    public async Task Recreate_LastNode_Returns409()
    {
        // Arrange: guard'ы прежние — теперь их выполняет воркер
        SetLiveSnapshot();
        _factory.WorkerApi.Reset();
        _factory.WorkerApi.Respond = _ => new WorkerApiResult(409,
            """{"title":"Recreate rejected","status":409,"detail":"нода s1a — последняя в скопе rclast-s1, пересоздание невозможно"}""");
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsync(
            "/api/ha/rclast-s1/nodes/s1a/recreate", null, TestContext.Current.CancellationToken);

        // Assert: 409 — последняя нода, нет источника basebackup
        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
        var body = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        body.GetProperty("title").GetString().Should().Be("Recreate rejected");
    }

    [Fact]
    public async Task Recreate_UnknownScope_Returns404()
    {
        // Arrange
        SetLiveSnapshot();
        _factory.WorkerApi.Reset();
        _factory.WorkerApi.Respond = _ => new WorkerApiResult(404,
            """{"title":"Not found","status":404,"detail":"HA-скоп nope-s1 не найден"}""");
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsync(
            "/api/ha/nope-s1/nodes/s1a/recreate", null, TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
        var body = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        body.GetProperty("title").GetString().Should().Be("Not found");
    }

    [Fact]
    public async Task Recreate_ClusterNotActive_Returns409()
    {
        // Arrange: config с state=removing
        SetLiveSnapshot();
        _factory.WorkerApi.Reset();
        _factory.WorkerApi.Respond = _ => new WorkerApiResult(409,
            """{"title":"Cluster not active","status":409,"detail":"кластер rcnotactive удаляется (TO_REMOVE) — операция запрещена"}""");
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsync(
            "/api/ha/rcnotactive-s1/nodes/s1b/recreate", null, TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
        var body = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        body.GetProperty("title").GetString().Should().Be("Cluster not active");
    }

    [Fact]
    public async Task Recreate_WorkerApiUnavailable_Returns503()
    {
        // Arrange: живых ключей api нет → 503 панели
        _factory.Snapshot = null;
        _factory.WorkerApi.Reset();
        _factory.WorkerApi.Throw = new WorkerApiUnavailableException("pgworker");
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsync(
            "/api/ha/rcdown-s1/nodes/s1b/recreate", null, TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
    }
}
