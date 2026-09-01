using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using AdminPanel.Etcd.Workers;
using FluentAssertions;
using Xunit;

namespace AdminPanel.IntegrationTests;

// POST /api/clusters/{c}/moves — прокси в API PgWorker (task etcd-via-worker-api):
// стаб-воркер; 201 queued/skipped 1:1, матрица 400/404/409 прежними телами,
// оператор сессии уходит воркеру (X-Requested-By — spec §3.7).
[Collection("api")]
public class MovesApiTests(AuthWebFactory factory)
{
    private readonly AuthWebFactory _factory = factory;

    private void SetLiveSnapshot()
        => _factory.Snapshot = InspectionSnapshots.Fixture(_factory.Time.GetUtcNow());

    private async Task<HttpClient> LoginAsync() => await ApiTestLogin.LoginAsync(_factory);

    [Fact]
    public async Task Moves_WithoutCookie_Returns401()
    {
        // Arrange
        _factory.WorkerApi.Reset();
        using var client = _factory.CreateClient();

        // Act
        using var response = await client.PostAsJsonAsync("/api/clusters/mvanon/moves",
            new { from = "shard1", to = "shard2", buckets = new[] { 0 } },
            TestContext.Current.CancellationToken);

        // Assert: default-deny закрывает мутацию как все /api/*
        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
        _factory.WorkerApi.Calls.Should().BeEmpty();
    }

    [Fact]
    public async Task Moves_QueueBuckets_Returns201AndSendsOperator()
    {
        // Arrange: стаб-воркер отвечает прежним DTO (queued по возрастанию)
        SetLiveSnapshot();
        _factory.WorkerApi.Reset();
        _factory.WorkerApi.Respond = _ => new WorkerApiResult(201,
            """{"cluster":"mvqueue","from":"shard1","to":"shard2","queued":[0,2,4],"skipped":[]}""");
        using var client = await LoginAsync();

        // Act: порядок в массиве обратный — сортирует воркер
        using var response = await client.PostAsJsonAsync("/api/clusters/mvqueue/moves",
            new { from = "shard1", to = "shard2", buckets = new[] { 4, 0, 2 } },
            TestContext.Current.CancellationToken);

        // Assert: 201 + DTO прежнего формата
        response.StatusCode.Should().Be(HttpStatusCode.Created);
        var dto = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        dto.GetProperty("queued").EnumerateArray().Select(e => e.GetInt32())
            .Should().Equal(0, 2, 4).And.BeInAscendingOrder();

        // Прокси-вызов: тело MoveBucketsRequest + оператор сессии «admin»
        // (user.Identity?.Name ?? "adminpanel" — как сегодня, OperationsModule §1.5)
        _factory.WorkerApi.Calls.Should().ContainSingle().Which.Should().Match<TestWorkerApi.Call>(c =>
            c.Worker == "pgworker" && c.Method == HttpMethod.Post && c.Path == "/api/clusters/mvqueue/moves"
            && c.RequestedBy == "admin");
        _factory.EtcdStub.WriteCalls.Should().Be(0); // панель не пишет в etcd
    }

    [Fact]
    public async Task Moves_Repeat_AllSkipped()
    {
        // Arrange: идемпотентность — повтор того же тела всё в skipped (Д6)
        SetLiveSnapshot();
        _factory.WorkerApi.Reset();
        _factory.WorkerApi.Respond = _ => new WorkerApiResult(201,
            """{"cluster":"mvrepeat","from":"shard1","to":"shard2","queued":[],"skipped":[0,2]}""");
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsJsonAsync("/api/clusters/mvrepeat/moves",
            new { from = "shard1", to = "shard2", buckets = new[] { 0, 2 } },
            TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.Created);
        var dto = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        dto.GetProperty("queued").GetArrayLength().Should().Be(0);
        dto.GetProperty("skipped").EnumerateArray().Select(e => e.GetInt32())
            .Should().BeEquivalentTo([0, 2]);
    }

    [Fact]
    public async Task Moves_ConflictingExistingTicket_Returns409()
    {
        // Arrange: иная заявка — 409 с телом воркера (Д7)
        SetLiveSnapshot();
        _factory.WorkerApi.Reset();
        _factory.WorkerApi.Respond = _ => new WorkerApiResult(409,
            """{"title":"Moves rejected","status":409,"detail":"на bucket_0 уже стоит заявка (op=move, to=shard9) — дождитесь её обработки или уберите ключ"}""");
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsJsonAsync("/api/clusters/mvconflict/moves",
            new { from = "shard1", to = "shard2", buckets = new[] { 0, 2 } },
            TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
        var problem = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        problem.GetProperty("title").GetString().Should().Be("Moves rejected");
    }

    [Fact]
    public async Task Moves_TargetToRemove_Returns409()
    {
        // Arrange: приёмник в демонтаже (Д9) — guard воркера
        SetLiveSnapshot();
        _factory.WorkerApi.Reset();
        _factory.WorkerApi.Respond = _ => new WorkerApiResult(409,
            """{"title":"Moves rejected","status":409,"detail":"шард-приёмник mvtorm/shard2 удаляется (TO_REMOVE) — выберите другой приёмник"}""");
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsJsonAsync("/api/clusters/mvtorm/moves",
            new { from = "shard1", to = "shard2", buckets = new[] { 0 } },
            TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    [Fact]
    public async Task Moves_UnknownShard_Returns404()
    {
        // Arrange
        SetLiveSnapshot();
        _factory.WorkerApi.Reset();
        _factory.WorkerApi.Respond = _ => new WorkerApiResult(404,
            """{"title":"Not found","status":404,"detail":"шард mvshard/shard9 не найден"}""");
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsJsonAsync("/api/clusters/mvshard/moves",
            new { from = "shard1", to = "shard9", buckets = new[] { 0 } },
            TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Moves_WorkerApiUnavailable_Returns503()
    {
        // Arrange: живых ключей нет → 503 панели, мутация не прошла
        _factory.Snapshot = null;
        _factory.WorkerApi.Reset();
        _factory.WorkerApi.Throw = new WorkerApiUnavailableException("pgworker");
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsJsonAsync("/api/clusters/mvdown/moves",
            new { from = "shard1", to = "shard2", buckets = new[] { 0 } },
            TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
    }
}
