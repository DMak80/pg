using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using AdminPanel.Etcd.Workers;
using FluentAssertions;
using Xunit;

namespace AdminPanel.IntegrationTests;

// POST /api/clusters/{c}/app-password/rotate — прокси в API PgWorker (task
// etcd-via-worker-api): стаб-воркер; 201/409/404/503 прежние тела, оператор
// сессии уходит заголовком X-Requested-By (spec §3.7).
[Collection("api")]
public class RotateAppPasswordApiTests(AuthWebFactory factory)
{
    private readonly AuthWebFactory _factory = factory;

    private void SetLiveSnapshot()
        => _factory.Snapshot = InspectionSnapshots.Fixture(_factory.Time.GetUtcNow());

    private async Task<HttpClient> LoginAsync() => await ApiTestLogin.LoginAsync(_factory);

    [Fact]
    public async Task Rotate_ActiveCluster_Returns201WithAudit()
    {
        // Arrange: стаб-воркер отвечает прежним DTO с оператором сессии
        SetLiveSnapshot();
        _factory.WorkerApi.Reset();
        _factory.WorkerApi.Respond = _ => new WorkerApiResult(201,
            """{"cluster":"rot1","requestedUnix":1755900000,"requestedBy":"admin"}""");
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsync(
            "/api/clusters/rot1/app-password/rotate", null, TestContext.Current.CancellationToken);

        // Assert — 201 с телом прежнего формата
        response.StatusCode.Should().Be(HttpStatusCode.Created);
        var dto = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        dto.GetProperty("cluster").GetString().Should().Be("rot1");
        dto.GetProperty("requestedBy").GetString().Should().Be("admin");

        // Прокси-вызов: оператор сессии «admin» уходит воркеру (X-Requested-By)
        _factory.WorkerApi.Calls.Should().ContainSingle().Which.Should().Match<TestWorkerApi.Call>(c =>
            c.Worker == "pgworker" && c.Method == HttpMethod.Post
            && c.Path == "/api/clusters/rot1/app-password/rotate"
            && c.RequestedBy == "admin");
        _factory.EtcdStub.WriteCalls.Should().Be(0); // панель не пишет в etcd
    }

    [Fact]
    public async Task Rotate_LiveTicket_Conflict()
    {
        // Arrange — заявка уже стоит (повтор до исполнения → 409, §9.8 п.2)
        SetLiveSnapshot();
        _factory.WorkerApi.Reset();
        _factory.WorkerApi.Respond = _ => new WorkerApiResult(409,
            """{"title":"Rotation rejected","status":409,"detail":"ротация app-пароля rot2 уже запрошена — дождитесь исполнения (ключ /pgworker/rotations/rot2)"}""");
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsync(
            "/api/clusters/rot2/app-password/rotate", null, TestContext.Current.CancellationToken);

        // Assert — 409 прежним телом
        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
        var problem = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        problem.GetProperty("title").GetString().Should().Be("Rotation rejected");
    }

    [Fact]
    public async Task Rotate_UnknownCluster_NotFound()
    {
        // Arrange — имени нет (404 по §9.8 п.1)
        SetLiveSnapshot();
        _factory.WorkerApi.Reset();
        _factory.WorkerApi.Respond = _ => new WorkerApiResult(404,
            """{"title":"Cluster not found","status":404,"detail":"кластер nosuch не найден"}""");
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsync(
            "/api/clusters/nosuch/app-password/rotate", null, TestContext.Current.CancellationToken);

        // Assert — 404
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Rotate_WorkerApiUnavailable_ServiceUnavailable()
    {
        // Arrange — живых ключей api нет → 503 панели
        _factory.Snapshot = null;
        _factory.WorkerApi.Reset();
        _factory.WorkerApi.Throw = new WorkerApiUnavailableException("pgworker");
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsync(
            "/api/clusters/rot5/app-password/rotate", null, TestContext.Current.CancellationToken);

        // Assert — 503
        response.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
        var problem = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        problem.GetProperty("title").GetString().Should().Be("API воркера недоступен");
    }
}
