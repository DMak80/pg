using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using AdminPanel.Etcd.Workers;
using FluentAssertions;
using Xunit;

namespace AdminPanel.IntegrationTests;

// PUT /api/clusters/{c}/config (t06, 02 §9.10) — прокси в API PgWorker:
// стаб-воркер; 204 при успехе; проксирует коды воркера (400 валидация /
// 404 / 409 / 503) без изменений; панель не пишет в etcd.
[Collection("api")]
public class UpdateClusterConfigApiTests(AuthWebFactory factory)
{
    private readonly AuthWebFactory _factory = factory;

    private void SetLiveSnapshot()
        => _factory.Snapshot = InspectionSnapshots.Fixture(_factory.Time.GetUtcNow());

    // AAA (t06): панельный PUT — 204 при успехе; в стаб-гейтвее зафиксированы
    // путь "/api/clusters/shop/config", метод PUT, тело с полем synchronousModeStrict.
    [Fact]
    public async Task PutClusterConfig_ProxiesWorkerCodes()
    {
        // Arrange — WAF панели, живой снапшот; стаб-воркер отвечает 204.
        SetLiveSnapshot();
        _factory.WorkerApi.Reset();
        _factory.WorkerApi.Respond = _ => new WorkerApiResult(204, null);
        using var client = await ApiTestLogin.LoginAsync(_factory);

        // Act — PUT /api/clusters/shop/config {"synchronousModeStrict": false}.
        using var response = await client.PutAsJsonAsync(
            "/api/clusters/shop/config",
            new { synchronousModeStrict = false },
            TestContext.Current.CancellationToken);

        // Assert — 204; прокси-вызов: метод/путь/тело; etcd панели не тронут.
        response.StatusCode.Should().Be(HttpStatusCode.NoContent);
        var call = _factory.WorkerApi.Calls.Should().ContainSingle().Subject;
        call.Worker.Should().Be("pgworker");
        call.Method.Should().Be(HttpMethod.Put);
        call.Path.Should().Be("/api/clusters/shop/config");
        call.Body.Should().BeOfType<AdminPanel.Api.Operations.UpdateClusterConfigRequest>()
            .Which.SynchronousModeStrict.Should().BeFalse();
        _factory.EtcdStub.WriteCalls.Should().Be(0);
    }

    // AAA (t06): коды воркера проксируются телом как есть (400/404/409/503).
    [Theory]
    [InlineData(400, "Validation failed")]
    [InlineData(404, "Cluster not found")]
    [InlineData(409, "Cluster not active")]
    [InlineData(503, "Concurrent write")]
    public async Task PutClusterConfig_ProxiesWorkerErrorBodies(int status, string title)
    {
        // Arrange — стаб-воркер отвечает ProblemDetails соответствующим кодом.
        SetLiveSnapshot();
        _factory.WorkerApi.Reset();
        _factory.WorkerApi.Respond = _ => new WorkerApiResult(status,
            $$"""{"title":"{{title}}","status":{{status}},"detail":"detail-text"}""");
        using var client = await ApiTestLogin.LoginAsync(_factory);

        // Act
        using var response = await client.PutAsJsonAsync(
            "/api/clusters/shop/config",
            new { synchronousModeStrict = true },
            TestContext.Current.CancellationToken);

        // Assert — статус и ProblemDetails-тело воркера без изменений.
        ((int)response.StatusCode).Should().Be(status);
        var problem = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        problem.GetProperty("title").GetString().Should().Be(title);
    }
}
