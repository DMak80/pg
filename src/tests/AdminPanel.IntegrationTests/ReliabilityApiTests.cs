using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using AdminPanel.Core;
using FluentAssertions;
using Xunit;

namespace AdminPanel.IntegrationTests;

// HTTP-контракт /api/reliability (spec §3.5): 401 без cookie; 503 без снапшота;
// 200 с числами из снапшота (пустые секции = подсистема выключена — толерантность).
[Collection("api")]
public class ReliabilityApiTests
{
    private readonly AuthWebFactory _factory;

    public ReliabilityApiTests(AuthWebFactory factory) => _factory = factory;

    // Логин: свежее окно rate-limiter'а (fixed-время фабрики) + cookie в клиенте
    // (самодостаточно — по телу общего логин-хелпера соседних api-тестов).
    private async Task<HttpClient> LoginAsync()
    {
        _factory.Time.Utc += TimeSpan.FromSeconds(61);
        var client = _factory.CreateClient();
        var login = await client.PostAsJsonAsync(
            "/api/auth/login",
            new { username = "admin", password = "adminpw" },
            TestContext.Current.CancellationToken);
        login.StatusCode.Should().Be(HttpStatusCode.NoContent);
        return client;
    }

    [Fact]
    public async Task Reliability_WithoutCookie_Returns401()
    {
        // Arrange
        using var client = _factory.CreateClient();

        // Act
        using var response = await client.GetAsync("/api/reliability", TestContext.Current.CancellationToken);

        // Assert: default-deny guard закрыл эндпоинт
        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task Reliability_NoSnapshot_Returns503()
    {
        // Arrange
        _factory.Snapshot = null;
        using var client = await LoginAsync();

        // Act
        using var response = await client.GetAsync("/api/reliability", TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
        var body = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        body.GetProperty("title").GetString().Should().Be("Snapshot not ready");
    }

    [Fact]
    public async Task Reliability_WithSnapshot_ReturnsNumbers()
    {
        // Arrange: логин ПЕРЕД сидированием — LoginAsync сдвигает fixed-время
        // фабрики (+61 с, окно лимитера), а хендлер берёт время фабрики
        var clientCreated = await LoginAsync();
        var now = _factory.Time.Utc;
        var t = now.ToUnixTimeSeconds();
        _factory.Snapshot = InspectionSnapshots.Clustered(now, now) with
        {
            PgWorkerWork = new List<WorkJournalInfo>
            {
                new("demo", "supervise", "supervising", "i1", t, null, null, null, null,
                    new HaSupervisionInfo("s1", "s1a", "accelerated", t - 200, t - 125, 75), null),
            },
            Backups = new List<ClusterBackupsInfo>
            {
                new("demo", null, new Dictionary<string, long?> { ["s1"] = t - 100 }),
            },
        };
        using var client = clientCreated;

        // Act
        using var response = await client.GetAsync("/api/reliability", TestContext.Current.CancellationToken);
        var body = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);

        // Assert: 200; кластер demo/шард s1; rpo.fullAgeSec≈100; rto.lastFailover.durationSec=75
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var shard = body.GetProperty("clusters")[0].GetProperty("shards")[0];
        shard.GetProperty("shard").GetString().Should().Be("s1");
        shard.GetProperty("rpo").GetProperty("fullAgeSec").GetInt64().Should().BeInRange(99, 101);
        var failover = shard.GetProperty("rto").GetProperty("lastFailover");
        failover.GetProperty("durationSec").GetInt64().Should().Be(75);
        failover.GetProperty("cause").GetString().Should().Be("accelerated");
    }
}
