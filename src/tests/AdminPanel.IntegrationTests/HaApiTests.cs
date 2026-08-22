using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using AdminPanel.Core;
using FluentAssertions;
using Xunit;

namespace AdminPanel.IntegrationTests;

// HTTP-контракт HA-эндпоинтов (spec §9.2): 401/503/200/404 + probe-поля DTO.
[Collection("api")]
public class HaApiTests
{
    private readonly AuthWebFactory _factory;

    public HaApiTests(AuthWebFactory factory) => _factory = factory;

    private async Task<HttpClient> LoginAsync() => await ApiTestLogin.LoginAsync(_factory);

    private async Task<JsonElement> GetJsonAsync(HttpClient client, string url)
    {
        using var response = await client.GetAsync(url, TestContext.Current.CancellationToken);
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        return await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Ha_WithoutCookie_Return401()
    {
        // Arrange
        using var client = _factory.CreateClient();

        // Act
        var list = await client.GetAsync("/api/ha", TestContext.Current.CancellationToken);
        var details = await client.GetAsync("/api/ha/demo-s1", TestContext.Current.CancellationToken);

        // Assert: default-deny guard закрыл новые эндпоинты без правок auth.
        list.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
        details.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task Ha_NoSnapshot_Return503ProblemDetails()
    {
        // Arrange
        _factory.Snapshot = null;
        using var client = await LoginAsync();

        // Act
        var list = await client.GetAsync("/api/ha", TestContext.Current.CancellationToken);
        var details = await client.GetAsync("/api/ha/demo-s1", TestContext.Current.CancellationToken);

        // Assert
        list.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
        list.Content.Headers.ContentType!.MediaType.Should().Be("application/problem+json");
        var body = await list.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        body.GetProperty("title").GetString().Should().Be("Snapshot not ready");
        details.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
    }

    [Fact]
    public async Task Ha_WithSnapshot_ReturnSummaries()
    {
        // Arrange
        _factory.Snapshot = InspectionSnapshots.Ha(_factory.Time.Utc, _factory.Time.Utc);
        using var client = await LoginAsync();

        // Act
        var summaries = await GetJsonAsync(client, "/api/ha");

        // Assert: порядок Scope Ordinal; агрегаты по членам (spec §3.17).
        summaries.GetArrayLength().Should().Be(2);
        var demo = summaries[0];
        demo.GetProperty("scope").GetString().Should().Be("demo-s1");
        demo.GetProperty("cluster").GetString().Should().Be("demo");
        demo.GetProperty("shard").GetString().Should().Be("s1");
        demo.GetProperty("matched").GetBoolean().Should().BeTrue();
        demo.GetProperty("leaderName").GetString().Should().Be("s1a");
        demo.GetProperty("membersTotal").GetInt32().Should().Be(2);
        demo.GetProperty("membersHealthy").GetInt32().Should().Be(2);
        demo.GetProperty("lagMaxBytes").GetInt64().Should().Be(17L * 1024 * 1024);
        var other = summaries[1];
        other.GetProperty("scope").GetString().Should().Be("other-scope");
        other.GetProperty("matched").GetBoolean().Should().BeFalse();
        other.GetProperty("leaderName").ValueKind.Should().Be(JsonValueKind.Null);
        other.GetProperty("membersHealthy").GetInt32().Should().Be(0);
        other.GetProperty("lagMaxBytes").ValueKind.Should().Be(JsonValueKind.Null);
    }

    [Fact]
    public async Task HaDetails_ReturnsMembersWithProbeFields()
    {
        // Arrange
        _factory.Snapshot = InspectionSnapshots.Ha(_factory.Time.Utc, _factory.Time.Utc);
        using var client = await LoginAsync();

        // Act
        var dto = await GetJsonAsync(client, "/api/ha/demo-s1");

        // Assert
        dto.GetProperty("optimeLeader").GetInt64().Should().Be(738273634528L);
        dto.GetProperty("rawConfig").GetString().Should().Contain("loop_wait");
        var member = dto.GetProperty("members")[1];
        member.GetProperty("name").GetString().Should().Be("s1b");
        member.GetProperty("timeline").GetInt64().Should().Be(1L);
        member.GetProperty("lagBytes").GetInt64().Should().Be(17L * 1024 * 1024);
        member.GetProperty("probeAtUtc").GetString().Should().NotBeNullOrEmpty();
        member.GetProperty("probeError").ValueKind.Should().Be(JsonValueKind.Null);
    }

    [Fact]
    public async Task HaDetails_MemberProbeError_Visible()
    {
        // Arrange
        _factory.Snapshot = InspectionSnapshots.Ha(_factory.Time.Utc, _factory.Time.Utc);
        using var client = await LoginAsync();

        // Act
        var dto = await GetJsonAsync(client, "/api/ha/other-scope");

        // Assert: ошибка пробы видна, DCS role/state остались (spec §3.5).
        var member = dto.GetProperty("members")[0];
        member.GetProperty("role").GetString().Should().Be("replica");
        member.GetProperty("state").GetString().Should().Be("stopped");
        member.GetProperty("timeline").ValueKind.Should().Be(JsonValueKind.Null);
        member.GetProperty("lagBytes").ValueKind.Should().Be(JsonValueKind.Null);
        member.GetProperty("probeError").GetString().Should().Be("connection refused");
    }

    [Fact]
    public async Task HaDetails_UnknownScope_Return404ProblemDetails()
    {
        // Arrange
        _factory.Snapshot = InspectionSnapshots.Ha(_factory.Time.Utc, _factory.Time.Utc);
        using var client = await LoginAsync();

        // Act
        using var response = await client.GetAsync("/api/ha/nope", TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
        response.Content.Headers.ContentType!.MediaType.Should().Be("application/problem+json");
        var body = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        body.GetProperty("title").GetString().Should().Be("Scope not found");
    }
}
