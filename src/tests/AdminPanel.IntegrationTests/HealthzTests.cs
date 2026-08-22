using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using FluentAssertions;
using Xunit;

namespace AdminPanel.IntegrationTests;

// Смоук живости панели: /api/healthz без авторизации отвечает контрактом {"status":"ok"}.
// t02: тест в общей коллекции "api" — второй хост в процессе невозможен (кеш DI-скана сборок).
[Collection("api")]
public class HealthzTests
{
    private readonly AuthWebFactory _factory;

    public HealthzTests(AuthWebFactory factory) => _factory = factory;

    [Fact]
    public async Task Healthz_ReturnsOkStatus()
    {
        // Arrange
        using var client = _factory.CreateClient();

        // Act
        var response = await client.GetAsync("/api/healthz", TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var body = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        body.GetProperty("status").GetString().Should().Be("ok");
    }
}
