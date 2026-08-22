using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using FluentAssertions;
using Microsoft.AspNetCore.Mvc.Testing;
using Xunit;

namespace AdminPanel.IntegrationTests;

// Смоук живости панели: /api/healthz без авторизации отвечает контрактом {"status":"ok"}.
public class HealthzTests
{
    [Fact]
    public async Task Healthz_ReturnsOkStatus()
    {
        // Arrange
        await using var factory = new WebApplicationFactory<Program>();
        using var client = factory.CreateClient();

        // Act
        var response = await client.GetAsync("/api/healthz", TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var body = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        body.GetProperty("status").GetString().Should().Be("ok");
    }
}
