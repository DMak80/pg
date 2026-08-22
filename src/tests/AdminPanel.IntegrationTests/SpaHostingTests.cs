using System.Net;
using System.Net.Http.Json;
using FluentAssertions;
using Microsoft.AspNetCore.Mvc.Testing;
using Xunit;

namespace AdminPanel.IntegrationTests;

// Раздача SPA и fallback-семантика /api/* (spec t07 §9): guard раньше fallback,
// авторизованный unknown-API-путь — 404 ProblemDetails, статика — без auth.
[Collection("api")]
public class SpaHostingTests
{
    private readonly AuthWebFactory _factory;

    public SpaHostingTests(AuthWebFactory factory) => _factory = factory;

    // Свежее окно лимитера: сдвиг времени — fixed window сбрасывается по windowId.
    private void NewRateWindow() => _factory.Time.Utc += TimeSpan.FromSeconds(61);

    [Fact]
    public async Task UnknownApiPath_WithoutCookie_Returns401()
    {
        // Arrange
        using var client = _factory.CreateClient(
            new WebApplicationFactoryClientOptions { AllowAutoRedirect = false });

        // Act
        var response = await client.GetAsync("/api/whatever", TestContext.Current.CancellationToken);

        // Assert: guard /api/* раньше fallback'ов — 401, а не 404/index.html.
        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task UnknownApiPath_WithCookie_Returns404ProblemDetails()
    {
        // Arrange: default-клиент хранит cookie; логин открывает сессию.
        NewRateWindow();
        using var client = _factory.CreateClient();
        var login = await client.PostAsJsonAsync(
            "/api/auth/login",
            new { username = "admin", password = "adminpw" },
            TestContext.Current.CancellationToken);
        login.StatusCode.Should().Be(HttpStatusCode.NoContent);

        // Act
        var response = await client.GetAsync("/api/whatever", TestContext.Current.CancellationToken);

        // Assert: специфичный /api-fallback бьёт SPA-fallback — 404 ProblemDetails, не index.html.
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
        response.Content.Headers.ContentType!.MediaType.Should().Be("application/problem+json");
    }

    [Fact]
    public async Task RootPath_WithoutCookie_IsNotUnauthorized()
    {
        // Arrange: без cookie; wwwroot в чистом чекауте пуст (ожидаем 404), с бандлом — 200.
        using var client = _factory.CreateClient(
            new WebApplicationFactoryClientOptions { AllowAutoRedirect = false });

        // Act
        var response = await client.GetAsync("/", TestContext.Current.CancellationToken);

        // Assert: статика/SPA-fallback не требуют авторизации; устойчиво к наличию бандла.
        response.StatusCode.Should().NotBe(HttpStatusCode.Unauthorized);
        ((int)response.StatusCode).Should().BeLessThan(500);
    }
}
