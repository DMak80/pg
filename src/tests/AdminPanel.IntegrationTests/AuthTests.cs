using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using FluentAssertions;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Xunit;

namespace AdminPanel.IntegrationTests;

// Управляемое время: изоляция окна rate-limiter'а между тестами общей фабрики.
public sealed class FixedTimeProvider : TimeProvider
{
    public DateTimeOffset Utc { get; set; } = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    public override DateTimeOffset GetUtcNow() => Utc;
}

// Единая на сборку фабрика (collection fixture "api"): статический кеш сборок
// attribute-DI не допускает второй хост в процессе (spec t02 §10, §14).
public sealed class AuthWebFactory : WebApplicationFactory<Program>
{
    public FixedTimeProvider Time { get; } = new();

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        // http-стенд: без AllowHttp Secure-cookie не вернётся по http (spec t02 §10, §14).
        builder.UseSetting("AdminPanel:Auth:Username", "admin");
        builder.UseSetting("AdminPanel:Auth:Password", "adminpw");
        builder.UseSetting("AdminPanel:Auth:AllowHttp", "true");

        // Подмена времени ПОСЛЕ композиции Program (ConfigureTestServices):
        // singleton-лимитер живёт на управляемом времени фабрики.
        builder.ConfigureTestServices(services =>
            services.Replace(ServiceDescriptor.Singleton(typeof(TimeProvider), Time)));
    }
}

// Единственный хост на тестовую сборку: AuthTests и HealthzTests.
[CollectionDefinition("api")]
public sealed class ApiCollection : ICollectionFixture<AuthWebFactory>;

// Интеграция auth-модуля: login/logout/me, 401 без cookie, rate-limit (spec t02 §10).
[Collection("api")]
public class AuthTests
{
    private readonly AuthWebFactory _factory;

    public AuthTests(AuthWebFactory factory) => _factory = factory;

    // Свежее окно лимитера: сдвиг времени — fixed window сбрасывается по windowId.
    private void NewRateWindow() => _factory.Time.Utc += TimeSpan.FromSeconds(61);

    [Fact]
    public async Task Login_ValidCredentials_Returns204AndSessionCookie()
    {
        // Arrange
        NewRateWindow();
        using var client = _factory.CreateClient(new WebApplicationFactoryClientOptions { AllowAutoRedirect = false });

        // Act
        var response = await client.PostAsJsonAsync(
            "/api/auth/login",
            new { username = "admin", password = "adminpw" },
            TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.NoContent);
        response.Headers.TryGetValues("Set-Cookie", out var cookies).Should().BeTrue();
        cookies.Should().Contain(c => c.StartsWith("adminpanel_session="));
    }

    [Fact]
    public async Task Login_WrongPassword_Returns401ProblemDetails()
    {
        // Arrange
        NewRateWindow();
        using var client = _factory.CreateClient();

        // Act
        var response = await client.PostAsJsonAsync(
            "/api/auth/login",
            new { username = "admin", password = "wrong" },
            TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
        response.Content.Headers.ContentType!.MediaType.Should().Be("application/problem+json");
    }

    [Fact]
    public async Task Login_WrongUsername_Returns401()
    {
        // Arrange
        NewRateWindow();
        using var client = _factory.CreateClient();

        // Act
        var response = await client.PostAsJsonAsync(
            "/api/auth/login",
            new { username = "root", password = "adminpw" },
            TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task Login_MalformedJson_Returns400()
    {
        // Arrange
        using var client = _factory.CreateClient();
        using var content = new StringContent("{ not json", Encoding.UTF8, "application/json");

        // Act
        var response = await client.PostAsync("/api/auth/login", content, TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task Login_RateLimit_SixthAttempt_Returns429()
    {
        // Arrange
        NewRateWindow();
        using var client = _factory.CreateClient();

        // Act: пять неудачных попыток исчерпывают окно 5/мин.
        for (var i = 0; i < 5; i++)
            await client.PostAsJsonAsync(
                "/api/auth/login",
                new { username = "admin", password = "wrong" },
                TestContext.Current.CancellationToken);
        var sixth = await client.PostAsJsonAsync(
            "/api/auth/login",
            new { username = "admin", password = "wrong" },
            TestContext.Current.CancellationToken);

        // Assert
        sixth.StatusCode.Should().Be(HttpStatusCode.TooManyRequests);
        sixth.Headers.TryGetValues("Retry-After", out _).Should().BeTrue();
    }

    [Fact]
    public async Task Me_WithoutCookie_Returns401NotRedirect()
    {
        // Arrange
        using var client = _factory.CreateClient(new WebApplicationFactoryClientOptions { AllowAutoRedirect = false });

        // Act
        var response = await client.GetAsync("/api/auth/me", TestContext.Current.CancellationToken);

        // Assert: ровно 401 — никаких 302-редиректов на логин (spec t02 §3.7).
        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task Me_WithCookie_ReturnsUsername()
    {
        // Arrange: default-клиент хранит cookie из Set-Cookie (HandleCookies=true).
        NewRateWindow();
        using var client = _factory.CreateClient();
        await client.PostAsJsonAsync(
            "/api/auth/login",
            new { username = "admin", password = "adminpw" },
            TestContext.Current.CancellationToken);

        // Act
        var response = await client.GetAsync("/api/auth/me", TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var body = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        body.GetProperty("username").GetString().Should().Be("admin");
    }

    [Fact]
    public async Task Logout_WithCookie_Returns204AndInvalidatesSession()
    {
        // Arrange
        NewRateWindow();
        using var client = _factory.CreateClient();
        await client.PostAsJsonAsync(
            "/api/auth/login",
            new { username = "admin", password = "adminpw" },
            TestContext.Current.CancellationToken);

        // Act
        var logout = await client.PostAsync("/api/auth/logout", null, TestContext.Current.CancellationToken);
        var me = await client.GetAsync("/api/auth/me", TestContext.Current.CancellationToken);

        // Assert
        logout.StatusCode.Should().Be(HttpStatusCode.NoContent);
        me.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task Api_DefaultDeny_WithoutCookie_Returns401()
    {
        // Arrange
        using var client = _factory.CreateClient(new WebApplicationFactoryClientOptions { AllowAutoRedirect = false });

        // Act: защищённые пути без cookie; healthz — исключение guard'а.
        var logout = await client.PostAsync("/api/auth/logout", null, TestContext.Current.CancellationToken);
        var me = await client.GetAsync("/api/auth/me", TestContext.Current.CancellationToken);
        var healthz = await client.GetAsync("/api/healthz", TestContext.Current.CancellationToken);

        // Assert
        logout.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
        me.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
        healthz.StatusCode.Should().Be(HttpStatusCode.OK);
    }
}
