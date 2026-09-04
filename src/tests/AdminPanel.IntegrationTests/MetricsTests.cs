using System.Net;
using FluentAssertions;
using Xunit;

namespace AdminPanel.IntegrationTests;

// Интеграционные тесты /metrics панели (t04, arch/18 §2.4/§3): scrape-грань
// открыта без cookie-авторизации, guard /api/* не затронут подключением метрик.
[Collection("api")]
public class MetricsTests
{
    private readonly AuthWebFactory _factory;

    public MetricsTests(AuthWebFactory factory) => _factory = factory;

    [Fact]
    public async Task Metrics_Responds_200_WithoutCookieAuth()
    {
        // Arrange: WAF-хост панели (общая фикстура), клиент без cookie
        using var client = _factory.CreateClient();

        // Act: GET /metrics без авторизации
        using var response = await client.GetAsync("/metrics", TestContext.Current.CancellationToken);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);

        // Assert: 200; тело содержит dotnet_ (Runtime-серии) — http-гистограмма
        // появится после первого запроса (см. Shared.Metrics.UnitTests, факт пинов)
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        body.Should().Contain("dotnet_");
        body.Should().Contain("panel_refresher_last_success_timestamp_seconds");
    }

    [Fact]
    public async Task Metrics_ApiGuard_NotAffected()
    {
        // Arrange: без cookie
        using var client = _factory.CreateClient();

        // Act: GET /api/overview (закрытый эндпоинт)
        using var response = await client.GetAsync("/api/overview", TestContext.Current.CancellationToken);

        // Assert: 401 — guard по-прежнему только /api/*, /metrics мимо него
        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }
}
