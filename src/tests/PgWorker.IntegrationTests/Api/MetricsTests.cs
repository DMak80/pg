using System.Net;
using FluentAssertions;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace PgWorker.IntegrationTests.Api;

// Интеграционные тесты /metrics PgWorker (arch/18 §3): scrape-грань открыта без
// ApiKey (симметрия /healthz), подключение метрик не ломает ApiKeyMiddleware.
[Collection(PgMetricsCollection.Name)]
public sealed class MetricsTests(PgMetricsFixture fx)
{
    // Фабрика-оверрайд с непустым ApiKey (InMemory-конфиг поверх — последний
    // источник выигрывает, паттерн SeedApiTests).
    private sealed class ApiKeyFactory(PgMetricsFixture fx) : MetricsApiFactory(fx.Etcd)
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            base.ConfigureWebHost(builder);
            builder.ConfigureAppConfiguration((_, cfg) => cfg.AddInMemoryCollection(
                new Dictionary<string, string?> { ["PgWorker:Api:ApiKey"] = "test-key" }));
        }
    }

    [Fact]
    public async Task Metrics_Responds_200_WithoutApiKey_EvenWhenApiKeySet()
    {
        // Arrange: фабрика-оверрайд с непустым ApiKey
        using var factory = new ApiKeyFactory(fx);
        using var client = factory.CreateClient();

        // Act: GET /metrics без X-Api-Key
        using var response = await client.GetAsync("/metrics", TestContext.Current.CancellationToken);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);

        // Assert: 200 и Prometheus text-format (Runtime-серии; воркер-серии циклов
        // проверяет Metrics_WorkerSeries_AfterFirstTick — марк-вызовы циклов там же)
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        body.Should().Contain("dotnet_");
    }

    [Fact]
    public async Task Metrics_ApiKeySecuredApi_StaysProtected()
    {
        // Arrange: непустой ApiKey (та же фабрика-оверрайд)
        using var factory = new ApiKeyFactory(fx);
        using var client = factory.CreateClient();

        // Act: GET /api/... без ключа
        using var response = await client.GetAsync("/api/clusters", TestContext.Current.CancellationToken);

        // Assert: 401 — ApiKeyMiddleware не сломан подключением метрик
        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task Metrics_WorkerSeries_AfterFirstTick()
    {
        // Arrange: фабрика с живыми циклами (hosted-сервисы не выключены);
        // первый тик ReconcileLoop на пустом etcd успешен ≤15 с (тик быстрее
        // ScanIntervalSec=5)
        using var client = fx.Factory.CreateClient();

        // Act: ждём появления серий в экспорте (retry-цикл до 15 с)
        string body = "";
        for (var i = 0; i < 30; i++)
        {
            body = await client.GetStringAsync("/metrics", TestContext.Current.CancellationToken);
            if (body.Contains("worker_loop_ticks_total") && body.Contains("worker_claims_held"))
                break;
            await Task.Delay(500, TestContext.Current.CancellationToken);
        }

        // Assert: серии циклов/клэймов §2.2 эмитятся живыми циклами
        body.Should().Contain("""worker_loop_ticks_total{otel_scope_name="PgWorker",loop="reconcile",ok="true"}""");
        body.Should().Contain("worker_claims_held");
    }
}
