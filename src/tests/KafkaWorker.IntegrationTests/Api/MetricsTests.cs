using System.Net;
using FluentAssertions;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace KafkaWorker.IntegrationTests.Api;

// Интеграционные тесты /metrics KafkaWorker (arch/18 §3): scrape-грань открыта
// без ApiKey (симметрия /healthz), подключение метрик не ломает ApiKeyMiddleware.
[Collection(KafkaMetricsCollection.Name)]
public sealed class MetricsTests(KafkaMetricsFixture fx)
{
    // Фабрика-оверрайд с непустым ApiKey (InMemory-конфиг поверх).
    private sealed class ApiKeyFactory(KafkaMetricsFixture fx) : MetricsApiFactory(fx.Etcd)
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            base.ConfigureWebHost(builder);
            builder.ConfigureAppConfiguration((_, cfg) => cfg.AddInMemoryCollection(
                new Dictionary<string, string?> { ["KafkaWorker:Api:ApiKey"] = "test-key" }));
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

        // Assert: 200 и Prometheus text-format (Runtime-серии)
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
        using var response = await client.GetAsync("/api/kafka/clusters", TestContext.Current.CancellationToken);

        // Assert: 401 — ApiKeyMiddleware не сломан подключением метрик
        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task Metrics_WorkerSeries_AfterFirstTick_SingleInstrumentation()
    {
        // Arrange: фабрика с живыми циклами (hosted-сервисы не выключены);
        // первый тик ReconcileLoop на пустом etcd успешен ≤15 с (тик быстрее
        // ScanIntervalSec=5)
        using var client = fx.Factory.CreateClient();

        // Act: ждём появления воркер-серии в экспорте (retry-цикл до 15 с)
        string body = "";
        for (var i = 0; i < 30; i++)
        {
            body = await client.GetStringAsync("/metrics", TestContext.Current.CancellationToken);
            if (body.Contains("worker_loop_last_success_timestamp_seconds"))
                break;
            await Task.Delay(500, TestContext.Current.CancellationToken);
        }

        // Assert: серия цикла эмитится тем же объектом instrumentation, что
        // пишут циклы и журнал (регрессия дубля регистрации в DI — ревью Ф7-1:
        // второй WorkerMetricsInstrumentation перекрывал первый, журнал
        // подписывался в одного, циклы тикали в другого). Ровно ОДНА серия
        // reconcile-цикла с scope воркера — двойной MeterProvider дал бы две.
        var matches = body.Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Where(l => l.StartsWith("worker_loop_last_success_timestamp_seconds{"))
            .ToList();
        matches.Should().ContainSingle(
            l => l.Contains("loop=\"reconcile\"") && l.Contains("otel_scope_name=\"KafkaWorker\""),
            "циклы и журнал обязаны писать в один объект метрик: {0}", string.Join(" | ", matches));
    }
}
