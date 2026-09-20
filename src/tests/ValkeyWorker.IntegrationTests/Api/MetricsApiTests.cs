using System.Net;
using FluentAssertions;
using Xunit;

namespace ValkeyWorker.IntegrationTests.Api;

// /metrics (spec §9.7): scrape-грань без клиентского серта внутри WAF;
// Meter ValkeyWorker в именах серий (otel_scope_name), серия reconcile-цикла —
// ровно одна (инструментация DI-синглтон).
[Collection(ValkeyMetricsCollection.Name)]
public sealed class MetricsApiTests(ValkeyMetricsFixture fx)
{
    [Fact]
    public async Task Metrics_200_PrometheusFormat()
    {
        // Arrange: фабрика по умолчанию.
        using var client = fx.Factory.CreateClient();

        // Act
        using var response = await client.GetAsync("/metrics", TestContext.Current.CancellationToken);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);

        // Assert: 200 и Prometheus text-format (Runtime-серии).
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        body.Should().Contain("dotnet_");
    }

    [Fact]
    public async Task Metrics_WorkerSeries_MeterValkeyWorker()
    {
        // Arrange: фабрика с живыми циклами; первый тик ReconcileLoop на пустом
        // etcd успешен ≤ 15 с (ScanIntervalSec=5).
        using var client = fx.Factory.CreateClient();

        // Act: ждём появления воркер-серии в экспорте (retry-цикл до 15 с).
        string body = "";
        for (var i = 0; i < 30; i++)
        {
            body = await client.GetStringAsync("/metrics", TestContext.Current.CancellationToken);
            if (body.Contains("worker_loop_last_success_timestamp_seconds"))
                break;
            await Task.Delay(500, TestContext.Current.CancellationToken);
        }

        // Assert: серия цикла с scope воркера ValkeyWorker — ровно ОДНА
        // (двойной MeterProvider дал бы две).
        var matches = body.Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Where(l => l.StartsWith("worker_loop_last_success_timestamp_seconds{"))
            .ToList();
        matches.Should().ContainSingle(
            l => l.Contains("loop=\"reconcile\"") && l.Contains("otel_scope_name=\"ValkeyWorker\""),
            "циклы и журнал обязаны писать в один объект метрик: {0}", string.Join(" | ", matches));
    }

    // AAA (t05): пустой домен — консервативный успех тика: серия самонаблюдения
    // коллектора обязана появиться в экспорте при живом воркере (без docker-нод).
    [Fact]
    public async Task Metrics_ValkeyCollectorLastSuccess_ПустойДоменУспех()
    {
        // Arrange: WAF-фабрика с живыми циклами на пустом etcd; первый тик
        // коллектора — сразу при старте (до Task.Delay), scrape 500 мс.
        using var client = fx.Factory.CreateClient();

        // Act: retry-цикл до 15 с.
        string body = "";
        for (var i = 0; i < 30; i++)
        {
            body = await client.GetStringAsync("/metrics", TestContext.Current.CancellationToken);
            if (body.Contains("valkey_collector_last_success_timestamp_seconds"))
                break;
            await Task.Delay(500, TestContext.Current.CancellationToken);
        }

        // Assert: серия самонаблюдения в scope ValkeyWorker.
        body.Should().Contain("valkey_collector_last_success_timestamp_seconds");
        body.Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Should().Contain(l => l.StartsWith("valkey_collector_last_success_timestamp_seconds")
                && l.Contains("otel_scope_name=\"ValkeyWorker\""));
    }
}
