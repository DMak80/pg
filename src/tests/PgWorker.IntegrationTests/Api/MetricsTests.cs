using System.Net;
using FluentAssertions;
using Xunit;

namespace PgWorker.IntegrationTests.Api;

// Интеграционные тесты /metrics PgWorker (arch/18 §3, t03): scrape-грань на том же
// mTLS-Kestrel-порту, что /healthz; защита API транспортная (mTLS — MtlsApiTests),
// здесь проверяем экспозицию Prometheus-формата и живые серии циклов.
[Collection(PgMetricsCollection.Name)]
public sealed class MetricsTests(PgMetricsFixture fx)
{
    [Fact]
    public async Task Metrics_Responds_200_PrometheusText()
    {
        // Arrange: фабрика с живыми циклами и /metrics-экспозицией
        using var client = fx.Factory.CreateClient();

        // Act: GET /metrics
        using var response = await client.GetAsync("/metrics", TestContext.Current.CancellationToken);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);

        // Assert: 200 и Prometheus text-format (Runtime-серии)
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        body.Should().Contain("dotnet_");
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
