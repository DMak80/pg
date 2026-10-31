using System.Net;
using FluentAssertions;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace KafkaWorker.IntegrationTests.Api;

// Интеграционные тесты /metrics KafkaWorker (arch/18 §3): scrape-грань открыта
// без ApiKey/cookie (симметрия /healthz); защита API — транспортная (mTLS: 401
// без валидного серта на /api покрывает MtlsApiTests); /metrics — открытая
// грань (arch/18 §3).
[Collection(KafkaMetricsCollection.Name)]
public sealed class MetricsTests(KafkaMetricsFixture fx)
{
    [Fact]
    public async Task Metrics_Responds_200_WithoutApiKey_EvenWhenApiKeySet()
    {
        // Arrange: фабрика по умолчанию (ApiKey в каноне нет — arch/16 §4)
        using var client = fx.Factory.CreateClient();

        // Act: GET /metrics без X-Api-Key
        using var response = await client.GetAsync("/metrics", TestContext.Current.CancellationToken);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);

        // Assert: 200 и Prometheus text-format (Runtime-серии)
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        body.Should().Contain("dotnet_");
    }

    [Fact]
    public async Task Metrics_WorkerSeries_AfterFirstTick_SingleInstrumentation()
    {
        // Arrange: фабрика с живыми циклами (hosted-сервисы не выключены);
        // первый тик ReconcileLoop на пустом etcd успешен ≤15 с (тик быстрее
        // ScanIntervalSec=5)
        using var client = fx.Factory.CreateClient();

        // Act: ждём именно целевую reconcile-серию, а не любую воркер-серию:
        // циклы стартуют независимо, при параллельной загрузке раньше может
        // появиться keepalive — это не признак первого reconcile-тика.
        var matches = new List<string>();
        for (var i = 0; i < 30; i++)
        {
            var body = await client.GetStringAsync("/metrics", TestContext.Current.CancellationToken);
            matches = body.Split('\n', StringSplitOptions.RemoveEmptyEntries)
                .Where(l => l.StartsWith("worker_loop_last_success_timestamp_seconds{"))
                .ToList();
            if (matches.Any(l => l.Contains("loop=\"reconcile\"") && l.Contains("otel_scope_name=\"KafkaWorker\"")))
                break;
            await Task.Delay(500, TestContext.Current.CancellationToken);
        }

        // Assert: серия цикла эмитится тем же объектом instrumentation, что
        // пишут циклы и журнал (регрессия дубля регистрации в DI — ревью Ф7-1:
        // второй WorkerMetricsInstrumentation перекрывал первый, журнал
        // подписывался в одного, циклы тикали в другого). Ровно ОДНА серия
        // reconcile-цикла с scope воркера — двойной MeterProvider дал бы две.
        matches.Should().ContainSingle(
            l => l.Contains("loop=\"reconcile\"") && l.Contains("otel_scope_name=\"KafkaWorker\""),
            "the loops and the journal must write to the same metrics object: {0}", string.Join(" | ", matches));
    }
}
