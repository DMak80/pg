using System.Net;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace PgWorker.IntegrationTests.Api;

// Интеграционные тесты /metrics PgWorker (arch/18 §3, t03): scrape-грань на том же
// mTLS-Kestrel-порту, что /healthz; защита API транспортная (mTLS — MtlsApiTests),
// здесь проверяем экспозицию Prometheus-формата и живые серии циклов.
[Collection(NonE2eCollection.Name)]
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

    // Канон-тест словаря §2.7 (arch/18 §6): все 7 бэкапных серий экспортируются
    // с каноническими именами и лейблами cluster/shard/result. Серии процессов
    // живут под клэймом живого кластера — instrumentation пинается напрямую:
    // тест фиксирует экспозицию имён/лейблов (маппинг марк-методов в серии).
    [Fact]
    public async Task Metrics_BackupSeries_CanonicalNamesAndLabels()
    {
        // Arrange — по одному факту каждой серии через singleton-инструментацию DI
        var m = fx.Factory.Services.GetRequiredService<Shared.Metrics.Worker.WorkerMetricsInstrumentation>();
        m.BackupFullAge("canon", new Dictionary<string, (long? LastValidUnix, long MaxAgeSec)>
        {
            ["s1"] = (1700000000, 86400),
            ["s2"] = (null, 43200),
        });
        m.BackupWalLag("canon", "s1", 3);
        m.BackupWalUploadedAge("canon", "s1", 42);
        m.BackupVerify("canon", "s1", "ok");
        m.BackupRestore("canon", "s1", "failed");
        m.BackupDrill("canon", "s1", "failed");

        // Act — scrape с retry (образец Metrics_WorkerSeries_AfterFirstTick):
        // экспортёр кэширует scrape-ответ — серия после пина появляется в окне
        // ожидания, прямой GET сразу после пина может вернуть прошлый снимок
        using var client = fx.Factory.CreateClient();
        var body = "";
        for (var i = 0; i < 30; i++)
        {
            body = await client.GetStringAsync("/metrics", TestContext.Current.CancellationToken);
            if (body.Contains("pgworker_backup_full_age_seconds")
                && body.Contains("pgworker_backup_drill_total"))
                break;
            await Task.Delay(500, TestContext.Current.CancellationToken);
        }

        // Assert — канонические имена §2.7 в фактическом экспорте
        foreach (var name in new[]
                 {
                     "pgworker_backup_full_age_seconds",
                     "pgworker_backup_full_max_age_seconds",
                     "pgworker_backup_wal_lag_segments",
                     "pgworker_backup_wal_last_uploaded_age_seconds",
                     "pgworker_backup_verify_total",
                     "pgworker_backup_restore_total",
                     "pgworker_backup_drill_total",
                 })
            body.Should().Contain(name, $"серия {name} словаря §2.7 обязана экспортироваться");

        // Лейблы: экспозиция OTel сортирует ключи лексикографически
        // (cluster, result, shard для counter'ов; cluster, shard для гейджей) —
        // канон §2.7 фиксирует НАБОР лейблов, порядок — деталь экспозиции
        body.Should().Contain("cluster=\"canon\",shard=\"s1\"");
        body.Should().Contain("cluster=\"canon\",result=\"ok\",shard=\"s1\"");
        body.Should().Contain("cluster=\"canon\",result=\"failed\",shard=\"s1\"");

        // Null-семантика age (spec §3.1): шард s2 без валидного — age-серия s2
        // НЕ эмитится, max_age s2 эмитится (порог пишется всегда)
        body.Should().Contain(
            """pgworker_backup_full_max_age_seconds{otel_scope_name="PgWorker",cluster="canon",shard="s2"} 43200""");
        body.Should().NotContain(
            """pgworker_backup_full_age_seconds{otel_scope_name="PgWorker",cluster="canon",shard="s2"}""");
    }
}
