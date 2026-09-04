using System.Diagnostics.Metrics;
using System.Net;
using FluentAssertions;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Shared.Metrics.Worker;

namespace Shared.Metrics.UnitTests;

// Интеграционные тесты /metrics на минимальном WebApplication-хосте: фиксируют
// ФАКТИЧЕСКИЕ экспортированные OTel-имена против словаря arch/18 §2 (риск M3/S1)
// и фактические значения лейбла process (канон = факт, ревью Ф4-5).
// При расхождении — тест правится на факт + arch/18 §2 тем же коммитом.
public sealed class MetricsEndpointTests
{
    // Минимальный хост с модулем метрик (порт 0 — случайный, без коллизий).
    private static async Task<WebApplication> StartHostAsync(
        string serviceName = "TestWorker", bool enabled = true, string path = "/metrics")
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Test:Metrics:Enabled"] = enabled ? "true" : "false",
            ["Test:Metrics:Path"] = path,
        });
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Services.AddAppMetrics(serviceName, builder.Configuration.GetSection("Test:Metrics"));
        builder.Services.AddSingleton(sp => new WorkerMetricsInstrumentation(
            sp.GetRequiredService<Meter>(), TimeProvider.System));
        var app = builder.Build();
        app.MapAppMetrics();
        await app.StartAsync();
        return app;
    }

    private static async Task<(HttpStatusCode Code, string Body)> GetAsync(WebApplication app, string path = "/metrics")
    {
        using var client = new HttpClient { BaseAddress = new Uri(app.Urls.First()) };
        using var response = await client.GetAsync(path);
        var body = await response.Content.ReadAsStringAsync();
        return (response.StatusCode, body);
    }

    [Fact]
    public async Task MetricsEndpoint_ExportsDictionaryNames()
    {
        // Arrange: минимальный хост + воркер-инструментарий; прогон всех марк-методов
        var app = await StartHostAsync();
        try
        {
            var sut = app.Services.GetRequiredService<WorkerMetricsInstrumentation>();
            var now = DateTimeOffset.UnixEpoch;
            sut.LoopTick("reconcile", ok: true);
            sut.LoopTick("reconcile", ok: false);
            sut.LoopDuration("reconcile", 0.42);
            sut.ClaimsHeld(3);
            sut.ProcessPhase("demo", "provision", "started", now);
            sut.Operation("provision", ok: true);
            sut.SnapshotTaken(now);

            // Act: первый запрос — прогрев, второй — фактическая проверка экспорта
            await GetAsync(app);
            var (code, body) = await GetAsync(app);

            // Assert: ВСЕ канонические имена arch/18 §2.2 присутствуют в экспорте
            code.Should().Be(HttpStatusCode.OK);
            body.Should().Contain("worker_loop_ticks_total");
            body.Should().Contain("worker_loop_last_success_timestamp_seconds");
            body.Should().Contain("worker_loop_duration_seconds");
            body.Should().Contain("worker_claims_held");
            body.Should().Contain("worker_process_phase_duration_seconds");
            body.Should().Contain("worker_operation_total");
            body.Should().Contain("worker_snapshot_age_seconds");
            // §2.1: Runtime- и ASP.NET-метры. Факт пинов 1.16.0-beta.1 (M3): на
            // минимальном хосте гистограмма http_server_request_duration_seconds
            // не эмитится; фактический ASP.NET-метр — http_server_active_requests
            // (+ kestrel_*/aspnetcore_memory_pool_*). Реальные сервисы сверяют
            // интеграционные тесты Ф3–Ф5; см. arch/18 §2.1.
            body.Should().Contain("dotnet_");
            body.Should().Contain("http_server_active_requests");
        }
        finally
        {
            await app.StopAsync(TestContext.Current.CancellationToken);
            await app.DisposeAsync();
        }
    }

    [Fact]
    public async Task MetricsEndpoint_ProcessLabelValues_Canonical()
    {
        // Arrange: все фактические op журналов обоих воркеров (arch/18 §2.2, канон = факт;
        // ревью Ф4.2-1: rollback/finalize — MoveProcess.cs:592/691/755)
        var pgOps = new[]
        {
            "provision", "deprovision", "adopt", "add-shard", "remove-shard",
            "rotate-app-password", "move", "rollback", "finalize", "repair", "abort",
        };
        var kafkaOps = new[]
        {
            "provision", "deprovision", "add-broker", "remove-broker", "reassign", "rotate", "regen", "topicsync",
        };
        var app = await StartHostAsync();
        try
        {
            var sut = app.Services.GetRequiredService<WorkerMetricsInstrumentation>();
            foreach (var op in pgOps.Concat(kafkaOps))
            {
                sut.OnJournalPhase("demo", op, "planned");

                // Act: терминальная done обязана закрыть фазовую серию и посчитать операцию
                sut.OnJournalPhase("demo", op, "done");
            }

            var (code, body) = await GetAsync(app);

            // Assert: после done фазовых серий нет вовсе; каждая op посчитана ok.
            // Факт экспортёра 1.16.0-beta.1: системный лейбл otel_scope_name (= имя
            // Meter) добавляется к сериям — фиксируем (канон = факт, arch/18 §2.2).
            code.Should().Be(HttpStatusCode.OK);
            body.Should().NotContain("worker_process_phase_duration_seconds{");
            body.Should().Contain("otel_scope_name=\"TestWorker\"");
            foreach (var op in pgOps.Concat(kafkaOps).Distinct())
                body.Should().Contain($"operation=\"{op}\",result=\"ok\"");
        }
        finally
        {
            await app.StopAsync(TestContext.Current.CancellationToken);
            await app.DisposeAsync();
        }
    }

    [Fact]
    public async Task MetricsEndpoint_Disabled_404()
    {
        // Arrange: Enabled=false — модуль полностью выключен
        var app = await StartHostAsync(enabled: false);
        try
        {
            // Act
            var (code, _) = await GetAsync(app);

            // Assert: эндпоинта нет
            code.Should().Be(HttpStatusCode.NotFound);
        }
        finally
        {
            await app.StopAsync(TestContext.Current.CancellationToken);
            await app.DisposeAsync();
        }
    }

    [Fact]
    public async Task MetricsEndpoint_CustomPath()
    {
        // Arrange: путь берётся из MetricsOptions.Path
        var app = await StartHostAsync(path: "/custom-metrics");
        try
        {
            // Act
            var (code, body) = await GetAsync(app, "/custom-metrics");

            // Assert
            code.Should().Be(HttpStatusCode.OK);
            body.Should().Contain("dotnet_");
        }
        finally
        {
            await app.StopAsync(TestContext.Current.CancellationToken);
            await app.DisposeAsync();
        }
    }
}
