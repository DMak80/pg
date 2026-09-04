// Порт Puzzle-модуля Infrastructure.App.Metrics (arch/18 §1; паттерн
// AdminPanel.Infrastructure — копия осознанная).
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using OpenTelemetry.Metrics;
using System.Diagnostics.Metrics;

namespace Shared.Metrics;

// Модуль метрик (arch/18 §1): DI-регистрация OTel-MeterProvider и scrape-эндпоинт.
// Конвенции: имя Meter = имя системы (dot-нотация инструментов, единицы
// секунды/штуки; финальные имена после экспорта — словарь arch/18 §2).
public static class MetricsModuleExtensions
{
    // Регистрация OTel-MeterProvider: сервисный Meter(serviceName) + System.Runtime
    // + http-метр ASP.NET; scrape-эндпоинт Prometheus-экспортёра на options.Path.
    // Метрики — пассивные наблюдатели: любые ошибки инструментария не роняют хост.
    public static IServiceCollection AddAppMetrics(
        this IServiceCollection services, string serviceName, IConfiguration metricsSection)
    {
        var options = new MetricsOptions();
        metricsSection.Bind(options);
        services.AddSingleton(options);

        if (!options.Enabled)
            return services;

        // Сервисный Meter регистрируется в DI: доменные инструменты пишут в него.
        var meter = new Meter(serviceName);
        services.AddSingleton(meter);

        services.AddOpenTelemetry()
            .WithMetrics(b => b
                .AddMeter(serviceName)              // сервисные инструменты
                .AddRuntimeInstrumentation()        // dotnet_* (arch/18 §2.1)
                .AddAspNetCoreInstrumentation()     // http_server_* (arch/18 §2.1)
                .AddPrometheusExporter(o => o.ScrapeEndpointPath = options.Path));
        return services;
    }

    // Эндпоинт-обёртка Prometheus-экспортёра (учёт Enabled/Path). Вызывать после Build().
    public static TApp MapAppMetrics<TApp>(this TApp app) where TApp : IApplicationBuilder
    {
        var options = app.ApplicationServices.GetRequiredService<MetricsOptions>();
        if (!options.Enabled)
            return app;
        app.UseOpenTelemetryPrometheusScrapingEndpoint(); // путь — из опций экспортёра
        return app;
    }
}
