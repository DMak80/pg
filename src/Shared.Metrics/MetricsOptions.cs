// Порт Puzzle-модуля Infrastructure.App.Metrics (arch/18 §1; паттерн
// AdminPanel.Infrastructure — копия осознанная).
namespace Shared.Metrics;

// [Config]-опции модуля метрик: секция "<Service>:Metrics".
public sealed class MetricsOptions
{
    /// <summary>false — модуль полностью выключен: ни MeterProvider, ни эндпоинта.</summary>
    public bool Enabled { get; set; } = true;

    /// <summary>Путь scrape-эндпоинта Prometheus (по умолчанию /metrics).</summary>
    public string Path { get; set; } = "/metrics";
}
