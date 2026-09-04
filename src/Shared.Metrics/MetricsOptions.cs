// Порт Puzzle-модуля Infrastructure.App.Metrics (arch/18 §1; паттерн
// AdminPanel.Infrastructure — копия осознанная).
namespace Shared.Metrics;

// [Config]-опции модуля метрик: секция "<Service>:Metrics".
// Не sealed: сервисы расширяют опции (напр., KafkaWorkerMetricsOptions —
// CollectIntervalSec, arch/18 §4).
public class MetricsOptions
{
    /// <summary>false — модуль полностью выключен: ни MeterProvider, ни эндпоинта.</summary>
    public bool Enabled { get; set; } = true;

    /// <summary>Путь scrape-эндпоинта Prometheus (по умолчанию /metrics).</summary>
    public string Path { get; set; } = "/metrics";
}
