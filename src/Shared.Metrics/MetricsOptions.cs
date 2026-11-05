// Порт Puzzle-модуля Infrastructure.App.Metrics (arch/metrics/01-framework.md §1; паттерн
// Shared.Core — порт с паттерна Puzzle, бывший AdminPanel.Infrastructure, унифицирован).
namespace Shared.Metrics;

// [Config]-опции модуля метрик: секция "<Service>:Metrics".
// Не sealed: сервисы расширяют опции (напр., KafkaWorkerMetricsOptions —
// CollectIntervalSec, arch/metrics/04-collectors.md).
public class MetricsOptions
{
    /// <summary>false — модуль полностью выключен: ни MeterProvider, ни эндпоинта.</summary>
    public bool Enabled { get; set; } = true;

    /// <summary>Путь scrape-эндпоинта Prometheus (по умолчанию /metrics).</summary>
    public string Path { get; set; } = "/metrics";
}
