using Shared.Metrics;

namespace Metrics.SdGenerator;

// [Config]-опции file_sd-генератора (arch/18 §5.4, §8): секция "SdGenerator".
// Read-only потребитель /pgworker/portalloc/ — ни новых ключей, ни записей в etcd.
public sealed class SdGeneratorOptions
{
    /// <summary>Дефолт интервала тика (сек); применяет NormalizeInterval к нечисловым/нулевым значениям.</summary>
    public const int DefaultRefreshIntervalSec = 15;

    /// <summary>Подключение etcd (HA-контур): endpoints с failover-перебором.</summary>
    public EtcdOptions Etcd { get; set; } = new();

    /// <summary>Интервал тика, сек; &lt;=0 нормализуется до 15 на старте (warning-лог — в Program.cs).</summary>
    public int RefreshIntervalSec { get; set; } = DefaultRefreshIntervalSec;

    /// <summary>Путь file_sd-файла (volume Prometheus, маунт /sd).</summary>
    public string OutputPath { get; set; } = "/sd/patroni-nodes.json";

    /// <summary>Самонаблюдение: секция "SdGenerator:Metrics" (паттерн arch/18 §7).</summary>
    public MetricsOptions Metrics { get; set; } = new();

    /// <summary>Endpoints etcd: перебор по порядку до первого успеха (failover).</summary>
    public sealed class EtcdOptions
    {
        public string[] Endpoints { get; set; } = [];
    }

    // <=0 → 15 (паттерн §4.2 valkey; warning-лог — в Program.cs при старте).
    public static int NormalizeInterval(int value)
        => value <= 0 ? DefaultRefreshIntervalSec : value;
}
