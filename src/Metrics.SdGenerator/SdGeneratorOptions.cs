using Shared.Metrics;

namespace Metrics.SdGenerator;

// [Config]-опции file_sd-генератора (arch/metrics/05-storage.md §4, §8): секция "SdGenerator".
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

    /// <summary>Путь второго file_sd-файла — file_sd агенты PgNode в нодах (
    /// arch/metrics/05-storage.md §4/§8): те же записи per-node, порт 8080 (http).</summary>
    public string PgNodeOutputPath { get; set; } = "/sd/pgnode-nodes.json";

    /// <summary>Самонаблюдение: секция "SdGenerator:Metrics" (паттерн arch/metrics/01-framework.md §2).</summary>
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
