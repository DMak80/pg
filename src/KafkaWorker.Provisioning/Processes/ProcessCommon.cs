using KafkaWorker.Core;
using KafkaWorker.Core.Model;

namespace KafkaWorker.Provisioning.Processes;

/// <summary>Исход процесса (arch/16 §5): для цикла ReconcileLoop.</summary>
public enum ProcessOutcome
{
    /// <summary>Продолжить следующими тиками (ждём ключей панели, подъёма брокеров).</summary>
    InProgress,

    /// <summary>Цель процесса достигнута.</summary>
    Done,
}

/// <summary>
/// Параметры процессов (appsettings KafkaWorker:Docker/Thresholds → A12):
/// диапазон клиентских портов, бюджет подъёма брокеров, advertised-хост
/// CLIENT-listener (null → имя docker-хоста ноды, arch/16 §2.1), образ брокера.
/// </summary>
public sealed record ProvisioningOptions(
    int PortFrom,
    int PortTo,
    int BrokerBootSec,
    int NodeDeadSec,
    string? AdvertisedClientHost,
    string NodeImage)
{
    public static ProvisioningOptions Default { get; } = new(16000, 16999, 600, 90, null, "apache/kafka:4.0.0");
}

/// <summary>
/// Параметры процесса reassign I (arch/16 §8): интервал тиков, размер батча
/// подач, бюджет exec CLI и окно дедупа переподачи одного батча.
/// </summary>
public sealed record ReassignOptions(
    int IntervalSec,
    int BatchPartitions,
    int ExecSec,
    int RetrySubmitSec)
{
    public static ReassignOptions Default { get; } = new(15, 10, 180, 120);
}

/// <summary>
/// Converge mutable-конфигов кластера как dynamic broker configs (arch/16 §5 E;
/// реализация — задача A11). Вызывается provisioning'ом (стартовый converge)
/// и Active-веткой цикла; применяется без рестартов брокеров.
/// </summary>
public interface IClusterConfigConverger
{
    Task<Result> ApplyAsync(string cluster, string bootstrap, string user, string password, string? caPem, KafkaClusterConfig config, CancellationToken ct);
}
