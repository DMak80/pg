namespace EtcdWorker.Core;

/// <summary>Провижининг-параметры состава (arch/25 §2/§8) для процессов
/// EtcdWorker.Provisioning — срез EtcdWorkerOptions (App) без зависимости от
/// App-сборки (прецедент S3ProvisioningOptions).</summary>
public sealed record EtcdProvisioningOptions(
    string Mode,
    string Network,
    string Image,
    string? AdvertisedHost,
    int PortFrom,
    int PortTo,
    string PlainHost,
    int NodeBootSec,
    int NodeDeadSec,
    int HealthIntervalSec);
