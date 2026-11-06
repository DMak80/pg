namespace EtcdWorker.Core;

/// <summary>Провижининг-параметры состава (arch/etcdworker/01-placement.md/§8) для процессов
/// EtcdWorker.Provisioning — срез EtcdWorkerOptions (App) без зависимости от
/// App-сборки (прецедент S3ProvisioningOptions). Параметры Snapshot* — значения
/// секции EtcdWorker:NodeSnapshot: единственный канал доставки конфига агентов
/// EtcdNode в env контейнеров узлов (arch/etcdnode/01-agent.md §3).</summary>
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
    int HealthIntervalSec,
    int SnapshotIntervalMin,
    int SnapshotMinIntervalSec,
    int SnapshotRetryIntervalSec,
    int SnapshotTimeoutSec,
    int SnapshotRetentionFiles,
    int SnapshotRetentionObjects,
    int SnapshotMaintenanceIntervalMin,
    string? SnapshotS3CaPem);
