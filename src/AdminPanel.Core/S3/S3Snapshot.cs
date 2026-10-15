using AdminPanel.Core;

namespace AdminPanel.Core.S3;

// Домен-снапшот S3 (arch/02 §12): отдельный от EtcdSnapshot/KafkaSnapshot/
// ValkeySnapshot — своя механика тика, теми же настройками endpoints.
// Immutable; refresher строит новый и атомарно заменяет в S3SnapshotStore.
public sealed record S3Snapshot(
    DateTimeOffset BuiltAtUtc,
    bool EtcdReachable,
    int ConsecutiveFailures,
    IReadOnlyList<S3ClusterInfo> Clusters,           // /s3/clusters/
    IReadOnlyList<S3RotationTicket> Rotations,       // /s3worker/rotations/ (arch/23 §8)
    IReadOnlyList<WorkerEndpoint> WorkerEndpoints,   // живые ключи /s3worker/api/ (arch/02 §2.3.4)
    IReadOnlyList<WorkerHealth> WorkerHealth,        // опрос /healthz живых инстансов
    IReadOnlyList<Alert> Alerts,                     // S3AlertEngine (arch/03 §12.4)
    IReadOnlyList<KeyParseError> ParseErrors,        // битые JSON s3-ключей (arch/23 §2)
    int UnknownKeyCount,
    WorkerApiCert? WorkerApiCert = null);            // целевой серт /workers/api_tls/s3worker (arch/02 §9.9)

// Кластер /s3/clusters/<C>/ (arch/23 §2): config + ноды + граф + факты.
public sealed record S3ClusterInfo(
    string Name,
    S3ClusterState State,                    // config.state; отсутствие = Active
    string Platform,                         // "owns3" (единственное значение)
    int NodesDeclared,                       // config.nodes (0..9)
    string SecretProfile,                    // имя per-install env-профиля (значение скрыто)
    long? CreatedUnix,
    string? Endpoints,                       // точка дискавери клиентов; null/пусто — критично у Active с нодами
    IReadOnlyList<S3NodeInfo> NodesList,
    IReadOnlyList<S3ReplicationLink> Links,  // граф репликации
    IReadOnlyList<string> DenyList,          // отозванные инстансы
    S3ConnectivityInfo? Connectivity = null, // зонд (arch/23 §3)
    S3SyncInfo? Sync = null,                 // зонд
    S3RotationTicket? Rotation = null);      // живая заявка ротации (джойн по кластеру)

// Инстанс <I>: state — raw-строка (NOT_INITIALIZED|PROVISIONING|RUNNING|
// UNREACHABLE|REMOVING|TO_REMOVE; толерантно к новым).
public sealed record S3NodeInfo(
    string Name,
    string? State,
    string? PlacedOn,                        // фактический docker-хост (резолвция placement)
    S3NodePlacement? Placement,              // селектор arch/23 §4
    S3NodeResources? Resources,              // cpu/mem/disk декларации
    S3NodeUsage? Usage);                     // занятость диска (надзор C)

// Селектор размещения: конъюнкция указанных атрибутов (arch/23 §4).
public sealed record S3NodePlacement(
    string? DockerHost,
    string? SwarmNode,
    IReadOnlyDictionary<string, string>? SwarmLabels);

// Декларация ресурсов: cpu — decimal; mem/disk — GiB (etcd хранит "<n>Gi").
public sealed record S3NodeResources(decimal? Cpu, int? MemGi, int? DiskGi);

// Занятость диска nodes/<I>/usage (arch/23 §2).
public sealed record S3NodeUsage(long UsedBytes, long TotalBytes, long UpdatedUnix);

// Связь графа репликации links (arch/23 §2).
public sealed record S3ReplicationLink(string A, string B);

// Связность connectivity (arch/23 §3): state — raw-строка OK|DEGRADED|DOWN;
// Links — матрица «нода → сосед → ok|offline» по настроенным линкам.
public sealed record S3ConnectivityInfo(
    string? State,
    long? CheckedUnix,
    IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> Links,
    string? ProbeError);

// Синхронность sync (arch/23 §3): state — raw-строка SYNCED|LAGGING|DIVERGED.
public sealed record S3SyncInfo(
    string? State,
    long? CheckedUnix,
    IReadOnlyList<S3SyncPair> Pairs,
    string? LastError);

// Пара сверки sync: from→to, missing/extra — количество расхождений.
public sealed record S3SyncPair(
    string From, string To, long Missing, long Extra, long? LagBytes, long? LagObjects);

// Заявка ротации /s3worker/rotations/<C> (arch/23 §8).
public sealed record S3RotationTicket(string Cluster, long RequestedUnix, string? RequestedBy);

// Состояние кластера: config.state; отсутствие = Active.
public enum S3ClusterState
{
    Active,
    NotInitialized,
    ToRemove,
}

// Маппинг config.state → enum (толерантно: незнакомое значение — Active).
public static class S3ClusterStates
{
    public static S3ClusterState Parse(string? raw) => raw switch
    {
        "NOT_INITIALIZED" => S3ClusterState.NotInitialized,
        "TO_REMOVE" => S3ClusterState.ToRemove,
        _ => S3ClusterState.Active,
    };
}
