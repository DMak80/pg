using AdminPanel.Core;

namespace AdminPanel.Core.Valkey;

// Домен-снапшот Valkey (arch/02 §11.1): отдельный от EtcdSnapshot/KafkaSnapshot —
// своя механика тика, теми же настройками endpoints. Immutable; refresher строит
// новый и атомарно заменяет в ValkeySnapshotStore.
public sealed record ValkeySnapshot(
    DateTimeOffset BuiltAtUtc,
    bool EtcdReachable,
    int ConsecutiveFailures,
    IReadOnlyList<ValkeyClusterInfo> Clusters,
    IReadOnlyList<ValkeyRotationTicket> Rotations,     // /valkeyworker/rotations/ (arch/20 §3)
    IReadOnlyList<WorkerEndpoint> WorkerEndpoints,     // живые ключи /valkeyworker/api/ (arch/02 §2.3.3)
    IReadOnlyList<WorkerHealth> WorkerHealth,          // опрос /healthz живых инстансов
    IReadOnlyList<ValkeyProbeResult> Probes,           // live-PING пробы (§4.6 spec)
    IReadOnlyList<Alert> Alerts,                       // ValkeyAlertEngine (arch/03 §8.4)
    IReadOnlyList<KeyParseError> ParseErrors,          // битые JSON valkey-ключей (arch/20 §5)
    int UnknownKeyCount,
    WorkerApiCert? WorkerApiCert = null);              // целевой серт /workers/api_tls/valkeyworker (arch/02 §9.9)

// Кластер /valkey/clusters/<C>/ (arch/20 §2): config + state + факт (nodes/endpoints).
public sealed record ValkeyClusterInfo(
    string Name,
    ValkeyClusterState State,                // Active|NotInitialized|ToRemove; отсутствие state = Active
    int Nodes,                               // всегда 1 (v1)
    long MaxmemoryBytes,
    string MaxmemoryPolicy,
    long? CreatedUnix,
    string? Endpoints,                       // null/пусто — воркер не дописал (алерт у Active)
    IReadOnlyList<ValkeyNodeInfo> NodesList, // node1 (v1 — один элемент)
    ValkeyRotationTicket? Rotation = null,   // живая заявка ротации (джойн по кластеру)
    bool HasCaPem = false);                  // ca_pem в etcd (t06): bool-флаг; сам PEM в API не отдаётся

// Нода node<k>: state — raw-строка (NOT_INITIALIZED|PROVISIONING|RUNNING|UNREACHABLE|
// REMOVING|TO_REMOVE; толерантно к новым); Live — из PING-пробы (null — проба молчит).
public sealed record ValkeyNodeInfo(
    string Name,
    string? State,
    decimal? Cpu,
    int? MemGi,
    int? DiskGi,
    bool? Live = null,
    string? ProbeError = null);

// Заявка ротации /valkeyworker/rotations/<C> (arch/20 §3): role app|admin + аудит.
public sealed record ValkeyRotationTicket(
    string Cluster, string Role, long RequestedUnix, string? RequestedBy);

// Результат live-пробы кластера (spec §4.6): одна нода в v1.
public sealed record ValkeyProbeResult(
    string Cluster, string Node, bool Live, long CheckedUnix, string? Error);

// Состояние кластера: config.state (arch/20 §2); отсутствие = Active.
public enum ValkeyClusterState
{
    Active,
    NotInitialized,
    ToRemove,
}

// Маппинг config.state → enum (arch/20 §5: незнакомое значение — толерантно, Active).
public static class ValkeyClusterStates
{
    public static ValkeyClusterState Parse(string? raw) => raw switch
    {
        "NOT_INITIALIZED" => ValkeyClusterState.NotInitialized,
        "TO_REMOVE" => ValkeyClusterState.ToRemove,
        _ => ValkeyClusterState.Active,
    };
}
