namespace AdminPanel.Core;

// Состояние кластера etcd: endpoints, members, alarms + свежесть и счётчик отказов (arch/02 §3, §2.4).
public sealed record EtcdStatus(
    bool Reachable,
    IReadOnlyList<EtcdEndpoint> Endpoints,
    IReadOnlyList<EtcdMember> Members,
    IReadOnlyList<EtcdAlarm> Alarms,
    string? ActiveEndpoint,
    bool QuorumSuspected,
    DateTimeOffset LastRefreshUtc,
    int ConsecutiveFailures);

// Один endpoint из настроек: результат персонального /v3/maintenance/status (или ошибки транспорта).
public sealed record EtcdEndpoint(
    string Url,
    bool Reachable,
    double? LatencyMs,
    string? Version,
    long? DbSizeBytes,
    ulong? LeaderMemberId,
    ulong? RaftIndex,
    ulong? RaftTerm,
    IReadOnlyList<string> Errors);

// Член etcd-кластера из /v3/cluster/member/list (isLeader в DTO вычисляет API t04 по EtcdStatus).
public sealed record EtcdMember(
    ulong Id,
    string? Name,
    IReadOnlyList<string> PeerUrls,
    IReadOnlyList<string> ClientUrls);

// Активная тревога из /v3/maintenance/alarm.
public sealed record EtcdAlarm(ulong MemberId, EtcdAlarmType Type);

// Значения enum-поля alarm в gateway: 0/1/2.
public enum EtcdAlarmType
{
    None = 0,
    NoSpace = 1,
    Corrupt = 2,
}
