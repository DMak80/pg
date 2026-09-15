using Shared.Etcd.Client;

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

// Транспортные records EtcdMember/EtcdAlarm/EtcdAlarmType переехали в общую сборку
// Shared.Etcd (t08): using Shared.Etcd.Client в шапке.
