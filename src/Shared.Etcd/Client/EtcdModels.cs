namespace Shared.Etcd.Client;

// Данные status-ответа без контекста endpoint (url/latency добавляет refresher панели).
// Revision — из header.revision (int64 → ulong-decimal-строка protojson; воркеры
// используют для compaction, t08).
public sealed record EtcdStatusPayload(
    string? Version,
    long? DbSizeBytes,
    ulong? LeaderMemberId,
    ulong? RaftIndex,
    ulong? RaftTerm,
    ulong? Revision);

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
