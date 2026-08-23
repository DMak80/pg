namespace AdminPanel.Core;

// Patroni DCS-scope /service/<scope>/ (arch/02 §2.2): leader, members, optime, raw config
// + заявка ресурсов на ноду (arch/02 §9.1).
public sealed record HaScope(
    string Scope,
    string? Cluster,
    string? Shard,
    bool Matched,
    string? LeaderName,
    long? OptimeLeader,
    bool Initialized,
    string? RequestCpu,                    // /service/<scope>/request_cpu (arch/02 §9.1)
    string? RequestMem,
    string? RequestDisk,
    IReadOnlyList<HaMember> Members,
    string? RawConfig);

// Член HA-кластера: что есть в etcd + поля Patroni-пробы (t06 — null).
public sealed record HaMember(
    string Name,
    string Host,
    int? Port,
    string? Role,
    string? State,
    long? Timeline,
    long? LagBytes,
    DateTimeOffset? ProbeAtUtc,
    string? ProbeError);
