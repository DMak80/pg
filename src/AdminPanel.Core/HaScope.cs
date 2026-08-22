namespace AdminPanel.Core;

// Patroni DCS-scope /service/<scope>/ (arch/02 §2.2): leader, members, optime, raw config.
public sealed record HaScope(
    string Scope,
    string? Cluster,
    string? Shard,
    bool Matched,
    string? LeaderName,
    string? OptimeLeader,
    bool Initialized,
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
