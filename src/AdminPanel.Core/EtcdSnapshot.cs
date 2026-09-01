namespace AdminPanel.Core;

// Слепок всего, что панель знает об инспектируемой системе (контракт arch/02 §3).
// Immutable: refresher строит новый и атомарно заменяет в SnapshotStore.
public sealed record EtcdSnapshot(
    DateTimeOffset BuiltAtUtc,
    EtcdStatus Etcd,
    IReadOnlyList<ClusterInfo> Clusters,
    IReadOnlyList<HaScope> HaScopes,
    IReadOnlyList<StandNode> StandNodes,
    IReadOnlyList<MoveTicket> MoveTickets,         // очередь заявок /pgworker/moves/ (arch/02 §2.3.1)
    IReadOnlyList<WorkerEndpoint> PgWorkerEndpoints, // живые ключи /pgworker/api/ (arch/02 §2.3.1)
    IReadOnlyList<ProbeResult> Probes,             // t03: всегда пусто (пробы — t06)
    IReadOnlyList<Alert> Alerts,                   // t03: всегда пусто (AlertEngine — t04)
    IReadOnlyList<KeyParseError> ParseErrors,      // расширение spec §3.4 (arch/02 §7)
    int UnknownKeyCount);

// Ключ, значение которого не удалось разобрать: виден в UI-details, кормит алерт key-malformed (t04).
public sealed record KeyParseError(string Key, string Reason);
