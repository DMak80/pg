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
    IReadOnlyList<ClusterBackupsInfo> Backups,     // префикс /pgworker/backups/ (arch/19 §4, +)
    IReadOnlyList<WorkerEndpoint> PgWorkerEndpoints, // живые ключи /pgworker/api/ (arch/02 §2.3.1)
    IReadOnlyList<WorkJournalInfo> PgWorkerWork,   // журналы /pgworker/work/ (arch/02 §2.3.1, arch/14 §3.3)
    IReadOnlyList<WorkerHealth> WorkerHealth,      // результаты опроса /healthz (arch/02 §3, spec D4)
    IReadOnlyList<ProbeResult> Probes,             // всегда пусто (пробы)
    IReadOnlyList<Alert> Alerts,                   // всегда пусто (AlertEngine)
    IReadOnlyList<KeyParseError> ParseErrors,      // расширение spec §3.4 (arch/02 §7)
    int UnknownKeyCount,
    BackupStorageInfo? BackupStorage = null,      // ключ /pgworker/backups/storage 
    BackupOrphansInfo? BackupOrphans = null,      // ключ /pgworker/backups/orphans 
    S3StorageInfo? S3Storage = null,       // live-S3-инвентарь (adminpanel/02 §2.5)
    WorkerApiCert? WorkerApiCert = null,         // целевой серт API PgWorker (adminpanel/02 §9.9)
    EtcdSnapshotExportInfo? EtcdSnapshots = null); // статус выгрузки etcd-снапшотов (adminpanel/02 §2.3.1)
    // Пулы /s3/clusters/ в EtcdSnapshot НЕ входят: домен S3 — отдельный S3Snapshot
    // (adminpanel/02 §12), страница бэкапов берёт пулы из него (02 §12.4).

// Ключ, значение которого не удалось разобрать: виден в UI-details, кормит алерт key-malformed.
public sealed record KeyParseError(string Key, string Reason);
