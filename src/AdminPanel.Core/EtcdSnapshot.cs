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
    IReadOnlyList<ClusterBackupsInfo> Backups,     // префикс /pgworker/backups/ (arch/backups/05-etcd-contract.md, +)
    IReadOnlyList<WorkerEndpoint> PgWorkerEndpoints, // живые ключи /pgworker/api/ (arch/02 §2.3.1)
    IReadOnlyList<WorkJournalInfo> PgWorkerWork,   // журналы /pgworker/work/ (arch/02 §2.3.1, arch/pgworker/00-etcd-contract.md §4)
    IReadOnlyList<WorkerHealth> WorkerHealth,      // результаты опроса /healthz (arch/02 §3, spec D4)
    IReadOnlyList<ProbeResult> Probes,             // всегда пусто (пробы)
    IReadOnlyList<Alert> Alerts,                   // всегда пусто (AlertEngine)
    IReadOnlyList<KeyParseError> ParseErrors,      // расширение spec §3.4 (arch/02 §7)
    int UnknownKeyCount,
    BackupStorageInfo? BackupStorage = null,      // ключ /pgworker/backups/storage
    BackupOrphansInfo? BackupOrphans = null,      // ключ /pgworker/backups/orphans
    S3StorageInfo? S3Storage = null,       // live-S3-инвентарь (adminpanel/02 §2.5)
    WorkerApiCert? WorkerApiCert = null,         // целевой серт API PgWorker (adminpanel/02 §9.9)
    EtcdSnapshotExportInfo? EtcdSnapshots = null,   // статус DR-выгрузки слепков (ключ /etcdworker/snapshots, 02 §2.3.5)
    BackupsDisabledInfo? BackupsDisabled = null,  // глобальный стоп бэкапов (u10, adminpanel/02 §2.3.1)
    IReadOnlyList<WalAgentInfo>? WalAgents = null,   // приёмники WAL agent/<N> (u11, arch/backups/05-etcd-contract.md)
    IReadOnlyList<EvacuationInfo>? Evacuations = null, // журналы эвакуаций (u12, arch/pgworker/00-etcd-contract.md §3)
    IReadOnlyList<PgRotationTicket>? PgRotations = null, // живые заявки ротаций (u07, arch/pgworker/00-etcd-contract.md §4)
    WorkerCoordination? Coordination = null,  // leader/claims/instances pg-домена (u14)
    EtcdDomainInfo? EtcdDomain = null,              // домен EtcdWorker /etcd/ (02 §2.3.5)
    IReadOnlyList<WorkerEndpoint>? EtcdWorkerEndpoints = null, // живые ключи /etcdworker/api/ (02 §2.3.5); null — не читались
    IReadOnlyList<WorkerHealth>? EtcdWorkerHealth = null,      // опрос /healthz инстансов EtcdWorker общим поллером (02 §2.3.5)
    EtcdBackupTargetInfo? EtcdBackupTarget = null,  // факт etcd-backup S3-кластера из S3-домена (03 §4)
    WorkerApiCert? EtcdWorkerApiCert = null);       // целевой серт /workers/api_tls/etcdworker (02 §9.9)
    // Пулы /s3/clusters/ в EtcdSnapshot НЕ входят: домен S3 — отдельный S3Snapshot
    // (adminpanel/02 §12), страница бэкапов берёт пулы из него (02 §12.4).

// Ключ, значение которого не удалось разобрать: виден в UI-details, кормит алерт key-malformed.
public sealed record KeyParseError(string Key, string Reason);

// Глобальный стоп бэкапов (u10; канон arch/backups/05-etcd-contract.md): значение ключа
// /pgworker/backups-disabled; null в снапшоте = ключа нет (бэкапы работают).
public sealed record BackupsDisabledInfo(long UpdatedUnix, string? UpdatedBy);

// Приёмник WAL ноды (u11; канон arch/backups/05-etcd-contract.md): ключ /pgworker/backups/<C>/<X>/agent/<N>
// — lease TTL 15 с (ключ жив = приёмник жив); Cluster/Shard — из ключа.
public sealed record WalAgentInfo(
    string Cluster, string Shard, string Node, string? Role, string State,
    long? LastDeliveredSegment, long? LastDeliveredUnix, bool PoolExhausted,
    long? TliJump, long? HistoryMissingTli, string? Error, long UpdatedUnix);

// Журнал аварийной эвакуации шарда (u12; канон arch/pgworker/00-etcd-contract.md §3/§5 D): buckets —
// словарь «старый шард → новый шард»; ReturnedUnix — при возврате
// (state=QUARANTINED, arch/pgworker/processes/d-bucket-evacuator.md D).
public sealed record EvacuationInfo(
    string Cluster, string Shard, long EvacuatedUnix, string Reason,
    IReadOnlyDictionary<string, string> Buckets, string State, long? ReturnedUnix);

// Живая заявка ротации per-cluster секретов (u07; ключ /pgworker/rotations/<C>,
// arch/pgworker/00-etcd-contract.md §4/§5 I; формат — §9.8).
public sealed record PgRotationTicket(string Cluster, long RequestedUnix, string? RequestedBy);

// Координация воркера домена (u14; leader/claims/instances; adminpanel/02 §3):
// отсутствие ключей — пустая координация, не ошибка; для pg и доменов
// kafka/valkey/s3 — один тип (джойны на DTO — по имени кластера).
public sealed record WorkerCoordination(
    string? LeaderInstance,
    IReadOnlyList<WorkerClaim> Claims,
    IReadOnlyList<WorkerInstanceInfo> Instances);

// Claim кластера (ключ <root>/claims/<C>): «кто держит кластер».
public sealed record WorkerClaim(string Cluster, string Instance, long? SinceUnix);

// Живой (lease) инстанс воркера (ключ <root>/instances/<id>).
public sealed record WorkerInstanceInfo(string InstanceId, long? SinceUnix);
