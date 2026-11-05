namespace AdminPanel.Core;

// Панельная модель WAL-потока шарда (ключ /pgworker/backups/<C>/<X>/wal,
// arch/backups/05-etcd-contract.md; adminpanel/02 §2.3.1;). Дубли воркерной модели — осознанные
// (бэкапные модели выведены за скоуп унификации); агрегат по кластерам —
// ClusterBackupsInfo (BackupInfo.cs, модель + Wal-словарь).
// WalChainError — разрыв цепочки (permanent; воркер переснимает полный).
public enum WalStreamInfoState { Active, Degraded, Stopped, WalChainError }

/// <summary>Границы RPO-зазора tli-прыжка приёмника из поля gap wal-ключа
/// (архивный формат arch/backups/05-etcd-contract.md): сегменты (From .. To] невосстановимы из
/// бэкапов.</summary>
public sealed record WalGapInfo(string From, string To, uint Tli);

/// <summary>WAL-поток шарда из ключа /pgworker/backups/&lt;C&gt;/&lt;X&gt;/wal.</summary>
public sealed record WalStreamInfo(
    string Cluster, string Shard, WalStreamInfoState State,
    string Slot, string MasterNode, long LastUploadedUnix,
    long? LagSegments, string? Error,
    // etcd-факт wal-сверки (строка сегмента из ключа; опционально —
    // старые/битые ключи поля не несут).
    string? LastUploadedSegment = null,
    // опционально — старые/битые ключи поля не несут.
    WalGapInfo? Gap = null);

/// <summary>Счётчик жизни цепочки WAL шарда (ключ /pgworker/backups/&lt;C&gt;/&lt;X&gt;/wal-life,
/// t15, arch/backups/05 §1; пишет только воркер): номер жизни, время и причина
/// последнего инкремента (shard-rebuilt|restore|foreign-chain).</summary>
public sealed record WalLifeInfo(
    string Cluster, string Shard, long ChainLife, long? BumpedUnix, string? Reason);

/// <summary>Одна запись реестра сирот из глобального ключа
/// /pgworker/backups/orphans ( дубль воркерной модели — осознанный):
/// префикс «&lt;C&gt;/&lt;X&gt;»,
/// kind shard|cluster, размер, первое наблюдение, состояние OBSERVED|DELETING;
/// HasValidFull — автозащита DR-hold (reliability, arch/backups/05-etcd-contract.md): в префиксе
/// есть валидный полный (full/&lt;id&gt;/backup_manifest), TTL-автоматика не
/// удалит; отсутствие поля (старый воркер) — false.</summary>
public sealed record BackupOrphanInfo(
    string Prefix, string Kind, long SizeBytes, long FirstSeenUnix, string State,
    bool HasValidFull = false);

/// <summary>Hold-флаг сироты из ключа /pgworker/backups/orphan-holds/&lt;C&gt;/&lt;X&gt;
/// (reliability, arch/backups/05-etcd-contract.md): защита «до разбора», ставит API воркера.</summary>
public sealed record OrphanHoldInfo(string Prefix, long SetUnix, string SetBy);

/// <summary>Заявка явного удаления сироты из ключа
/// /pgworker/backups/orphan-deletes/&lt;C&gt;/&lt;X&gt; (reliability,
/// arch/backups/05-etcd-contract.md): исполнит sweeper ближайшим проходом.</summary>
public sealed record OrphanDeleteRequestInfo(string Prefix, long RequestedUnix, string RequestedBy);

/// <summary>Реестр сирот установки из глобального ключа
/// /pgworker/backups/orphans ( пишет только лидер-проход воркера);
/// Holds/DeleteRequests (reliability) — джойн hold-ключей и заявок
/// удаления по префиксу (панель — толерантный читатель).</summary>
public sealed record BackupOrphansInfo(
    IReadOnlyList<BackupOrphanInfo> Orphans, long UpdatedUnix,
    IReadOnlyDictionary<string, OrphanHoldInfo>? Holds = null,
    IReadOnlyDictionary<string, OrphanDeleteRequestInfo>? DeleteRequests = null);

/// <summary>Вердикт занятости bucket бэкапов : OK/WARN/CRIT.</summary>
public enum BackupStorageState { Ok, Warn, Crit }

/// <summary>Занятость bucket бэкапов из глобального ключа
/// /pgworker/backups/storage ( дубль воркерной модели — осознанный).</summary>
public sealed record BackupStorageInfo(
    long UsedBytes, long? QuotaBytes, double? UsedPercent,
    BackupStorageState State, long UpdatedUnix);

/// <summary>Статус DR-выгрузки слепков etcd в S3 (ключ
/// /etcdworker/snapshots, arch/etcdworker/00-etcd-contract.md; пишет EtcdWorker): null-поля —
/// толерантный парсинг (битые/старые ключи); поля enabled в формате нет —
/// выгрузка безусловна; ключа нет — правила молчат.</summary>
public sealed record EtcdSnapshotExportInfo(
    string? State, long? LastUploadedUnix, string? LastObject,
    string? LastSha256, long? SizeBytes, int? IntervalMin, string? Error);
