namespace AdminPanel.Core;

// Панельная модель WAL-потока шарда (ключ /pgworker/backups/<C>/<X>/wal,
// arch/19 §4; adminpanel/02 §2.3.1; t03). Дубли воркерной модели — осознанные
// (бэкапные модели выведены за скоуп унификации t08); агрегат по кластерам —
// ClusterBackupsInfo (BackupInfo.cs, t02-модель + Wal-словарь t03).
// t07: Broken — разрыв цепочки (permanent; воркер переснимает полный).
public enum WalStreamInfoState { Active, Degraded, Stopped, Broken }

/// <summary>WAL-поток шарда из ключа /pgworker/backups/&lt;C&gt;/&lt;X&gt;/wal.</summary>
public sealed record WalStreamInfo(
    string Cluster, string Shard, WalStreamInfoState State,
    string Slot, string MasterNode, long LastUploadedUnix,
    long? LagSegments, string? Error,
    // t08: etcd-факт wal-сверки (строка сегмента из ключа; опционально —
    // старые/битые ключи поля не несут).
    string? LastUploadedSegment = null);

/// <summary>Одна запись реестра сирот из глобального ключа
/// /pgworker/backups/orphans (t07; дубль воркерной модели — осознанный):
/// префикс «&lt;C&gt;/&lt;X&gt;»,
/// kind shard|cluster, размер, первое наблюдение, состояние OBSERVED|DELETING;
/// HasValidFull — автозащита DR-hold (reliability t04, arch/19 §4): в префиксе
/// есть валидный полный (full/&lt;id&gt;/backup_manifest), TTL-автоматика не
/// удалит; отсутствие поля (старый воркер) — false.</summary>
public sealed record BackupOrphanInfo(
    string Prefix, string Kind, long SizeBytes, long FirstSeenUnix, string State,
    bool HasValidFull = false);

/// <summary>Hold-флаг сироты из ключа /pgworker/backups/orphan-holds/&lt;C&gt;/&lt;X&gt;
/// (reliability t04, arch/19 §4): защита «до разбора», ставит API воркера.</summary>
public sealed record OrphanHoldInfo(string Prefix, long SetUnix, string SetBy);

/// <summary>Заявка явного удаления сироты из ключа
/// /pgworker/backups/orphan-deletes/&lt;C&gt;/&lt;X&gt; (reliability t04,
/// arch/19 §4): исполнит sweeper ближайшим проходом.</summary>
public sealed record OrphanDeleteRequestInfo(string Prefix, long RequestedUnix, string RequestedBy);

/// <summary>Реестр сирот установки из глобального ключа
/// /pgworker/backups/orphans (t07; пишет только лидер-проход воркера);
/// Holds/DeleteRequests (reliability t04) — джойн hold-ключей и заявок
/// удаления по префиксу (панель — толерантный читатель).</summary>
public sealed record BackupOrphansInfo(
    IReadOnlyList<BackupOrphanInfo> Orphans, long UpdatedUnix,
    IReadOnlyDictionary<string, OrphanHoldInfo>? Holds = null,
    IReadOnlyDictionary<string, OrphanDeleteRequestInfo>? DeleteRequests = null);

/// <summary>Вердикт занятости bucket бэкапов (t06): OK/WARN/CRIT.</summary>
public enum BackupStorageState { Ok, Warn, Crit }

/// <summary>Занятость bucket бэкапов из глобального ключа
/// /pgworker/backups/storage (t06; дубль воркерной модели — осознанный).</summary>
public sealed record BackupStorageInfo(
    long UsedBytes, long? QuotaBytes, double? UsedPercent,
    BackupStorageState State, long UpdatedUnix);

/// <summary>Статус выгрузки etcd-снапшотов в S3 (t08, adminpanel/02 §2.3.1):
/// ключ /pgworker/etcd-snapshots, пишет PgWorker; null-поля — толерантный
/// парсинг (битые/старые ключи); enabled=false/ключа нет — правила молчат.</summary>
public sealed record EtcdSnapshotExportInfo(
    bool Enabled, string? State, long? LastUploadedUnix, string? LastObject,
    string? LastSha256, long? SizeBytes, int? IntervalMin, string? Error);
