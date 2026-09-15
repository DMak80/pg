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
/// kind shard|cluster, размер, первое наблюдение, состояние OBSERVED|DELETING.</summary>
public sealed record BackupOrphanInfo(
    string Prefix, string Kind, long SizeBytes, long FirstSeenUnix, string State);

/// <summary>Реестр сирот установки из глобального ключа
/// /pgworker/backups/orphans (t07; пишет только лидер-проход воркера).</summary>
public sealed record BackupOrphansInfo(
    IReadOnlyList<BackupOrphanInfo> Orphans, long UpdatedUnix);

/// <summary>Вердикт занятости bucket бэкапов (t06): OK/WARN/CRIT.</summary>
public enum BackupStorageState { Ok, Warn, Crit }

/// <summary>Занятость bucket бэкапов из глобального ключа
/// /pgworker/backups/storage (t06; дубль воркерной модели — осознанный).</summary>
public sealed record BackupStorageInfo(
    long UsedBytes, long? QuotaBytes, double? UsedPercent,
    BackupStorageState State, long UpdatedUnix);
