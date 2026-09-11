namespace AdminPanel.Core;

// Панельная модель WAL-потока шарда (ключ /pgworker/backups/<C>/<X>/wal,
// arch/19 §4; adminpanel/02 §2.3.1; t03). Дубли воркерной модели — осознанные
// (unify — t08-unify-adminpanel-duplicates); агрегат по кластерам —
// ClusterBackupsInfo (BackupInfo.cs, t02-модель + Wal-словарь t03).
public enum WalStreamInfoState { Active, Degraded, Stopped }

/// <summary>WAL-поток шарда из ключа /pgworker/backups/&lt;C&gt;/&lt;X&gt;/wal.</summary>
public sealed record WalStreamInfo(
    string Cluster, string Shard, WalStreamInfoState State,
    string Slot, string MasterNode, long LastUploadedUnix,
    long? LagSegments, string? Error);

/// <summary>Вердикт занятости bucket бэкапов (t06): OK/WARN/CRIT.</summary>
public enum BackupStorageState { Ok, Warn, Crit }

/// <summary>Занятость bucket бэкапов из глобального ключа
/// /pgworker/backups/storage (t06; дубль воркерной модели — осознанный,
/// унификация t08-unify-adminpanel-duplicates).</summary>
public sealed record BackupStorageInfo(
    long UsedBytes, long? QuotaBytes, double? UsedPercent,
    BackupStorageState State, long UpdatedUnix);
