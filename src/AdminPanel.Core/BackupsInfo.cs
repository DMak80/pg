namespace AdminPanel.Core;

// Панельная модель подсистемы бэкапов (чтение /pgworker/backups/, arch/19 §4;
// adminpanel/02 §2.3.1): только поля статусов WAL — политика/полные панели в t03
// не нужны (UI бэкапов — t08). Дубли с воркерной моделью — осознанные (roadmap t08).
public enum WalStreamInfoState { Active, Degraded, Stopped }

/// <summary>WAL-поток шарда из ключа /pgworker/backups/&lt;C&gt;/&lt;X&gt;/wal.</summary>
public sealed record WalStreamInfo(
    string Cluster, string Shard, WalStreamInfoState State,
    string Slot, string MasterNode, long LastUploadedUnix,
    long? LagSegments, string? Error);

public sealed record ClusterBackupsInfo(
    string Cluster, IReadOnlyDictionary<string, WalStreamInfo?> Shards);
