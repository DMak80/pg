namespace AdminPanel.Core;

/// <summary>Факт etcd-backup S3-кластера (t32, adminpanel/03 §4): источник —
/// S3-домен (/s3/clusters/, adminpanel/02 §12); SnapshotRefresher вносит из
/// S3SnapshotStore готовым агрегатом (паттерн live-инвентаря S3Storage).
/// Cluster — имя первого живого (не TO_REMOVE) кластера с etcd_backup=true;
/// null при прочитанном домене — кластера нет (алерт
/// etcd-backup-cluster-missing). Запись null — S3-домен ещё не читался
/// (алерт молчит).</summary>
public sealed record EtcdBackupTargetInfo(string? Cluster);
