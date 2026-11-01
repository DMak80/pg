using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using Shared.Core.DI;

namespace AdminPanel.Core.Alerting.Rules;

// etcd-backup-cluster-missing (critical, adminpanel/03 §4): S3-домен
// прочитан, но в /s3/clusters/ нет живого (не TO_REMOVE) кластера с признаком
// etcd_backup=true — DR-слепкам etcd некуда выгружаться. Молчание, пока
// S3-домен не читался (факт EtcdBackupTarget вносит SnapshotRefresher из
// S3SnapshotStore — паттерн live-инвентаря S3Storage).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class EtcdBackupClusterMissingRule : IAlertRule
{
    public const string KindName = "etcd-backup-cluster-missing";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        if (snapshot.EtcdBackupTarget is not { } target || target.Cluster is not null)
            yield break; // домен не читался либо цель жива — молчим

        yield return new Alert(
            KindName,
            AlertSeverity.Critical,
            KindName,
            "etcd",
            "etcd backup cluster is missing: no /s3/clusters/ config with etcd_backup=true — DR snapshots cannot be uploaded",
            new Dictionary<string, string>(),
            null,
            "EtcdWorker has nowhere to upload control-plane DR snapshots — control-plane changes are not protected against host loss",
            AlertRemedy.OperatorApi,
            "create an etcd-backup S3 cluster — the flag and the special key are set by S3Worker (arch/23 §6)");
    }
}
