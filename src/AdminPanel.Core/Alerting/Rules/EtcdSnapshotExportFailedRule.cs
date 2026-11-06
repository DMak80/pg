using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using Shared.Core.DI;

namespace AdminPanel.Core.Alerting.Rules;

// etcd-snapshot-export-failed (critical, adminpanel/03 §4): выгрузка
// DR-слепков etcd в S3 провалилась (state=FAILED статуса EtcdWorker, текст
// error, в т.ч. маркер etcd-backup-missing: цель не заявлена/недоступна) —
// контроль-плейн не защищён от потери хоста. Нет ключа — молчание
// (толерантный читатель).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class EtcdSnapshotExportFailedRule : IAlertRule
{
    public const string KindName = "etcd-snapshot-export-failed";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        var export = snapshot.EtcdSnapshots;
        if (export is not { State: "FAILED" })
            yield break; // нет ключа — молчим

        yield return new Alert(
            KindName,
            AlertSeverity.Critical,
            KindName,
            "etcd",
            $"etcd snapshot export to S3 failed: {export.Error ?? "no reason"}",
            new Dictionary<string, string>
            {
                ["state"] = "FAILED",
                ["lastUploadedUnix"] = export.LastUploadedUnix?.ToString() ?? string.Empty,
            },
            null,
            "control plane is not protected against host loss — etcd snapshot export to S3 failed",
            AlertRemedy.OperatorRunbook,
            "arch/etcdnode/02-snapshots.md: check the etcd-backup S3 cluster (/s3/clusters/, etcd_backup=true) and the status key /etcdnode/snapshots; the EtcdNode agent completes the export on a RetryIntervalSec tick");
    }
}
