using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using Shared.Core.DI;

namespace AdminPanel.Core.Alerting.Rules;

// etcd-snapshot-export-failed (critical, reliability t08, adminpanel/03 §4):
// выгрузка снапшотов etcd в S3 провалилась (state=FAILED, текст error) —
// контроль-плейн не защищён от потери хоста. Выключенная опция/нет ключа —
// молчание (образец backup-full-stale: выключенная подсистема не алертит).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class EtcdSnapshotExportFailedRule : IAlertRule
{
    public const string KindName = "etcd-snapshot-export-failed";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        var export = snapshot.EtcdSnapshots;
        if (export is not { Enabled: true, State: "FAILED" })
            yield break; // выключено/нет ключа — молчим

        yield return new Alert(
            KindName,
            AlertSeverity.Critical,
            KindName,
            "etcd",
            $"выгрузка снапшотов etcd в S3 провалилась: {export.Error ?? "без причины"}",
            new Dictionary<string, string>
            {
                ["state"] = "FAILED",
                ["lastUploadedUnix"] = export.LastUploadedUnix?.ToString() ?? string.Empty,
            },
            null,
            "контроль-плейн не защищён от потери хоста — выгрузка снапшотов etcd в S3 провалилась",
            AlertRemedy.OperatorRunbook,
            "docs/runbook.md §«Восстановление etcd из S3-выгрузки (t08)»: проверь доступность S3 (PGW_BACKUP_S3_*) и статус-ключ /pgworker/etcd-snapshots; воркер доводит выгрузку тиком RetryIntervalSec");
    }
}
