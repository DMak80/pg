using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using Shared.Core.DI;

namespace AdminPanel.Core.Alerting.Rules;

// etcd-snapshot-export-stale (warning, reliability, adminpanel/03 §4):
// «выгрузка молчит» — last_uploaded_unix отсутствует или старше 2×interval_min
// (панельный дефолт 360 мин). Порог честен благодаря семантике «покрытия»
// (adminpanel/02 §2.3.1): поле двигается каждым выгруженным слепком — живой
// контур с работающей выгрузкой под порог не попадает. Выключенная опция/нет
// ключа — молчание.
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class EtcdSnapshotExportStaleRule : IAlertRule
{
    public const string KindName = "etcd-snapshot-export-stale";

    // Панельный дефолт планового интервала (мин) при отсутствии interval_min.
    public const int DefaultIntervalMin = 360;

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        var export = snapshot.EtcdSnapshots;
        if (export is not { Enabled: true })
            yield break; // выключено/ключа нет — молчим

        var intervalMin = export.IntervalMin ?? DefaultIntervalMin;
        var nowUnix = context.NowUtc.ToUnixTimeSeconds();
        if (export.LastUploadedUnix is { } uploaded && nowUnix - uploaded <= 2L * intervalMin * 60)
            yield break; // свежо

        var message = export.LastUploadedUnix is { } last
            ? $"etcd snapshot export silent for {nowUnix - last} s — threshold 2×{intervalMin} min"
            : "no successful etcd snapshot export while the option is enabled";
        yield return new Alert(
            KindName,
            AlertSeverity.Warning,
            KindName,
            "etcd",
            message,
            new Dictionary<string, string>
            {
                ["intervalMin"] = intervalMin.ToString(),
                ["lastUploadedUnix"] = export.LastUploadedUnix?.ToString() ?? string.Empty,
            },
            null,
            "export is silent — control-plane changes accumulate only in the leader's local volume (both worker instances down / eternal transient / snapshot leadership not executing)",
            AlertRemedy.OperatorRunbook,
            "check healthz of both PgWorker instances and the status key /pgworker/etcd-snapshots; S3 access — runbook");
    }
}
