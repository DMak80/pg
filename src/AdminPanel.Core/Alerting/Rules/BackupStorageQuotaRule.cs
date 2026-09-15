using AdminPanel.Core.Alerting;
using Shared.Core.DI;

namespace AdminPanel.Core.Alerting.Rules;

// backup-storage-quota (t06, arch/19 §4): занятость bucket по state глобального
// ключа /pgworker/backups/storage; ключа нет → молчим (подсистема не включена).
// Реакции автоматикой нет — действует оператор (расширить квоту/ужать ретенцию).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class BackupStorageQuotaRule : IAlertRule
{
    public const string KindName = "backup-storage-quota";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        var storage = snapshot.BackupStorage;
        if (storage is null)
            yield break; // ключа нет — подсистема не включена

        if (storage.State is not (BackupStorageState.Warn or BackupStorageState.Crit))
            yield break; // OK — молчим

        var severity = storage.State == BackupStorageState.Crit
            ? AlertSeverity.Critical
            : AlertSeverity.Warning;
        var percent = storage.UsedPercent is { } p ? $"{p:0.##}%" : "н/д";
        var quota = storage.QuotaBytes is { } q ? $"{q} б" : "не задана";
        yield return new Alert(
            $"{KindName}:storage",
            severity,
            KindName,
            "backups-storage",
            $"хранилище бэкапов: занято {storage.UsedBytes} б из квоты {quota} ({percent}) — состояние {storage.State}",
            new Dictionary<string, string>
            {
                ["usedBytes"] = storage.UsedBytes.ToString(),
                ["quotaBytes"] = storage.QuotaBytes?.ToString() ?? string.Empty,
                ["usedPercent"] = storage.UsedPercent?.ToString() ?? string.Empty,
                ["state"] = storage.State.ToString(),
            },
            null, // SinceUnix — проставляет AlertEngine
            "ключ пишет ретенционный проход PgWorker по расписанию Retention:IntervalSec (list всего bucket); занятость — факт, реакции автоматикой нет",
            AlertRemedy.OperatorRunbook,
            "расширь квоту (PgWorker:Backups:Quota:Bytes) или ужать политику ретенции кластера (POST /api/clusters/<C>/backups/policy)");
    }
}
