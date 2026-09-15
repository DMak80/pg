using AdminPanel.Core.Alerting;
using Shared.Core.DI;

namespace AdminPanel.Core.Alerting.Rules;

// backup-verify-failed (critical, t04): в статусе любого COMPLETED-полного
// шарда verify.state=FAILED — полный невалиден (checksums/цепочка); текст —
// verify.error; воркер переснимает автоматически (свежесть не считает).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class BackupVerifyFailedRule : IAlertRule
{
    public const string KindName = "backup-verify-failed";
    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var cluster in snapshot.Backups)
        foreach (var (shard, failure) in cluster.ShardVerifyFailures ?? new Dictionary<string, ShardVerifyFailure>())
            yield return new Alert(
                $"{KindName}:{cluster.Cluster}/{shard}",
                AlertSeverity.Critical,
                KindName,
                $"{cluster.Cluster}/{shard}",
                $"полный бэкап {failure.Id} шарда {shard} кластера {cluster.Cluster} невалиден: {failure.Error}",
                new Dictionary<string, string>
                {
                    ["backupId"] = failure.Id,
                    ["checkedUnix"] = failure.CheckedUnix?.ToString() ?? string.Empty,
                },
                null,
                "проверка полного бэкапа провалена (pg_verifybackup/WAL-цепочка) — восстановимость под угрозой",
                AlertRemedy.OperatorRunbook,
                "воркер переснимает полный автоматически (verify FAILED не считается свежестью); для разбора — runbook t05; удаление битого — t06");
    }
}
