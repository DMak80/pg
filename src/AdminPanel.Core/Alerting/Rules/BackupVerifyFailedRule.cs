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
                $"full backup {failure.Id} of shard {shard} of cluster {cluster.Cluster} is invalid: {failure.Error}",
                new Dictionary<string, string>
                {
                    ["backupId"] = failure.Id,
                    ["checkedUnix"] = failure.CheckedUnix?.ToString() ?? string.Empty,
                },
                null,
                "full backup verification failed (pg_verifybackup/WAL chain): recoverability at risk",
                AlertRemedy.OperatorRunbook,
                "worker re-takes the full backup automatically (verify FAILED is not counted as freshness); for analysis see runbook t05; deletion of the broken backup is t06");
    }
}
