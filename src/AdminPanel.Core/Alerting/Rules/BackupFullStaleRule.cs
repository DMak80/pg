using AdminPanel.Core.Alerting;
using Shared.Core.DI;
using Microsoft.Extensions.Options;

namespace AdminPanel.Core.Alerting.Rules;

// backup-full-stale (critical): возраст последнего COMPLETED полного
// шарда > full_max_age_sec политики кластера (нет policy — панельный дефолт);
// COMPLETED нет вовсе — «полного никогда не было»; пустой префикс кластера
// (подсистема не включена) — молчим (arch/19 §4). Воркер алерты не пишет.
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class BackupFullStaleRule(IOptions<AlertsOptions> options) : IAlertRule
{
    public const string KindName = "backup-full-stale";

    // Каталожный дефолт — суточное окно; фолбэк при опечатке конфига (spec §3.11).
    public const long DefaultMaxAgeSec = 86400;

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        var nowUnix = context.NowUtc.ToUnixTimeSeconds();
        foreach (var cluster in snapshot.Backups)
        {
            var maxAge = cluster.FullMaxAgeSec is > 0 ? cluster.FullMaxAgeSec.Value : EffectiveDefault();
            foreach (var (shard, lastCompleted) in cluster.ShardLastCompletedUnix)
            {
                if (lastCompleted is { } finished && nowUnix - finished <= maxAge)
                    continue; // свежий полный — ок

                var description = lastCompleted is { } stale
                    ? $"last full backup of shard {shard} in cluster {cluster.Cluster} is {nowUnix - stale} s old — threshold {maxAge} s"
                    : $"full backup of shard {shard} in cluster {cluster.Cluster} never completed (no COMPLETED)";
                yield return new Alert(
                    $"{KindName}:{cluster.Cluster}/{shard}",
                    AlertSeverity.Critical,
                    KindName,
                    $"{cluster.Cluster}/{shard}",
                    description,
                    new Dictionary<string, string>
                    {
                        ["maxAgeSeconds"] = maxAge.ToString(),
                        ["lastCompletedUnix"] = lastCompleted?.ToString() ?? string.Empty,
                    },
                    null,
                    "daily window without a valid full backup: cluster recoverability at risk — check /pgworker/backups/<C>/ statuses (FAILED job errors) and S3 storage",
                    AlertRemedy.OperatorRunbook,
                    "worker retries automatically with backoff; if stuck — S3/network/backup_exec role");
            }
        }
    }

    private long EffectiveDefault()
    {
        var configured = options.Value.BackupFullMaxAgeSec;
        return configured > 0 ? configured : DefaultMaxAgeSec;
    }
}
