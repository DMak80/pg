using AdminPanel.Core.Alerting;
using Shared.Core.DI;
using Microsoft.Extensions.Options;

namespace AdminPanel.Core.Alerting.Rules;

// backup-full-stale (critical, t02): возраст последнего COMPLETED полного
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
                    ? $"последний полный бэкап шарда {shard} кластера {cluster.Cluster} старше {nowUnix - stale} c — порог {maxAge} c"
                    : $"полный бэкап шарда {shard} кластера {cluster.Cluster} никогда не завершался (нет COMPLETED)";
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
                    "суточное окно без валидного полного бэкапа: восстановимость кластера под угрозой — проверь статусы /pgworker/backups/<C>/ (FAILED-ошибки джобов) и S3-хранилище",
                    AlertRemedy.OperatorRunbook,
                    "воркер сам переснимает с бэкоффом; висит — S3/сеть/роль backup_exec");
            }
        }
    }

    private long EffectiveDefault()
    {
        var configured = options.Value.BackupFullMaxAgeSec;
        return configured > 0 ? configured : DefaultMaxAgeSec;
    }
}
