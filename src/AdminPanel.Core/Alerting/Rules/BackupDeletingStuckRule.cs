using AdminPanel.Core.Alerting;
using Shared.Core.DI;
using Microsoft.Extensions.Options;

namespace AdminPanel.Core.Alerting.Rules;

// backup-deleting-stuck (warning, t06, arch/19 §4): полный в DELETING дольше
// Alerts:Backups:DeletingStaleSec (возраст по finished_unix, иначе started_unix)
// — ретенция не может довести удаление (S3-отказ и т.п.).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class BackupDeletingStuckRule(IOptions<AlertsOptions> options) : IAlertRule
{
    public const string KindName = "backup-deleting-stuck";

    // Каталожный дефолт — 6 ч; фолбэк при опечатке конфига (spec §3.11).
    public const int DefaultDeletingStaleSec = 21600;

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        var nowUnix = context.NowUtc.ToUnixTimeSeconds();
        var staleSec = options.Value.Backups.DeletingStaleSec > 0
            ? options.Value.Backups.DeletingStaleSec
            : DefaultDeletingStaleSec;
        foreach (var backups in snapshot.Backups)
        foreach (var (shard, fulls) in backups.DeletingFulls ?? new Dictionary<string, IReadOnlyList<DeletingFullInfo>>())
        {
            var stuck = fulls
                .Select(f => (Full: f, Age: nowUnix - (f.FinishedUnix ?? f.StartedUnix)))
                .Where(p => p.Age > staleSec)
                .OrderByDescending(p => p.Age)
                .FirstOrDefault();
            if (stuck.Full is null)
                continue;

            yield return new Alert(
                $"{KindName}:{backups.Cluster}/{shard}",
                AlertSeverity.Warning,
                KindName,
                $"{backups.Cluster}/{shard}",
                $"полный бэкап {stuck.Full.Id} шарда {shard} кластера {backups.Cluster} висит в DELETING {stuck.Age} c (порог {staleSec} c) — ретенция не может довести удаление",
                new Dictionary<string, string>
                {
                    ["id"] = stuck.Full.Id,
                    ["ageSeconds"] = stuck.Age.ToString(),
                    ["staleSeconds"] = staleSec.ToString(),
                },
                null, // SinceUnix — проставляет AlertEngine
                "удаление идемпотентно: каждый ретенционный проход повторяет list+delete префикса full/<id>/ — затор означает transient-отказ S3",
                AlertRemedy.WorkerAuto,
                "проверь доступность S3 (endpoint/креды/сеть) и журналы /pgworker/work/<C> (phase=backups-retention) — повторные проходы доведут удаление сами");
        }
    }
}
