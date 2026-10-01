using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using Shared.Core.DI;

namespace AdminPanel.Core.Alerting.Rules;

// backup-drill-stale (warning, reliability t02, arch/19 §3.6): у шарда
// Active-кластера есть COMPLETED-полные и интервал дрилов включён
// (policy.drill.interval_days, нет — панельный дефолт 1, канон arch/19 §9),
// а успешного дрилла нет вовсе либо он старше 2×интервала — «восстановимость
// не доказывается» (дриллы молча не исполняются: вечный transient, гонки
// выключения). Пустой префикс бэкапов кластера (нет COMPLETED) — правило
// молчит (подсистема не включена — образец backup-full-stale). Последний
// FAILED не «освежает»: молчание — только при свежем SUCCEEDED.
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class BackupDrillStaleRule : IAlertRule
{
    public const string KindName = "backup-drill-stale";

    // Панельный дефолт интервала — канон arch/19 §9 (Drill:IntervalDays=1).
    public const int DefaultIntervalDays = 1;

    private const long DaySec = 86400;

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        var nowUnix = context.NowUtc.ToUnixTimeSeconds();
        foreach (var backups in snapshot.Backups)
        {
            var intervalDays = backups.Policy?.DrillIntervalDays ?? DefaultIntervalDays;
            if (intervalDays <= 0)
                continue; // выключено — молчим (0 — легитимное отключение дрилов)

            foreach (var (shard, lastCompleted) in backups.ShardLastCompletedUnix)
            {
                // пустой префикс (нет COMPLETED-полных) — подсистема не включена.
                if (lastCompleted is null)
                    continue;

                // только живой Active-кластер: демонтаж удаляет ключи бэкапов сам.
                var cluster = snapshot.Clusters.FirstOrDefault(
                    c => c.Name == backups.Cluster && c.State == ClusterState.Active
                         && c.Shards.Any(s => s.Name == shard));
                if (cluster is null)
                    continue;

                // Успешного дрилла нет → 0 («горит при наличии полных»);
                // FAILED/RUNNING/cleaning не считаются успехом.
                var lastSuccess = backups.ShardsDrills?.TryGetValue(shard, out var drill) == true
                                  && drill is { State: "SUCCEEDED", FinishedUnix: { } finished }
                    ? finished
                    : 0;
                if (nowUnix - lastSuccess <= 2L * intervalDays * DaySec)
                    continue; // свежий успех (или в пределах 2×интервала) — ок

                var description = lastSuccess == 0
                    ? $"у шарда {shard} кластера {backups.Cluster} нет успешного дрилла при включённом интервале {intervalDays} сут"
                    : $"успешный дрилл шарда {shard} кластера {backups.Cluster} старше {nowUnix - lastSuccess} c — порог 2×{intervalDays} сут";
                yield return new Alert(
                    $"{KindName}:{backups.Cluster}/{shard}",
                    AlertSeverity.Warning,
                    KindName,
                    $"{backups.Cluster}/{shard}",
                    description,
                    new Dictionary<string, string>
                    {
                        ["intervalDays"] = intervalDays.ToString(),
                        ["lastSuccessUnix"] = lastSuccess == 0 ? string.Empty : lastSuccess.ToString(),
                    },
                    null,
                    "восстановимость не доказывается — дриллы молча не исполняются (вечный transient/выключение)",
                    AlertRemedy.OperatorRunbook,
                    "проверь ключ /pgworker/backups/<C>/<X>/drill и журнал backup-drill; последний FAILED не «освежает» — молчание только при свежем SUCCEEDED");
            }
        }
    }
}
