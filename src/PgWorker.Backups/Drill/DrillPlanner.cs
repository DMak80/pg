using PgWorker.Core.Model;
using PgWorker.Etcd.Parsing;

namespace PgWorker.Backups.Drill;

/// <summary>Чистый отбор кандидата дрилла (reliability t02, arch/19 §3.6):
/// один шард за проход кластера — наименее свежий по последнему терминальному
/// дриллу («никогда» = 0 — обслуживается первым). Гварды: интервал ≤ 0 — выкл;
/// незавершённый дрилл кластера (RUNNING либо терминальный с phase=cleaning)
/// блокирует новые; restore-заявка владеет ЖЦ шарда; ToRemove/без
/// COMPLETED-полных — не кандидаты. Без I/O — юнит-тестируемо.</summary>
public static class DrillPlanner
{
    public const long DaySec = 86400;

    public static string? SelectCandidate(
        IReadOnlyList<ShardSpec> shards,
        IReadOnlyDictionary<string, ShardBackups> backups,
        IReadOnlyDictionary<string, DrillState> drills,
        int intervalDays,
        long nowUnix)
    {
        // 1) интервал выключен / любой незавершённый дрилл кластера → null.
        if (intervalDays <= 0)
            return null;
        foreach (var drill in drills.Values)
        {
            var unfinished = drill.State == DrillStatus.Running
                || (drill.State is DrillStatus.Succeeded or DrillStatus.Failed
                    && drill.Phase == "cleaning");
            if (unfinished)
                return null;
        }

        string? best = null;
        var bestFinished = long.MaxValue;
        foreach (var shard in shards)
        {
            // 2) кандидаты: !ToRemove; есть COMPLETED-полный; нет активной restore.
            if (shard.ToRemove)
                continue;
            if (!backups.TryGetValue(shard.Name, out var sb)
                || !sb.Full.Any(f => f.State == FullBackupStatus.Completed))
                continue;
            if (sb.Restores.Any(r => r.State is RestoreStatus.Planned
                or RestoreStatus.Running or RestoreStatus.Rejoining))
                continue;

            // 3) готовность: drills[X] нет → готов (last=0); терминальный без
            // cleaning → готов iff now − (FinishedUnix ?? 0) ≥ intervalDays×DaySec.
            long lastFinished = 0;
            if (drills.TryGetValue(shard.Name, out var drill)
                && drill.State is DrillStatus.Succeeded or DrillStatus.Failed)
                lastFinished = drill.FinishedUnix ?? 0;
            if (lastFinished > 0 && nowUnix - lastFinished < intervalDays * DaySec)
                continue;

            // 4) выбор: MIN lastFinished, tie-break по имени (Ordinal) — детерминизм.
            if (lastFinished < bestFinished
                || (lastFinished == bestFinished
                    && string.CompareOrdinal(shard.Name, best) < 0))
            {
                best = shard.Name;
                bestFinished = lastFinished;
            }
        }

        return best;
    }
}
