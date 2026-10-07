namespace AdminPanel.Core;

using AdminPanel.Core.Alerting.Rules;

// Грань «Надёжность» (arch/adminpanel/03 §1/§3): RPO/RTO-числа per-cluster/
// per-shard — ЧИСТАЯ функция над etcd-снапшотом (spec §3.4; по образцу
// AlertEngine-правил). Без etcd/IO/часов: nowUtc приходит параметром.

/// <summary>Источник сводного RPO-потенциала: wal — хвост WAL-потока,
/// full — последний валидный полный, off — подсистема бэкапов не включена.</summary>
public enum RpoMode { Wal, Full, Off }

public sealed record ClusterReliability(string Cluster, IReadOnlyList<ShardReliability> Shards);

public sealed record ShardReliability(string Shard, bool Declared, RpoBlock? Rpo, RtoBlock? Rto);

/// <summary>RPO-блок шарда: возраст валидного полного + id, лаг/возраст WAL,
/// сводный «потеряем ≈ N» и порог индикации (Policy.FullMaxAgeSec ?? 86400 —
/// дефолт BackupFullStaleRule).</summary>
public sealed record RpoBlock(
    RpoMode Mode, long? FullAgeSec, string? FullId, long? WalLagSegments, long? WalAgeSec,
    long? RpoPotentialSec, long ThresholdFullAgeSec);

/// <summary>HA-факт надзора как RTO-число: закрытый — DurationSec; открытый —
/// Ongoing с тикающей OngoingSec = now − DetectedUnix.</summary>
public sealed record HaFactRto(
    string Shard, string Node, string Cause, long DetectedUnix, long? ResolvedUnix,
    long? DurationSec, bool Ongoing, long OngoingSec);

/// <summary>Длительность операционной заявки (drill/restore): терминальная —
/// DurationSec; активная — Ongoing от старта (restore — от ЗАЯВКИ, включая
/// очередь: честный операторский RTO).</summary>
public sealed record OpRto(string State, long? DurationSec, long? OngoingSec, long? FinishedUnix, string? Error);

public sealed record RtoBlock(HaFactRto? LastFailover, HaFactRto? LastRebuild, OpRto? LastDrill, OpRto? LastRestore);

public static class ReliabilityCalculator
{
    // Шарды таблицы — живая декларация кластера; факт/бэкап-данные шарда вне
    // декларации не отображаются (R10). Пустые секции = подсистема выключена /
    // надзор старой версии (толерантность).
    public static IReadOnlyList<ClusterReliability> Calculate(EtcdSnapshot snapshot, DateTimeOffset nowUtc)
    {
        var now = nowUtc.ToUnixTimeSeconds();
        var list = new List<ClusterReliability>();
        foreach (var cluster in snapshot.Clusters)
        {
            var backups = snapshot.Backups.FirstOrDefault(b => b.Cluster == cluster.Name);
            var work = snapshot.PgWorkerWork.FirstOrDefault(w => w.Cluster == cluster.Name);
            var shards = new List<ShardReliability>();
            foreach (var shard in cluster.Shards)
                shards.Add(new ShardReliability(shard.Name, Declared: true,
                    Rpo: RpoOf(backups, shard.Name, now),
                    Rto: RtoOf(backups, work, shard.Name, now)));
            list.Add(new ClusterReliability(cluster.Name, shards));
        }

        return list;
    }

    // RPO-блок: кластера нет в Backups / шарда нет в ShardLastCompletedUnix —
    // Mode=Off (подсистема не включена, секция молчит).
    private static RpoBlock? RpoOf(ClusterBackupsInfo? backups, string shard, long now)
    {
        if (backups is null || !backups.ShardLastCompletedUnix.TryGetValue(shard, out var lastCompleted))
            return new RpoBlock(RpoMode.Off, null, null, null, null, null, ThresholdOf(backups));

        long? fullAgeSec = lastCompleted is { } completed ? Math.Max(0, now - completed) : null;
        long? walAgeSec = null;
        long? walLag = null;
        RpoMode mode;
        long? potential;
        if (backups.Shards is { } shards && shards.TryGetValue(shard, out var wal) && wal is not null)
        {
            walAgeSec = Math.Max(0, now - wal.LastUploadedUnix);
            walLag = wal.LagSegments;
        }

        if (backups.Shards is { } shardsMap
            && shardsMap.TryGetValue(shard, out var walInfo)
            && walInfo is { }
            && walInfo.State is WalStreamInfoState.Active or WalStreamInfoState.Degraded)
        {
            mode = RpoMode.Wal;
            potential = walAgeSec;
        }
        else
        {
            // BROKEN/STOPPED или wal-ключа нет — потеряем хвост после последнего
            // валидного полного (полного нет — «никогда», числа нет).
            mode = RpoMode.Full;
            potential = fullAgeSec;
        }

        return new RpoBlock(mode, fullAgeSec, ValidFullIdOf(backups, shard),
            walLag, walAgeSec, potential, ThresholdOf(backups));
    }

    // Свежесть/источник — последний ВАЛИДНЫЙ полный (COMPLETED, verify ≠ FAILED):
    // max FinishedUnix среди валидных; FAILED новее — не свежесть.
    private static string? ValidFullIdOf(ClusterBackupsInfo backups, string shard)
    {
        if (backups.ShardsFulls is not { } fulls || !fulls.TryGetValue(shard, out var list))
            return null;
        return list
            .Where(f => f.State == "COMPLETED" && f.VerifyState != "FAILED")
            .OrderByDescending(f => f.FinishedUnix ?? 0)
            .FirstOrDefault()?.Id;
    }

    // Порог индикации — из ПОЛНОЙ policy кластера (policy-ключ); policy нет —
    // каталожный дефолт правила BackupFullStaleRule (конфиг-фолбэка у
    // калькулятора нет — он не читает панельный AlertsOptions).
    private static long ThresholdOf(ClusterBackupsInfo? backups)
        => backups?.Policy?.FullMaxAgeSec ?? BackupFullStaleRule.DefaultMaxAgeSec;

    // RTO-блок: факты надзора из work-ключа + drill/restore из бэкап-снапшота.
    private static RtoBlock? RtoOf(ClusterBackupsInfo? backups, WorkJournalInfo? work, string shard, long now)
    {
        var drill = backups?.ShardsDrills is { } drills && drills.TryGetValue(shard, out var d)
            ? DrillRto(d, now)
            : null;
        var restore = backups?.ShardsRestores is { } restores && restores.TryGetValue(shard, out var list)
            ? RestoreRto(list, now)
            : null;
        var block = new RtoBlock(
            FactRto(work?.LastFailover, shard, now),
            FactRto(work?.LastRebuild, shard, now),
            drill,
            restore);
        return block;
    }

    // Факт чужого шарда не отображается (работа — по шарду таблицы, R10):
    // открытый — Ongoing с тикающей длительностью; закрытый — DurationSec.
    private static HaFactRto? FactRto(HaSupervisionInfo? fact, string shard, long now)
    {
        if (fact is null || fact.Shard != shard)
            return null;
        if (fact.ResolvedUnix is null)
            return new HaFactRto(fact.Shard, fact.Node, fact.Cause, fact.DetectedUnix, null, null,
                Ongoing: true, OngoingSec: Math.Max(0, now - fact.DetectedUnix));
        return new HaFactRto(fact.Shard, fact.Node, fact.Cause, fact.DetectedUnix,
            fact.ResolvedUnix, fact.DurationSec, Ongoing: false, OngoingSec: 0);
    }

    // Drill: RUNNING — ongoing от StartedUnix, иначе FinishedUnix − StartedUnix.
    private static OpRto DrillRto(DrillInfo drill, long now)
        => drill.State == "RUNNING"
            ? new OpRto(drill.State, null, OngoingSec: Math.Max(0, now - drill.StartedUnix),
                drill.FinishedUnix, drill.Error)
            : new OpRto(drill.State,
                DurationSec: drill.FinishedUnix is { } fin ? Math.Max(0, fin - drill.StartedUnix) : null,
                OngoingSec: null, drill.FinishedUnix, drill.Error);

    // Restore: последняя заявка по max RequestedUnix; активная — ongoing от
    // ЗАЯВКИ (включая очередь), терминальная — FinishedUnix − RequestedUnix.
    private static OpRto? RestoreRto(IReadOnlyList<RestoreOperationInfo> list, long now)
    {
        if (list.Count == 0)
            return null;
        var restore = list.OrderByDescending(r => r.RequestedUnix).First();
        return restore.State is "PLANNED" or "RUNNING" or "REJOINING"
            ? new OpRto(restore.State, null, OngoingSec: Math.Max(0, now - restore.RequestedUnix),
                restore.FinishedUnix, restore.Error)
            : new OpRto(restore.State,
                DurationSec: restore.FinishedUnix is { } fin ? Math.Max(0, fin - restore.RequestedUnix) : null,
                OngoingSec: null, restore.FinishedUnix, restore.Error);
    }
}
