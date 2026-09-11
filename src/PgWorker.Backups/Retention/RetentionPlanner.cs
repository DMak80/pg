using System.Globalization;
using PgWorker.Etcd.Parsing;

namespace PgWorker.Backups;

/// <summary>Итог GFS-отбора (t06, arch/19 §4): Keep — id оставляемых COMPLETED,
/// Delete — кандидаты на удаление в порядке исполнения (verify-FAILED первыми,
/// далее по возрастанию started_unix — место освобождается от самого старого).</summary>
public sealed record RetentionSelection(
    IReadOnlySet<string> Keep,
    IReadOnlyList<string> Delete);

/// <summary>Чистые функции ретенции (t06, arch/19 §4/§5): без I/O, момент —
/// аргументом (TimeProvider не нужен). Полное юнит-покрытие — риск «удалили
/// нужное» закрывается тестами детерминированности (spec §2 п.5).</summary>
public static partial class RetentionPlanner
{
    /// <summary>GFS-отбор по календарю UTC (spec §3.1): дневные — все COMPLETED
    /// последних retention.days календарных суток (включая текущую); недельные —
    /// последний COMPLETED каждой из retention.weeks свежейших предыдущих
    /// ISO-недель; месячные — аналогично по календарным месяцам. Активные
    /// (PLANNED/RUNNING/UPLOADING) и DELETING в отборе не участвуют. Guard:
    /// самый свежий COMPLETED — всегда в Keep; если после отбора Delete непусто,
    /// а Keep пуст — старейший кандидат возвращается в Keep.</summary>
    public static RetentionSelection SelectKeep(
        IReadOnlyList<FullBackupState> fulls, BackupPolicy policy, long nowUnix)
    {
        var now = DateTimeOffset.FromUnixTimeSeconds(nowUnix).UtcDateTime;
        var completed = fulls
            .Where(f => f.State == FullBackupStatus.Completed)
            .OrderBy(f => f.StartedUnix)
            .ToList();

        var keep = new HashSet<string>();

        // Дневные: все COMPLETED последних retention.days календарных суток UTC
        // (окно [today − (days−1) .. today] по дате started_unix).
        var dayCutoff = DateOnly.FromDateTime(now).AddDays(-(policy.RetentionDays - 1));
        var dailyWindow = completed
            .Where(f => DateOnly.FromDateTime(
                DateTimeOffset.FromUnixTimeSeconds(f.StartedUnix).UtcDateTime) >= dayCutoff)
            .ToList();
        foreach (var f in dailyWindow)
            keep.Add(f.Id);

        // Недельные: полные вне дневного окна группируются по ISO-неделе UTC;
        // из каждой из retention.weeks свежейших групп — последний (max started_unix).
        AddCalendarPoint(completed, dailyWindow, keep, policy.RetentionWeeks, ISOWeekYearOf);

        // Месячные: аналогично по календарному месяцу UTC.
        AddCalendarPoint(completed, dailyWindow, keep, policy.RetentionMonths, MonthOf);

        var freshest = completed.MaxBy(f => f.StartedUnix);
        if (freshest is not null)
            keep.Add(freshest.Id); // guard: самый свежий COMPLETED — всегда

        var delete = completed
            .Where(f => !keep.Contains(f.Id))
            .OrderBy(f => f.Verify is { State: BackupVerifyStatus.Failed } ? 0 : 1)
            .ThenBy(f => f.StartedUnix)
            .Select(f => f.Id)
            .ToList();

        // Guard-доводка: удалять нечего, если не остаётся ни одного COMPLETED
        // (последний валидный не удаляется — spec §3.1, AC2).
        if (delete.Count > 0 && keep.Count == 0)
        {
            var rescued = delete[0];
            keep.Add(rescued);
            delete.RemoveAt(0);
        }

        return new RetentionSelection(keep, delete);
    }

    // Группа ISO-недели: (ISO-week-year, номер недели). Год — ОБЯЗАТЕЛЬНО
    // ISOWeek.GetYear, НЕ календарный d.Year: дни на стыке календарных годов
    // одной ISO-недели (2024-12-30 и 2025-01-05 — обе ISO-неделя 1 ISO-2025;
    // 2027-01-01..03 — ISO-неделя 53 ISO-2026) при календарном годе рвутся на
    // ДВЕ группы — расщеплённая неделя расходует два слота retention.weeks,
    // реальная недельная точка вытесняется и удаляется (замечание ревью Ф4 №1).
    private static (int Year, int Week) ISOWeekYearOf(FullBackupState f)
    {
        var d = DateTimeOffset.FromUnixTimeSeconds(f.StartedUnix).UtcDateTime;
        return (ISOWeek.GetYear(d), ISOWeek.GetWeekOfYear(d));
    }

    private static (int Year, int Month) MonthOf(FullBackupState f)
    {
        var d = DateTimeOffset.FromUnixTimeSeconds(f.StartedUnix).UtcDateTime;
        return (d.Year, d.Month);
    }

    // Универсальная гранула (недели/месяцы): вне дневного окна → группы по
    // календарному периоду UTC → N свежайших групп (сравнение (Year, Num)
    // лексикографически) → из каждой последний COMPLETED (max started_unix).
    // counter = 0 → гранула исключена (spec AC1: weeks=0/months=0).
    private static void AddCalendarPoint<TGroup>(
        List<FullBackupState> completed, List<FullBackupState> dailyWindow,
        HashSet<string> keep, int counter,
        Func<FullBackupState, TGroup> groupOf)
        where TGroup : IComparable<TGroup>
    {
        if (counter <= 0)
            return;

        var dailyIds = dailyWindow.Select(f => f.Id).ToHashSet();
        var outsideDaily = completed
            .Where(f => !dailyIds.Contains(f.Id))
            .GroupBy(groupOf)
            .OrderByDescending(g => g.Key)
            .Take(counter);
        foreach (var group in outsideDaily)
        {
            var last = group.MaxBy(f => f.StartedUnix); // ПОСЛЕДНИЙ периода (AC1)
            if (last is not null)
                keep.Add(last.Id);
        }
    }
}
