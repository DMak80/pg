using System.Globalization;
using PgWorker.Etcd.Parsing;

namespace PgWorker.Backups;

/// <summary>Итог GFS-отбора (t06, arch/19 §4): Keep — id оставляемых COMPLETED,
/// Delete — кандидаты на удаление в порядке исполнения (verify-FAILED первыми,
/// далее по возрастанию started_unix — место освобождается от самого старого).</summary>
public sealed record RetentionSelection(
    IReadOnlySet<string> Keep,
    IReadOnlyList<string> Delete);

/// <summary>Вердикт занятости хранилища: OK/WARN/CRIT относительно квоты (t06).</summary>
public enum StorageState
{
    Ok,
    Warn,
    Crit,
}

/// <summary>Результат EvaluateStorage: state + фактические числа; UsedPercent —
/// null при незаданной квоте.</summary>
public sealed record StorageVerdict(
    StorageState State, long UsedBytes, long QuotaBytes, double? UsedPercent);

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

    /// <summary>План чистки WAL (spec §3.1): сегменты строго ниже cutoff (TLI
    /// ниже ИЛИ тот же TLI с позицией log·256+seg ниже cutoff) и `.history`
    /// TLI ниже стартового — на удаление. Сам cutoff, сегменты выше/новее,
    /// `.history` cutoff-TLI и новее, `.partial` и нераспознаваемые имена —
    /// НЕ входят (консервативно: объект TLI&gt;cutoff позицией ниже cutoff не
    /// трогаем — контроль t03 его игнорирует, удалять незачем).</summary>
    public static IReadOnlyList<string> SelectWalForDeletion(
        IReadOnlyList<string> objectNames, WalFileName cutoff)
    {
        var result = new List<string>();
        foreach (var name in objectNames)
        {
            if (WalFileName.TryParse(name) is { } segment)
            {
                var below = segment.Tli < cutoff.Tli
                    || (segment.Tli == cutoff.Tli
                        && (long)(segment.Log * WalFileName.SegsPerLog + segment.Seg)
                           < (long)(cutoff.Log * WalFileName.SegsPerLog + cutoff.Seg));
                if (below)
                    result.Add(name);
            }
            else if (WalFileName.TryParseHistory(name) is { } tli && tli < cutoff.Tli)
                result.Add(name); // .history старых TLI — сегменты их диапазонов удалены
        }

        return result;
    }

    /// <summary>Вердикт занятости (spec §3.1): квота 0/не задана → OK без
    /// процентов; иначе usedPercent &gt;= crit → CRIT, &gt;= warn → WARN, иначе OK.</summary>
    public static StorageVerdict EvaluateStorage(long usedBytes, long quotaBytes, int warnPercent, int critPercent)
    {
        if (quotaBytes <= 0)
            return new StorageVerdict(StorageState.Ok, usedBytes, 0, null);

        var percent = Math.Round(usedBytes * 100.0 / quotaBytes, 2);
        var state = percent >= critPercent ? StorageState.Crit
            : percent >= warnPercent ? StorageState.Warn
            : StorageState.Ok;
        return new StorageVerdict(state, usedBytes, quotaBytes, percent);
    }

    /// <summary>Гигиена FAILED-истории (spec §3.3 п.5): держать последние
    /// keepFailed по started_unix, старше — del. Бэкофф t02 не ломается: n
    /// остаётся ≤ keepFailed, а BaseSec·2^(n−1) упирается в MaxSec раньше
    /// границы (AC5).</summary>
    public static IReadOnlyList<string> SelectFailedForPrune(
        IReadOnlyList<FullBackupState> fulls, int keepFailed)
        => fulls
            .Where(f => f.State == FullBackupStatus.Failed)
            .OrderByDescending(f => f.StartedUnix)
            .Skip(Math.Max(0, keepFailed))
            .OrderBy(f => f.StartedUnix) // старейшие вперёд (порядок исполнения)
            .Select(f => f.Id)
            .ToList();

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
