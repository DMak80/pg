using System.Globalization;
using PgWorker.Etcd.Parsing;

namespace PgWorker.Backups;

// Чистые решения планировщика полных бэкапов (arch/19 §2, t02): без I/O,
// юнит-тестируемы. Записи шарда отсортированы по Id (= время, парсер t01).
public static class BackupPlanner
{
    public static bool HasActive(IReadOnlyList<FullBackupState> fulls)
        => fulls.Any(f => f.State
            is FullBackupStatus.Planned or FullBackupStatus.Running or FullBackupStatus.Uploading);

    // Rolling-правило (t02): нет COMPLETED ИЛИ возраст последнего COMPLETED
    // (finished_unix; толерантно started_unix) больше full_max_age_sec.
    public static bool IsDue(IReadOnlyList<FullBackupState> fulls, long fullMaxAgeSec, long nowUnix)
    {
        var lastCompleted = fulls
            .Where(f => f.State == FullBackupStatus.Completed)
            .OrderByDescending(f => f.StartedUnix)
            .FirstOrDefault();
        if (lastCompleted is null)
            return true;

        var finished = lastCompleted.FinishedUnix ?? lastCompleted.StartedUnix;
        return nowUnix - finished > fullMaxAgeSec;
    }

    // Бэкофф переснятия FAILED: n = FAILED с последнего COMPLETED; окно =
    // min(BaseSec·2^(n−1), MaxSec) от последней попытки (max started_unix
    // после последнего COMPLETED); n = 0 → без задержки.
    public static bool BackoffPassed(
        IReadOnlyList<FullBackupState> fulls, int baseSec, int maxSec, long nowUnix)
    {
        var lastCompletedStarted = fulls
            .Where(f => f.State == FullBackupStatus.Completed)
            .Select(f => (long?)f.StartedUnix)
            .Max() ?? long.MinValue;
        var tail = fulls.Where(f => f.StartedUnix > lastCompletedStarted).ToList();
        var failures = tail.Count(f => f.State == FullBackupStatus.Failed);
        if (failures == 0)
            return true;

        var lastAttempt = tail.Max(f => f.StartedUnix);
        var delaySec = Math.Min((long)baseSec << Math.Min(failures - 1, 30), maxSec);
        return nowUnix >= lastAttempt + delaySec;
    }

    // id = YYYYMMDDHHMMSSZ UTC (сортируемый); коллизия в пределах шарда → -2/-3…
    public static string NextId(IEnumerable<string> existingIds, DateTime nowUtc)
    {
        var baseId = nowUtc.ToString("yyyyMMddHHmmss", CultureInfo.InvariantCulture) + "Z";
        if (!existingIds.Contains(baseId))
            return baseId;

        for (var suffix = 2; ; suffix++)
        {
            var candidate = $"{baseId}-{suffix.ToString(CultureInfo.InvariantCulture)}";
            if (!existingIds.Contains(candidate))
                return candidate;
        }
    }
}
