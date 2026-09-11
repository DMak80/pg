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

    // Валидный полный (t04, arch/19 §2): COMPLETED и verify ≠ FAILED
    // (null — не проверялся; PENDING — идёт; OK — проверен).
    private static bool IsValid(FullBackupState f)
        => f.State == FullBackupStatus.Completed
           && f.Verify is not { State: BackupVerifyStatus.Failed };

    // Rolling-правило (t02 + t04 + t05 §3.5): нет ВАЛИДНОГО COMPLETED — true;
    // иначе возраст последнего валидного (finished_unix; толерантно
    // started_unix) больше full_max_age_sec ИЛИ wal-ключа нет (t05: цепочка
    // сброшена restore'ом — полный переснимается немедленно, инвариант
    // «поднятый шард всегда имеет валидную цепочку или активный полный»).
    public static bool IsDue(
        IReadOnlyList<FullBackupState> fulls, bool walKeyExists, long fullMaxAgeSec, long nowUnix)
    {
        var lastValid = fulls
            .Where(IsValid)
            .OrderByDescending(f => f.FinishedUnix ?? f.StartedUnix)
            .FirstOrDefault();
        if (lastValid is null)
            return true;

        var finished = lastValid.FinishedUnix ?? lastValid.StartedUnix;
        return nowUnix - finished > fullMaxAgeSec || !walKeyExists;
    }

    // Бэкофф переснятия: n = попытки после последнего ВАЛИДНОГО — FAILED-джобы +
    // COMPLETED с verify=FAILED; окно = min(BaseSec·2^(n−1), MaxSec) от последней
    // попытки (max started_unix после последнего валидного); n = 0 → без задержки.
    public static bool BackoffPassed(
        IReadOnlyList<FullBackupState> fulls, int baseSec, int maxSec, long nowUnix)
    {
        var lastValidStarted = fulls
            .Where(IsValid)
            .Select(f => (long?)f.StartedUnix)
            .Max() ?? long.MinValue;
        var tail = fulls.Where(f => f.StartedUnix > lastValidStarted).ToList();
        var failures = tail.Count(f =>
            f.State == FullBackupStatus.Failed || f.Verify is { State: BackupVerifyStatus.Failed });
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
