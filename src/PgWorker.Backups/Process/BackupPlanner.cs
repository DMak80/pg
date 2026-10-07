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

    // Unix последнего ВАЛИДНОГО полного (finished_unix, толерантно started_unix —
    // образец IsDue); null — валидного нет. Источник возрастной серии
    // pgworker_backup_full_age_seconds (t14, arch/18 §2.7): метрика не вводит
    // третьего толкования «валидный».
    public static long? LastValidUnix(IReadOnlyList<FullBackupState> fulls)
    {
        var last = fulls
            .Where(IsValid)
            .OrderByDescending(f => f.FinishedUnix ?? f.StartedUnix)
            .FirstOrDefault();
        return last is null ? null : last.FinishedUnix ?? last.StartedUnix;
    }

    // Rolling-правило (t02 + t04 + t05 §3.5 + t07 §3.2 + t27): нет ВАЛИДНОГО
    // COMPLETED — true; иначе возраст последнего валидного (finished_unix;
    // толерантно started_unix) больше full_max_age_sec. Инвариант «поднятый
    // шард всегда имеет валидную цепочку или свежий полный» (arch/19 §2):
    // цепочку заведёт контроль (§3) от wal_start_segment свежего COMPLETED,
    // когда агенты донесут ПЕРВЫЙ ЗАКРЫТЫЙ сегмент — приёмник (t27) выгружает
    // только закрытые сегменты, поэтому на шарде без нагрузки ключ wal
    // появляется позже первого полного; прежний сигнал «ключа нет — пересъём»
    // в этом окне давал серию полных подряд (t27 E2E-факт: 4 COMPLETED за 26 с)
    // без пользы. Полный также обязан быть НОВЕЕ последней COMPLETED-restore
    // шарда: restore перестраивает шард из бэкапа — до пересъёма «валидный»
    // полный описывает прежнюю жизнь шарда (сигнал — факт restore, не ключ:
    // WalStream восстанавливает wal-ключ немедленно — инцидент E2E-гейта t05,
    // 2026-09-13).
    public static bool IsDue(
        IReadOnlyList<FullBackupState> fulls, long fullMaxAgeSec, long nowUnix,
        long? lastRestoreFinishedUnix = null, bool walChainBroken = false)
    {
        // t07 (arch/19 §2): BROKEN wal-ключа — пересъём безусловно (разрыв лечит
        // только новый полный); бэкофф серии FAILED применяется вызывающим как
        // всегда — шторм пересъёмов исключён.
        if (walChainBroken)
            return true;

        var lastValidUnix = LastValidUnix(fulls);
        if (lastValidUnix is null)
            return true;

        if (lastRestoreFinishedUnix is { } restored && lastValidUnix <= restored)
            return true;

        return nowUnix - lastValidUnix.Value > fullMaxAgeSec;
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
