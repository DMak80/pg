using PgWorker.Etcd.Parsing;

namespace PgWorker.Backups;

/// <summary>Чистые решения бюджетов зависших джобов (t07, arch/19 §6): отбор
/// кандидатов таймаута по статусу/возрасту — без I/O, юнит-тестируемо.
/// Применение — супервизия существующих процессов (BackupProcess/
/// BackupVerifyProcess/RestoreProcess): kill+rm + FAILED/квота попытки по
/// общим правилам arch/17 (transient-транспорт — статус не меняем).</summary>
public static class SupervisionTimeouts
{
    /// <summary>Возраст джоба (now − started) превысил бюджет. Строго больше —
    /// пограничный тик не карает.</summary>
    public static bool IsTimedOut(long startedUnix, long nowUnix, long timeoutSec)
        => nowUnix - startedUnix > timeoutSec;

    /// <summary>Активные полные (PLANNED/RUNNING/UPLOADING) с возрастом
    /// (now − started_unix) &gt; таймаута — кандидаты на FAILED job-timeout
    /// (COMPLETED/FAILED/DELETING не в счёт; порядок входа сохранён).</summary>
    public static IReadOnlyList<FullBackupState> SelectTimedOut(
        IReadOnlyList<FullBackupState> fulls, long nowUnix, long timeoutSec)
        => fulls
            .Where(f => f.State
                is FullBackupStatus.Planned or FullBackupStatus.Running or FullBackupStatus.Uploading)
            .Where(f => IsTimedOut(f.StartedUnix, nowUnix, timeoutSec))
            .ToList();
}
