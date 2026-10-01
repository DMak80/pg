namespace PgWorker.Backups.Drill;

// Парсер stdout drill-джоба — обёртка RestoreJobLog (протокол идентичен:
// фазы downloading|recovering + result-JSON; решение гейта плана 2026-10-01).
public static class DrillJobLog
{
    public static Restore.RestoreJobMarkers Parse(string logs)
        => Restore.RestoreJobLog.Parse(logs);
}
