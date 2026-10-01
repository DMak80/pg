namespace PgWorker.Backups.Drill;

// Команда drill-джоба (reliability t02, arch/19 §3.6) — механика restore-джоба
// t05 ЦЕЛИКОМ (решение пользователя, гейт плана 2026-10-01): один inline-bash
// скрипт RestoreJobCommand на оба сценария — дрилл доказывает ровно тот DR-путь
// (spec §2.2); TARGET_TIME="" → latest (без recovery_target_*), unix-socket,
// pg_is_in_recovery-поллинг, pg_current_wal_lsn. Лишнее для дрилла поле
// system_id в result воркер дрилла игнорирует (в ключ drill не попадает).
public static class DrillJobCommand
{
    public static IReadOnlyList<string> Build() => Restore.RestoreJobCommand.Build();
}
