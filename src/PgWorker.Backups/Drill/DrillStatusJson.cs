using System.Text.Json;
using PgWorker.Etcd.Parsing;

namespace PgWorker.Backups.Drill;

// Сериализация статуса дрилла в JSON канона /pgworker/backups/<C>/<X>/drill
// (arch/19 §4, reliability t02): пишет ТОЛЬКО воркер под клэймом; null/опциональные
// поля не сериализуются (образец RestoreStatusJson). Парсинг — снапшотный
// BackupsParser.TryParseDrill (PgWorker.Etcd), панель — своим парсером.
public static class DrillStatusJson
{
    private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);

    public static string Serialize(DrillState state)
    {
        var o = new Dictionary<string, object?>
        {
            ["state"] = StateName(state.State),
            ["id"] = state.Id,
            ["backup_id"] = state.BackupId,
            ["started_unix"] = state.StartedUnix,
        };
        if (state.FinishedUnix is { } finished)
            o["finished_unix"] = finished;
        if (state.Phase is { } phase)
            o["phase"] = phase;
        if (state.RestoredToLsn is { } lsn)
            o["restored_to_lsn"] = lsn;
        if (state.Error is { } error)
            o["error"] = error;

        return JsonSerializer.Serialize(o, Options);
    }

    private static string StateName(DrillStatus state) => state switch
    {
        DrillStatus.Running => "RUNNING",
        DrillStatus.Succeeded => "SUCCEEDED",
        _ => "FAILED",
    };
}
