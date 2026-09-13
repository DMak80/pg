using System.Text.Json;
using PgWorker.Etcd.Parsing;

namespace PgWorker.Backups.Restore;

// Сериализация статуса restore в JSON канона /pgworker/backups/<C>/<X>/restore/<id>
// (arch/19 §4, t05): пишет ТОЛЬКО воркер (держатель клэйма); обязательные поля
// заявки — всегда, опциональные — по факту (null не сериализуем). Roundtrip с
// BackupsParser t01 — гарантия согласованности writer/reader (юнит-тест).
public static class RestoreStatusJson
{
    private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);

    public static string Serialize(RestoreOperationState state)
    {
        var o = new Dictionary<string, object?>
        {
            ["state"] = StateName(state.State),
            ["backup_id"] = state.BackupId,
            ["source"] = state.Source,
            ["target"] = state.Target,
            ["node"] = state.Node,
            ["requested_unix"] = state.RequestedUnix,
            ["requested_by"] = state.RequestedBy,
        };
        if (state.StartedUnix is { } started)
            o["started_unix"] = started;
        if (state.FinishedUnix is { } finished)
            o["finished_unix"] = finished;
        if (state.Phase is { } phase)
            o["phase"] = phase;
        if (state.RestoredToLsn is { } lsn)
            o["restored_to_lsn"] = lsn;
        if (state.SystemId is { } sysid)
            o["system_id"] = sysid;
        if (state.Error is { } error)
            o["error"] = error;

        return JsonSerializer.Serialize(o, Options);
    }

    private static string StateName(RestoreStatus state) => state switch
    {
        RestoreStatus.Planned => "PLANNED",
        RestoreStatus.Running => "RUNNING",
        RestoreStatus.Rejoining => "REJOINING",
        RestoreStatus.Completed => "COMPLETED",
        _ => "FAILED",
    };
}
