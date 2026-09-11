using System.Text.Json;
using PgWorker.Etcd.Parsing;

namespace PgWorker.Backups.Job;

// Сериализация статуса полного в JSON канона /pgworker/backups/<C>/<X>/full/<id>
// (arch/19 §4): пишет ТОЛЬКО воркер (держатель клэйма); поля состояния —
// по факту (null/опциональные не сериализуем).
public static class BackupStatusJson
{
    private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);

    public static string Serialize(FullBackupState state)
    {
        var o = new Dictionary<string, object?>
        {
            ["state"] = StateName(state.State),
            ["node"] = state.Node,
            ["role"] = state.Role == BackupSourceRole.Replica ? "replica" : "master",
            ["started_unix"] = state.StartedUnix,
        };
        if (state.FinishedUnix is { } finished)
            o["finished_unix"] = finished;
        if (state.WalStartSegment is { } wal)
            o["wal_start_segment"] = wal;
        if (state.SizeBytes is { } size)
            o["size_bytes"] = size;
        if (state.Error is { } error)
            o["error"] = error;
        if (state.Verify is { } verify)
        {
            var v = new Dictionary<string, object?> { ["state"] = VerifyName(verify.State) };
            if (verify.CheckedUnix is { } checkedUnix)
                v["checked_unix"] = checkedUnix;
            if (verify.Error is { } verifyError)
                v["error"] = verifyError;
            o["verify"] = v;
        }

        return JsonSerializer.Serialize(o, Options);
    }

    private static string StateName(FullBackupStatus state) => state switch
    {
        FullBackupStatus.Planned => "PLANNED",
        FullBackupStatus.Running => "RUNNING",
        FullBackupStatus.Uploading => "UPLOADING",
        FullBackupStatus.Completed => "COMPLETED",
        FullBackupStatus.Failed => "FAILED",
        _ => "DELETING",
    };

    private static string VerifyName(BackupVerifyStatus state) => state switch
    {
        BackupVerifyStatus.Pending => "PENDING",
        BackupVerifyStatus.Ok => "OK",
        _ => "FAILED",
    };
}
