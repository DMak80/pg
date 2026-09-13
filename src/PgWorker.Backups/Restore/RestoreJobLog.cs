using System.Text.Json;

namespace PgWorker.Backups.Restore;

/// <summary>Result-JSON restore-джоба: {"ok":true,"restored_to_lsn":"0/…",
/// "system_id":"…"} либо {"ok":false,"error":"…"} (arch/19 §3.5, t05);
/// system_id — с фикса гонки re-bootstrap (2026-09-13); старый образ джоба
/// поле не пишет — null.</summary>
public sealed record RestoreJobResult(bool Ok, string? RestoredToLsn, string? Error, string? SystemId = null);

/// <summary>Маркеры stdout restore-джоба на момент поллинга: последняя фаза
/// (downloading|recovering) и финальный result (null — ещё не напечатан).</summary>
public sealed record RestoreJobMarkers(string? Phase, RestoreJobResult? Result);

// Парсер stdout-протокола restore-джоба (копия структуры BackupJobLog): воркер
// разбирает ТОЛЬКО свои JSON-строки, незнакомые строки (шум pg_ctl/mc) игнорирует.
public static class RestoreJobLog
{
    public static RestoreJobMarkers Parse(string logs)
    {
        string? phase = null;
        RestoreJobResult? result = null;
        foreach (var line in logs.Split('\n'))
        {
            var trimmed = line.Trim();
            if (!trimmed.StartsWith('{') || !trimmed.EndsWith('}'))
                continue;
            try
            {
                using var doc = JsonDocument.Parse(trimmed);
                var root = doc.RootElement;
                if (root.ValueKind != JsonValueKind.Object)
                    continue;

                if (root.TryGetProperty("phase", out var phaseEl)
                    && phaseEl.ValueKind == JsonValueKind.String)
                    phase = phaseEl.GetString();

                if (root.TryGetProperty("ok", out var okEl) && okEl.ValueKind is JsonValueKind.True or JsonValueKind.False)
                    result = new RestoreJobResult(
                        okEl.ValueKind == JsonValueKind.True,
                        GetString(root, "restored_to_lsn"),
                        GetString(root, "error"),
                        GetString(root, "system_id"));
            }
            catch (JsonException)
            {
                // чужая JSON-подобная строка — не наш контракт
            }
        }

        return new RestoreJobMarkers(phase, result);
    }

    private static string? GetString(JsonElement root, string name)
        => root.TryGetProperty(name, out var e) && e.ValueKind == JsonValueKind.String ? e.GetString() : null;
}
