using System.Text.Json;

namespace PgWorker.Backups.Job;

/// <summary>Result-JSON джоба: {"ok":true,"wal_start_segment":…,"size_bytes":…}
/// либо {"ok":false,"error":…} (arch/19 §2).</summary>
public sealed record BackupJobResult(bool Ok, string? WalStartSegment, long? SizeBytes, string? Error);

/// <summary>Маркеры stdout джоба на момент поллинга: последняя фаза, её
/// wal_start_segment и финальный result (null — ещё не напечатан).</summary>
public sealed record BackupJobMarkers(string? Phase, string? WalStartSegment, BackupJobResult? Result);

// Парсер stdout-протокола джоба: воркер разбирает ТОЛЬКО свои JSON-строки,
// незнакомые строки (шум pg_basebackup/mc) игнорирует (arch/19 §2/§10).
public static class BackupJobLog
{
    public static BackupJobMarkers Parse(string logs)
    {
        string? phase = null;
        string? walStart = null;
        BackupJobResult? result = null;
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
                {
                    phase = phaseEl.GetString();
                    if (root.TryGetProperty("wal_start_segment", out var walEl)
                        && walEl.ValueKind == JsonValueKind.String)
                        walStart = walEl.GetString();
                }

                if (root.TryGetProperty("ok", out var okEl) && okEl.ValueKind is JsonValueKind.True or JsonValueKind.False)
                    result = new BackupJobResult(
                        okEl.ValueKind == JsonValueKind.True,
                        GetString(root, "wal_start_segment"),
                        GetLong(root, "size_bytes"),
                        GetString(root, "error"));
            }
            catch (JsonException)
            {
                // чужая JSON-подобная строка — не наш контракт
            }
        }

        return new BackupJobMarkers(phase, walStart, result);
    }

    private static string? GetString(JsonElement root, string name)
        => root.TryGetProperty(name, out var e) && e.ValueKind == JsonValueKind.String ? e.GetString() : null;

    private static long? GetLong(JsonElement root, string name)
        => root.TryGetProperty(name, out var e) && e.ValueKind == JsonValueKind.Number && e.TryGetInt64(out var v) ? v : null;
}
