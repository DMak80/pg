using System.Text.Json;

namespace PgWorker.Backups.Job;

/// <summary>Result-JSON verify-джоба: {"ok":true} либо
/// {"ok":false,"phase":"download|verify","error":…} (arch/19 §5, t04).</summary>
public sealed record VerifyJobResult(bool Ok, string? Phase, string? Error);

// Парсер stdout verify-джоба: последняя своя JSON-строка; чужие строки игнорит
// (паттерн BackupJobLog).
public static class VerifyLog
{
    public static VerifyJobResult? Parse(string logs)
    {
        VerifyJobResult? result = null;
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
                if (root.TryGetProperty("ok", out var okEl) && okEl.ValueKind is JsonValueKind.True or JsonValueKind.False)
                    result = new VerifyJobResult(
                        okEl.ValueKind == JsonValueKind.True,
                        root.TryGetProperty("phase", out var phaseEl) && phaseEl.ValueKind == JsonValueKind.String
                            ? phaseEl.GetString()
                            : null,
                        root.TryGetProperty("error", out var errEl) && errEl.ValueKind == JsonValueKind.String
                            ? errEl.GetString()
                            : null);
            }
            catch (JsonException)
            {
                // чужая JSON-подобная строка — не наш контракт
            }
        }

        return result;
    }
}
