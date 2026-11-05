using System.Text.Json;

namespace PgWorker.Provisioning.Endpoints;

/// <summary>
/// Имя лидера HA-контура из значения ключа <c>/service/&lt;C&gt;-&lt;X&gt;/leader</c>
/// (arch/pgworker/processes/f-move.md F, шаг 2 резолва мастера). Значение пишет Patroni — ГОЛОЕ имя
/// члена; исторические сиды демо/фикстур кладут JSON-обёртку
/// <c>{"name":"…"}</c> — обе формы разбираются, прочий мусор → null.
/// </summary>
public static class LeaderKeyName
{
    public static string? From(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;
        var trimmed = value.Trim();
        if (trimmed.StartsWith('{'))
        {
            try
            {
                using var doc = JsonDocument.Parse(trimmed);
                return doc.RootElement.TryGetProperty("name", out var name)
                    && name.GetString() is { Length: > 0 } leaderName
                    ? leaderName
                    : null;
            }
            catch (JsonException)
            {
                return null;
            }
        }

        // Голое имя Patroni (случайные кавычки снимаем).
        var bare = trimmed.Trim('"');
        return bare.Length > 0 ? bare : null;
    }
}
