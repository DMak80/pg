using AdminPanel.Core;
using Shared.Etcd.Client;

namespace AdminPanel.Etcd.S3;

/// <summary>Парсер домена /s3/clusters/ (t36, arch/23 §2; adminpanel/02 §12):
/// Kv[] → список пулов (read-минимум). Толерантность unknownKeys/parseError —
/// как у прочих доменов: битый config — parseError-запись без падения тика;
/// имена нод — из ключей заявки nodes/&lt;I&gt;/state|placement; endpoints —
/// строка "h1:p1,h2:p2" по порядку.</summary>
public static class S3PoolParser
{
    public static (IReadOnlyList<S3PoolInfo> Pools, IReadOnlyList<string> Errors, int UnknownKeys) Parse(
        IReadOnlyList<Kv> kvs)
    {
        var errors = new List<string>();
        var unknownKeys = 0;
        var names = new SortedSet<string>(StringComparer.Ordinal);
        foreach (var kv in kvs)
        {
            // /s3/clusters/<C>/config → имя пула (маркер существования).
            var parts = kv.Key.Split('/');
            if (parts.Length == 5 && parts[1] == "s3" && parts[2] == "clusters" && parts[4] == "config")
                names.Add(parts[3]);
        }

        var pools = new List<S3PoolInfo>();
        foreach (var name in names)
        {
            var config = kvs.FirstOrDefault(kv => kv.Key == $"/s3/clusters/{name}/config");
            string? secretProfile = null;
            var active = true;
            if (config is not null)
            {
                try
                {
                    using var doc = System.Text.Json.JsonDocument.Parse(config.Value);
                    var root = doc.RootElement;
                    if (root.ValueKind != System.Text.Json.JsonValueKind.Object)
                        throw new System.Text.Json.JsonException();
                    if (root.TryGetProperty("secret_profile", out var sp) && sp.ValueKind == System.Text.Json.JsonValueKind.String)
                        secretProfile = sp.GetString();
                    // state отсутствует = Active (arch/23 §2); TO_REMOVE — не Active.
                    if (root.TryGetProperty("state", out var st)
                        && st.ValueKind == System.Text.Json.JsonValueKind.String
                        && st.GetString() == "TO_REMOVE")
                        active = false;
                }
                catch (System.Text.Json.JsonException)
                {
                    errors.Add($"/s3/clusters/{name}/config: битый JSON");
                    continue; // parseError-путь: пул пропущен, тик жив
                }
            }

            // Ноды: имена из ключей заявки; state — значение листа (JSON-строка).
            var nodeNames = new SortedSet<string>(StringComparer.Ordinal);
            var states = new Dictionary<string, string>();
            foreach (var kv in kvs)
            {
                var parts = kv.Key.Split('/');
                if (parts.Length != 7 || parts[1] != "s3" || parts[2] != "clusters"
                    || parts[3] != name || parts[4] != "nodes")
                {
                    continue;
                }
                nodeNames.Add(parts[5]);
                switch (parts[6])
                {
                    case "state":
                        states[parts[5]] = Unquote(kv.Value);
                        break;
                    case "placement":
                        break; // заявка — имя уже добавлено
                    default:
                        unknownKeys++; // неизвестный лист ноды (usage/pki/…)
                        break;
                }
            }

            var endpointsRaw = kvs.FirstOrDefault(kv => kv.Key == $"/s3/clusters/{name}/endpoints");
            var endpoints = endpointsRaw is null ? [] : ParseEndpoints(endpointsRaw.Value);

            pools.Add(new S3PoolInfo(
                name, secretProfile, active,
                [.. nodeNames.Select(n => new S3PoolNodeInfo(n, states.GetValueOrDefault(n)))],
                endpoints));
        }

        return (pools, errors, unknownKeys);
    }

    // "RUNNING" → RUNNING (JSON-строка etcd); прочее — как есть.
    private static string Unquote(string raw)
        => raw.Length > 1 && raw[0] == '"'
            ? TryUnquote(raw) ?? raw
            : raw;

    private static string? TryUnquote(string raw)
    {
        try
        {
            return System.Text.Json.JsonSerializer.Deserialize<string>(raw);
        }
        catch (System.Text.Json.JsonException)
        {
            return null;
        }
    }

    // "h1:18001,h2:18002" → упорядоченный массив (битые элементы — долой).
    private static string[] ParseEndpoints(string raw)
        => [.. raw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(e =>
            {
                var colon = e.LastIndexOf(':');
                return colon > 0 && colon < e.Length - 1
                    && int.TryParse(e[(colon + 1)..], out var port) && port > 0;
            })];
}
