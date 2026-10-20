using AdminPanel.Core;
using Shared.Etcd.Client;

namespace AdminPanel.Etcd.Parsing;

/// <summary>Парсер домена /etcd/ (t32, adminpanel/02 §2.3.5): config →
/// декларация; nodes/&lt;k&gt;/{state,placed_on} → managed-узлы. Битые значения —
/// parseError-записи, тик не падает; unknown-листы узлов игнорируются.</summary>
public static class EtcdDomainParser
{
    public static (EtcdDomainInfo? Domain, IReadOnlyList<KeyParseError> Errors) Parse(
        Kv? configKv, IReadOnlyList<Kv> nodeKvs)
    {
        var errors = new List<KeyParseError>();
        int? declared = null;
        long? created = null;
        if (configKv is { } cfg)
        {
            try
            {
                using var doc = System.Text.Json.JsonDocument.Parse(cfg.Value);
                var root = doc.RootElement;
                if (root.ValueKind == System.Text.Json.JsonValueKind.Object)
                {
                    if (root.TryGetProperty("nodes", out var n) && n.ValueKind == System.Text.Json.JsonValueKind.Number && n.TryGetInt32(out var declaredNodes))
                        declared = declaredNodes;
                    if (root.TryGetProperty("created_unix", out var c) && c.ValueKind == System.Text.Json.JsonValueKind.Number && c.TryGetInt64(out var createdUnix))
                        created = createdUnix;
                }
                else
                    errors.Add(new KeyParseError(cfg.Key, "config is not a JSON object"));
            }
            catch (System.Text.Json.JsonException ex)
            {
                errors.Add(new KeyParseError(cfg.Key, $"broken /etcd/config JSON: {ex.Message}"));
            }
        }

        // nodes/<k>/{state,placed_on}: сегменты ["", "etcd", "nodes", <k>, <leaf>].
        var nodes = new Dictionary<int, ManagedNodeView>();
        foreach (var kv in nodeKvs)
        {
            var segments = kv.Key.Split('/');
            if (segments.Length != 5 || segments[1] != "etcd" || segments[2] != "nodes")
                continue;
            if (!int.TryParse(segments[3], out var k) || k <= 0)
                continue;
            var value = string.IsNullOrWhiteSpace(kv.Value) ? null : kv.Value.Trim().Trim('"');
            switch (segments[4])
            {
                case "state":
                    nodes[k] = new ManagedNodeView(k, value, nodes.TryGetValue(k, out var byState) ? byState.PlacedOn : null);
                    break;
                case "placed_on":
                    nodes[k] = new ManagedNodeView(k, nodes.TryGetValue(k, out var byPlaced) ? byPlaced.State : null, value);
                    break;
                // прочие листы — unknown, не ошибка
            }
        }

        return (new EtcdDomainInfo(declared, created,
            nodes.Values.OrderBy(n => n.K).ToList()), errors);
    }
}
