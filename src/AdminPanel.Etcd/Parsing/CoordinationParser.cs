using System.Text.Json;
using AdminPanel.Core;
using Shared.Etcd.Client;

namespace AdminPanel.Etcd.Parsing;

// Разбор координации воркера (u14, adminpanel/02 §3 WorkerCoordination):
// leader — JSON {"instance","since_unix"?} (ключа нет → LeaderInstance=null);
// claims <root>/claims/<C> → WorkerClaim(C, instance, since_unix?);
// instances <root>/instances/<id> → WorkerInstanceInfo(id, since_unix?).
// Отсутствие всех ключей — пустая координация (не ошибка); битый JSON —
// KeyParseError, тик не роняет (толерантность, домен воркера не трогаем).
// Значение instances-ключа может быть пустым или JSON — парсим только при
// непустом теле, иначе SinceUnix=null.
public static class CoordinationParser
{
    public static (WorkerCoordination? Coordination, IReadOnlyList<KeyParseError> Errors) Parse(
        string prefixRoot, Kv? leaderKv, IReadOnlyList<Kv> claimsKv, IReadOnlyList<Kv> instancesKv)
    {
        var errors = new List<KeyParseError>();

        string? leader = null;
        long? leaderSince = null;
        if (leaderKv is { } leaderEntry)
        {
            try
            {
                using var doc = JsonDocument.Parse(leaderEntry.Value);
                var root = doc.RootElement;
                if (root.ValueKind != JsonValueKind.Object)
                {
                    errors.Add(new(leaderEntry.Key, "leader value is not a JSON object"));
                }
                else
                {
                    leader = root.TryGetProperty("instance", out var inst) && inst.ValueKind == JsonValueKind.String
                        ? inst.GetString()
                        : null;
                    leaderSince = OptionalUnix(root, "since_unix");
                    if (leader is null)
                        errors.Add(new(leaderEntry.Key, "broken leader value (instance required)"));
                }
            }
            catch (JsonException e)
            {
                errors.Add(new(leaderEntry.Key, $"broken leader JSON: {e.Message}"));
            }
        }

        var claims = new List<WorkerClaim>();
        foreach (var kv in claimsKv)
        {
            // <root>/claims/<C> → лист ключа — имя кластера. Чужие/неканонические
            // ключи (не под <root>/claims/) пропускаются молча — домен воркера
            // шире читаемого нами (range может вернуть соседние ключи в тестах).
            var claimsLeaf = LeafOf(kv.Key, "/claims/");
            if (claimsLeaf is null)
                continue;
            var cluster = claimsLeaf;
            if (cluster.Length == 0)
            {
                errors.Add(new(kv.Key, "empty cluster name in claims key"));
                continue;
            }

            try
            {
                using var doc = JsonDocument.Parse(kv.Value);
                var root = doc.RootElement;
                if (root.ValueKind != JsonValueKind.Object)
                {
                    errors.Add(new(kv.Key, "claim value is not a JSON object"));
                    continue;
                }

                var instance = root.TryGetProperty("instance", out var inst) && inst.ValueKind == JsonValueKind.String
                    ? inst.GetString()
                    : null;
                if (instance is null)
                {
                    errors.Add(new(kv.Key, "broken claim value (instance required)"));
                    continue;
                }

                claims.Add(new WorkerClaim(cluster, instance, OptionalUnix(root, "since_unix")));
            }
            catch (JsonException e)
            {
                errors.Add(new(kv.Key, $"broken claim JSON: {e.Message}"));
            }
        }

        var instances = new List<WorkerInstanceInfo>();
        foreach (var kv in instancesKv)
        {
            // <root>/instances/<id> → лист ключа — instance id (чужие ключи —
            // молча мимо, как claims).
            var instanceId = LeafOf(kv.Key, "/instances/");
            if (instanceId is null)
                continue;
            if (instanceId.Length == 0)
            {
                errors.Add(new(kv.Key, "empty instance id in instances key"));
                continue;
            }

            // Пустое тело (или не-JSON-объект) — инстанс без since (SinceUnix=null);
            // битый JSON-объект — parseError (толерантность).
            if (string.IsNullOrWhiteSpace(kv.Value))
            {
                instances.Add(new WorkerInstanceInfo(instanceId, null));
                continue;
            }

            try
            {
                using var doc = JsonDocument.Parse(kv.Value);
                if (doc.RootElement.ValueKind != JsonValueKind.Object)
                {
                    instances.Add(new WorkerInstanceInfo(instanceId, null));
                    continue;
                }

                instances.Add(new WorkerInstanceInfo(
                    instanceId, OptionalUnix(doc.RootElement, "since_unix")));
            }
            catch (JsonException e)
            {
                errors.Add(new(kv.Key, $"broken instance JSON: {e.Message}"));
            }
        }

        var coordination = new WorkerCoordination(
            leader,
            claims.OrderBy(c => c.Cluster, StringComparer.Ordinal).ToList(),
            instances.OrderBy(i => i.InstanceId, StringComparer.Ordinal).ToList());
        return (coordination, errors);
    }

    // Лист ключа под префикс-семейством ("<root>/claims/…"): null — ключ не
    // принадлежит семейству (чужой домен/неканонический путь — молча мимо).
    private static string? LeafOf(string key, string familySuffix)
    {
        var familyStart = key.IndexOf(familySuffix, StringComparison.Ordinal);
        return familyStart < 0
            ? null
            : key[(familyStart + familySuffix.Length)..];
    }

    private static long? OptionalUnix(JsonElement root, string name)
        => root.TryGetProperty(name, out var el) && el.ValueKind == JsonValueKind.Number
            && el.TryGetInt64(out var value)
                ? value
                : null;
}
