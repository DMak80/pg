using System.Text.Json;
using AdminPanel.Core;
using AdminPanel.Etcd.Client;

namespace AdminPanel.Etcd.Parsing;

public sealed record BackupsParseResult(
    IReadOnlyList<ClusterBackupsInfo> Clusters,
    IReadOnlyList<KeyParseError> Errors);

// Чистая функция: KV префикса /pgworker/backups/ (arch/19 §4, t02): policy
// full_max_age_sec + per-shard последний COMPLETED finished_unix. Битые
// значения — KeyParseError + пропуск записи (паттерн панели). Шард с любыми
// ключами полных попадает в словарь: null = COMPLETED не было («полного
// никогда не было», spec §3.5); шарды без ключей ВООБЩЕ в словарь не
// попадают (правило молчит — подсистема не включена).
public static class BackupsParser
{
    public const string Prefix = "/pgworker/backups/";

    public static BackupsParseResult Parse(IReadOnlyList<Kv> kvs)
    {
        // "/pgworker/backups/<C>/policy" | "/pgworker/backups/<C>/<X>/full/<id>"
        var policies = new Dictionary<string, long?>();
        var shards = new Dictionary<string, Dictionary<string, long?>>();
        var errors = new List<KeyParseError>();
        foreach (var kv in kvs)
        {
            var segments = kv.Key.Split('/');
            if (segments.Length < 5 || segments[1] != "pgworker" || segments[2] != "backups")
                continue; // чужой префикс

            var cluster = segments[3];
            if (cluster.Length == 0)
                continue;

            if (segments.Length == 5 && segments[4] == "policy")
            {
                try
                {
                    using var doc = JsonDocument.Parse(kv.Value);
                    policies[cluster] = doc.RootElement.ValueKind == JsonValueKind.Object
                        && doc.RootElement.TryGetProperty("full_max_age_sec", out var age)
                        && age.ValueKind == JsonValueKind.Number
                        && age.TryGetInt64(out var value)
                        ? value
                        : null;
                }
                catch (JsonException e)
                {
                    errors.Add(new(kv.Key, $"битый JSON policy: {e.Message}"));
                }

                continue;
            }

            if (segments.Length == 7 && segments[5] == "full" && segments[4].Length > 0 && segments[6].Length > 0)
            {
                try
                {
                    using var doc = JsonDocument.Parse(kv.Value);
                    var root = doc.RootElement;
                    if (root.ValueKind == JsonValueKind.Object
                        && root.TryGetProperty("state", out var state)
                        && state.ValueKind == JsonValueKind.String)
                    {
                        // Ключи бэкапов есть → шард В СЛОВАРЕ даже без COMPLETED:
                        // null = «полного никогда не было» (spec §3.5/§9.6);
                        // молчание правила — только для ПУСТОГО префикса (шарда
                        // нет в словаре вовсе).
                        var perShard = GetOrAdd(shards, cluster);
                        perShard.TryAdd(segments[4], null);

                        if (state.GetString() == "COMPLETED"
                            && root.TryGetProperty("finished_unix", out var finished)
                            && finished.ValueKind == JsonValueKind.Number
                            && finished.TryGetInt64(out var value))
                        {
                            var current = perShard[segments[4]];
                            perShard[segments[4]] = current is null || value > current ? value : current;
                        }
                    }
                }
                catch (JsonException e)
                {
                    errors.Add(new(kv.Key, $"битый JSON full: {e.Message}"));
                }
            }
        }

        // Кластер с policy, но без шардов — в списке с пустым словарём
        // (правило по пустому словарю молчит).
        var clusters = shards.Keys
            .Concat(policies.Keys.Where(p => !shards.ContainsKey(p)))
            .Distinct()
            .OrderBy(c => c, StringComparer.Ordinal)
            .Select(c => new ClusterBackupsInfo(
                c,
                policies.TryGetValue(c, out var age) ? age : null,
                (shards.TryGetValue(c, out var perShard)
                    ? perShard
                    : new Dictionary<string, long?>())
                .ToDictionary(p => p.Key, p => p.Value)))
            .ToList();
        return new(clusters, errors);
    }

    private static Dictionary<string, long?> GetOrAdd(
        Dictionary<string, Dictionary<string, long?>> source, string cluster)
    {
        if (source.TryGetValue(cluster, out var perShard))
            return perShard;
        var created = new Dictionary<string, long?>();
        source[cluster] = created;
        return created;
    }
}
