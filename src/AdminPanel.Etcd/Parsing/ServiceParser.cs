using System.Globalization;
using System.Text.Json;
using AdminPanel.Core;
using AdminPanel.Etcd.Client;

namespace AdminPanel.Etcd.Parsing;

// Результат разбора префикса /service/ (spec §6.2).
public sealed record ServiceParseResult(
    IReadOnlyList<HaScope> Scopes,
    IReadOnlyList<KeyParseError> Errors,
    int UnknownKeyCount);

// Парсер Patroni DCS /service/<scope>/…: leader (JSON или plain-строка стенда), members, optime, initialize.
public static class ServiceParser
{
    private sealed class ScopeAcc(string scope)
    {
        public readonly string Scope = scope;
        public string? LeaderRaw;
        public string? OptimeRaw;
        public string? InitializeRaw;
        public string? RawConfig;
        public string? RequestCpu;
        public string? RequestMem;
        public string? RequestDisk;
        public readonly List<(string Name, string Raw)> Members = [];
    }

    public static ServiceParseResult Parse(
        IReadOnlyList<Kv> kvs,
        IReadOnlyList<ClusterInfo> clusters,
        IReadOnlyDictionary<string, IReadOnlyDictionary<string, (string Host, int Patroni)>>? portAlloc = null)
    {
        var unknown = 0;
        var accs = new Dictionary<string, ScopeAcc>();

        foreach (var kv in kvs)
        {
            // "/service/<scope>/…" → ["", "service", <scope>, …]
            var segments = kv.Key.Split('/');
            if (segments.Length < 4 || segments[1] != "service" || segments[2].Length == 0)
            {
                unknown++;
                continue;
            }

            var acc = GetOrAdd(accs, segments[2], static scope => new ScopeAcc(scope));
            switch (segments[3])
            {
                case "leader" when segments.Length == 4:
                    acc.LeaderRaw = kv.Value;
                    break;

                case "config" when segments.Length == 4:
                    acc.RawConfig = kv.Value; // raw-JSON для деталей HA (arch/02 §2.2)
                    break;

                case "initialize" when segments.Length == 4:
                    acc.InitializeRaw = kv.Value;
                    break;

                case "optime" when segments.Length == 5 && segments[4] == "leader":
                    acc.OptimeRaw = kv.Value;
                    break;

                case "request_cpu" when segments.Length == 4:
                    acc.RequestCpu = NullIfBlank(kv.Value);
                    break;

                case "request_mem" when segments.Length == 4:
                    acc.RequestMem = NullIfBlank(kv.Value);
                    break;

                case "request_disk" when segments.Length == 4:
                    acc.RequestDisk = NullIfBlank(kv.Value);
                    break;

                case "members" when segments.Length == 5 && segments[4].Length > 0:
                    acc.Members.Add((segments[4], kv.Value));
                    break;

                default:
                    unknown++;
                    break;
            }
        }

        var scopes = accs.Values
            .OrderBy(a => a.Scope, StringComparer.Ordinal)
            .Select(a =>
            {
                var (cluster, shard, matched) = ScopeMatcher.Match(a.Scope, clusters);

                // Канонический адрес ноды — portalloc PgWorker (host + patroni-порт):
                // Patroni пишет в conn_url контейнерный IP, недостижимый с хоста
                // панели; portalloc — тот же источник, из которого собран DSN шарда.
                IReadOnlyDictionary<string, (string Host, int Patroni)>? alloc = null;
                if (matched && cluster is not null && shard is not null)
                    portAlloc?.TryGetValue(cluster, out alloc);

                return new HaScope(
                    a.Scope,
                    cluster,
                    shard,
                    matched,
                    ParseLeader(a.LeaderRaw),
                    ParseOptime(a.OptimeRaw),
                    a.InitializeRaw is { Length: > 0 },
                    NullIfBlank(a.RequestCpu),
                    NullIfBlank(a.RequestMem),
                    NullIfBlank(a.RequestDisk),
                    a.Members
                        .OrderBy(m => m.Name, StringComparer.Ordinal)
                        .Select(m =>
                        {
                            var member = ParseMember(m.Name, m.Raw);
                            return alloc is not null
                                && alloc.TryGetValue($"{shard}/{m.Name}", out var entry)
                                ? member with { Host = entry.Host, Port = entry.Patroni }
                                : member;
                        })
                        .ToList(),
                    a.RawConfig);
            })
            .ToList();

        return new ServiceParseResult(scopes, [], unknown);
    }

    // Разбор /pgworker/portalloc/<cluster>: {"shard1/shard1a":{"host":"local",
    // "pg":15000,"patroni":18000,…}} → cluster → "shard/node" → (host, patroni).
    // Битые JSON-значения толерантно пропускаются — без записи нода остаётся
    // на conn_url из DCS.
    public static IReadOnlyDictionary<string, IReadOnlyDictionary<string, (string Host, int Patroni)>> ParsePortAlloc(
        IReadOnlyList<Kv> kvs)
    {
        var result = new Dictionary<string, IReadOnlyDictionary<string, (string, int)>>();
        foreach (var kv in kvs)
        {
            // "/pgworker/portalloc/<cluster>" → ["", "pgworker", "portalloc", <cluster>]
            var segments = kv.Key.Split('/');
            if (segments.Length != 4 || segments[3] is "")
                continue;

            try
            {
                using var doc = JsonDocument.Parse(kv.Value);
                if (doc.RootElement.ValueKind != JsonValueKind.Object)
                    continue;
                var nodes = new Dictionary<string, (string, int)>();
                foreach (var node in doc.RootElement.EnumerateObject())
                {
                    if (node.Value.ValueKind != JsonValueKind.Object
                        || !node.Value.TryGetProperty("host", out var host)
                        || host.ValueKind != JsonValueKind.String
                        || !node.Value.TryGetProperty("patroni", out var patroni)
                        || patroni.ValueKind != JsonValueKind.Number)
                        continue;
                    nodes[node.Name] = (host.GetString()!, patroni.GetInt32());
                }
                result[segments[3]] = nodes;
            }
            catch (JsonException)
            {
                // толерантно: битый кластер не ломает разбор остальных
            }
        }
        return result;
    }

    // leader: JSON {"name":…} (Patroni) либо plain-строка-имя (стенд) — arch/02 §2.2.
    private static string? ParseLeader(string? raw)
    {
        if (raw is null)
            return null; // нет ключа = нет лидера

        try
        {
            using var doc = JsonDocument.Parse(raw);
            if (doc.RootElement.ValueKind == JsonValueKind.Object)
                return JsonValues.ReadString(doc.RootElement, "name")?.Trim();
        }
        catch (JsonException)
        {
            // не JSON — трактуем как строку-имя
        }

        var trimmed = raw.Trim();
        return trimmed.Length == 0 ? null : trimmed;
    }

    private static long? ParseOptime(string? raw)
        => raw is not null
            && long.TryParse(raw.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var lsn)
            ? lsn
            : null;

    // Пустое/пробельное значение request_* = отсутствие заявки.
    private static string? NullIfBlank(string? raw)
        => string.IsNullOrWhiteSpace(raw) ? null : raw.Trim();

    private static HaMember ParseMember(string name, string raw)
    {
        var host = name;
        int? port = null;
        string? role = null;
        string? state = null;
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            role = JsonValues.ReadString(root, "role");
            state = JsonValues.ReadString(root, "state");
            var connUrl = JsonValues.ReadString(root, "conn_url");
            if (connUrl is not null)
            {
                // Patroni пишет conn_url как URI (postgres://host:port/dbname);
                // старый стенд — plain host:port (URI без authority туда не попадает).
                if (Uri.TryCreate(connUrl, UriKind.Absolute, out var uri) && !string.IsNullOrEmpty(uri.Host))
                {
                    host = uri.Host;
                    port = uri.Port > 0 ? uri.Port : null;
                }
                else
                {
                    var colon = connUrl.LastIndexOf(':');
                    if (colon > 0
                        && int.TryParse(connUrl[(colon + 1)..], NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedPort))
                    {
                        host = connUrl[..colon];
                        port = parsedPort;
                    }
                    else
                        host = connUrl;
                }
            }
        }
        catch (JsonException)
        {
            // толерантно: member без валидного JSON остаётся именем-хостом
        }

        return new HaMember(name, host, port, role, state, null, null, null, null);
    }

    private static TValue GetOrAdd<TKey, TValue>(Dictionary<TKey, TValue> dictionary, TKey key, Func<TKey, TValue> factory)
        where TKey : notnull
    {
        if (!dictionary.TryGetValue(key, out var value))
        {
            value = factory(key);
            dictionary[key] = value;
        }

        return value;
    }
}
