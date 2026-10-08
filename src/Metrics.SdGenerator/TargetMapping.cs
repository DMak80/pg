using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using Shared.Etcd.Client;

namespace Metrics.SdGenerator;

// Группа file_sd: один таргет + фиксированные лейблы
// cluster/shard/node (лейблы конечны — arch/18 §2, риск M1). Таргет — сетевой
// <alias>:8008 (запись с alias, единая сеть контура) либо advertised
// host:patroni по host-публикации (без alias — легаси/усыновлённые, arch/18 §2.5).
public sealed record SdTargetGroup(string Target, string Cluster, string Shard, string Node);

// Чистая функция маппинга /pgworker/portalloc/<C> → file_sd-группы (spec §3.2):
// детерминированный порядок, толерантность к битым/развивающимся записям
// (паттерн панели — state-значения строкой, система развивается).
public static class TargetMapping
{
    public const string PortallocPrefix = "/pgworker/portalloc/";

    /// <summary>Контейнерный порт Patroni REST — константа контракта ноды
    /// (arch/14 §2.1), не host-порт: сетевой таргет едины сети контура.</summary>
    public const int PatroniRestPort = 8008;

    // DTO записи portalloc (формат — PgWorker.Core PortallocEntry, без ссылки на Core:
    // генератор независим от воркеров; незнакомые поля — игнор).
    private sealed record PortallocEntry(
        [property: JsonPropertyName("host")] string Host,
        [property: JsonPropertyName("pg")] int Pg,
        [property: JsonPropertyName("patroni")] int Patroni,
        [property: JsonPropertyName("doorman")] int Doorman,
        [property: JsonPropertyName("alias")] string? Alias = null);

    private static readonly JsonSerializerOptions Json = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    // Kv[] → группы; warn(cluster, error) вызывается на битые/незнакомые записи (skip).
    public static IReadOnlyList<SdTargetGroup> Map(IReadOnlyList<Kv> kvs, Action<string, string> warn)
    {
        var groups = new List<SdTargetGroup>();
        foreach (var kv in kvs)
        {
            // Чужие префиксы игнорируются молча (фильтр канала portalloc).
            if (!kv.Key.StartsWith(PortallocPrefix, StringComparison.Ordinal))
                continue;
            var cluster = kv.Key[PortallocPrefix.Length..];
            MapKey(cluster, kv.Value, groups, warn);
        }

        // Порядок детерминирован: (Cluster, Shard, Node).
        return groups
            .OrderBy(g => g.Cluster, StringComparer.Ordinal)
            .ThenBy(g => g.Shard, StringComparer.Ordinal)
            .ThenBy(g => g.Node, StringComparer.Ordinal)
            .ToList();
    }

    // Детерминированный file_sd JSON: [{"targets":["h:p"],"labels":{cluster,shard,node}}…]
    public static string Serialize(IReadOnlyList<SdTargetGroup> groups)
        => JsonSerializer.Serialize(groups.Select(g => new
        {
            targets = new[] { g.Target },
            labels = new { cluster = g.Cluster, shard = g.Shard, node = g.Node },
        }));

    private static void MapKey(
        string cluster, string raw, List<SdTargetGroup> groups, Action<string, string> warn)
    {
        Dictionary<string, PortallocEntry>? entries;
        try
        {
            // Битый JSON / не-словарь → пропуск записи ключа ЦЕЛИКОМ.
            entries = JsonSerializer.Deserialize<Dictionary<string, PortallocEntry>>(raw, Json);
            if (entries is null)
                throw new JsonException("portalloc-значение — не словарь");
        }
        catch (JsonException e)
        {
            warn(cluster, $"битая запись portalloc: {e.Message}");
            return;
        }

        foreach (var (name, entry) in entries)
        {
            // "<shard>/<node>" → лейблы; запись без разделителя — битая (warn, skip записи).
            var separator = name.IndexOf('/', StringComparison.Ordinal);
            if (separator <= 0 || separator == name.Length - 1)
            {
                warn(cluster, $"запись «{name}» без shard/node — пропуск");
                continue;
            }

            // host пустой — битая запись (warn + skip).
            if (string.IsNullOrWhiteSpace(entry.Host))
            {
                warn(cluster, $"запись «{name}» без host — пропуск");
                continue;
            }

            // patroni <= 0 — усыплённая нода (штатная семантика arch/14 §2.4):
            // без Patroni-REST не скрейпима, пропуск без warning — в обеих ветках.
            if (entry.Patroni <= 0)
                continue;

            // Per-node правило (t15, arch/18 §2.5): alias есть → сетевой таргет
            // <alias>:<контейнерный порт> (порты host-публикации не участвуют);
            // пустой alias — отсутствие (advertised-ветка, как без поля).
            var target = !string.IsNullOrWhiteSpace(entry.Alias)
                ? string.Create(CultureInfo.InvariantCulture, $"{entry.Alias}:{PatroniRestPort}")
                : string.Create(CultureInfo.InvariantCulture, $"{entry.Host}:{entry.Patroni}");

            groups.Add(new SdTargetGroup(
                Target: target,
                Cluster: cluster,
                Shard: name[..separator],
                Node: name[(separator + 1)..]));
        }
    }
}
