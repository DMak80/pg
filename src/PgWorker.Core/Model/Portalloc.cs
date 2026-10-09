using System.Text.Json;
using System.Text.Json.Serialization;

namespace PgWorker.Core.Model;

/// <summary>
/// Формат значения /pgworker/portalloc/&lt;C&gt; (spec §4.3, arch/14 §3):
/// плоский lowercase-JSON {"&lt;shard&gt;/&lt;node&gt;":{host,pg,patroni,doorman,object}}
/// — единый контракт для процессов, панели-диагностики и тестов; object — имя
/// docker-контейнера усыновлённой ноды (arch/14 §2.4), отсутствует у канонических;
/// alias/net (t15, arch/14 §2.4) — сетевая идентичность канонической ноды для
/// скрейпа (docker-имя + имя сети контура), отсутствуют без ключа ScrapeNetwork.
/// </summary>
public sealed record PortallocEntry(
    [property: JsonPropertyName("host")] string Host,
    [property: JsonPropertyName("pg")] int Pg,
    [property: JsonPropertyName("patroni")] int Patroni,
    [property: JsonPropertyName("doorman")] int Doorman,
    [property: JsonPropertyName("object")] string? Object = null,
    [property: JsonPropertyName("alias")] string? Alias = null,
    [property: JsonPropertyName("net")] string? Net = null)
{
    public NodeAddress ToAddress()
        => new(Host, new NodePorts(Pg, Patroni, Doorman), Object, Alias, Net);

    public static PortallocEntry From(NodeAddress address)
        => new(address.Host, address.Ports.Pg, address.Ports.Patroni, address.Ports.Doorman,
            address.Object, address.ScrapeAlias, address.ScrapeNetwork);
}

/// <summary>Сериализация словаря portalloc в контрактный плоский формат.</summary>
public static class Portalloc
{
    private static readonly JsonSerializerOptions Json = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    public static string Serialize(IReadOnlyDictionary<string, NodeAddress> addresses)
        => JsonSerializer.Serialize(
            addresses.ToDictionary(p => p.Key, p => PortallocEntry.From(p.Value)), Json);

    /// <summary>Парсинг значения ключа; битый JSON → Result.Failed.</summary>
    public static Result<IReadOnlyDictionary<string, NodeAddress>> Parse(string cluster, string raw)
    {
        try
        {
            var parsed = JsonSerializer.Deserialize<Dictionary<string, PortallocEntry>>(raw, Json);
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(
                (IReadOnlyDictionary<string, NodeAddress>)(parsed ?? [])
                    .ToDictionary(p => p.Key, p => p.Value.ToAddress()));
        }
        catch (JsonException e)
        {
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(
                new ApplicationException($"битый portalloc {cluster}: {e.Message}", e));
        }
    }
}

/// <summary>Сетевая идентичность записей portalloc (t15, arch/14 §2.4): при
/// заданном ключе ScrapeNetwork каноническим записям (без object) дописываются
/// alias = pgw-&lt;C&gt;-&lt;X&gt;-&lt;n&gt; (из ключа "&lt;X&gt;/&lt;n&gt;") и net = имя сети. Чистая,
/// идемпотентная; null/пустой ключ — словарь без изменений (дефолт поставки:
/// ни attach, ни полей).</summary>
public static class PortallocIdentity
{
    public static IReadOnlyDictionary<string, NodeAddress> Decorate(
        IReadOnlyDictionary<string, NodeAddress> addresses, string cluster, string? scrapeNetwork)
    {
        if (string.IsNullOrWhiteSpace(scrapeNetwork))
            return addresses;
        return addresses.ToDictionary(
            p => p.Key,
            p => p.Value.Object is null
                ? p.Value with
                {
                    ScrapeAlias = $"pgw-{cluster}-{p.Key.Replace('/', '-')}",
                    ScrapeNetwork = scrapeNetwork,
                }
                : p.Value);
    }
}
