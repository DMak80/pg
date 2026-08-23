using System.Text.Json;
using System.Text.Json.Serialization;

namespace PgWorker.Core.Model;

/// <summary>
/// Формат значения /pgworker/portalloc/&lt;C&gt; (spec §4.3, arch/14 §3):
/// плоский lowercase-JSON {"&lt;shard&gt;/&lt;node&gt;":{host,pg,patroni,doorman}}
/// — единый контракт для процессов, панели-диагностики и тестов.
/// </summary>
public sealed record PortallocEntry(
    [property: JsonPropertyName("host")] string Host,
    [property: JsonPropertyName("pg")] int Pg,
    [property: JsonPropertyName("patroni")] int Patroni,
    [property: JsonPropertyName("doorman")] int Doorman)
{
    public NodeAddress ToAddress()
        => new(Host, new NodePorts(Pg, Patroni, Doorman));

    public static PortallocEntry From(NodeAddress address)
        => new(address.Host, address.Ports.Pg, address.Ports.Patroni, address.Ports.Doorman);
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
