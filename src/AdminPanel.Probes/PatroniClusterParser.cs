using System.Text.Json;
using System.Text.Json.Serialization;

namespace AdminPanel.Probes;

// Распаренный член ответа GET /cluster (Patroni-формат, arch/02 §6.1; spec §4.6).
public sealed record PatroniClusterMember(
    string? Name,
    string? Role,
    string? State,
    long? Timeline,
    long? LagBytes);

// Парсер JSON ответа Patroni /cluster: {"members":[{name,role,state,timeline,lag,…},…]}.
// Толерантен: отсутствующие поля, null-лаг, строковые числа (arch/02 §8).
public static class PatroniClusterParser
{
    // AllowReadingFromString — тот же приём, что EtcdGateway для decimal-строк etcd (t03 §4.2).
    private static readonly JsonSerializerOptions Json = new()
    {
        NumberHandling = JsonNumberHandling.AllowReadingFromString,
        PropertyNameCaseInsensitive = true,
    };

    public static IReadOnlyList<PatroniClusterMember> Parse(string json)
    {
        var response = JsonSerializer.Deserialize<ClusterResponse>(json, Json);
        // DTO → контрактный record: null-ответ (пустой ввод) — пустой список.
        return [.. (response?.Members ?? []).Select(m =>
            new PatroniClusterMember(m.Name, m.Role, m.State, m.Timeline, m.Lag))];
    }

    private sealed class ClusterResponse
    {
        [JsonPropertyName("members")]
        public List<MemberDto>? Members { get; set; }
    }

    private sealed class MemberDto
    {
        [JsonPropertyName("name")]
        public string? Name { get; set; }

        [JsonPropertyName("role")]
        public string? Role { get; set; }

        [JsonPropertyName("state")]
        public string? State { get; set; }

        [JsonPropertyName("timeline")]
        public long? Timeline { get; set; }

        // Patroni в переходных состояниях члена (starting/creating replica при
        // пересоздании ноды) отдаёт "lag": "unknown" — нечисловая строка валит
        // весь парсер и ломает пробы ВСЕХ членов скопа (каждая проба парсит
        // полный /cluster). Терпим: нечисловое → null (лаг неизвестен).
        [JsonPropertyName("lag")]
        [JsonConverter(typeof(LenientNullableLongConverter))]
        public long? Lag { get; set; }
    }

    // long? с прощением строк: число-строка ("123") читается, прочее ("unknown")
    // — null, без JsonException.
    private sealed class LenientNullableLongConverter : JsonConverter<long?>
    {
        public override long? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            => reader.TokenType switch
            {
                JsonTokenType.Number => reader.GetInt64(),
                JsonTokenType.String when long.TryParse(reader.GetString(), out var value) => value,
                _ => null,
            };

        public override void Write(Utf8JsonWriter writer, long? value, JsonSerializerOptions options)
            => writer.WriteNumberValue(value ?? 0);
    }
}
