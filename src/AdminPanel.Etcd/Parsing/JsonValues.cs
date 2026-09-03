using System.Text.Json;

namespace AdminPanel.Etcd.Parsing;

// Толерантное чтение полей JSON-значений ключей: строки-числа, отсутствующие поля (arch/02 §8).
internal static class JsonValues
{
    public static string? ReadString(JsonElement root, string name)
        => root.ValueKind == JsonValueKind.Object
            && root.TryGetProperty(name, out var element)
            && element.ValueKind is JsonValueKind.String or JsonValueKind.Number
            ? element.ToString()
            : null;

    public static long? ReadLong(JsonElement root, string name)
    {
        if (root.ValueKind != JsonValueKind.Object || !root.TryGetProperty(name, out var element))
            return null;

        return element.ValueKind switch
        {
            JsonValueKind.Number => element.TryGetInt64(out var value) ? value : null,
            JsonValueKind.String when long.TryParse(element.GetString(), out var value) => value,
            _ => null,
        };
    }

    // int-поля ключей (t06: brokers_total/brokers_remaining): вне int → null.
    public static int? ReadInt(JsonElement root, string name)
    {
        var value = ReadLong(root, name);
        return value is null or > int.MaxValue or < int.MinValue ? null : (int?)value.Value;
    }
}
