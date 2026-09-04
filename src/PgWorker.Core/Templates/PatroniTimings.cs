using System.Text.Json;

namespace PgWorker.Core.Templates;

/// <summary>
/// Канонические тайминги Patroni (arch/14 §2.1/§5 C, t09) — единый источник
/// для bootstrap.dcs SPILO_CONFIGURATION и для конвергенции динамического
/// DCS-конфига в надзоре. Полы Patroni 4.x: loop_wait≥1, retry_timeout≥3,
/// ttl≥20 и правило loop_wait + 2*retry_timeout ≤ ttl — заниженное значение
/// Patroni молча поднимает до пола и записывает обратно в DCS (t09:
/// заявленный ttl=5 превращался в 20 без ведома воркера). Нода, начавшая
/// работу на чужом/дефолтном/мусорном конфиге, приводится к канону
/// конвергенцией (PATCH /config) — «старое с плохими параметрами» не живёт
/// параллельно канону.
/// </summary>
public static class PatroniTimings
{
    public const int Ttl = 20;
    public const int LoopWait = 1;
    public const int RetryTimeout = 3;
    public const bool SynchronousMode = true;

    /// <summary>
    /// Расхождение фактического динамического конфига (GET /config, JSON)
    /// с каноном → минимальный патч-документ для PATCH /config (Patroni
    /// мержит его в DCS-конфиг и раздаёт нодам в пределах loop_wait).
    /// Конвергентно → null — мутаций нет (не второй регулярный писатель).
    /// </summary>
    public static string? DivergencePatch(string? configJson)
    {
        JsonElement root;
        try
        {
            root = JsonDocument.Parse(configJson ?? "null").RootElement;
        }
        catch (JsonException)
        {
            root = default; // битый/чужой документ — конвергируем все поля
        }

        var patch = new List<string>();
        AddIfDivergent(patch, root, "ttl", Ttl);
        AddIfDivergent(patch, root, "loop_wait", LoopWait);
        AddIfDivergent(patch, root, "retry_timeout", RetryTimeout);
        AddIfDivergent(patch, root, "synchronous_mode", SynchronousMode);

        return patch.Count == 0
            ? null
            : $"{{{string.Join(",", patch)}}}";
    }

    // Поле отсутствует (в т.ч. в битом/чужом документе) или расходится — в патч.
    private static void AddIfDivergent(List<string> patch, JsonElement root, string name, int expected)
    {
        var diverges = root.ValueKind != JsonValueKind.Object
                       || !root.TryGetProperty(name, out var actual)
                       || !actual.TryGetInt32(out var value)
                       || value != expected;
        if (diverges)
            patch.Add($"\"{name}\":{expected}");
    }

    private static void AddIfDivergent(List<string> patch, JsonElement root, string name, bool expected)
    {
        var diverges = root.ValueKind != JsonValueKind.Object
                       || !root.TryGetProperty(name, out var actual)
                       || (actual.ValueKind is not (JsonValueKind.True or JsonValueKind.False))
                       || actual.GetBoolean() != expected;
        if (diverges)
            patch.Add($"""
                       "{name}":{(expected ? "true" : "false")}
                       """);
    }
}
