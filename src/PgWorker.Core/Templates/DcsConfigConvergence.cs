using System.Text.Json;

namespace PgWorker.Core.Templates;

/// <summary>Итог сверки живого конфига с каноном: патч + счётчики для журнала.</summary>
/// <param name="Patch">Минимальный патч-документ; null — конвергентно, мутаций нет.</param>
/// <param name="Updated">Расходящиеся значения обновлены.</param>
/// <param name="Added">Отсутствующие в живом конфиге добавлены.</param>
/// <param name="Removed">Лишние ключи живого конфига удаляются null-патчем.</param>
/// <param name="PostmasterTouched">Патч затрагивает хотя бы одно postmaster-имя
/// (только для текста журнала — решения на списке НЕ строятся, spec §4.3 п.5).</param>
public sealed record ConvergenceDivergence(
    string? Patch, int Updated, int Added, int Removed, bool PostmasterTouched);

/// <summary>
/// Патч-билдер конвергенции DCS-конфига (t11, arch/14 §5 C; поглощает
/// PatroniTimings.DivergencePatch t09 — тайминги по-прежнему из PatroniTimings):
/// расхождение фактического динамического конфига (GET /config, JSON) с каноном
/// → минимальный патч-документ для PATCH /config. Конвергентно → null —
/// мутаций нет («не второй регулярный писатель», t09). desiredParameters ==
/// null → патч только таймингов (нет/неполна заявка — домен не наш). Битый/
/// чужой JSON → полный патч (безопасный исход t09: непонятный конфиг
/// приводится к канону). Нормализация НЕ семантическая: "2048MB" ≠ "2GB",
/// "on" ≠ "true" — источники нашего формата едины (bootstrap из того же
/// набора), посторонние форматы конвергируются первым патчем и далее стабильны.
/// Порядок ключей патча: тайминги, затем параметры в порядке желаемого набора,
/// удаления — в конце (стабильный детерминированный документ).
/// </summary>
public static class DcsConfigConvergence
{
    /// <summary>
    /// Имена pg_settings.context=postmaster, реально встречающиеся в
    /// PGTune-выводе/каноне — ТОЛЬКО для текста журнала (пометка
    /// pending_restart); решений на этом списке не строятся (spec §4.3 п.5).
    /// </summary>
    public static readonly IReadOnlySet<string> PostmasterParameters = new HashSet<string>(
        StringComparer.Ordinal)
    {
        "max_connections", "shared_buffers", "huge_pages", "wal_buffers",
        "max_worker_processes", "max_parallel_workers", "autovacuum_max_workers",
        "autovacuum_work_mem", "wal_level", "max_wal_senders", "max_replication_slots",
    };

    /// <summary>Минимальный патч-документ для PATCH /config; null — конвергентно.</summary>
    public static string? DivergencePatch(
        string? configJson, IReadOnlyList<(string Name, string RawValue)>? desiredParameters)
        => Analyze(configJson, desiredParameters).Patch;

    /// <summary>Сверка с итогом для журнала (счётчики updated/added/removed).</summary>
    public static ConvergenceDivergence Analyze(
        string? configJson, IReadOnlyList<(string Name, string RawValue)>? desiredParameters)
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

        var timingPatch = new List<string>();
        AddIfDivergent(timingPatch, root, "ttl", PatroniTimings.Ttl);
        AddIfDivergent(timingPatch, root, "loop_wait", PatroniTimings.LoopWait);
        AddIfDivergent(timingPatch, root, "retry_timeout", PatroniTimings.RetryTimeout);
        AddIfDivergent(timingPatch, root, "synchronous_mode", PatroniTimings.SynchronousMode);

        var updated = 0;
        var added = 0;
        var removed = 0;
        var postmaster = false;
        var parameterPatch = new List<string>();

        if (desiredParameters is not null)
        {
            // Живой блок postgresql.parameters: отсутствует/не объект (в т.ч.
            // битый документ) — все desired считаются добавляемыми.
            var hasLive = false;
            JsonElement parameters = default;
            if (root.ValueKind == JsonValueKind.Object
                && root.TryGetProperty("postgresql", out var postgresql)
                && postgresql.ValueKind == JsonValueKind.Object
                && postgresql.TryGetProperty("parameters", out var parametersElement)
                && parametersElement.ValueKind == JsonValueKind.Object)
            {
                hasLive = true;
                parameters = parametersElement;
            }

            var desiredNames = new HashSet<string>(StringComparer.Ordinal);
            foreach (var (name, rawValue) in desiredParameters)
            {
                desiredNames.Add(name);

                // Нормализация живого значения к строке: строка — как есть;
                // число — инвариантный GetRawText() (60 → "60"); bool —
                // "true"/"false"; null/объект/массив — расхождение (в патч).
                string? live = null;
                var keyExists = false;
                if (hasLive && parameters.TryGetProperty(name, out var value))
                {
                    keyExists = true;
                    live = value.ValueKind switch
                    {
                        JsonValueKind.String => value.GetString(),
                        JsonValueKind.Number => value.GetRawText(),
                        JsonValueKind.True => "true",
                        JsonValueKind.False => "false",
                        _ => null,
                    };
                }

                if (live is not null && string.Equals(live, rawValue, StringComparison.Ordinal))
                    continue; // конвергентно

                if (keyExists)
                    updated++; // ключ есть, значение разошлось/не нормализуемо
                else
                    added++; // ключа в живом конфиге нет

                parameterPatch.Add($"{JsonSerializer.Serialize(name)}:{JsonSerializer.Serialize(rawValue)}");
                if (PostmasterParameters.Contains(name))
                    postmaster = true;
            }

            // Лишние ключи живого конфига (нет в desired) → удаление null-значением
            // (Patroni-семантика PATCH /config: null удаляет ключ). Удаления — в конце.
            if (hasLive)
                foreach (var property in parameters.EnumerateObject())
                    if (!desiredNames.Contains(property.Name))
                    {
                        removed++;
                        parameterPatch.Add($"{JsonSerializer.Serialize(property.Name)}:null");
                    }
        }

        // Сборка ОДНОГО документа: тайминги, затем вложенный postgresql.parameters
        // (если есть параметрическая часть); пустой — null (конвергентно).
        var parts = new List<string>(timingPatch);
        if (parameterPatch.Count > 0)
            parts.Add($"\"postgresql\":{{\"parameters\":{{{string.Join(",", parameterPatch)}}}}}");
        var patch = parts.Count == 0 ? null : $"{{{string.Join(",", parts)}}}";

        return new ConvergenceDivergence(patch, updated, added, removed, postmaster);
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
