namespace PgWorker.Core.Tuning;

/// <summary>
/// Единый желаемый набор pg-параметров (t11, arch/14 §2.1/§5 C):
/// merge(PGTune ∪ канон) — ОДИН источник и для bootstrap (SpiloEnvBuilder,
/// SPILO_CONFIGURATION), и для конвергенции живого DCS-конфига
/// (DcsConfigConvergence, PATCH /config). Bootstrap и конвергенция не могут
/// разойтись по определению. Значения — СЫРЫЕ строки ("60", "2047MB", "on",
/// "logical") без YAML/JSON-обвязки: цитирование — деталь сериализатора.
/// </summary>
public static class PgParametersCanon
{
    /// <summary>
    /// Канон PgWorker поверх PGTune: P3 (логическое декодирование + failover
    /// slots, wal_level=logical всегда) и лог-блок. Константы
    /// max_connections/shared_buffers/effective_cache_size/
    /// checkpoint_completion_target/random_page_cost из канона УБРАТЫ — их
    /// несёт PGTune. Значения БЕЗ кавычек (переехали из SpiloEnvBuilder, t11).
    /// </summary>
    public static readonly IReadOnlyList<(string Name, string RawValue)> CanonParameters =
    [
        ("wal_level", "logical"), // P3: логическое декодирование + failover slots
        ("hot_standby", "on"),
        ("sync_replication_slots", "on"),
        ("max_slot_wal_keep_size", "16GB"),
        ("max_wal_senders", "10"),
        ("max_replication_slots", "10"),
        ("wal_keep_size", "2048MB"),
        ("checkpoint_timeout", "15min"),
        ("logging_collector", "on"),
        ("log_directory", "log"),
        ("log_filename", "postgresql-%Y-%m-%d.log"),
        ("log_rotation_age", "1d"),
        ("log_rotation_size", "100MB"),
    ];

    /// <summary>
    /// Merge(PGTune ∪ канон): сначала вывод PgTune.Calculate в порядке §5.2
    /// спецификации алгоритма (минус excludeParams — параметр отсутствует
    /// вовсе, никаких пустых значений; exclude применяется ЗДЕСЬ, ядро всегда
    /// даёт полный вывод), затем канон P3/лог-блок поверх с перезаписью по
    /// имени без дубликатов: позиция первого вхождения сохраняется, новые
    /// ключи канона — в конец.
    /// </summary>
    public static IReadOnlyList<(string Name, string RawValue)> Desired(
        PgTuneResult tuning, IReadOnlySet<string>? excludeParams)
    {
        var exclude = excludeParams ?? new HashSet<string>(StringComparer.Ordinal);
        var lines = new List<(string Name, string RawValue)>();
        var positions = new Dictionary<string, int>(StringComparer.Ordinal);

        void Upsert(string name, string rawValue)
        {
            if (positions.TryGetValue(name, out var position))
            {
                lines[position] = (name, rawValue);
                return;
            }

            positions[name] = lines.Count;
            lines.Add((name, rawValue));
        }

        foreach (var parameter in tuning.Parameters)
            if (!exclude.Contains(parameter.Name))
                Upsert(parameter.Name, parameter.Value);
        foreach (var (name, rawValue) in CanonParameters)
            Upsert(name, rawValue);

        return lines;
    }
}
