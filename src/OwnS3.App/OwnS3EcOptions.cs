using OwnS3.Storage.Erasure;

namespace OwnS3.App;

// Парсинг/валидации EC-конфигурации (канон 06 §1): список путей OWNS3_DATA_DIR
// (запятая; trim; пустой элемент или дубликат — отказ старта ДО Kestrel),
// топология (setSize env или авто-дефолт; валидации N ≥ 2, N % setSize == 0,
// setSize 4–16, parity 1..setSize/2).
public static class OwnS3EcOptions
{
    // Список путей данных; пустой элемент/дубликат — InvalidOperationException.
    public static IReadOnlyList<string> ParseDataDirs(string raw)
    {
        // пустой элемент списка — отказ (пустой путь — не диск)
        var parts = raw.Split(',', StringSplitOptions.TrimEntries);
        if (parts.Length == 0 || parts.Any(string.IsNullOrEmpty))
            throw new InvalidOperationException("OWNS3_DATA_DIR пуст или содержит пустой элемент — отказ старта");
        var distinct = new HashSet<string>(parts, StringComparer.Ordinal);
        if (distinct.Count != parts.Length)
            throw new InvalidOperationException(
                "OWNS3_DATA_DIR: дубликат пути в списке — отказ старта (порядок путей = канонический порядок дисков)");
        return parts;
    }

    // Топология по числу путей и конфигу: setSize null = авто (наибольший делитель
    // из {16,8,4}; нет делителя — отказ). Валидации канона 06 §1.
    public static ErasureTopology BuildTopology(int driveCount, int? setSize, int parity)
    {
        if (driveCount < 2)
            throw new InvalidOperationException($"EC-топология: число дисков {driveCount} меньше 2 — режим XL Erasure требует список путей");
        var effectiveSetSize = setSize ?? ErasureTopology.DefaultSetSize(driveCount);
        if (effectiveSetSize == 0)
            throw new InvalidOperationException(
                $"OWNS3_EC_SET_SIZE не задан, авто-дефолт для {driveCount} путей не существует (нет делителя из 4/8/16) — отказ старта");
        var topology = new ErasureTopology(driveCount, effectiveSetSize, parity);
        topology.Validate();
        return topology;
    }
}
