namespace EtcdNode.Core;

/// <summary>Дифф полной карты ключей (arch/etcdnode/02-snapshots.md §3):
/// сравнение текущей карты key→mod_revision с эталонной картой последней
/// успешной выгрузки. Изменение (появление/исчезновение/рост mod_revision)
/// ХОТЯ БЫ одного ключа ВНЕ списка пульсов — важное.</summary>
public static class ChangeMapDiff
{
    /// <summary>true — есть важные изменения (вне PulseKeys).</summary>
    public static bool IsImportant(
        IReadOnlyDictionary<string, long> baseline,
        IReadOnlyDictionary<string, long> current)
    {
        foreach (var (key, revision) in current)
        {
            if (PulseKeys.IsPulse(key))
                continue;
            if (!baseline.TryGetValue(key, out var was) || was != revision)
                return true; // новый ключ вне пульсов или изменился
        }

        foreach (var key in baseline.Keys)
        {
            if (!PulseKeys.IsPulse(key) && !current.ContainsKey(key))
                return true; // ключ вне пульсов исчез
        }

        return false;
    }
}
