namespace OwnS3.Protocol.Requests;

// Коллекция заголовков модели (arch/owns3/03 §1): регистронезависимый доступ по
// имени, сырые ключи сохранены (для canonical headers и diagnostics).
public sealed class S3HeaderCollection
{
    private readonly List<(string Name, string Value)> _entries;

    private S3HeaderCollection(List<(string, string)> entries) => _entries = entries;

    public static S3HeaderCollection FromPairs(params (string Name, string Value)[] pairs) =>
        new([.. pairs]);

    public static S3HeaderCollection FromPairs(IEnumerable<(string Name, string Value)> pairs) =>
        new([.. pairs]);

    public static S3HeaderCollection Empty { get; } = new([]);

    /// <summary>Первое значение заголовка (регистронезависимо) или null.</summary>
    public string? First(string name) =>
        _entries.FirstOrDefault(e => string.Equals(e.Name, name, StringComparison.OrdinalIgnoreCase)).Value;

    /// <summary>Все значения заголовка в порядке следования (регистронезависимо).</summary>
    public IReadOnlyList<string> Values(string name) =>
        _entries.Where(e => string.Equals(e.Name, name, StringComparison.OrdinalIgnoreCase))
            .Select(e => e.Value)
            .ToList();

    /// <summary>Сырые пары имя/значение как пришли (для канонизации и лога).</summary>
    public IEnumerable<(string Name, string Value)> Raw => _entries;
}
