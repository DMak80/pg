using System.Text.Json;
using PgWorker.Etcd.Client;

namespace PgWorker.UnitTests;

// Загрузчик JSON-фикстур парсеров: массив {"key","value","modRevision"} → декодированные Kv.
// Фикстуры копируются в выходной каталог (None-ItemGroup csproj).
public static class EtcdFixtures
{
    // Формат файла фикстуры: [{"key":"/…","value":"…","modRevision":n}, …].
    private sealed record FixtureKv(string Key, string Value, ulong ModRevision);

    private static readonly JsonSerializerOptions Json = new() { PropertyNameCaseInsensitive = true };

    public static IReadOnlyList<Kv> LoadKv(string fileName)
    {
        var path = Path.Combine(AppContext.BaseDirectory, "EtcdFixtures", fileName);
        var items = JsonSerializer.Deserialize<List<FixtureKv>>(File.ReadAllText(path), Json) ?? [];
        return items.Select(i => new Kv(i.Key, i.Value, i.ModRevision)).ToList();
    }

    public static string LoadText(string fileName)
        => File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "EtcdFixtures", fileName));
}
