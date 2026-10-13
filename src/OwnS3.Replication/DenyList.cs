using System.Text.Json;
using System.Text.Json.Serialization;

namespace OwnS3.Replication;

// Запись отзыва соседа (глава 07 §2/spec §4.5): имя + публичный ключ + время.
public sealed record DenyEntry(string Name, string PublicKeyPem, long RevokedAtUnixMs);

// Локальный deny-list репликации (глава 07 §2): отлучённые соседи; join отказывает
// по имени ИЛИ публичному ключу (смена ключа под новым именем не обходит отзыв).
// Файл <sysDir>/replication/denylist.json — НЕ реплицируется: отлучение по всей
// mesh — оркестратор зовёт delete с revoke на каждом сервере. Персист tmp+rename;
// каталог создаётся при первой записи. Битый файл — fail-fast старта (повреждён
// вручную), не «тихий пустой список».
public sealed class DenyList(string filePath, TimeProvider timeProvider)
{
    private static readonly JsonSerializerOptions Json = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.Never,
    };

    private readonly object _lock = new();
    private readonly Dictionary<string, DenyEntry> _entries = LoadFile(filePath);

    // Файла нет → пуст (отзывов ещё не было); битый JSON → InvalidOperationException.
    private static Dictionary<string, DenyEntry> LoadFile(string path)
    {
        if (!File.Exists(path))
            return new Dictionary<string, DenyEntry>(StringComparer.Ordinal);
        List<DenyEntry>? entries;
        try
        {
            entries = JsonSerializer.Deserialize<List<DenyEntry>>(File.ReadAllText(path), Json);
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException(
                $"denylist.json повреждён (fail-fast): {path} — {ex.Message}", ex);
        }
        if (entries is null)
            throw new InvalidOperationException($"denylist.json: пустой файл {path}");
        var map = new Dictionary<string, DenyEntry>(StringComparer.Ordinal);
        foreach (var entry in entries)
            map[entry.Name] = entry;
        return map;
    }

    // Решение отзыва: совпадение имени ИЛИ публичного ключа (глава 07 §2).
    public bool IsDenied(string name, string publicKeyPem)
    {
        lock (_lock)
            return _entries.Values.Any(e =>
                string.Equals(e.Name, name, StringComparison.Ordinal)
                || string.Equals(e.PublicKeyPem, publicKeyPem, StringComparison.Ordinal));
    }

    // Отзыв (delete ?revoke=true, глава 07 §2): идемпотентен — повторный Add
    // обновляет RevokedAtUnixMs; персист tmp+rename; каталог — при первой записи.
    public void Add(string name, string publicKeyPem)
    {
        lock (_lock)
        {
            _entries[name] = new DenyEntry(name, publicKeyPem,
                timeProvider.GetUtcNow().ToUnixTimeMilliseconds());
            Persist();
        }
    }

    // Снятие отзыва (DELETE /admin/replication/denylist/{name}, глава 07 §2):
    // true — имя было в списке; неизвестное — false без IO.
    public bool Remove(string name)
    {
        lock (_lock)
        {
            if (!_entries.Remove(name))
                return false;
            Persist();
            return true;
        }
    }

    // Текущий список отзывов (снапшот для GET admin-грани).
    public IReadOnlyList<DenyEntry> Snapshot
    {
        get { lock (_lock) return _entries.Values.ToList().AsReadOnly(); }
    }

    // Персист атомарно (tmp + rename) — образец ReplicationTargets.Persist.
    private void Persist()
    {
        var dir = Path.GetDirectoryName(filePath);
        if (!string.IsNullOrEmpty(dir))
            Directory.CreateDirectory(dir);
        var tmp = filePath + ".tmp";
        File.WriteAllText(tmp, JsonSerializer.Serialize(_entries.Values.ToList(), Json));
        File.Move(tmp, filePath, overwrite: true);
    }
}
