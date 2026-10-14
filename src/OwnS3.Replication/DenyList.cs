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

    // Полная перезапись deny-list (PUT /replication/denylist, глава 07 §2):
    // идемпотентная замена всего списка; валидация ДО персиста — невалидный состав
    // бросает ArgumentException, снапшот не тронут (прецедент ReplicationTargets.Replace);
    // каждая запись получает revokedAtUnixMs = момент применения перезаписи.
    public void Replace(IReadOnlyList<(string Name, string PublicKeyPem)> entries)
    {
        ValidateEntries(entries);
        lock (_lock)
        {
            var now = timeProvider.GetUtcNow().ToUnixTimeMilliseconds();
            _entries.Clear();
            foreach (var (name, publicKeyPem) in entries)
                _entries[name] = new DenyEntry(name, publicKeyPem, now);
            Persist();
        }
    }

    // Валидация записей перезаписи (глава 07 §2): непустые name/publicKeyPem, PEM
    // публичного ключа ECDSA P-256, уникальность имён и ключей (образец
    // ReplicationTargets.Validate — текст ошибок по-русски, имя параметра entries).
    private static void ValidateEntries(IReadOnlyList<(string Name, string PublicKeyPem)> entries)
    {
        var nameDuplicates = DuplicatesOf(entries.Select(e => e.Name).ToList());
        if (nameDuplicates.Count > 0)
            throw new ArgumentException(
                $"Дубликаты имён записей deny-list: {string.Join(",", nameDuplicates)}", nameof(entries));
        var keyDuplicates = DuplicatesOf(entries.Select(e => e.PublicKeyPem).ToList());
        if (keyDuplicates.Count > 0)
            throw new ArgumentException(
                $"Дубликаты публичных ключей deny-list: {string.Join(",", keyDuplicates)}", nameof(entries));
        foreach (var (name, publicKeyPem) in entries)
        {
            if (string.IsNullOrWhiteSpace(name) || string.IsNullOrWhiteSpace(publicKeyPem))
                throw new ArgumentException(
                    $"Запись deny-list {name}: name и publicKeyPem обязательны", nameof(entries));
            try
            {
                using var key = System.Security.Cryptography.ECDsa.Create();
                key.ImportFromPem(publicKeyPem); // SUBJECT PUBLIC KEY ECDSA P-256
            }
            catch (Exception ex)
            {
                throw new ArgumentException(
                    $"Запись deny-list {name}: невалидный PEM публичного ключа ECDSA P-256 — {ex.Message}",
                    nameof(entries));
            }
        }
    }

    private static List<string> DuplicatesOf(List<string> values) =>
        values.GroupBy(v => v, StringComparer.Ordinal).Where(g => g.Count() > 1)
            .Select(g => g.Key).ToList();

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
