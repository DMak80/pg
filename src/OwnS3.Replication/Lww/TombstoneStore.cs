using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace OwnS3.Replication.Lww;

// Tombstone-хранилище приёмника (t35, глава 07 §7): негативные записи
// tombstones/<sha256hex(bucket)>/<sha256hex(key)>.json {bucket, key, deleteTimeUnixMs}.
// Хранилище ЕДИНО: пишется и локальным клиентским DELETE (fetch-ответы «ключ удалён»),
// и применением реплики object-delete, и как защита от воскрешения.
// Retention-чистку по расписанию зовёт цикл обслуживания (§8-а) — сам store чисток
// не планирует.
public sealed class TombstoneStore(string tombstonesDir)
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    // Модель файла tombstone.
    private sealed record TombstoneFile(string Bucket, string Key, long DeleteTimeUnixMs);

    // Запись удаления: файл хранит МАКСИМУМ (новейшее) время — старый tombstone
    // не откатывает новый.
    public void Put(string bucket, string key, long deleteTimeUnixMs)
    {
        var existing = GetDeleteTime(bucket, key);
        if (existing is not null && existing >= deleteTimeUnixMs)
            return;
        var dir = Path.Combine(tombstonesDir, Sha256Hex(bucket));
        Directory.CreateDirectory(dir);
        var tmp = Path.Combine(dir, Sha256Hex(key) + ".json.tmp");
        var path = Path.Combine(dir, Sha256Hex(key) + ".json");
        File.WriteAllText(tmp, JsonSerializer.Serialize(new TombstoneFile(bucket, key, deleteTimeUnixMs), JsonOptions));
        File.Move(tmp, path, overwrite: true);
    }

    // null — tombstone-знания нет.
    public long? GetDeleteTime(string bucket, string key)
    {
        var path = Path.Combine(tombstonesDir, Sha256Hex(bucket), Sha256Hex(key) + ".json");
        if (!File.Exists(path))
            return null;
        try
        {
            var file = JsonSerializer.Deserialize<TombstoneFile>(File.ReadAllText(path), JsonOptions);
            return file?.DeleteTimeUnixMs;
        }
        catch (JsonException)
        {
            return null; // битая запись — знания нет (не роняет приём)
        }
    }

    // Снятие tombstone: применён новее-tombstone PUT (запись жива — негативное знание снято).
    public void Remove(string bucket, string key)
    {
        var path = Path.Combine(tombstonesDir, Sha256Hex(bucket), Sha256Hex(key) + ".json");
        if (File.Exists(path))
            File.Delete(path);
    }

    // Retention-чистка (зовёт §8-а): tombstones старше retentionHours удаляются;
    // возвращает число удалённых. После истечения retention опоздавший старый PUT
    // применяется свободно — окно воскрешения ограничено retention (глава 07 §12).
    public int Cleanup(DateTimeOffset now, int retentionHours)
    {
        if (!Directory.Exists(tombstonesDir))
            return 0;
        var removed = 0;
        var threshold = now.AddHours(-retentionHours).ToUnixTimeMilliseconds();
        foreach (var dir in Directory.EnumerateDirectories(tombstonesDir))
        foreach (var path in Directory.EnumerateFiles(dir, "*.json"))
        {
            try
            {
                var file = JsonSerializer.Deserialize<TombstoneFile>(File.ReadAllText(path), JsonOptions);
                if (file is null || file.DeleteTimeUnixMs < threshold)
                {
                    File.Delete(path);
                    removed++;
                }
            }
            catch (JsonException)
            {
                File.Delete(path);
                removed++;
            }
        }
        return removed;
    }

    private static string Sha256Hex(string value) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value))).ToLowerInvariant();
}
