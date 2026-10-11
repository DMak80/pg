using System.Text.Json;

namespace OwnS3.Storage.Erasure;

// Модель volume.json v2 (camelCase-JSON, канон 06 §2) + IO на диске позиции.
internal sealed record ErasureVolumeInfo(string Magic, int FormatVersion, string VolumeId, string Mode,
    string DeploymentId, string DiskId, int SetSize, string[][] Sets, int[] Distribution, string DistributionAlgo)
{
    internal const string MagicValue = "OWNS3-VOL";
    internal const int CurrentFormatVersion = 2;
    internal const string ModeValue = "XL Erasure";
    internal const string DistributionAlgoValue = "SIPMOD";

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    // Путь volume.json позиции (в .owns3.sys корня диска).
    internal static string VolumeJsonPath(string root) => Path.Combine(root, ".owns3.sys", "volume.json");

    // Чтение: битый JSON/чужая структура — InvalidOperationException (fail-fast всего
    // сервиса — правила валидации старта применяет ErasurePool.Initialize).
    internal static ErasureVolumeInfo Read(string root)
    {
        var path = VolumeJsonPath(root);
        ErasureVolumeInfo info;
        try
        {
            info = JsonSerializer.Deserialize<ErasureVolumeInfo>(File.ReadAllText(path), JsonOptions)
                ?? throw new InvalidOperationException($"volume.json {path}: пуст");
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException($"volume.json {path}: не читается — {ex.Message}");
        }

        return info;
    }

    // Атомарная запись: tmp + rename (по образцу XlMetaFile.Write).
    internal static void Write(string root, ErasureVolumeInfo info)
    {
        var path = VolumeJsonPath(root);
        var dir = Path.GetDirectoryName(path)!;
        Directory.CreateDirectory(dir);
        var tmp = path + ".tmp";
        using (var stream = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            var bytes = JsonSerializer.SerializeToUtf8Bytes(info, JsonOptions);
            stream.Write(bytes, 0, bytes.Length);
            stream.Flush(flushToDisk: true);
        }

        File.Move(tmp, path, overwrite: true);
    }
}
