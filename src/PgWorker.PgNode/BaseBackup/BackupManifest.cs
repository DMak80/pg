using System.Text.Json;

namespace PgWorker.PgNode.BaseBackup;

/// <summary>Файл манифеста полного бэкапа: путь, размер и sha256 (hex).</summary>
public sealed record ManifestFile(string Path, long Size, string Sha256Hex);

/// <summary>Разбор backup_manifest сервера (spec t01 §3.5): массив Files с
/// Path/Size/Checksum; записи без Checksum (symlink-записи PG с LinkTarget,
/// spec §6.15) по содержимому не сверяются и в карту не входят. Алгоритм
/// проверки, отличный от SHA256, — исключение (при съёме задан
/// MANIFEST_CHECKSUMS 'SHA256': иной алгоритм означает несовместимость и
/// сверка была бы ложной).</summary>
public static class BackupManifest
{
    public const string Sha256Algorithm = "SHA256";

    /// <summary>Словарь Path→ManifestFile по записям с checksum; битый JSON/
    /// отсутствие обязательных полей у сверяемой записи — JsonException
    /// (verify трактует как permanent-порчу набора).</summary>
    public static IReadOnlyDictionary<string, ManifestFile> ParseFiles(string manifestJson)
    {
        using var doc = JsonDocument.Parse(manifestJson);
        var root = doc.RootElement;
        if (root.ValueKind != JsonValueKind.Object
            || !root.TryGetProperty("Files", out var files)
            || files.ValueKind != JsonValueKind.Array)
            throw new JsonException("backup manifest: Files array is missing");

        var result = new Dictionary<string, ManifestFile>(StringComparer.Ordinal);
        foreach (var entry in files.EnumerateArray())
        {
            if (entry.ValueKind != JsonValueKind.Object
                || !entry.TryGetProperty("Path", out var pathElement)
                || pathElement.GetString() is not { Length: > 0 } path)
                throw new JsonException("backup manifest: malformed file entry");

            // Symlink-запись манифеста PG (LinkTarget) не несёт Checksum:
            // сверка содержимого для неё не определена (в tar это typeflag 2,
            // verify его не хэширует) — пропускаем, манифест остаётся валидным.
            if (!entry.TryGetProperty("Checksum", out var checksumElement)
                || checksumElement.GetString() is not { Length: > 0 } checksum)
                continue;

            if (!entry.TryGetProperty("Size", out var sizeElement)
                || !sizeElement.TryGetInt64(out var size))
                throw new JsonException($"backup manifest: entry '{path}' has no size");

            if (entry.TryGetProperty("Checksum-Algorithm", out var algorithm)
                && !string.Equals(algorithm.GetString(), Sha256Algorithm, StringComparison.Ordinal))
            {
                throw new JsonException(
                    $"backup manifest: unsupported checksum algorithm '{algorithm.GetString()}'");
            }

            result[path] = new ManifestFile(path, size, checksum.ToLowerInvariant());
        }

        return result;
    }
}
