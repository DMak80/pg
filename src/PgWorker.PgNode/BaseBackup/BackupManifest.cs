using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace PgWorker.PgNode.BaseBackup;

/// <summary>Файл манифеста полного бэкапа: путь, размер и sha256 (hex).</summary>
public sealed record ManifestFile(string Path, long Size, string Sha256Hex);

/// <summary>Диапазон WAL манифеста (WAL-Ranges): timeline + Start-LSN/End-LSN
/// (hex-строки формата PG `X/Y`). Сегменты диапазона сервер включил в tar
/// (`pg_wal/<segment>`), но не перечислил в Files — verify сверяет их
/// присутствие по диапазонам (arch/pgnode/05 §2).</summary>
public sealed record ManifestWalRange(uint Timeline, string StartLsn, string EndLsn);

/// <summary>Разбор backup_manifest сервера (spec t01 §3.5): массив Files с
/// Path/Size/Checksum; записи без Checksum (symlink-записи PG с LinkTarget,
/// spec §6.15) по содержимому не сверяются и в карту не входят. Алгоритм
/// проверки, отличный от SHA256, — исключение (при съёме задан
/// MANIFEST_CHECKSUMS 'SHA256': иной алгоритм означает несовместимость и
/// сверка была бы ложной). Ошибки ФОРМЫ JSON (в т.ч. поле-не-строка —
/// InvalidOperationException у JsonElement) нормализуются в JsonException:
/// verify трактует их как permanent-порчу набора, а не транзиент.</summary>
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
                || StringValue(pathElement) is not { Length: > 0 } path)
                throw new JsonException("backup manifest: malformed file entry");

            // Symlink-запись манифеста PG (LinkTarget) не несёт Checksum:
            // сверка содержимого для неё не определена (в tar это typeflag 2,
            // verify его не хэширует) — пропускаем, манифест остаётся валидным.
            if (!entry.TryGetProperty("Checksum", out var checksumElement)
                || StringValue(checksumElement) is not { Length: > 0 } checksum)
                continue;

            if (!entry.TryGetProperty("Size", out var sizeElement)
                || sizeElement.ValueKind != JsonValueKind.Number
                || !sizeElement.TryGetInt64(out var size))
                throw new JsonException($"backup manifest: entry '{path}' has no size");

            if (entry.TryGetProperty("Checksum-Algorithm", out var algorithm)
                && !string.Equals(StringValue(algorithm), Sha256Algorithm, StringComparison.Ordinal))
            {
                throw new JsonException(
                    $"backup manifest: unsupported checksum algorithm '{StringValue(algorithm)}'");
            }

            result[path] = new ManifestFile(path, size, checksum.ToLowerInvariant());
        }

        return result;
    }

    /// <summary>WAL-Ranges манифеста (Timeline/Start-LSN/End-LSN). Поля нет —
    /// пусто (сверять нечего); форма нарушена — JsonException (permanent).</summary>
    public static IReadOnlyList<ManifestWalRange> ParseWalRanges(string manifestJson)
    {
        using var doc = JsonDocument.Parse(manifestJson);
        if (doc.RootElement.ValueKind != JsonValueKind.Object
            || !doc.RootElement.TryGetProperty("WAL-Ranges", out var ranges))
            return [];

        if (ranges.ValueKind != JsonValueKind.Array)
            throw new JsonException("backup manifest: WAL-Ranges is not an array");

        var result = new List<ManifestWalRange>();
        foreach (var range in ranges.EnumerateArray())
        {
            if (range.ValueKind != JsonValueKind.Object
                || !range.TryGetProperty("Timeline", out var timelineElement)
                || timelineElement.ValueKind != JsonValueKind.Number
                || !timelineElement.TryGetUInt32(out var timeline)
                || StringValue(range.TryGetProperty("Start-LSN", out var startElement) ? startElement : default)
                    is not { Length: > 0 } startLsn
                || StringValue(range.TryGetProperty("End-LSN", out var endElement) ? endElement : default)
                    is not { Length: > 0 } endLsn)
                throw new JsonException("backup manifest: malformed WAL-Ranges entry");

            result.Add(new ManifestWalRange(timeline, startLsn, endLsn));
        }

        return result;
    }

    /// <summary>Проверка `Manifest-Checksum` (PG18, backup_manifest.c
    /// SendBackupManifest): хэш SHA-256 считается по содержимому манифеста ДО
    /// добавления самого поля — всё, что стоит перед литералом
    /// `"Manifest-Checksum"` (включая завершающую запятую), и ничего после.
    /// null — поля нет (сверка не определена); false — значение есть, но не
    /// сходится с содержимым (порча манифеста).</summary>
    public static bool? VerifyChecksum(string manifestJson)
    {
        const string marker = "\"Manifest-Checksum\"";
        const int sha256HexLength = 64;
        var keyIndex = manifestJson.IndexOf(marker, StringComparison.Ordinal);
        if (keyIndex < 0)
            return null;
        var colon = manifestJson.IndexOf(':', keyIndex + marker.Length);
        if (colon < 0)
            return false;
        var quote = manifestJson.IndexOf('"', colon + 1);
        if (quote < 0)
            return false;
        var end = manifestJson.IndexOf('"', quote + 1);
        if (end < 0)
            return false;
        var value = manifestJson[(quote + 1)..end];
        if (value.Length != sha256HexLength)
            return false;
        var hashed = manifestJson[..keyIndex];
        var actual = Convert.ToHexString(
            SHA256.HashData(Encoding.UTF8.GetBytes(hashed))).ToLowerInvariant();
        return string.Equals(actual, value.ToLowerInvariant(), StringComparison.Ordinal);
    }

    // Строковое поле JSON: не-строка — null (вызывающий решает: отсутствие
    // обязательного поля — JsonException, необязательного — пропуск).
    // GetString() на числе/объекте бросает InvalidOperationException — эту
    // форму ошибки нормализует этот хелпер (permanent-порча, не транзиент).
    private static string? StringValue(JsonElement element)
        => element.ValueKind == JsonValueKind.String ? element.GetString() : null;
}
