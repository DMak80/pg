using System.Globalization;
using Microsoft.AspNetCore.Http;
using OwnS3.Protocol.Errors;

namespace OwnS3.App.Handlers;

// Чистые протокольные валидации аргументов (глава 02 + глава 03 §6): тестируются
// юнитами без хоста; ошибки — канонические коды S3.
public static class OperationValidation
{
    public const long MaxObjectSize = 5L * 1024 * 1024 * 1024;   // 5 ГБ (глава 02)
    public const int MaxParts = 10000;
    public const int MaxDeleteKeys = 1000;
    public const int DefaultPagingLimit = 1000;

    // partNumber: обязателен, число, 1–10000 (глава 02 UploadPart).
    public static int ParsePartNumber(string? value)
    {
        if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var part))
            throw new S3ProtocolException(S3ErrorCode.InvalidArgument, "Part number must be an integer between 1 and 10000, inclusive");
        if (part is < 1 or > MaxParts)
            throw new S3ProtocolException(S3ErrorCode.InvalidArgument, "Part number must be an integer between 1 and 10000, inclusive");
        return part;
    }

    // Лимит DeleteObjects: пустой или > 1000 ключей → MalformedXML (глава 02).
    public static void ValidateDeleteKeys(int count)
    {
        if (count is 0 or > MaxDeleteKeys)
            throw new S3ProtocolException(S3ErrorCode.MalformedXML,
                "The XML you provided was not well-formed or did not validate against our published schema.");
    }

    // Манифест Complete: минимум 1 часть, порядок строго возрастает (глава 02).
    public static void ValidateCompleteManifest(IReadOnlyList<(int PartNumber, string ETag)> parts)
    {
        if (parts.Count == 0)
            throw new S3ProtocolException(S3ErrorCode.MalformedXML,
                "The XML you provided was not well-formed or did not validate against our published schema.");
        for (var i = 1; i < parts.Count; i++)
        {
            if (parts[i].PartNumber <= parts[i - 1].PartNumber)
                throw new S3ProtocolException(S3ErrorCode.InvalidPartOrder,
                    "The list of parts was not in ascending order. The parts list must be specified in order by part number.");
        }
    }

    // encoding-type: единственное значение url (глава 02, листинги).
    public static string? ParseEncodingType(string? value)
    {
        if (value is null)
            return null;
        if (value != "url")
            throw new S3ProtocolException(S3ErrorCode.InvalidArgument,
                "Invalid Encoding Method specified in Request");
        return value;
    }

    // max-keys/max-parts/max-uploads: дефолт 1000, максимум 1000 (обрезка),
    // отрицательное — InvalidArgument (глава 02).
    public static int ParsePagingLimit(string? value, string parameterName)
    {
        if (value is null)
            return DefaultPagingLimit;
        if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) || parsed < 0)
            throw new S3ProtocolException(S3ErrorCode.InvalidArgument,
                $"Argument {parameterName} must be an integer between 0 and 2147483647");
        return Math.Min(parsed, DefaultPagingLimit);
    }

    // x-amz-metadata-directive: строго COPY/REPLACE (референс — точное сравнение).
    public static string ParseMetadataDirective(string? value)
    {
        var directive = value ?? "COPY";
        if (directive is not ("COPY" or "REPLACE"))
            throw new S3ProtocolException(S3ErrorCode.InvalidArgument,
                "Unknown metadata directive.");
        return directive;
    }

    // x-amz-copy-source: <bucket>/<key> или /<bucket>/<key>, URL-декодированный
    // (глава 02; AWS SDK .NET шлёт без ведущего слэша — обе формы принимаются).
    public static (string Bucket, string Key) ParseCopySource(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new S3ProtocolException(S3ErrorCode.InvalidArgument, "Copy Source must mention the source bucket and key");
        var decoded = Uri.UnescapeDataString(value);
        if (decoded.StartsWith('/'))
            decoded = decoded[1..];
        if (decoded.Length < 3)
            throw new S3ProtocolException(S3ErrorCode.InvalidArgument, "Copy Source must mention the source bucket and key");
        var slash = decoded.IndexOf('/');
        if (slash <= 0 || slash == decoded.Length - 1)
            throw new S3ProtocolException(S3ErrorCode.InvalidArgument, "Copy Source must mention the source bucket and key");
        return (decoded[..slash], decoded[(slash + 1)..]);
    }

    // Range Get/Head: одиночная спецификация распознаётся (bytes=a-b | a- | -N);
    // множественная — трактуется как отсутствующая (200 полным объектом);
    // форматная невалидность (a > b, bytes=-0) — InvalidRange (глава 02).
    public static Storage.ByteRange? ParseRange(string? value)
    {
        if (string.IsNullOrWhiteSpace(value) || !value.StartsWith("bytes=", StringComparison.Ordinal))
            return null;
        var spec = value["bytes=".Length..];
        if (spec.Contains(','))
            return null;   // множественный диапазон — игнорируется (200 полным)
        var dash = spec.IndexOf('-');
        if (dash < 0)
            return null;
        var startText = spec[..dash];
        var endText = spec[(dash + 1)..];

        if (startText.Length == 0)
        {
            // суффиксный bytes=-N; N=0 — невалидная спецификация (416).
            if (endText.Length == 0 || !long.TryParse(endText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var suffix) || suffix <= 0)
                throw new S3ProtocolException(S3ErrorCode.InvalidRange, "The requested range is not satisfiable");
            return new Storage.ByteRange(null, -suffix);
        }

        if (!long.TryParse(startText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var start) || start < 0)
            throw new S3ProtocolException(S3ErrorCode.InvalidRange, "The requested range is not satisfiable");
        if (endText.Length == 0)
            return new Storage.ByteRange(start, null);
        if (!long.TryParse(endText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var end) || end < start)
            throw new S3ProtocolException(S3ErrorCode.InvalidRange, "The requested range is not satisfiable");
        return new Storage.ByteRange(start, end);
    }

    // Conditional-даты — RFC 7231 IMF-fixdate; невалидная игнорируется (глава 02).
    public static DateTimeOffset? ParseHttpDate(string? value) =>
        DateTimeOffset.TryParseExact(value, "R", CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var parsed)
            ? parsed
            : null;

    // EntityTooLarge: объект/часть > 5 ГБ по Content-Length /
    // x-amz-decoded-content-length (уровень хендлера, глава 02).
    public static void ValidateObjectSize(long contentLength)
    {
        if (contentLength > MaxObjectSize)
            throw new S3ProtocolException(S3ErrorCode.EntityTooLarge,
                "Your proposed upload exceeds the maximum allowed size");
    }

    // Wire-имена алгоритмов набора x-amz-checksum-* (симметрия трейлерам, глава 03 §3).
    private static readonly IReadOnlyDictionary<string, ChecksumAlgorithm> KnownChecksumAlgorithms =
        new Dictionary<string, ChecksumAlgorithm>(StringComparer.Ordinal)
        {
            ["crc32"] = ChecksumAlgorithm.Crc32,
            ["crc32c"] = ChecksumAlgorithm.Crc32C,
            ["sha1"] = ChecksumAlgorithm.Sha1,
            ["sha256"] = ChecksumAlgorithm.Sha256,
        };

    // x-amz-checksum-<alg> (глава 02 PutObject): опционален, не более одного
    // на запрос. Возвращает null при отсутствии ИЛИ единственном неизвестном
    // алгоритме (игнор); более одного заголовка семейства (включая неизвестные
    // алгоритмы) → InvalidArgument «Invalid checksum provided.»; известный —
    // валидация base64 и длины по алгоритму (crc32/crc32c — 4 байта, sha1 — 20,
    // sha256 — 32) → ChecksumExpectation, иначе InvalidArgument с фактическим
    // именем алгоритма. x-amz-sdk-checksum-algorithm не читается.
    public static ChecksumExpectation? ParseChecksumHeader(IHeaderDictionary headers)
    {
        const string prefix = "x-amz-checksum-";
        string? knownWireName = null, knownValue = null;
        var familyCount = 0;
        foreach (var (name, values) in headers)
        {
            if (!name.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                continue;
            familyCount++;
            var wireName = name[prefix.Length..].ToLowerInvariant();
            if (KnownChecksumAlgorithms.ContainsKey(wireName))
            {
                knownWireName = wireName;
                knownValue = values.ToString();
            }
        }

        if (familyCount == 0)
            return null;
        if (familyCount > 1)
            throw new S3ProtocolException(S3ErrorCode.InvalidArgument, "Invalid checksum provided.");
        // известный алгоритм и его значение присваиваются атомарно — оба не null.
        if (knownWireName is null || knownValue is null)
            return null;   // единственный заголовок семейства — неизвестный алгоритм: игнор

        var expectedLength = knownWireName is "crc32" or "crc32c" ? 4
            : knownWireName == "sha1" ? 20 : 32;
        Span<byte> decoded = stackalloc byte[32];
        if (!Convert.TryFromBase64String(knownValue, decoded, out var written) || written != expectedLength)
            throw new S3ProtocolException(S3ErrorCode.InvalidArgument,
                $"Value for x-amz-checksum-{knownWireName} header is invalid.");
        return new ChecksumExpectation(KnownChecksumAlgorithms[knownWireName], knownValue);
    }
}

// Алгоритм заголовочной контрольной суммы x-amz-checksum-<alg> (глава 02).
public enum ChecksumAlgorithm { Crc32, Crc32C, Sha1, Sha256 }

// Ожидание клиента: ровно один известный checksum-заголовок + его base64-значение.
public sealed record ChecksumExpectation(ChecksumAlgorithm Algorithm, string ExpectedBase64);
