namespace OwnS3.Protocol.Requests;

// Разбор path-style пути (arch/owns3/03 §6): '/' | '/{bucket}' | '/{bucket}/'
// | '/{bucket}/{key+}'; сегменты ключа URL-декодируются, декодированный '/'
// — часть ключа.
public static class S3PathParser
{
    public static S3Path Parse(string rawPath)
    {
        // Срезается только ведущий '/': хвостовой слэш — часть ключа S3
        // (кроме вырожденного /{bucket}/ — bucket-level, ниже).
        var path = rawPath.StartsWith('/') ? rawPath[1..] : rawPath;
        if (path.Length == 0)
            return new S3Path(null, null);

        var slash = path.IndexOf('/');
        if (slash < 0)
            return new S3Path(path, null);

        var bucket = path[..slash];
        var rawKey = path[(slash + 1)..];
        // /{bucket}/ (trailing slash, пустой ключ) — bucket-level запрос
        // (канон 03 §6): пустой ключ = его отсутствие; внутренние пустые
        // сегменты (a//b) не затрагиваются — rawKey непустой.
        if (rawKey.Length == 0)
            return new S3Path(bucket, null);
        // Декодирование по сегментам: пустые сегменты сохраняются (ключи со слэшами).
        var decodedKey = string.Join("/", rawKey.Split('/').Select(Uri.UnescapeDataString));
        return new S3Path(bucket, decodedKey);
    }
}

/// <summary>Разобранный path-style путь: бакет и декодированный ключ (могут быть null).</summary>
public sealed record S3Path(string? Bucket, string? Key);
