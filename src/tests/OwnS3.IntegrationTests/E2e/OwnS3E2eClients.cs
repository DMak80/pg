using System.Security.Cryptography;
using System.Text;
using Amazon.Runtime;
using Amazon.S3;

namespace OwnS3.IntegrationTests.E2e;

/// <summary>Роль клиента окружения (литералы e2e-секретов; матрица 05 §3).</summary>
public enum OwnS3Role { Root, Writer, Reader, Admin }

/// <summary>Режим flexible checksums клиента (канон arch/owns3/03 §3).
/// Per-client: AmazonS3Config.RequestChecksumCalculation — глобальной статики
/// AWSConfigs.RequestChecksumCalculation в AWSSDK.Core 3.7.501.1 нет
/// (проверено компиляцией; единственный компилируемый способ настройки).</summary>
public enum OwnS3ChecksumMode { WhenRequired, WhenSupported }

/// <summary>Реальные клиенты E2E-серии: фабрика AWS SDK .NET (никаких
/// собственных подписантов) + хелперы ETag/данных.</summary>
public static class OwnS3E2eClients
{
    /// <summary>Клиент AWS SDK .NET против ownS3-окружения: path-style,
    /// us-east-1, таймаут 10 c; режим checksum — параметром (основной —
    /// WhenRequired: режим, в который обязан быть настроен клиент бэкапов,
    /// канон 03 §3 — иначе свежий SDK шлёт неподдерживаемый
    /// STREAMING-UNSIGNED-PAYLOAD-TRAILER → 400).</summary>
    public static IAmazonS3 CreateS3(OwnS3E2eEnvironment fx, OwnS3Role role,
        OwnS3ChecksumMode checksum = OwnS3ChecksumMode.WhenRequired)
    {
        var (access, secret) = role switch
        {
            OwnS3Role.Root => (OwnS3E2eEnvironment.RootUser, OwnS3E2eEnvironment.RootPassword),
            OwnS3Role.Writer => (OwnS3E2eEnvironment.WriterAccessKey, OwnS3E2eEnvironment.WriterSecretKey),
            OwnS3Role.Reader => (OwnS3E2eEnvironment.ReaderAccessKey, OwnS3E2eEnvironment.ReaderSecretKey),
            OwnS3Role.Admin => (OwnS3E2eEnvironment.AdminAccessKey, OwnS3E2eEnvironment.AdminSecretKey),
            _ => throw new ArgumentOutOfRangeException(nameof(role)),
        };
        var config = new AmazonS3Config
        {
            ServiceURL = fx.S3BaseUrl,
            ForcePathStyle = true,
            AuthenticationRegion = "us-east-1",
            Timeout = TimeSpan.FromSeconds(10),
            RequestChecksumCalculation = checksum == OwnS3ChecksumMode.WhenRequired
                ? RequestChecksumCalculation.WHEN_REQUIRED
                : RequestChecksumCalculation.WHEN_SUPPORTED,
        };
        return new AmazonS3Client(new BasicAWSCredentials(access, secret), config);
    }
}

/// <summary>ETag-семантика (канон arch/owns3/02 §1): MD5 простого PUT;
/// составной multipart-ETag «N-md5(concat БАЙТОВ hex-ETag'ов частей)».
/// Сверки тест считает сам.</summary>
public static class OwnS3E2eETag
{
    /// <summary>Значение для conditional-заголовков SDK (ETag — в кавычках).</summary>
    public static string Quote(string etag) => $"\"{etag.Trim('"')}\"";

    public static string Md5Hex(byte[] body)
        => Convert.ToHexString(MD5.HashData(body)).ToLowerInvariant();

    public static string Multipart(int parts, IReadOnlyList<string> partEtags)
    {
        // Канон 02 §1: конкатенация hex-СТРОК ETag частей без разделителей,
        // md5 от неё (ASCII-байты hex-текста — как считает сервер).
        var concat = string.Concat(partEtags.Select(e => e.Trim('"')));
        return $"{parts}-{Md5Hex(Encoding.ASCII.GetBytes(concat))}";
    }
}

/// <summary>Детерминированные тела тестов (seed; размеры КиБ–МиБ).</summary>
public static class OwnS3E2eBytes
{
    public static byte[] Deterministic(long length, int seed)
    {
        var data = new byte[length];
        new Random(seed).NextBytes(data);
        return data;
    }
}
