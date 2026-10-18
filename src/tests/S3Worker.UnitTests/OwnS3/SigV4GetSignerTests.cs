using System.Security.Cryptography;
using System.Text;
using FluentAssertions;
using OwnS3.Protocol.Auth;
using OwnS3.Protocol.Errors;
using OwnS3.Protocol.Requests;
using S3Worker.Core.OwnS3;
using Xunit;

namespace S3Worker.UnitTests.OwnS3;

// Клиентская SigV4-подпись GET (owns3/05 §7): паттерн «двойная реализация» —
// ожидаемая подпись пересчитывается в теле теста отдельным кодом (HMAC-цепочка)
// и сверяется с выводом сигнера.
public class SigV4GetSignerTests
{
    private const string AccessKey = "AKIDEXAMPLE";
    private const string SecretKey = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
    private static readonly DateTimeOffset FixedTime =
        new(2026, 1, 15, 10, 30, 0, TimeSpan.Zero);

    [Fact]
    public void Sign_DeterministicVector_ExactHeaderSignature()
    {
        // Arrange: фиксированные дата/креды/путь; ожидание — независимый пересчёт.
        const string path = "/admin/storage";
        const string hostHeader = "h1:9000";
        const string region = "us-east-1";
        const string amzDate = "20260115T103000Z";
        const string date = "20260115";
        const string payloadHash = SigV4GetSigner.UnsignedPayload; // UNSIGNED-PAYLOAD (эталон ownS3)

        // Двойная реализация: canonical request → string-to-sign → ключ → подпись.
        var canonicalRequest =
            $"GET\n{path}\n\nhost:{hostHeader}\n" +
            $"x-amz-content-sha256:{payloadHash}\nx-amz-date:{amzDate}\n" +
            "\n" +
            "host;x-amz-content-sha256;x-amz-date\n" + payloadHash;
        var scope = $"{date}/{region}/s3/aws4_request";
        var stringToSign =
            $"AWS4-HMAC-SHA256\n{amzDate}\n{scope}\n" +
            Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(canonicalRequest))).ToLowerInvariant();
        var key = Hmac(Encoding.UTF8.GetBytes("AWS4" + SecretKey), Encoding.UTF8.GetBytes(date));
        key = Hmac(key, Encoding.UTF8.GetBytes(region));
        key = Hmac(key, Encoding.UTF8.GetBytes("s3"));
        key = Hmac(key, Encoding.UTF8.GetBytes("aws4_request"));
        var expectedSignature = Convert.ToHexString(Hmac(key, Encoding.UTF8.GetBytes(stringToSign))).ToLowerInvariant();

        // Act
        var signed = SigV4GetSigner.Sign(path, hostHeader, AccessKey, SecretKey, region, FixedTime);

        // Assert: заголовок точен по всем трём секциям.
        signed.AmzDate.Should().Be(amzDate);
        signed.ContentSha256.Should().Be(payloadHash);
        signed.Authorization.Should().Be(
            $"AWS4-HMAC-SHA256 Credential={AccessKey}/{scope}, " +
            "SignedHeaders=host;x-amz-content-sha256;x-amz-date, " +
            $"Signature={expectedSignature}");
    }

    [Fact]
    public void Sign_DifferentDateOrCreds_DifferentSignature()
    {
        // Arrange: два сигна с разными секретами.
        var first = SigV4GetSigner.Sign("/admin/storage", "h1:9000", AccessKey, SecretKey, "us-east-1", FixedTime);
        var second = SigV4GetSigner.Sign("/admin/storage", "h1:9000", AccessKey, "other-secret", "us-east-1", FixedTime);

        // Assert: подпись чувствительна к кредам (санити двойной реализации).
        first.Authorization.Should().NotBe(second.Authorization);
    }

    // AAA: обобщение до произвольного метода (arch/24 §5 G) — METHOD входит в
    // canonical request первой строкой; POST/DELETE дают разные подписи, GET-
    // сигнатура не меняется (регрессия).
    [Theory]
    [InlineData("POST")]
    [InlineData("DELETE")]
    [InlineData("PUT")]
    public void Sign_MethodInCanonicalRequest(string method)
    {
        // Arrange: двойная реализация для фактического метода.
        const string path = "/admin/replication/join";
        const string hostHeader = "h1:9000";
        const string amzDate = "20260115T103000Z";
        const string date = "20260115";
        const string payloadHash = SigV4GetSigner.UnsignedPayload;
        var canonicalRequest =
            $"{method}\n{path}\n\nhost:{hostHeader}\n" +
            $"x-amz-content-sha256:{payloadHash}\nx-amz-date:{amzDate}\n" +
            "\n" +
            "host;x-amz-content-sha256;x-amz-date\n" + payloadHash;
        var scope = $"{date}/us-east-1/s3/aws4_request";
        var stringToSign =
            $"AWS4-HMAC-SHA256\n{amzDate}\n{scope}\n" +
            Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(canonicalRequest))).ToLowerInvariant();
        var key = Hmac(Encoding.UTF8.GetBytes("AWS4" + SecretKey), Encoding.UTF8.GetBytes(date));
        key = Hmac(key, Encoding.UTF8.GetBytes("us-east-1"));
        key = Hmac(key, Encoding.UTF8.GetBytes("s3"));
        key = Hmac(key, Encoding.UTF8.GetBytes("aws4_request"));
        var expectedSignature = Convert.ToHexString(Hmac(key, Encoding.UTF8.GetBytes(stringToSign))).ToLowerInvariant();

        // Act
        var signed = SigV4GetSigner.SignRequest(method, path, hostHeader, AccessKey, SecretKey, "us-east-1", FixedTime);

        // Assert: подпись построена с фактическим методом.
        signed.Authorization.Should().Contain($"Signature={expectedSignature}");
        signed.Authorization.Should().NotBe(
            SigV4GetSigner.SignRequest("GET", path, hostHeader, AccessKey, SecretKey, "us-east-1", FixedTime).Authorization);
    }

    // AAA: подпись с canonical query проходит серверный SigV4HeaderVerifier
    // (roundtrip — CanonicalRequestBuilder включает RawQuery в подпись; sync-report
    // с marker требует канонизации query, spec t33 §3.6).
    [Fact]
    public void SignRequest_WithCanonicalQuery_PassesServerVerifier()
    {
        // Arrange: подпись с query; на проводе — та же закодированная строка.
        const string query = "bucket=b&marker=m%2Fnext";
        var signed = SigV4GetSigner.SignRequest(
            "GET", "/admin/replication/sync-report", "h:1", AccessKey, SecretKey,
            canonicalQuery: query, now: DateTimeOffset.UtcNow);
        var model = new S3RequestModel
        {
            Method = "GET",
            RawPath = "/admin/replication/sync-report",
            RawQuery = query,
            Headers = S3HeaderCollection.FromPairs(
                ("Authorization", signed.Authorization),
                ("host", "h:1"),
                ("x-amz-date", signed.AmzDate),
                ("x-amz-content-sha256", signed.ContentSha256)),
            Host = "h:1",
            OpenBody = () => new MemoryStream(),
        };

        // Act: серверный верификатор (время — системное; skew ±15 мин покрывает подпись).
        var result = new SigV4HeaderVerifier(TimeProvider.System).Verify(model, _ => SecretKey);

        // Assert: Ok — подпись согласована с серверной канонизацией query.
        result.Should().BeOfType<SigV4Result.Ok>();
    }

    // AAA: подпись БЕЗ query на запросе С query → SignatureDoesNotMatch
    // (сервер включает RawQuery в canonical request — расхождение строк).
    [Fact]
    public void SignRequest_NoQueryOnRequestWithQuery_SignatureDoesNotMatch()
    {
        // Arrange: подпись с пустой query, запрос несёт marker.
        var signed = SigV4GetSigner.SignRequest(
            "GET", "/admin/replication/sync-report", "h:1", AccessKey, SecretKey,
            now: DateTimeOffset.UtcNow);
        var model = new S3RequestModel
        {
            Method = "GET",
            RawPath = "/admin/replication/sync-report",
            RawQuery = "bucket=b&marker=m/next",
            Headers = S3HeaderCollection.FromPairs(
                ("Authorization", signed.Authorization),
                ("host", "h:1"),
                ("x-amz-date", signed.AmzDate),
                ("x-amz-content-sha256", signed.ContentSha256)),
            Host = "h:1",
            OpenBody = () => new MemoryStream(),
        };

        // Act
        var result = new SigV4HeaderVerifier(TimeProvider.System).Verify(model, _ => SecretKey);

        // Assert
        result.Should().BeOfType<SigV4Result.Fail>().Which.Code.Should().Be(S3ErrorCode.SignatureDoesNotMatch);
    }

    // AAA: маркер с !'()* — Uri.EscapeDataString их НЕ кодирует, серверный
    // EncodeValue кодирует (%21%27%28%29%2A): roundtrip обязан сходиться
    // (иначе sync-report навсегда SignatureDoesNotMatch для таких страниц).
    [Fact]
    public void SignRequest_MarkerWithRfc3986SpecialChars_PassesVerifier()
    {
        // Arrange: query через CanonicalQuery (per-символьное кодирование,
        // семантически равно серверному UriEncoding.EncodeValue).
        var query = SigV4GetSigner.CanonicalQuery(("bucket", "b"), ("marker", "m!'()*"));
        query.Should().Be("bucket=b&marker=m%21%27%28%29%2A");
        var signed = SigV4GetSigner.SignRequest(
            "GET", "/admin/replication/sync-report", "h:1", AccessKey, SecretKey,
            canonicalQuery: query, now: DateTimeOffset.UtcNow);
        var model = new S3RequestModel
        {
            Method = "GET",
            RawPath = "/admin/replication/sync-report",
            RawQuery = query,
            Headers = S3HeaderCollection.FromPairs(
                ("Authorization", signed.Authorization),
                ("host", "h:1"),
                ("x-amz-date", signed.AmzDate),
                ("x-amz-content-sha256", signed.ContentSha256)),
            Host = "h:1",
            OpenBody = () => new MemoryStream(),
        };

        // Act: серверный верификатор — FormDecode + EncodeQuery согласованы.
        var result = new SigV4HeaderVerifier(TimeProvider.System).Verify(model, _ => SecretKey);

        // Assert
        result.Should().BeOfType<SigV4Result.Ok>();
    }

    // AAA: CanonicalQuery — сортировка Ordinal, null/пустые значения не шлются,
    // спецсимволы кодируются RFC 3986 (совместимость с EncodeQuery сервера).
    [Fact]
    public void CanonicalQuery_OrdinalSortAndEmptyFilter()
    {
        // Arrange: пары в неупорядоченном виде, часть — пустые/null.
        var query = SigV4GetSigner.CanonicalQuery(
            ("marker", "m/next"), ("bucket", "b"), ("prefix", ""), ("empty", null));

        // Assert: bucket первым (Ordinal), prefix/empty отфильтрованы, '/' закодирован.
        query.Should().Be("bucket=b&marker=m%2Fnext");
    }

    private static byte[] Hmac(byte[] key, byte[] data) => HMACSHA256.HashData(key, data);
}
