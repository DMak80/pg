using System.IO.Hashing;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using OwnS3.Protocol.Auth;
using OwnS3.UnitTests;

namespace OwnS3.IntegrationTests.Api;

// Сверки целостности тела (глава 03 §1/§3): sha256-режим, Content-MD5,
// чанковая подпись и трейлеры — отказы при порче, полный drain при валидных.
[Collection(OwnS3TestCollection.Name)]
public sealed class BodyIntegrityScenarios(OwnS3AppFactory factory)
{
    private static readonly byte[] Body = Encoding.UTF8.GetBytes("hello world");

    private OwnS3TestClient NewClient() => new(factory.CreateClient());

    [Fact]
    public async Task PutObject_CorrectSha256_ReachesStubAfterFullVerification()
    {
        // Arrange: подпись с фактическим hex-sha256 тела
        var payload = Convert.ToHexString(SHA256.HashData(Body)).ToLowerInvariant();

        // Act
        var response = await NewClient().SendSignedAsync("PUT", "/bucket/key",
            payloadString: payload, body: Body);

        // Assert: сверка прошла (drain заглушки) — 500 от заглушки
        response.StatusCode.Should().Be(HttpStatusCode.InternalServerError);
    }

    [Fact]
    public async Task PutObject_CorruptedSha256_400InvalidRequest()
    {
        // Arrange: подпись по sha256 ДРУГОГО тела — сверка при дочитывании
        var payload = Convert.ToHexString(SHA256.HashData("other body"u8)).ToLowerInvariant();

        // Act
        var response = await NewClient().SendSignedAsync("PUT", "/bucket/key",
            payloadString: payload, body: Body);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await RoutingScenarios.ErrorXmlAsync(response)).Should().Be("InvalidRequest");
    }

    [Fact]
    public async Task PutObject_ContentMd5Mismatch_400BadDigest()
    {
        // Arrange: UNSIGNED-PAYLOAD + Content-MD5 от другого содержимого
        var wrongMd5 = Convert.ToBase64String(MD5.HashData("other"u8));

        // Act
        var response = await NewClient().SendSignedAsync("PUT", "/bucket/key",
            headers: new Dictionary<string, string> { ["Content-MD5"] = wrongMd5 }, body: Body);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await RoutingScenarios.ErrorXmlAsync(response)).Should().Be("BadDigest");
    }

    [Fact]
    public async Task ChunkedPut_ValidChain_ReachesStubAfterFullVerification()
    {
        // Arrange: подпись с payload-строкой режима; seed из подписи запроса
        var client = NewClient();
        var date = DateTimeOffset.UtcNow;
        var payload = PayloadHashModeClassifier.StreamingValue;

        // Заголовочная подпись с режимной payload-строкой (seed) — как SendSignedAsync,
        // но тело собирается signer'ом; собираем вручную для доступа к seed.
        var host = client.Http.BaseAddress!.Authority;
        var headers = new List<(string, string)>
        {
            ("host", host),
            ("x-amz-content-sha256", payload),
            ("x-amz-date", TestSigV4Signer.AmzDateOf(date)),
            ("content-encoding", "aws-chunked"),
            ("x-amz-decoded-content-length", Body.Length.ToString()),
        };
        var signature = TestSigV4Signer.HeaderSignature(OwnS3AppFactory.WriterSecretKey, "PUT", "/bucket/key",
            string.Empty, headers, payload, date, "us-east-1");
        var chunked = TestSigV4Signer.BuildChunkedBody(OwnS3AppFactory.WriterSecretKey, Body, signature, date, "us-east-1");

        using var request = new HttpRequestMessage(HttpMethod.Put, "/bucket/key");
        request.Headers.TryAddWithoutValidation("x-amz-date", TestSigV4Signer.AmzDateOf(date));
        request.Headers.TryAddWithoutValidation("x-amz-content-sha256", payload);
        request.Headers.TryAddWithoutValidation("Authorization",
            TestSigV4Signer.BuildAuthorization(OwnS3AppFactory.WriterAccessKey, "us-east-1", date,
                ["content-encoding", "host", "x-amz-content-sha256", "x-amz-date", "x-amz-decoded-content-length"], signature));
        request.Content = new ByteArrayContent(chunked);
        request.Content.Headers.TryAddWithoutValidation("Content-Encoding", "aws-chunked");
        request.Content.Headers.TryAddWithoutValidation("x-amz-decoded-content-length", Body.Length.ToString());
        request.Content.Headers.ContentType = null;

        // Act
        var response = await client.Http.SendAsync(request, TestContext.Current.CancellationToken);

        // Assert: полная цепочка сверкана — 500 от заглушки
        response.StatusCode.Should().Be(HttpStatusCode.InternalServerError);
    }

    [Fact]
    public async Task ChunkedPut_BrokenChunk2Signature_403()
    {
        // Arrange: 2-чанковое тело, hex подписи второго фрейма испорчен
        var data = Encoding.UTF8.GetBytes("0123456789abcdefghij");
        var client = NewClient();
        var date = DateTimeOffset.UtcNow;
        var payload = PayloadHashModeClassifier.StreamingValue;
        var host = client.Http.BaseAddress!.Authority;
        var headers = new List<(string, string)>
        {
            ("host", host),
            ("x-amz-content-sha256", payload),
            ("x-amz-date", TestSigV4Signer.AmzDateOf(date)),
            ("content-encoding", "aws-chunked"),
            ("x-amz-decoded-content-length", data.Length.ToString()),
        };
        var signature = TestSigV4Signer.HeaderSignature(OwnS3AppFactory.WriterSecretKey, "PUT", "/bucket/key",
            string.Empty, headers, payload, date, "us-east-1");
        var chunked = TestSigV4Signer.BuildChunkedBody(OwnS3AppFactory.WriterSecretKey, data, signature, date,
            "us-east-1", chunkSize: 10);
        // Портим подпись второго фрейма данных.
        var marker = Encoding.ASCII.GetBytes(";chunk-signature=");
        var first = IndexOf(chunked, marker, 0);
        var second = IndexOf(chunked, marker, first + 1);
        chunked[second + marker.Length] = chunked[second + marker.Length] == '0' ? (byte)'1' : (byte)'0';

        using var request = new HttpRequestMessage(HttpMethod.Put, "/bucket/key");
        request.Headers.TryAddWithoutValidation("x-amz-date", TestSigV4Signer.AmzDateOf(date));
        request.Headers.TryAddWithoutValidation("x-amz-content-sha256", payload);
        request.Headers.TryAddWithoutValidation("Authorization",
            TestSigV4Signer.BuildAuthorization(OwnS3AppFactory.WriterAccessKey, "us-east-1", date,
                ["content-encoding", "host", "x-amz-content-sha256", "x-amz-date", "x-amz-decoded-content-length"], signature));
        request.Content = new ByteArrayContent(chunked);
        request.Content.Headers.TryAddWithoutValidation("Content-Encoding", "aws-chunked");
        request.Content.Headers.TryAddWithoutValidation("x-amz-decoded-content-length", data.Length.ToString());
        request.Content.Headers.ContentType = null;

        // Act
        var response = await client.Http.SendAsync(request, TestContext.Current.CancellationToken);

        // Assert: отказ в момент чтения второго чанка
        response.StatusCode.Should().Be(HttpStatusCode.Forbidden);
        (await RoutingScenarios.ErrorXmlAsync(response)).Should().Be("SignatureDoesNotMatch");
    }

    [Fact]
    public async Task ChunkedPut_BrokenTrailerChecksum_400BadDigest()
    {
        // Arrange: трейлер crc32 от другого содержимого (подпись трейлера корректна)
        var client = NewClient();
        var date = DateTimeOffset.UtcNow;
        var payload = PayloadHashModeClassifier.StreamingTrailerValue;
        var host = client.Http.BaseAddress!.Authority;
        var wrongCrc = Crc32.Hash("other"u8.ToArray()).Reverse().ToArray();
        var headers = new List<(string, string)>
        {
            ("host", host),
            ("x-amz-content-sha256", payload),
            ("x-amz-date", TestSigV4Signer.AmzDateOf(date)),
            ("content-encoding", "aws-chunked"),
            ("x-amz-decoded-content-length", Body.Length.ToString()),
            ("x-amz-trailer", "x-amz-checksum-crc32"),
        };
        var signature = TestSigV4Signer.HeaderSignature(OwnS3AppFactory.WriterSecretKey, "PUT", "/bucket/key",
            string.Empty, headers, payload, date, "us-east-1");
        var chunked = TestSigV4Signer.BuildChunkedBody(OwnS3AppFactory.WriterSecretKey, Body, signature, date,
            "us-east-1", trailerName: "x-amz-checksum-crc32", trailerChecksum: wrongCrc);

        using var request = new HttpRequestMessage(HttpMethod.Put, "/bucket/key");
        request.Headers.TryAddWithoutValidation("x-amz-date", TestSigV4Signer.AmzDateOf(date));
        request.Headers.TryAddWithoutValidation("x-amz-content-sha256", payload);
        request.Headers.TryAddWithoutValidation("x-amz-trailer", "x-amz-checksum-crc32");
        request.Headers.TryAddWithoutValidation("Authorization",
            TestSigV4Signer.BuildAuthorization(OwnS3AppFactory.WriterAccessKey, "us-east-1", date,
                ["content-encoding", "host", "x-amz-content-sha256", "x-amz-date", "x-amz-decoded-content-length", "x-amz-trailer"], signature));
        request.Content = new ByteArrayContent(chunked);
        request.Content.Headers.TryAddWithoutValidation("Content-Encoding", "aws-chunked");
        request.Content.Headers.TryAddWithoutValidation("x-amz-decoded-content-length", Body.Length.ToString());
        request.Content.Headers.ContentType = null;

        // Act
        var response = await client.Http.SendAsync(request, TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await RoutingScenarios.ErrorXmlAsync(response)).Should().Be("BadDigest");
    }

    [Fact]
    public async Task StreamingModeOnGet_400InvalidRequest()
    {
        // Arrange: x-amz-content-sha256 = STREAMING-* на GET (арх-правка 4)
        // Act
        var response = await NewClient().SendSignedAsync("GET", "/bucket/key",
            payloadString: PayloadHashModeClassifier.StreamingValue);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await RoutingScenarios.ErrorXmlAsync(response)).Should().Be("InvalidRequest");
    }

    private static int IndexOf(byte[] haystack, byte[] needle, int start)
    {
        for (var i = start; i <= haystack.Length - needle.Length; i++)
        {
            var match = true;
            for (var j = 0; j < needle.Length; j++)
                if (haystack[i + j] != needle[j]) { match = false; break; }
            if (match) return i;
        }
        return -1;
    }
}
