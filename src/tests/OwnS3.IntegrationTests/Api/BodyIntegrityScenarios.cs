using System.IO.Hashing;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using OwnS3.Protocol.Auth;
using OwnS3.Replication;
using OwnS3.Replication.Outbox;
using OwnS3.UnitTests;

namespace OwnS3.IntegrationTests.Api;

// Сверки целостности тела (глава 03 §1/§3): sha256-режим, Content-MD5,
// чанковая подпись и трейлеры — отказы при порче, полный drain при валидных.
// Параметризация режима (t40): логика — в базе, single/EC-наследники
// объявляют фикстуру/коллекцию.
public abstract class BodyIntegrityScenariosBase(OwnS3AppFactory factory)
{
    private static readonly byte[] Body = Encoding.UTF8.GetBytes("hello world");

    protected OwnS3TestClient NewClient() => new(factory.CreateClient());

    [Fact]
    public async Task PutObject_CorrectSha256_ReachesStorageAfterFullVerification()
    {
        // Arrange: подпись с фактическим hex-sha256 тела
        var payload = Convert.ToHexString(SHA256.HashData(Body)).ToLowerInvariant();

        // Act
        var response = await NewClient().SendSignedAsync("PUT", "/bucket/key",
            payloadString: payload, body: Body);

        // Assert: сверка прошла — запрос дошёл до Storage (бакета нет)
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await RoutingScenarios.ErrorXmlAsync(response)).Should().Be("NoSuchBucket");
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
    public async Task ChunkedPut_ValidChain_ReachesStorageAfterFullVerification()
    {
        // Arrange: подпись с payload-строкой режима; seed из подписи запроса
        var client = NewClient();
        var date = OwnS3AppFactory.HostTime;
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

        // Assert: полная цепочка сверкана — дошло до Storage (бакета нет)
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await RoutingScenarios.ErrorXmlAsync(response)).Should().Be("NoSuchBucket");
    }

    [Fact]
    public async Task ChunkedPut_BrokenChunk2Signature_403()
    {
        // Arrange: 2-чанковое тело, hex подписи второго фрейма испорчен
        var data = Encoding.UTF8.GetBytes("0123456789abcdefghij");
        var client = NewClient();
        var date = OwnS3AppFactory.HostTime;
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
        var date = OwnS3AppFactory.HostTime;
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

    [Fact]
    public async Task DeleteObjects_ContentMd5Mismatch_400BadDigest()
    {
        // Arrange: валидный Delete-XML + Content-MD5 от другого содержимого —
        // сверка тела DeleteObjects через HashingBodyStream (глава 02)
        var xml = """<?xml version="1.0" encoding="utf-8"?><Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Object><Key>k</Key></Object><Quiet>false</Quiet></Delete>""";
        var wrongMd5 = Convert.ToBase64String(MD5.HashData("other"u8));

        // Act
        var response = await NewClient().SendSignedAsync("POST", "/bucket?delete",
            headers: new Dictionary<string, string> { ["Content-MD5"] = wrongMd5 },
            body: Encoding.UTF8.GetBytes(xml));

        // Assert: сверка MD5 при дочитывании XML-тела — BadDigest
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await RoutingScenarios.ErrorXmlAsync(response)).Should().Be("BadDigest");
    }

    [Fact]
    public async Task DeleteObjects_CorrectContentMd5_ReachesStorage()
    {
        // Arrange: контрольный кейс — корректный MD5 того же XML
        var xml = """<?xml version="1.0" encoding="utf-8"?><Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Object><Key>k</Key></Object><Quiet>false</Quiet></Delete>""";
        var md5 = Convert.ToBase64String(MD5.HashData(Encoding.UTF8.GetBytes(xml)));

        // Act
        var response = await NewClient().SendSignedAsync("POST", "/bucket?delete",
            headers: new Dictionary<string, string> { ["Content-MD5"] = md5 },
            body: Encoding.UTF8.GetBytes(xml));

        // Assert: сверка пройдена — запрос дошёл до Storage (бакета нет)
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await RoutingScenarios.ErrorXmlAsync(response)).Should().Be("NoSuchBucket");
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

    // Контрольная сумма тела по алгоритму (конвенция: crc32 — big-endian от BCL
    // Crc32; crc32c — OwnS3.Protocol.Auth.Crc32C, big-endian).
    private static string ChecksumOf(string algorithm, byte[] data) => algorithm switch
    {
        "crc32" => Convert.ToBase64String(Crc32.Hash(data).Reverse().ToArray()),
        "crc32c" => Convert.ToBase64String(Crc32C.Hash(data)),
        "sha1" => Convert.ToBase64String(SHA1.HashData(data)),
        "sha256" => Convert.ToBase64String(SHA256.HashData(data)),
        _ => throw new ArgumentOutOfRangeException(algorithm),
    };

    // (а) корректный checksum при UNSIGNED-PAYLOAD — единственная серверная сверка
    // тела прошла, запрос дошёл до Storage (NoSuchBucket-шаблон серии).
    [Theory]
    [InlineData("crc32")]
    [InlineData("crc32c")]
    [InlineData("sha1")]
    [InlineData("sha256")]
    public async Task PutObject_CorrectChecksum_UnsignedPayload_ReachesStorage(string algorithm)
    {
        // Arrange: UNSIGNED-PAYLOAD (дефолт клиента) + корректная сумма тела
        // Act
        var response = await NewClient().SendSignedAsync("PUT", "/bucket/key",
            headers: new Dictionary<string, string> { [$"x-amz-checksum-{algorithm}"] = ChecksumOf(algorithm, Body) },
            body: Body);

        // Assert: сверка прошла — Storage отказал по бакету
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await RoutingScenarios.ErrorXmlAsync(response)).Should().Be("NoSuchBucket");
    }

    // (б) порча checksum → 400 BadDigest (каждый алгоритм).
    [Theory]
    [InlineData("crc32")]
    [InlineData("crc32c")]
    [InlineData("sha1")]
    [InlineData("sha256")]
    public async Task PutObject_CorruptedChecksum_400BadDigest(string algorithm)
    {
        // Arrange: сумма ДРУГОГО тела
        var wrong = ChecksumOf(algorithm, "other body"u8.ToArray());

        // Act
        var response = await NewClient().SendSignedAsync("PUT", "/bucket/key",
            headers: new Dictionary<string, string> { [$"x-amz-checksum-{algorithm}"] = wrong },
            body: Body);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await RoutingScenarios.ErrorXmlAsync(response)).Should().Be("BadDigest");
    }

    // (в) форматно-невалидный (не base64 / неверная длина) → 400 InvalidArgument.
    [Theory]
    [InlineData("x-amz-checksum-crc32", "!!!not-base64!!!")]
    [InlineData("x-amz-checksum-sha256", "AAAA")]
    public async Task PutObject_InvalidChecksumFormat_400InvalidArgument(string header, string value)
    {
        // Arrange / Act
        var response = await NewClient().SendSignedAsync("PUT", "/bucket/key",
            headers: new Dictionary<string, string> { [header] = value }, body: Body);

        // Assert: отказ ДО чтения тела — Storage не звался, канонический код
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await RoutingScenarios.ErrorXmlAsync(response)).Should().Be("InvalidArgument");
    }

    // (г) hex-sha256 подпись + sha256-checksum — обе сверки независимо:
    // г1 — обе корректны; г2 — подпись корректна, checksum битой; г3 — подпись битая,
    // checksum корректен (InvalidRequest первее).
    [Fact]
    public async Task PutObject_Sha256SignatureAndChecksum_IndependentChecks()
    {
        var client = NewClient();
        var payload = Convert.ToHexString(SHA256.HashData(Body)).ToLowerInvariant();
        var goodChecksum = Convert.ToBase64String(SHA256.HashData(Body));
        var badChecksum = Convert.ToBase64String(SHA256.HashData("other body"u8.ToArray()));

        // Act / Assert (г1): обе сверки прошли — до Storage
        (await client.SendSignedAsync("PUT", "/bucket/key", payloadString: payload,
                headers: new Dictionary<string, string> { ["x-amz-checksum-sha256"] = goodChecksum }, body: Body))
            .StatusCode.Should().Be(HttpStatusCode.NotFound);

        // Act / Assert (г2): подпись корректна, checksum битой → BadDigest
        var corrupted = await client.SendSignedAsync("PUT", "/bucket/key", payloadString: payload,
            headers: new Dictionary<string, string> { ["x-amz-checksum-sha256"] = badChecksum }, body: Body);
        corrupted.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await RoutingScenarios.ErrorXmlAsync(corrupted)).Should().Be("BadDigest");

        // Act / Assert (г3): подпись битая, checksum корректен → InvalidRequest (порядок)
        var brokenSignature = Convert.ToHexString(SHA256.HashData("other body"u8.ToArray())).ToLowerInvariant();
        var invalidRequest = await client.SendSignedAsync("PUT", "/bucket/key", payloadString: brokenSignature,
            headers: new Dictionary<string, string> { ["x-amz-checksum-sha256"] = goodChecksum }, body: Body);
        invalidRequest.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await RoutingScenarios.ErrorXmlAsync(invalidRequest)).Should().Be("InvalidRequest");
    }

    // (д) два checksum-заголовка → 400 InvalidArgument.
    [Fact]
    public async Task PutObject_TwoChecksumHeaders_400InvalidArgument()
    {
        // Arrange / Act
        var response = await NewClient().SendSignedAsync("PUT", "/bucket/key",
            headers: new Dictionary<string, string>
            {
                ["x-amz-checksum-sha256"] = Convert.ToBase64String(SHA256.HashData(Body)),
                ["x-amz-checksum-crc32"] = ChecksumOf("crc32", Body),
            }, body: Body);

        // Assert: «Invalid checksum provided.» (семейство, включая разные алгоритмы)
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await RoutingScenarios.ErrorXmlAsync(response)).Should().Be("InvalidArgument");
    }

    // (е) пустое тело + корректная сумма пустоты — сверка проходит на EOF первого Read.
    [Theory]
    [InlineData("crc32",  "AAAAAA==")]
    [InlineData("crc32c", "AAAAAA==")]
    [InlineData("sha1",   "2jmj7l5rSw0yVb/vlWAYkK/YBwk=")]
    [InlineData("sha256", "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=")]
    public async Task PutObject_EmptyBody_EmptyChecksum_ReachesStorage(string algorithm, string emptySum)
    {
        // Arrange / Act
        var response = await NewClient().SendSignedAsync("PUT", "/bucket/key",
            headers: new Dictionary<string, string> { [$"x-amz-checksum-{algorithm}"] = emptySum },
            body: []);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await RoutingScenarios.ErrorXmlAsync(response)).Should().Be("NoSuchBucket");
    }

    // (ж) чанковый trailer-режим с заголовочным checksum: заголовок игнорируется
    // (авторитет — трейлер); валидный трейлер + БИТОЙ заголовок → запрос доходит до Storage.
    [Fact]
    public async Task ChunkedPut_TrailerMode_HeaderChecksumIgnored()
    {
        // Arrange: чанковое тело с ВАЛИДНЫМ трейлером crc32 + битой заголовочной суммой
        var client = NewClient();
        var date = OwnS3AppFactory.HostTime;
        var payload = PayloadHashModeClassifier.StreamingTrailerValue;
        var host = client.Http.BaseAddress!.Authority;
        var goodTrailer = Crc32.Hash(Body).Reverse().ToArray();
        var headers = new List<(string, string)>
        {
            ("host", host),
            ("x-amz-content-sha256", payload),
            ("x-amz-date", TestSigV4Signer.AmzDateOf(date)),
            ("content-encoding", "aws-chunked"),
            ("x-amz-decoded-content-length", Body.Length.ToString()),
            ("x-amz-trailer", "x-amz-checksum-crc32"),
            ("x-amz-checksum-crc32", "AAAAAA=="), // битая заголовочная — обязана игнорироваться
        };
        var signature = TestSigV4Signer.HeaderSignature(OwnS3AppFactory.WriterSecretKey, "PUT", "/bucket/key",
            string.Empty, headers, payload, date, "us-east-1");
        var chunked = TestSigV4Signer.BuildChunkedBody(OwnS3AppFactory.WriterSecretKey, Body, signature, date,
            "us-east-1", trailerName: "x-amz-checksum-crc32", trailerChecksum: goodTrailer);

        using var request = new HttpRequestMessage(HttpMethod.Put, "/bucket/key");
        request.Headers.TryAddWithoutValidation("x-amz-date", TestSigV4Signer.AmzDateOf(date));
        request.Headers.TryAddWithoutValidation("x-amz-content-sha256", payload);
        request.Headers.TryAddWithoutValidation("x-amz-trailer", "x-amz-checksum-crc32");
        request.Headers.TryAddWithoutValidation("x-amz-checksum-crc32", "AAAAAA==");
        request.Headers.TryAddWithoutValidation("Authorization",
            TestSigV4Signer.BuildAuthorization(OwnS3AppFactory.WriterAccessKey, "us-east-1", date,
                ["content-encoding", "host", "x-amz-checksum-crc32", "x-amz-content-sha256", "x-amz-date",
                 "x-amz-decoded-content-length", "x-amz-trailer"], signature));
        request.Content = new ByteArrayContent(chunked);
        request.Content.Headers.TryAddWithoutValidation("Content-Encoding", "aws-chunked");
        request.Content.Headers.TryAddWithoutValidation("x-amz-decoded-content-length", Body.Length.ToString());
        request.Content.Headers.ContentType = null;

        // Act
        var response = await client.Http.SendAsync(request, TestContext.Current.CancellationToken);

        // Assert: трейлерная сверка прошла, заголовок проигнорирован — до Storage
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await RoutingScenarios.ErrorXmlAsync(response)).Should().Be("NoSuchBucket");
    }

    // (з) UploadPart с checksum-заголовком — прежнее поведение (заголовок не читается).
    [Fact]
    public async Task UploadPart_ChecksumHeader_Ignored()
    {
        // Arrange: битая сумма на part-запросе — не 400, прежний исход Storage
        // Act
        var response = await NewClient().SendSignedAsync("PUT", "/bucket/key?partNumber=1&uploadId=u",
            headers: new Dictionary<string, string>
            {
                ["x-amz-checksum-sha256"] = Convert.ToBase64String(SHA256.HashData("other"u8.ToArray())),
            }, body: Encoding.UTF8.GetBytes("part"));

        // Assert: NoSuchBucket от Storage — хендлер части checksum-заголовки не читает
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await RoutingScenarios.ErrorXmlAsync(response)).Should().Be("NoSuchBucket");
    }

    // (и) коммит-семантика на живом бакете: корректный checksum → 200 + ETag hex-MD5
    // (не зависит от алгоритма); битой → 400 BadDigest и объект НЕ закоммичен
    // (GetObject → NoSuchKey; событие репликации — ассерт кейса (и2) ниже).
    [Fact]
    public async Task PutObject_Checksum_CommitSemanticsAndEtag()
    {
        // Arrange: бакет кейса (admin)
        var client = NewClient();
        (await client.SendSignedAsync("PUT", "/b-ci-chk", credentials: OwnS3TestClient.Admin()))
            .StatusCode.Should().Be(HttpStatusCode.OK);
        var md5Hex = Convert.ToHexString(MD5.HashData(Body)).ToLowerInvariant();

        // Act / Assert: crc32-put → 200, ETag = MD5
        var put = await client.SendSignedAsync("PUT", "/b-ci-chk/ok.bin",
            headers: new Dictionary<string, string> { ["x-amz-checksum-crc32"] = ChecksumOf("crc32", Body) },
            body: Body);
        put.StatusCode.Should().Be(HttpStatusCode.OK);
        put.Headers.ETag!.Tag.Should().Be($"\"{md5Hex}\"", "ETag — hex-MD5, от алгоритма checksum не зависит");

        // Act / Assert: sha256-put того же тела — тот же ETag (независимость)
        var put2 = await client.SendSignedAsync("PUT", "/b-ci-chk/ok2.bin",
            headers: new Dictionary<string, string> { ["x-amz-checksum-sha256"] = ChecksumOf("sha256", Body) },
            body: Body);
        put2.Headers.ETag!.Tag.Should().Be($"\"{md5Hex}\"");

        // Act / Assert: битая сумма → 400 BadDigest, объекта нет (NoSuchKey)
        var broken = await client.SendSignedAsync("PUT", "/b-ci-chk/broken.bin",
            headers: new Dictionary<string, string> { ["x-amz-checksum-crc32"] = ChecksumOf("crc32", "other"u8.ToArray()) },
            body: Body);
        broken.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await RoutingScenarios.ErrorXmlAsync(broken)).Should().Be("BadDigest");
        var get = await client.SendSignedAsync("GET", "/b-ci-chk/broken.bin");
        get.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await RoutingScenarios.ErrorXmlAsync(get)).Should().Be("NoSuchKey");
    }
}

// single-режим: свой том на класс (как до t40).
public sealed class BodyIntegrityScenarios(OwnS3AppFactory factory) : BodyIntegrityScenariosBase(factory),
    IClassFixture<OwnS3AppFactory>;

// (и2) критерий 2 «событие репликации не отправлялось»: ассерт на фабрике
// С соседями (ReplicationAppFactory — успешные put ставят события object-put
// в outbox; на базовой фабрике без соседей журнал не ведётся вовсе — там ассерт
// пустоты был бы тривиально-зелёным, см. ReplicationEventsScenarios.
// NoNeighbors_NoOutboxDirectory). Успешный checksum-put → событие есть;
// битой → события нет (сверка до коммит-поинта).
public sealed class BodyIntegrityReplicationScenarios(ReplicationAppFactory factory)
    : IClassFixture<ReplicationAppFactory>
{
    private OwnS3TestClient NewClient() => new(factory.CreateClient());

    private string OutboxDir => Path.Combine(factory.TempVolumeDir, ".owns3.sys", "replication", "outbox");

    // Декод всех событий журнала (бинарный формат v1, глава 07 §4) — образец
    // ReplicationEventsScenarios.ReadOutbox.
    private static List<ReplicationEvent> ReadOutbox(string outboxDir)
    {
        var events = new List<ReplicationEvent>();
        if (!Directory.Exists(outboxDir))
            return events;
        foreach (var segment in Directory.GetFiles(outboxDir, "seg-*.log").OrderBy(f => f))
        {
            var bytes = File.ReadAllBytes(segment);
            var offset = 0;
            while (offset < bytes.Length)
            {
                if (!OutboxEntryCodec.TryDecode(bytes, offset, out var e, out offset))
                    break;
                events.Add(e);
            }
        }
        return events;
    }

    private static string Crc32Of(byte[] data) =>
        Convert.ToBase64String(System.IO.Hashing.Crc32.Hash(data).Reverse().ToArray());

    [Fact]
    public async Task PutObject_BadChecksum_NoReplicationEvent()
    {
        // Arrange: сосед задан (клиентские put ставят события); бакет кейса
        var client = NewClient();
        (await client.SendSignedAsync("PUT", "/b-ci-repl", credentials: OwnS3TestClient.Admin()))
            .StatusCode.Should().Be(HttpStatusCode.OK);
        var body = Encoding.UTF8.GetBytes("hello world");

        // Act: успешный put с корректным crc32 → 200
        (await client.SendSignedAsync("PUT", "/b-ci-repl/ok.bin",
            headers: new Dictionary<string, string> { ["x-amz-checksum-crc32"] = Crc32Of(body) },
            body: body)).StatusCode.Should().Be(HttpStatusCode.OK);

        // Assert: коммит-поинт пройден — событие object-put поставлено
        ReadOutbox(OutboxDir).Should().Contain(e =>
            e.Type == ReplicationEventType.ObjectPut && e.Bucket == "b-ci-repl" && e.Key == "ok.bin");

        // Act: битой put → 400 BadDigest
        var broken = await client.SendSignedAsync("PUT", "/b-ci-repl/broken.bin",
            headers: new Dictionary<string, string> { ["x-amz-checksum-crc32"] = Crc32Of("other"u8.ToArray()) },
            body: body);
        broken.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await RoutingScenarios.ErrorXmlAsync(broken)).Should().Be("BadDigest");

        // Assert: сверка ДО коммит-поинта — события для broken.bin НЕТ
        ReadOutbox(OutboxDir).Should().NotContain(e =>
            e.Type == ReplicationEventType.ObjectPut && e.Bucket == "b-ci-repl" && e.Key == "broken.bin");
    }
}
