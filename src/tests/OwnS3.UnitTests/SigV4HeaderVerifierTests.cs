using System.Text;
using OwnS3.Protocol.Auth;
using OwnS3.Protocol.Errors;
using OwnS3.Protocol.Requests;

namespace OwnS3.UnitTests;

// Заголовочная SigV4: верификатор по официальным векторам AWS (внешний источник)
// и независимому signer'у; исходы-отказы — по канону главы 03 §1 + arch-правки.
public sealed class SigV4HeaderVerifierTests
{
    private static void AssertFail(SigV4Result result, S3ErrorCode code) =>
        result.Should().BeOfType<SigV4Result.Fail>().Which.Code.Should().Be(code);

    private static SigV4HeaderVerifier SutAt(DateTimeOffset now) => new(new FixedTimeProvider(now));

    private static string Resolver(string ak) =>
        ak == TestVectors.AccessKey ? TestVectors.SecretKey : null!;

    // Вектор 1: официальный пример AWS GET Object — подпись f0e8bd...
    [Fact]
    public void Verify_AwsVector1_GetObject_Ok()
    {
        // Arrange
        var model = new S3RequestModel
        {
            Method = "GET",
            RawPath = "/test.txt",
            RawQuery = "",
            Host = TestVectors.Host,
            Headers = S3HeaderCollection.FromPairs(
                ("Host", TestVectors.Host),
                ("Range", "bytes=0-9"),
                ("x-amz-content-sha256", TestVectors.EmptySha256),
                ("x-amz-date", TestVectors.AmzDate),
                ("Authorization", "AWS4-HMAC-SHA256 Credential=" + TestVectors.AccessKey + "/" + TestVectors.Scope +
                    ", SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=" + TestVectors.GetObjSignature)),
            OpenBody = () => Stream.Null,
        };
        var sut = SutAt(TestVectors.FixedTime);

        // Act
        var result = sut.Verify(model, Resolver);

        // Assert: подпись официального примера проходит; seed = значение подписи
        result.Should().BeOfType<SigV4Result.Ok>()
            .Which.SeedSignature.Should().Be(TestVectors.GetObjSignature);
    }

    // Вектор 2: официальный пример AWS ListObjects — подпись 34b483...
    [Fact]
    public void Verify_AwsVector2_ListObjects_Ok()
    {
        // Arrange
        var model = new S3RequestModel
        {
            Method = "GET",
            RawPath = "/",
            RawQuery = "max-keys=2&prefix=J",
            Host = TestVectors.Host,
            Headers = S3HeaderCollection.FromPairs(
                ("Host", TestVectors.Host),
                ("x-amz-content-sha256", TestVectors.EmptySha256),
                ("x-amz-date", TestVectors.AmzDate),
                ("Authorization", "AWS4-HMAC-SHA256 Credential=" + TestVectors.AccessKey + "/" + TestVectors.Scope +
                    ", SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=" + TestVectors.ListObjSignature)),
            OpenBody = () => Stream.Null,
        };
        var sut = SutAt(TestVectors.FixedTime);

        // Act
        var result = sut.Verify(model, Resolver);

        // Assert
        result.Should().BeOfType<SigV4Result.Ok>()
            .Which.SeedSignature.Should().Be(TestVectors.ListObjSignature);
    }

    // Вектор 4: официальный пример AWS PUT (кодированный путь, sha256 тела).
    [Fact]
    public void Verify_AwsVector4_PutObject_WithBodyHash_Ok()
    {
        // Arrange: RawPath как прислал клиент — уже кодированный
        var model = new S3RequestModel
        {
            Method = "PUT",
            RawPath = "/test%24file.text",
            RawQuery = "",
            Host = TestVectors.Host,
            Headers = S3HeaderCollection.FromPairs(
                ("Host", TestVectors.Host),
                ("x-amz-content-sha256", TestVectors.PutBodySha),
                ("x-amz-date", TestVectors.AmzDate),
                ("x-amz-storage-class", "REDUCED_REDUNDANCY"),
                ("Authorization", "AWS4-HMAC-SHA256 Credential=" + TestVectors.AccessKey + "/" + TestVectors.Scope +
                    ", SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-storage-class, Signature=" + TestVectors.PutSignature)),
            OpenBody = () => new MemoryStream(Encoding.UTF8.GetBytes(TestVectors.PutBody)),
        };
        var sut = SutAt(TestVectors.FixedTime);

        // Act
        var result = sut.Verify(model, Resolver);

        // Assert
        result.Should().BeOfType<SigV4Result.Ok>()
            .Which.Mode.Should().Be(PayloadHashMode.HexSha256);
    }

    [Fact]
    public void Verify_BrokenSignature_Fails()
    {
        // Arrange: последний байт hex подписи вектора 1 заменён
        var broken = TestVectors.GetObjSignature[..63] + (TestVectors.GetObjSignature[^1] == '0' ? '1' : '0');
        var model = Vector1Model(broken);
        var sut = SutAt(TestVectors.FixedTime);

        // Act
        var result = sut.Verify(model, Resolver);

        // Assert
        AssertFail(result, S3ErrorCode.SignatureDoesNotMatch);
    }

    [Fact]
    public void Verify_BrokenScope_AuthorizationHeaderMalformed()
    {
        // Arrange: сервис в scope заменён на sns (парсится с отказом)
        var model = Vector1Model(TestVectors.GetObjSignature, scope: "20130524/us-east-1/sns/aws4_request");
        var sut = SutAt(TestVectors.FixedTime);

        // Act
        var result = sut.Verify(model, Resolver);

        // Assert
        AssertFail(result, S3ErrorCode.AuthorizationHeaderMalformed);
    }

    [Theory]
    [InlineData("AWS AKIAIOSFODNN7EXAMPLE:bXclmGdFPCW7qBUXiqxZ4iKzOoQzR1Df")]       // SigV2
    [InlineData("AWS4-HMAC-SHA512 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=abc")] // иной алгоритм
    public void Verify_UnsupportedAlgorithm_InvalidRequest(string authorization)
    {
        // Arrange
        var model = new S3RequestModel
        {
            Method = "GET",
            RawPath = "/test.txt",
            RawQuery = "",
            Host = TestVectors.Host,
            Headers = S3HeaderCollection.FromPairs(
                ("Host", TestVectors.Host),
                ("x-amz-content-sha256", TestVectors.EmptySha256),
                ("x-amz-date", TestVectors.AmzDate),
                ("Authorization", authorization)),
            OpenBody = () => Stream.Null,
        };
        var sut = SutAt(TestVectors.FixedTime);

        // Act
        var result = sut.Verify(model, Resolver);

        // Assert: «механизм не поддерживается» (arch-правка 8)
        AssertFail(result, S3ErrorCode.InvalidRequest);
        result.As<SigV4Result.Fail>().Detail.Should().Be(AuthorizationHeaderParser.UnsupportedMechanismMessage);
    }

    [Fact]
    public void Verify_SkewBeyond15Minutes_RequestTimeTooSkewed()
    {
        // Arrange: время провайдера вне ±15 мин от x-amz-date
        var model = Vector1Model(TestVectors.GetObjSignature);
        var sut = SutAt(TestVectors.SkewedTime);

        // Act
        var result = sut.Verify(model, Resolver);

        // Assert
        AssertFail(result, S3ErrorCode.RequestTimeTooSkewed);
    }

    [Fact]
    public void Verify_UnknownAccessKey_InvalidAccessKeyId()
    {
        // Arrange: resolver не знает ключ → подпись неизвестного не вычисляется
        var model = Vector1Model(TestVectors.GetObjSignature);
        var sut = SutAt(TestVectors.FixedTime);

        // Act
        var result = sut.Verify(model, _ => null);

        // Assert
        AssertFail(result, S3ErrorCode.InvalidAccessKeyId);
    }

    [Fact]
    public void Verify_MissingDateHeaders_AuthorizationHeaderMalformed()
    {
        // Arrange: нет ни x-amz-date, ни Date (arch-правка 2 — Missing)
        var model = Vector1Model(TestVectors.GetObjSignature, includeAmzDate: false);
        var sut = SutAt(TestVectors.FixedTime);

        // Act
        var result = sut.Verify(model, Resolver);

        // Assert
        AssertFail(result, S3ErrorCode.AuthorizationHeaderMalformed);
    }

    [Theory]
    [InlineData("2013-05-24T00:00:00Z")] // не формат yyyyMMdd'T'HHmmss'Z'
    [InlineData("garbage")]
    public void Verify_InvalidAmzDateFormat_AuthorizationHeaderMalformed(string amzDate)
    {
        // Arrange: невалидная строка x-amz-date (arch-правка 2 — Invalid)
        var model = Vector1Model(TestVectors.GetObjSignature, amzDate: amzDate);
        var sut = SutAt(TestVectors.FixedTime);

        // Act
        var result = sut.Verify(model, Resolver);

        // Assert
        AssertFail(result, S3ErrorCode.AuthorizationHeaderMalformed);
    }

    [Fact]
    public void Verify_UnsignedPayload_Ok()
    {
        // Arrange: подпись строит независимый signer, payload = UNSIGNED-PAYLOAD
        var headers = new List<(string, string)>
        {
            ("Host", TestVectors.Host),
            ("x-amz-content-sha256", PayloadHashModeClassifier.UnsignedPayloadValue),
            ("x-amz-date", TestVectors.AmzDate),
        };
        var signature = TestSigV4Signer.HeaderSignature(TestVectors.SecretKey, "GET", "/test.txt", "",
            headers, PayloadHashModeClassifier.UnsignedPayloadValue, TestSigV4Signer.DefaultDate, TestVectors.Region);
        var model = new S3RequestModel
        {
            Method = "GET",
            RawPath = "/test.txt",
            RawQuery = "",
            Host = TestVectors.Host,
            Headers = S3HeaderCollection.FromPairs([.. headers,
                ("Authorization", TestSigV4Signer.BuildAuthorization(TestVectors.AccessKey, TestVectors.Region,
                    TestSigV4Signer.DefaultDate, ["host", "x-amz-content-sha256", "x-amz-date"], signature))]),
            OpenBody = () => Stream.Null,
        };
        var sut = SutAt(TestVectors.FixedTime);

        // Act
        var result = sut.Verify(model, Resolver);

        // Assert
        result.Should().BeOfType<SigV4Result.Ok>()
            .Which.Mode.Should().Be(PayloadHashMode.UnsignedPayload);
    }

    [Fact]
    public void Verify_UnsupportedPayloadValue_InvalidRequest()
    {
        // Arrange: STREAMING-UNSIGNED-PAYLOAD-TRAILER — вне перечня режимов
        var model = Vector1Model(TestVectors.GetObjSignature,
            payloadValue: "STREAMING-UNSIGNED-PAYLOAD-TRAILER");
        var sut = SutAt(TestVectors.FixedTime);

        // Act
        var result = sut.Verify(model, Resolver);

        // Assert
        AssertFail(result, S3ErrorCode.InvalidRequest);
    }

    [Fact]
    public void Verify_StreamingModeOnGet_InvalidRequest()
    {
        // Arrange: чанковый режим на GET — только PUT (arch-правка 4)
        var model = Vector1Model(TestVectors.GetObjSignature,
            payloadValue: PayloadHashModeClassifier.StreamingValue);
        var sut = SutAt(TestVectors.FixedTime);

        // Act
        var result = sut.Verify(model, Resolver);

        // Assert
        AssertFail(result, S3ErrorCode.InvalidRequest);
    }

    [Fact]
    public void Verify_PlusInQueryValue_CanonicalizedAsPercent2B_Ok()
    {
        // Arrange: клиент шлёт prefix=a%2Bb; канонический query — prefix=a%2Bb (arch-правка 9)
        var headers = new List<(string, string)>
        {
            ("Host", TestVectors.Host),
            ("x-amz-content-sha256", TestVectors.EmptySha256),
            ("x-amz-date", TestVectors.AmzDate),
        };
        var signature = TestSigV4Signer.HeaderSignature(TestVectors.SecretKey, "GET", "/",
            "prefix=a%2Bb", headers, TestVectors.EmptySha256, TestSigV4Signer.DefaultDate, TestVectors.Region);
        var model = new S3RequestModel
        {
            Method = "GET",
            RawPath = "/",
            RawQuery = "prefix=a%2Bb",
            Host = TestVectors.Host,
            Headers = S3HeaderCollection.FromPairs([.. headers,
                ("Authorization", TestSigV4Signer.BuildAuthorization(TestVectors.AccessKey, TestVectors.Region,
                    TestSigV4Signer.DefaultDate, ["host", "x-amz-content-sha256", "x-amz-date"], signature))]),
            OpenBody = () => Stream.Null,
        };
        var sut = SutAt(TestVectors.FixedTime);

        // Act
        var result = sut.Verify(model, Resolver);

        // Assert: канонизация вернула '+' как %2B — подпись сошлась
        result.Should().BeOfType<SigV4Result.Ok>();
    }

    [Fact]
    public void Verify_MultiValueHeaders_JoinedWithComma_Ok()
    {
        // Arrange: многозначный заголовок — join через запятую
        var headers = new List<(string, string)>
        {
            ("Host", TestVectors.Host),
            ("x-amz-meta-tag", "first"),
            ("x-amz-meta-tag", "second"),
            ("x-amz-content-sha256", TestVectors.EmptySha256),
            ("x-amz-date", TestVectors.AmzDate),
        };
        var signature = TestSigV4Signer.HeaderSignature(TestVectors.SecretKey, "GET", "/test.txt", "",
            [("host", TestVectors.Host), ("x-amz-meta-tag", "first,second"),
             ("x-amz-content-sha256", TestVectors.EmptySha256), ("x-amz-date", TestVectors.AmzDate)],
            TestVectors.EmptySha256, TestSigV4Signer.DefaultDate, TestVectors.Region);
        var model = new S3RequestModel
        {
            Method = "GET",
            RawPath = "/test.txt",
            RawQuery = "",
            Host = TestVectors.Host,
            Headers = S3HeaderCollection.FromPairs([.. headers,
                ("Authorization", TestSigV4Signer.BuildAuthorization(TestVectors.AccessKey, TestVectors.Region,
                    TestSigV4Signer.DefaultDate, ["host", "x-amz-content-sha256", "x-amz-date", "x-amz-meta-tag"], signature))]),
            OpenBody = () => Stream.Null,
        };
        var sut = SutAt(TestVectors.FixedTime);

        // Act
        var result = sut.Verify(model, Resolver);

        // Assert
        result.Should().BeOfType<SigV4Result.Ok>();
    }

    [Fact]
    public void Verify_TrimAll_CollapsesInnerSpaces_Ok()
    {
        // Arrange: значение заголовка с внутренними последовательностями пробелов —
        // canonical содержит «a b c» (Trimall, arch-правка 7)
        var headers = new List<(string, string)>
        {
            ("Host", TestVectors.Host),
            ("x-amz-note", " a  b \t c "),
            ("x-amz-content-sha256", TestVectors.EmptySha256),
            ("x-amz-date", TestVectors.AmzDate),
        };
        var signature = TestSigV4Signer.HeaderSignature(TestVectors.SecretKey, "GET", "/test.txt", "",
            [("host", TestVectors.Host), ("x-amz-note", "a b c"),
             ("x-amz-content-sha256", TestVectors.EmptySha256), ("x-amz-date", TestVectors.AmzDate)],
            TestVectors.EmptySha256, TestSigV4Signer.DefaultDate, TestVectors.Region);
        var model = new S3RequestModel
        {
            Method = "GET",
            RawPath = "/test.txt",
            RawQuery = "",
            Host = TestVectors.Host,
            Headers = S3HeaderCollection.FromPairs([.. headers,
                ("Authorization", TestSigV4Signer.BuildAuthorization(TestVectors.AccessKey, TestVectors.Region,
                    TestSigV4Signer.DefaultDate, ["host", "x-amz-content-sha256", "x-amz-date", "x-amz-note"], signature))]),
            OpenBody = () => Stream.Null,
        };
        var sut = SutAt(TestVectors.FixedTime);

        // Act
        var result = sut.Verify(model, Resolver);

        // Assert
        result.Should().BeOfType<SigV4Result.Ok>();
    }

    [Fact]
    public void Verify_HeaderNameCaseInsensitive_Ok()
    {
        // Arrange: модель несёт «X-Amz-Date» — доступ регистронезависимый
        var headers = new List<(string, string)>
        {
            ("Host", TestVectors.Host),
            ("x-amz-content-sha256", TestVectors.EmptySha256),
            ("X-Amz-Date", TestVectors.AmzDate),
        };
        var signature = TestSigV4Signer.HeaderSignature(TestVectors.SecretKey, "GET", "/test.txt", "",
            headers.Select(h => (h.Item1, h.Item2)), TestVectors.EmptySha256,
            TestSigV4Signer.DefaultDate, TestVectors.Region);
        var model = new S3RequestModel
        {
            Method = "GET",
            RawPath = "/test.txt",
            RawQuery = "",
            Host = TestVectors.Host,
            Headers = S3HeaderCollection.FromPairs([.. headers,
                ("Authorization", TestSigV4Signer.BuildAuthorization(TestVectors.AccessKey, TestVectors.Region,
                    TestSigV4Signer.DefaultDate, ["host", "x-amz-content-sha256", "x-amz-date"], signature))]),
            OpenBody = () => Stream.Null,
        };
        var sut = SutAt(TestVectors.FixedTime);

        // Act
        var result = sut.Verify(model, Resolver);

        // Assert
        result.Should().BeOfType<SigV4Result.Ok>();
    }

    [Fact]
    public void Verify_HostNotSigned_SignatureDoesNotMatch()
    {
        // Arrange: host отсутствует в SignedHeaders — подпись строится БЕЗ host,
        // канонический запрос верификатора добавляет host → подпись расходится
        var headers = new List<(string, string)>
        {
            ("Host", TestVectors.Host),
            ("x-amz-content-sha256", TestVectors.EmptySha256),
            ("x-amz-date", TestVectors.AmzDate),
        };
        var signature = TestSigV4Signer.HeaderSignature(TestVectors.SecretKey, "GET", "/test.txt", "",
            headers.Where(h => !string.Equals(h.Item1, "host", StringComparison.OrdinalIgnoreCase)),
            TestVectors.EmptySha256, TestSigV4Signer.DefaultDate, TestVectors.Region);
        var model = new S3RequestModel
        {
            Method = "GET",
            RawPath = "/test.txt",
            RawQuery = "",
            Host = TestVectors.Host,
            Headers = S3HeaderCollection.FromPairs([.. headers,
                ("Authorization", TestSigV4Signer.BuildAuthorization(TestVectors.AccessKey, TestVectors.Region,
                    TestSigV4Signer.DefaultDate, ["x-amz-content-sha256", "x-amz-date"], signature))]),
            OpenBody = () => Stream.Null,
        };
        var sut = SutAt(TestVectors.FixedTime);

        // Act
        var result = sut.Verify(model, Resolver);

        // Assert
        AssertFail(result, S3ErrorCode.SignatureDoesNotMatch);
    }

    private static S3RequestModel Vector1Model(string signature, string? scope = null,
        string? payloadValue = null, string? amzDate = null, bool includeAmzDate = true)
    {
        var headers = new List<(string, string)> { ("Host", TestVectors.Host) };
        headers.Add(("Range", "bytes=0-9"));
        headers.Add(("x-amz-content-sha256", payloadValue ?? TestVectors.EmptySha256));
        if (includeAmzDate)
            headers.Add(("x-amz-date", amzDate ?? TestVectors.AmzDate));
        headers.Add(("Authorization", "AWS4-HMAC-SHA256 Credential=" + TestVectors.AccessKey + "/" +
            (scope ?? TestVectors.Scope) + ", SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=" + signature));
        return new S3RequestModel
        {
            Method = "GET",
            RawPath = "/test.txt",
            RawQuery = "",
            Host = TestVectors.Host,
            Headers = S3HeaderCollection.FromPairs(headers),
            OpenBody = () => Stream.Null,
        };
    }
}
