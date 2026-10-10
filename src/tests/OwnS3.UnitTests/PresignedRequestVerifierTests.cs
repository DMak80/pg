using System.Text;
using OwnS3.Protocol.Auth;
using OwnS3.Protocol.Errors;
using OwnS3.Protocol.Requests;

namespace OwnS3.UnitTests;

// Presigned SigV4 (arch/owns3/03 §2): вектор 3 AWS (внешний источник) +
// signer-позитивы и исходы-отказы (полнота параметров, операция, Expires,
// просрочка, skew, ключ, подпись).
public sealed class PresignedRequestVerifierTests
{
    private static void AssertFail(SigV4Result result, S3ErrorCode code) =>
        result.Should().BeOfType<SigV4Result.Fail>().Which.Code.Should().Be(code);

    private static PresignedRequestVerifier SutAt(DateTimeOffset now) => new(new FixedTimeProvider(now));

    private static string Resolver(string ak) =>
        ak == TestVectors.AccessKey ? TestVectors.SecretKey : null!;

    // Строка presigned-параметров вектора 3 (как строит клиент; канонический порядок).
    private const string Vector3Query =
        "X-Amz-Algorithm=AWS4-HMAC-SHA256" +
        "&X-Amz-Credential=" + TestVectors.AccessKey + "%2F20130524%2Fus-east-1%2Fs3%2Faws4_request" +
        "&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host" +
        "&X-Amz-Signature=" + TestVectors.PresignedSignature;

    [Fact]
    public void Verify_AwsVector3_PresignedGet_Ok()
    {
        // Arrange: официальный пример AWS presigned GET
        var model = new S3RequestModel
        {
            Method = "GET",
            RawPath = "/test.txt",
            RawQuery = Vector3Query,
            Host = TestVectors.Host,
            Headers = S3HeaderCollection.FromPairs(("Host", TestVectors.Host)),
            OpenBody = () => Stream.Null,
        };
        var sut = SutAt(TestVectors.FixedTime);

        // Act
        var result = sut.Verify(model, "GetObject", Resolver);

        // Assert: query-подпись официального примера проходит
        result.Should().BeOfType<SigV4Result.Ok>("фактический: {0}", result)
            .Which.SeedSignature.Should().Be(TestVectors.PresignedSignature);
    }

    [Fact]
    public void Verify_ExtraSignedQueryParam_IncludedInSignature_Ok()
    {
        // Arrange: response-content-type входит в подпись (signer-позитив)
        // Каноническая сортировка по ключу ordinal: «X-Amz-*» раньше «response-*»
        var canonicalQuery =
            "X-Amz-Algorithm=AWS4-HMAC-SHA256" +
            "&X-Amz-Credential=" + TestVectors.AccessKey + "%2F20130524%2Fus-east-1%2Fs3%2Faws4_request" +
            "&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host" +
            "&response-content-type=image%2Fjpeg";
        var signature = TestSigV4Signer.HeaderSignature(TestVectors.SecretKey, "GET", "/test.txt",
            canonicalQuery, [("host", TestVectors.Host)],
            PayloadHashModeClassifier.UnsignedPayloadValue, TestSigV4Signer.DefaultDate, TestVectors.Region);
        var rawQuery = "X-Amz-Algorithm=AWS4-HMAC-SHA256" +
            "&X-Amz-Credential=" + TestVectors.AccessKey + "%2F20130524%2Fus-east-1%2Fs3%2Faws4_request" +
            "&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host" +
            "&response-content-type=image%2Fjpeg&X-Amz-Signature=" + signature;
        var model = new S3RequestModel
        {
            Method = "GET",
            RawPath = "/test.txt",
            RawQuery = rawQuery,
            Host = TestVectors.Host,
            Headers = S3HeaderCollection.FromPairs(("Host", TestVectors.Host)),
            OpenBody = () => Stream.Null,
        };
        var sut = SutAt(TestVectors.FixedTime);

        // Act
        var result = sut.Verify(model, "GetObject", Resolver);

        // Assert: лишний (не-X-Amz-*) параметр подписан — сверка прошла
        result.Should().BeOfType<SigV4Result.Ok>();
    }

    [Fact]
    public void Verify_Expired_AccessDenied()
    {
        // Arrange: now − X-Amz-Date > X-Amz-Expires — строгая просрочка
        // (arch-правка 10: без skew-допуска)
        var model = PresignedModel(expires: "1");
        var sut = SutAt(TestVectors.SkewedTime); // now = +20 мин; истёк в +1 с

        // Act
        var result = sut.Verify(model, "GetObject", Resolver);

        // Assert
        AssertFail(result, S3ErrorCode.AccessDenied);
    }

    [Theory]
    [InlineData("604801")] // больше 7 дней
    [InlineData("-1")]     // отрицательное
    [InlineData("abc")]    // не число
    public void Verify_BadExpires_AuthorizationQueryParametersError(string expires)
    {
        // Arrange
        var model = PresignedModel(expires: expires);
        var sut = SutAt(TestVectors.FixedTime);

        // Act
        var result = sut.Verify(model, "GetObject", Resolver);

        // Assert
        AssertFail(result, S3ErrorCode.AuthorizationQueryParametersError);
    }

    [Theory]
    [InlineData("X-Amz-Algorithm")]
    [InlineData("X-Amz-Credential")]
    [InlineData("X-Amz-Date")]
    [InlineData("X-Amz-Expires")]
    [InlineData("X-Amz-SignedHeaders")]
    [InlineData("X-Amz-Signature")]
    public void Verify_MissingRequiredParameter_AuthorizationQueryParametersError(string missing)
    {
        // Arrange: из query удалён ровно один обязательный параметр
        var query = string.Join("&", Vector3Query.Split('&')
            .Where(p => !p.StartsWith(missing + "=", StringComparison.Ordinal)));
        var model = new S3RequestModel
        {
            Method = "GET",
            RawPath = "/test.txt",
            RawQuery = query,
            Host = TestVectors.Host,
            Headers = S3HeaderCollection.FromPairs(("Host", TestVectors.Host)),
            OpenBody = () => Stream.Null,
        };
        var sut = SutAt(TestVectors.FixedTime);

        // Act
        var result = sut.Verify(model, "GetObject", Resolver);

        // Assert
        AssertFail(result, S3ErrorCode.AuthorizationQueryParametersError);
    }

    [Fact]
    public void Verify_OperationNotAllowed_AuthorizationQueryParametersError()
    {
        // Arrange: ListObjects вне списка 10 разрешённых
        var model = PresignedModel();
        var sut = SutAt(TestVectors.FixedTime);

        // Act
        var result = sut.Verify(model, "ListObjects", Resolver);

        // Assert
        AssertFail(result, S3ErrorCode.AuthorizationQueryParametersError);
    }

    [Fact]
    public void Verify_UsedAfter15MinutesInsideWindow_Ok()
    {
        // Arrange: использование через N > 15 мин после подписи внутри окна
        // X-Amz-Expires — Abs-skew для прошедших дат отсутствует (arch-правка 10)
        var model = PresignedModel();
        var sut = SutAt(TestVectors.SkewedTime); // now = дата подписи + 20 мин; окно 86400 с

        // Act
        var result = sut.Verify(model, "GetObject", Resolver);

        // Assert: непросроченный presigned принимается независимо от возраста
        result.Should().BeOfType<SigV4Result.Ok>();
    }

    [Fact]
    public void Verify_FutureDateBeyond15Minutes_RequestTimeTooSkewed()
    {
        // Arrange: X-Amz-Date в будущем дальше now + 15 минут (подпись «из
        // будущего»: now = дата подписи − 20 мин) — arch-правка 10
        var model = PresignedModel();
        var sut = SutAt(TestSigV4Signer.DefaultDate.AddMinutes(-20));

        // Act
        var result = sut.Verify(model, "GetObject", Resolver);

        // Assert: отступление от референса — нормализация в RequestTimeTooSkewed
        AssertFail(result, S3ErrorCode.RequestTimeTooSkewed);
    }

    [Fact]
    public void Verify_ExactlyAtExpiryBoundary_Ok()
    {
        // Arrange: now − X-Amz-Date == X-Amz-Expires (ровно на границе, Expires=60)
        // — просрочка строгое неравенство, граница ещё валидна (arch-правка 10);
        // подпись пересчитана signer'ом: Expires входит в canonical query
        var canonicalQuery =
            "X-Amz-Algorithm=AWS4-HMAC-SHA256" +
            "&X-Amz-Credential=" + TestVectors.AccessKey + "%2F20130524%2Fus-east-1%2Fs3%2Faws4_request" +
            "&X-Amz-Date=20130524T000000Z&X-Amz-Expires=60&X-Amz-SignedHeaders=host";
        var signature = TestSigV4Signer.HeaderSignature(TestVectors.SecretKey, "GET", "/test.txt",
            canonicalQuery, [("host", TestVectors.Host)],
            PayloadHashModeClassifier.UnsignedPayloadValue, TestSigV4Signer.DefaultDate, TestVectors.Region);
        var model = PresignedModel(expires: "60", signature: signature);
        var sut = SutAt(TestSigV4Signer.DefaultDate.AddSeconds(60));

        // Act
        var result = sut.Verify(model, "GetObject", Resolver);

        // Assert: граница окна — не просрочка
        result.Should().BeOfType<SigV4Result.Ok>();
    }

    [Fact]
    public void Verify_UnknownAccessKey_InvalidAccessKeyId()
    {
        // Arrange
        var model = PresignedModel();
        var sut = SutAt(TestVectors.FixedTime);

        // Act
        var result = sut.Verify(model, "GetObject", _ => null);

        // Assert
        AssertFail(result, S3ErrorCode.InvalidAccessKeyId);
    }

    [Fact]
    public void Verify_BrokenSignature_SignatureDoesNotMatch()
    {
        // Arrange: последний символ подписи заменён
        var broken = TestVectors.PresignedSignature[..63] +
            (TestVectors.PresignedSignature[^1] == '0' ? '1' : '0');
        var model = PresignedModel(signature: broken);
        var sut = SutAt(TestVectors.FixedTime);

        // Act
        var result = sut.Verify(model, "GetObject", Resolver);

        // Assert
        AssertFail(result, S3ErrorCode.SignatureDoesNotMatch);
    }

    // Presigned-модель с подменяемыми частями (подпись/Expires) — для негативов
    // подпись остаётся векторной (сверяется раньше просрочки только при валидности).
    private static S3RequestModel PresignedModel(string? expires = null, string? signature = null)
    {
        var query = Vector3Query
            .Replace("&X-Amz-Expires=86400", "&X-Amz-Expires=" + (expires ?? "86400"))
            .Replace("&X-Amz-Signature=" + TestVectors.PresignedSignature, "&X-Amz-Signature=" + (signature ?? TestVectors.PresignedSignature));
        return new S3RequestModel
        {
            Method = "GET",
            RawPath = "/test.txt",
            RawQuery = query,
            Host = TestVectors.Host,
            Headers = S3HeaderCollection.FromPairs(("Host", TestVectors.Host)),
            OpenBody = () => new MemoryStream([]),
        };
    }
}
