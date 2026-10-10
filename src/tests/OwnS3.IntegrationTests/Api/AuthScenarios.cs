using System.Net;
using System.Text;
using OwnS3.UnitTests;

namespace OwnS3.IntegrationTests.Api;

// Аутентификация (глава 03 §1–2 + арх-правки 2/8): все исходы заголовочного
// режима и presigned.
[Collection(OwnS3TestCollection.Name)]
public sealed class AuthScenarios(OwnS3AppFactory factory)
{
    private OwnS3TestClient NewClient() => new(factory.CreateClient());

    [Fact]
    public async Task ValidSignature_ReachesStub_500()
    {
        // Arrange / Act
        var response = await NewClient().SendSignedAsync("GET", "/bucket/key");

        // Assert: подпись прошла, запрос дошёл до заглушки
        response.StatusCode.Should().Be(HttpStatusCode.InternalServerError);
    }

    [Fact]
    public async Task BrokenSignature_403()
    {
        // Arrange: подпись с другим секретом
        var client = NewClient();

        // Act
        var response = await client.SendSignedAsync("GET", "/bucket/key",
            credentials: new OwnS3TestClient.Credentials(OwnS3AppFactory.WriterAccessKey, "wrong-secret"));

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.Forbidden);
        (await RoutingScenarios.ErrorXmlAsync(response)).Should().Be("SignatureDoesNotMatch");
    }

    [Fact]
    public async Task Anonymous_403()
    {
        // Arrange / Act
        var response = await NewClient().SendAnonymousAsync("GET", "/bucket/key");

        // Assert: public-доступа нет
        response.StatusCode.Should().Be(HttpStatusCode.Forbidden);
        (await RoutingScenarios.ErrorXmlAsync(response)).Should().Be("AccessDenied");
    }

    [Fact]
    public async Task SkewedDate_403()
    {
        // Arrange: x-amz-date на 20 минут в будущем относительно HostTime
        // Act
        var response = await NewClient().SendSignedAsync("GET", "/bucket/key",
            at: OwnS3AppFactory.HostTime.AddMinutes(20));

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.Forbidden);
        (await RoutingScenarios.ErrorXmlAsync(response)).Should().Be("RequestTimeTooSkewed");
    }

    [Fact]
    public async Task UnknownAccessKey_403()
    {
        // Arrange / Act
        var response = await NewClient().SendSignedAsync("GET", "/bucket/key",
            credentials: new OwnS3TestClient.Credentials("nobody", "secret"));

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.Forbidden);
        (await RoutingScenarios.ErrorXmlAsync(response)).Should().Be("InvalidAccessKeyId");
    }

    [Fact]
    public async Task BrokenScope_400()
    {
        // Arrange: сервис в scope — sns (заголовок малформирован)
        var client = NewClient();
        var date = OwnS3AppFactory.HostTime;
        var secret = OwnS3AppFactory.WriterSecretKey;
        var headers = new List<(string, string)>
        {
            ("host", client.Http.BaseAddress!.Authority),
            ("x-amz-content-sha256", OwnS3.Protocol.Auth.PayloadHashModeClassifier.UnsignedPayloadValue),
            ("x-amz-date", TestSigV4Signer.AmzDateOf(date)),
        };
        var signature = TestSigV4Signer.HeaderSignature(secret, "GET", "/bucket/key", "",
            headers, OwnS3.Protocol.Auth.PayloadHashModeClassifier.UnsignedPayloadValue, date, "us-east-1");
        // Подмена сервиса в credential scope.
        var amzDate = TestSigV4Signer.AmzDateOf(date);
        var authorization =
            $"AWS4-HMAC-SHA256 Credential={OwnS3AppFactory.WriterAccessKey}/{amzDate[..8]}/us-east-1/sns/aws4_request, " +
            $"SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={signature}";

        using var request = new HttpRequestMessage(HttpMethod.Get, "/bucket/key");
        request.Headers.TryAddWithoutValidation("x-amz-date", amzDate);
        request.Headers.TryAddWithoutValidation("x-amz-content-sha256",
            OwnS3.Protocol.Auth.PayloadHashModeClassifier.UnsignedPayloadValue);
        request.Headers.TryAddWithoutValidation("Authorization", authorization);

        // Act
        var response = await client.Http.SendAsync(request, TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await RoutingScenarios.ErrorXmlAsync(response)).Should().Be("AuthorizationHeaderMalformed");
    }

    [Theory]
    [InlineData("2013-05-24T00:00:00Z")]   // не формат yyyyMMdd'T'HHmmss'Z'
    [InlineData("garbage")]
    public async Task InvalidAmzDateFormat_400(string badDate)
    {
        // Arrange: подпись по валидной дате, заголовок подменён на битый формат
        var client = NewClient();
        var date = OwnS3AppFactory.HostTime;
        var secret = OwnS3AppFactory.WriterSecretKey;
        var headers = new List<(string, string)>
        {
            ("host", client.Http.BaseAddress!.Authority),
            ("x-amz-content-sha256", OwnS3.Protocol.Auth.PayloadHashModeClassifier.UnsignedPayloadValue),
            ("x-amz-date", TestSigV4Signer.AmzDateOf(date)),
        };
        var signature = TestSigV4Signer.HeaderSignature(secret, "GET", "/bucket/key", "",
            headers, OwnS3.Protocol.Auth.PayloadHashModeClassifier.UnsignedPayloadValue, date, "us-east-1");

        using var request = new HttpRequestMessage(HttpMethod.Get, "/bucket/key");
        request.Headers.TryAddWithoutValidation("x-amz-date", badDate);
        request.Headers.TryAddWithoutValidation("x-amz-content-sha256",
            OwnS3.Protocol.Auth.PayloadHashModeClassifier.UnsignedPayloadValue);
        request.Headers.TryAddWithoutValidation("Authorization",
            TestSigV4Signer.BuildAuthorization(OwnS3AppFactory.WriterAccessKey, "us-east-1", date,
                ["host", "x-amz-content-sha256", "x-amz-date"], signature));

        // Act
        var response = await client.Http.SendAsync(request, TestContext.Current.CancellationToken);

        // Assert: Missing/Invalid x-amz-date (арх-правка 2 — оба исхода)
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await RoutingScenarios.ErrorXmlAsync(response)).Should().Be("AuthorizationHeaderMalformed");
    }

    [Theory]
    [InlineData("AWS AKIAIOSFODNN7EXAMPLE:bXclmGdFPCW7qBUXiqxZ4iKzOoQzR1Df")]   // SigV2
    [InlineData("AWS4-HMAC-SHA512 Credential=testwriter/20130524/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=abc")]
    public async Task UnsupportedAlgorithm_400(string authorization)
    {
        // Arrange
        var client = NewClient();
        using var request = new HttpRequestMessage(HttpMethod.Get, "/bucket/key");
        request.Headers.TryAddWithoutValidation("Authorization", authorization);

        // Act
        var response = await client.Http.SendAsync(request, TestContext.Current.CancellationToken);

        // Assert: «механизм не поддерживается» (арх-правка 8)
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await RoutingScenarios.ErrorXmlAsync(response)).Should().Be("InvalidRequest");
    }

    [Fact]
    public async Task Presigned_GetObject_ReachesStub()
    {
        // Arrange / Act
        var client = NewClient();
        var url = client.BuildPresignedUrl("GET", "/bucket/key", OwnS3TestClient.Writer());
        var response = await client.SendPresignedAsync("GET", url);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.InternalServerError);
    }

    [Fact]
    public async Task Presigned_Expired_403()
    {
        // Arrange: подпись задолго до HostTime, окно 60 с — строгая просрочка
        // now − X-Amz-Date > X-Amz-Expires (arch-правка 10)
        var client = NewClient();
        var url = client.BuildPresignedUrl("GET", "/bucket/key", OwnS3TestClient.Writer(),
            expiresSeconds: 60, at: OwnS3AppFactory.HostTime.AddDays(-1));

        // Act
        var response = await client.SendPresignedAsync("GET", url);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.Forbidden);
        (await RoutingScenarios.ErrorXmlAsync(response)).Should().Be("AccessDenied");
    }

    [Fact]
    public async Task Presigned_UsedAfter15MinutesInsideWindow_ReachesStub()
    {
        // Arrange: presigned подписан 20 минут назад, окно 86400 с — Abs-skew
        // для прошедших дат отсутствует (arch-правка 10): URL валиден всё окно
        var client = NewClient();
        var url = client.BuildPresignedUrl("GET", "/bucket/key", OwnS3TestClient.Writer(),
            expiresSeconds: 86400, at: OwnS3AppFactory.HostTime.AddMinutes(-20));

        // Act
        var response = await client.SendPresignedAsync("GET", url);

        // Assert: внутри окна — подпись валидна, запрос дошёл до заглушки
        response.StatusCode.Should().Be(HttpStatusCode.InternalServerError);
    }

    [Fact]
    public async Task Presigned_FutureDateBeyond15Minutes_403()
    {
        // Arrange: X-Amz-Date на 20 минут в будущем от HostTime — дальше
        // now + 15 минут (arch-правка 10: skew presigned — только на будущее)
        var client = NewClient();
        var url = client.BuildPresignedUrl("GET", "/bucket/key", OwnS3TestClient.Writer(),
            expiresSeconds: 86400, at: OwnS3AppFactory.HostTime.AddMinutes(20));

        // Act
        var response = await client.SendPresignedAsync("GET", url);

        // Assert: отступление от референса — нормализация в RequestTimeTooSkewed
        response.StatusCode.Should().Be(HttpStatusCode.Forbidden);
        (await RoutingScenarios.ErrorXmlAsync(response)).Should().Be("RequestTimeTooSkewed");
    }

    [Fact]
    public async Task Presigned_ExpiresOver604800_400()
    {
        // Arrange / Act
        var client = NewClient();
        var url = client.BuildPresignedUrl("GET", "/bucket/key", OwnS3TestClient.Writer(),
            expiresSeconds: 604801);
        var response = await client.SendPresignedAsync("GET", url);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await RoutingScenarios.ErrorXmlAsync(response)).Should().Be("AuthorizationQueryParametersError");
    }

    [Fact]
    public async Task Presigned_MissingParameter_400()
    {
        // Arrange: URL без X-Amz-Expires
        var client = NewClient();
        var full = client.BuildPresignedUrl("GET", "/bucket/key", OwnS3TestClient.Writer());
        var url = RemoveQueryParameter(full, "X-Amz-Expires");

        // Act
        var response = await client.SendPresignedAsync("GET", url);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await RoutingScenarios.ErrorXmlAsync(response)).Should().Be("AuthorizationQueryParametersError");
    }

    [Fact]
    public async Task Presigned_OperationNotAllowed_400()
    {
        // Arrange: presigned на ListObjects — вне списка 10 (глава 03 §2)
        var client = NewClient();
        var url = client.BuildPresignedUrl("GET", "/bucket", OwnS3TestClient.Writer());

        // Act
        var response = await client.SendPresignedAsync("GET", url);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await RoutingScenarios.ErrorXmlAsync(response)).Should().Be("AuthorizationQueryParametersError");
    }

    private static string RemoveQueryParameter(string url, string name)
    {
        var q = url.IndexOf('?');
        var pairs = url[(q + 1)..].Split('&').Where(p =>
            !p.StartsWith(name + "=", StringComparison.Ordinal));
        return url[..q] + "?" + string.Join("&", pairs);
    }
}
