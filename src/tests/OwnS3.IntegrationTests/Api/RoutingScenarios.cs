using System.Net;
using System.Text;
using System.Xml.Linq;
using OwnS3.Protocol.Auth;

namespace OwnS3.IntegrationTests.Api;

// Роутинг 22 операций (глава 02) на реальном объектном слое (t37): немутационные
// кейсы — на никогда не создаваемом бакете «bucket» (404 NoSuchBucket/NoSuchKey,
// multipart — 500 заглушки до t38); мутационные — на уникальном бакете своего
// кейса; GetBucketLocation — полный успех 200; вне-наборные сабресурсы — 501 до
// аутентификации; не-матч — 400; OPTIONS — 200 без CORS.
public sealed class RoutingScenarios(OwnS3AppFactory factory) : IClassFixture<OwnS3AppFactory>
{
    private OwnS3TestClient NewClient() => new(factory.CreateClient());

    // Немутационные операции (бакет «bucket» никогда не создаётся):
    // (метод, путь+query, ожидаемый статус, ожидаемый код ошибки XML или null).
    public static readonly TheoryData<string, string, HttpStatusCode, string?> StorageOperations = new()
    {
        // GET / — ListBuckets: пустой список бакетов, статус не зависит от содержимого
        { "GET",     "/",                            HttpStatusCode.OK,                   null },
        { "DELETE",  "/bucket",                      HttpStatusCode.NotFound,             "NoSuchBucket" },
        { "HEAD",    "/bucket",                      HttpStatusCode.NotFound,             null }, // HEAD: только статус
        { "GET",     "/bucket?prefix=x",             HttpStatusCode.NotFound,             "NoSuchBucket" },
        { "GET",     "/bucket?list-type=2",          HttpStatusCode.NotFound,             "NoSuchBucket" },
        { "GET",     "/bucket?versions",             HttpStatusCode.NotFound,             "NoSuchBucket" },
        // multipart-заглушки — 500 InternalError (граница t38)
        { "GET",     "/bucket?uploads",              HttpStatusCode.InternalServerError,  "InternalError" },
        { "GET",     "/bucket/key",                  HttpStatusCode.NotFound,             "NoSuchBucket" },
        { "HEAD",    "/bucket/key",                  HttpStatusCode.NotFound,             null }, // HEAD: только статус
        { "DELETE",  "/bucket/key",                  HttpStatusCode.NotFound,             "NoSuchBucket" },
        { "DELETE",  "/bucket/key?uploadId=u",       HttpStatusCode.InternalServerError,  "InternalError" },
        { "GET",     "/bucket/key?uploadId=u",       HttpStatusCode.InternalServerError,  "InternalError" },
        { "POST",    "/bucket/key?uploads",          HttpStatusCode.InternalServerError,  "InternalError" },
        { "GET",     "/bucket/key?attributes",       HttpStatusCode.NotFound,             "NoSuchBucket" },
    };

    [Theory, MemberData(nameof(StorageOperations))]
    public async Task StorageOperation_ReachesStorage_WithExpectedOutcome(string method, string pathAndQuery,
        HttpStatusCode expectedStatus, string? expectedCode)
    {
        // Arrange: атрибуты обязательны для GetObjectAttributes; тела POST — пустой
        // массив; мутация бакета (DELETE /bucket) — право admin (глава 05 §3)
        var client = NewClient();
        var headers = pathAndQuery.Contains("attributes", StringComparison.Ordinal)
            ? new Dictionary<string, string> { ["x-amz-object-attributes"] = "ETag" }
            : null;
        var isAdminMutation = method == "DELETE" && pathAndQuery == "/bucket";

        // Act
        var response = await client.SendSignedAsync(method, pathAndQuery,
            headers: headers, credentials: isAdminMutation ? OwnS3TestClient.Admin() : null,
            body: method == "POST" && pathAndQuery.Contains("uploads") ? [] : null);

        // Assert: операция определена и дошла до Storage (HEAD-ошибки — без тела)
        response.StatusCode.Should().Be(expectedStatus);
        if (expectedCode is not null && method != "HEAD")
            (await ErrorXmlAsync(response)).Should().Be(expectedCode);
    }

    [Fact]
    public async Task CreateBucket_Admin_200()
    {
        // Arrange: мутационный кейс — уникальный бакет СВОЕГО кейса (ограничение 8)
        var client = NewClient();

        // Act
        var response = await client.SendSignedAsync("PUT", "/b-create",
            credentials: OwnS3TestClient.Admin());

        // Assert: бакет реально создан на томе
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        Directory.Exists(Path.Combine(factory.TempVolumeDir, "b-create")).Should().BeTrue();
    }

    [Fact]
    public async Task PutObject_ReachesStorage()
    {
        // Arrange / Act: бакет не создавался — 404 от Storage
        var response = await NewClient().SendSignedAsync("PUT", "/bucket/key",
            body: Encoding.UTF8.GetBytes("hello"));

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await ErrorXmlAsync(response)).Should().Be("NoSuchBucket");
    }

    [Fact]
    public async Task CopyObject_ReachesStorage()
    {
        // Arrange / Act: dest-бакета нет
        var response = await NewClient().SendSignedAsync("PUT", "/bucket/dest-key",
            headers: new Dictionary<string, string> { ["x-amz-copy-source"] = "/bucket/src-key" });

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await ErrorXmlAsync(response)).Should().Be("NoSuchBucket");
    }

    [Fact]
    public async Task UploadPart_And_UploadPartCopy_ReachStorage()
    {
        // Arrange
        var client = NewClient();

        // Act
        var uploadPart = await client.SendSignedAsync("PUT", "/bucket/key?partNumber=1&uploadId=u",
            body: Encoding.UTF8.GetBytes("part"));
        var uploadPartCopy = await client.SendSignedAsync("PUT", "/bucket/key?partNumber=1&uploadId=u",
            headers: new Dictionary<string, string> { ["x-amz-copy-source"] = "/bucket/src" });

        // Assert: multipart-заглушки — не меняются (до t38)
        uploadPart.StatusCode.Should().Be(HttpStatusCode.InternalServerError);
        uploadPartCopy.StatusCode.Should().Be(HttpStatusCode.InternalServerError);
    }

    [Fact]
    public async Task DeleteObjects_ReachesStorage()
    {
        // Arrange: валидный XML Delete с одним ключом
        var xml = """<?xml version="1.0" encoding="utf-8"?><Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Object><Key>k</Key></Object><Quiet>false</Quiet></Delete>""";

        // Act
        var response = await NewClient().SendSignedAsync("POST", "/bucket?delete",
            body: Encoding.UTF8.GetBytes(xml));

        // Assert: бакета нет — 404 от Storage
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await ErrorXmlAsync(response)).Should().Be("NoSuchBucket");
    }

    [Fact]
    public async Task CompleteMultipartUpload_ReachesStorage()
    {
        // Arrange: манифест из одной части (валидация порядка проходит)
        var xml = """<?xml version="1.0" encoding="utf-8"?><CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Part><PartNumber>1</PartNumber><ETag>"e"</ETag></Part></CompleteMultipartUpload>""";

        // Act
        var response = await NewClient().SendSignedAsync("POST", "/bucket/key?uploadId=u",
            body: Encoding.UTF8.GetBytes(xml));

        // Assert: multipart-заглушка (до t38)
        response.StatusCode.Should().Be(HttpStatusCode.InternalServerError);
    }

    [Theory]
    [InlineData("/bucket/caf%C3%A9")]   // ключ «café» — UTF-8 percent-кодирование
    [InlineData("/bucket/100%25")]      // ключ «100%» — литеральный процент
    public async Task EncodedKey_SignedByRawPath_ReachesStorage(string pathAndQuery)
    {
        // Arrange: подпись по СЫРОМУ кодированному пути (модель строится из
        // IHttpRequestFeature.RawTarget — не декодированного Request.Path)
        var client = NewClient();

        // Act
        var response = await client.SendSignedAsync("GET", pathAndQuery);

        // Assert: подпись сошлась на сырых байтах пути — операция определена,
        // дошла до Storage: бакета нет
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await ErrorXmlAsync(response)).Should().Be("NoSuchBucket");
    }

    [Fact]
    public async Task GetBucketLocation_Returns200WithEmptyLocationConstraint()
    {
        // Arrange / Act
        var response = await NewClient().SendSignedAsync("GET", "/bucket?location");

        // Assert: полностью протокольная операция t36 — успех без объектного слоя
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Content.Headers.ContentType!.MediaType.Should().Be("application/xml");
        (await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).Should().Be(
            """<?xml version="1.0" encoding="utf-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/" />""");
    }

    [Theory]
    [InlineData("GET")]
    [InlineData("PUT")]
    [InlineData("DELETE")]
    public async Task OutOfScopeSubresource_NotImplemented_BeforeAuthentication(string method)
    {
        // Arrange: битая подпись — исход тот же (501 до аутентификации)
        var client = NewClient();

        // Act
        var response = await client.SendSignedAsync(method, "/bucket/key?acl",
            at: OwnS3AppFactory.HostTime.AddHours(1)); // skew-невалидная подпись

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.NotImplemented);
        (await ErrorXmlAsync(response)).Should().Be("NotImplemented");
    }

    [Theory]
    [InlineData("GET", "/bucket/key?tagging")]
    [InlineData("GET", "/bucket?versioning")]
    [InlineData("PUT", "/bucket?policy")]
    [InlineData("GET", "/bucket/key?select&select-type=2")]
    public async Task OutOfScopeSubresources_NotImplemented(string method, string pathAndQuery)
    {
        // Arrange / Act
        var response = await NewClient().SendSignedAsync(method, pathAndQuery);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.NotImplemented);
    }

    [Theory]
    [InlineData("POST", "/bucket")]
    [InlineData("PATCH", "/bucket/key")]
    [InlineData("POST", "/")]
    public async Task UnmatchedRequest_InvalidArgumentUnsupported(string method, string pathAndQuery)
    {
        // Arrange / Act
        var response = await NewClient().SendSignedAsync(method, pathAndQuery);

        // Assert: не-матч → 400 InvalidArgument «Unsupported request» (арх-правка 6)
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await ErrorXmlAsync(response)).Should().Be("InvalidArgument");
    }

    [Theory]
    [InlineData("OPTIONS", "/")]
    [InlineData("OPTIONS", "/bucket/key")]
    public async Task Options_Returns200WithoutCorsHeaders(string method, string pathAndQuery)
    {
        // Arrange / Act
        var response = await NewClient().SendAnonymousAsync(method, pathAndQuery);

        // Assert: пустой 200 до аутентификации, CORS-заголовков нет
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Headers.Should().NotContain(h => h.Key.StartsWith("Access-Control", StringComparison.OrdinalIgnoreCase));
    }

    internal static async Task<string> ErrorXmlAsync(HttpResponseMessage response)
    {
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        if (body.Length == 0)
            return string.Empty;
        var xml = XDocument.Parse(body);
        return xml.Root!.Name.LocalName == "Error"
            ? xml.Root.Element("Code")!.Value
            : xml.Root.Name.LocalName;
    }
}
