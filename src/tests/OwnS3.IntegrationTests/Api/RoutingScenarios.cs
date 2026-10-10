using System.Net;
using System.Text;
using System.Xml.Linq;
using OwnS3.Protocol.Auth;

namespace OwnS3.IntegrationTests.Api;

// Роутинг 22 операций (глава 02): каждая матчится по дискриминаторам и доходит
// до заглушки объектного слоя (500 InternalError); GetBucketLocation — полный
// успех 200; вне-наборные сабресурсы — 501 до аутентификации; не-матч — 400;
// OPTIONS — 200 без CORS.
[Collection(OwnS3TestCollection.Name)]
public sealed class RoutingScenarios(OwnS3AppFactory factory)
{
    private OwnS3TestClient NewClient() => new(factory.CreateClient());

    // 21 операция (все, кроме полностью протокольной GetBucketLocation):
    // (метод, путь+query, минимальные заголовки/тело для прохождения валидаций).
    public static readonly TheoryData<string, string> StubOperations = new()
    {
        { "GET",     "/" },
        { "PUT",     "/bucket" },
        { "DELETE",  "/bucket" },
        { "HEAD",    "/bucket" },
        { "GET",     "/bucket?prefix=x" },
        { "GET",     "/bucket?list-type=2" },
        { "GET",     "/bucket?versions" },
        { "GET",     "/bucket?uploads" },
        { "GET",     "/bucket/key" },
        { "HEAD",    "/bucket/key" },
        { "DELETE",  "/bucket/key" },
        { "DELETE",  "/bucket/key?uploadId=u" },
        { "GET",     "/bucket/key?uploadId=u" },
        { "POST",    "/bucket/key?uploads" },
        { "GET",     "/bucket/key?attributes" },
    };

    [Theory, MemberData(nameof(StubOperations))]
    public async Task StubOperation_ReachesObjectStore_WithInternalError(string method, string pathAndQuery)
    {
        // Arrange: бакетные мутации — право admin (глава 05 §3)
        var client = NewClient();
        var headers = pathAndQuery.Contains("attributes", StringComparison.Ordinal)
            ? new Dictionary<string, string> { ["x-amz-object-attributes"] = "ETag" }
            : null;
        var isAdminMutation = method is "PUT" or "DELETE" && pathAndQuery == "/bucket";

        // Act
        var response = await client.SendSignedAsync(method, pathAndQuery,
            headers: headers, credentials: isAdminMutation ? OwnS3TestClient.Admin() : null,
            body: method == "POST" && pathAndQuery.Contains("uploads") ? [] : null);

        // Assert: операция определена и дошла до заглушки (500 InternalError);
        // HEAD-ответ ошибки — без тела (глава 03 §5)
        response.StatusCode.Should().Be(HttpStatusCode.InternalServerError);
        if (method != "HEAD")
            (await ErrorXmlAsync(response)).Should().Be("InternalError");
    }

    [Fact]
    public async Task PutObject_ReachesObjectStore()
    {
        // Arrange / Act
        var response = await NewClient().SendSignedAsync("PUT", "/bucket/key",
            body: Encoding.UTF8.GetBytes("hello"));

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.InternalServerError);
    }

    [Fact]
    public async Task CopyObject_ReachesObjectStore()
    {
        // Arrange / Act
        var response = await NewClient().SendSignedAsync("PUT", "/bucket/dest-key",
            headers: new Dictionary<string, string> { ["x-amz-copy-source"] = "/bucket/src-key" });

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.InternalServerError);
    }

    [Fact]
    public async Task UploadPart_And_UploadPartCopy_ReachObjectStore()
    {
        // Arrange
        var client = NewClient();

        // Act
        var uploadPart = await client.SendSignedAsync("PUT", "/bucket/key?partNumber=1&uploadId=u",
            body: Encoding.UTF8.GetBytes("part"));
        var uploadPartCopy = await client.SendSignedAsync("PUT", "/bucket/key?partNumber=1&uploadId=u",
            headers: new Dictionary<string, string> { ["x-amz-copy-source"] = "/bucket/src" });

        // Assert
        uploadPart.StatusCode.Should().Be(HttpStatusCode.InternalServerError);
        uploadPartCopy.StatusCode.Should().Be(HttpStatusCode.InternalServerError);
    }

    [Fact]
    public async Task DeleteObjects_ReachesObjectStore()
    {
        // Arrange: валидный XML Delete с одним ключом
        var xml = """<?xml version="1.0" encoding="utf-8"?><Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Object><Key>k</Key></Object><Quiet>false</Quiet></Delete>""";

        // Act
        var response = await NewClient().SendSignedAsync("POST", "/bucket?delete",
            body: Encoding.UTF8.GetBytes(xml));

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.InternalServerError);
    }

    [Fact]
    public async Task CompleteMultipartUpload_ReachesObjectStore()
    {
        // Arrange: манифест из одной части (валидация порядка проходит)
        var xml = """<?xml version="1.0" encoding="utf-8"?><CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Part><PartNumber>1</PartNumber><ETag>"e"</ETag></Part></CompleteMultipartUpload>""";

        // Act
        var response = await NewClient().SendSignedAsync("POST", "/bucket/key?uploadId=u",
            body: Encoding.UTF8.GetBytes(xml));

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.InternalServerError);
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
            at: DateTimeOffset.UtcNow.AddHours(1)); // skew-невалидная подпись

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
