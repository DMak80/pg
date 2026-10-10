using System.Net;
using System.Text;

namespace OwnS3.IntegrationTests.Api;

// Права (глава 05 §3): репрезентативный набор ролей — reader/writer/admin.
[Collection(OwnS3TestCollection.Name)]
public sealed class AccessScenarios(OwnS3AppFactory factory)
{
    private OwnS3TestClient NewClient() => new(factory.CreateClient());

    [Fact]
    public async Task Reader_GetObject_PassesRights_ReachesStub()
    {
        // Arrange / Act
        var response = await NewClient().SendSignedAsync("GET", "/bucket/key",
            credentials: OwnS3TestClient.Reader());

        // Assert: права пройдены — падение в заглушку
        response.StatusCode.Should().Be(HttpStatusCode.InternalServerError);
    }

    [Fact]
    public async Task Reader_PutObject_AccessDenied()
    {
        // Arrange / Act
        var response = await NewClient().SendSignedAsync("PUT", "/bucket/key",
            credentials: OwnS3TestClient.Reader(), body: Encoding.UTF8.GetBytes("x"));

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.Forbidden);
        (await RoutingScenarios.ErrorXmlAsync(response)).Should().Be("AccessDenied");
    }

    [Fact]
    public async Task Writer_PutObject_PassesRights_ReachesStub()
    {
        // Arrange / Act
        var response = await NewClient().SendSignedAsync("PUT", "/bucket/key",
            credentials: OwnS3TestClient.Writer(), body: Encoding.UTF8.GetBytes("x"));

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.InternalServerError);
    }

    [Fact]
    public async Task Writer_CreateBucket_AccessDenied()
    {
        // Arrange / Act
        var response = await NewClient().SendSignedAsync("PUT", "/bucket",
            credentials: OwnS3TestClient.Writer());

        // Assert: мутации бакетов — только admin
        response.StatusCode.Should().Be(HttpStatusCode.Forbidden);
    }

    [Fact]
    public async Task Admin_CreateBucket_PassesRights_ReachesStub()
    {
        // Arrange / Act
        var response = await NewClient().SendSignedAsync("PUT", "/bucket",
            credentials: OwnS3TestClient.Admin());

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.InternalServerError);
    }

    [Theory]
    [InlineData("/bucket/key?uploadId=u")]        // ListParts
    [InlineData("/bucket?uploads")]               // ListMultipartUploads
    public async Task Reader_MultipartListings_PassWithoutOwnerFilter(string pathAndQuery)
    {
        // Arrange / Act: read-only допуск к листингам загрузок без фильтра «своих» —
        // фильтр появляется с данными загрузок (t38, spec §3.4 шаг 5)
        var response = await NewClient().SendSignedAsync("GET", pathAndQuery,
            credentials: OwnS3TestClient.Reader());

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.InternalServerError);
    }
}
