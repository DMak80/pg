using System.Net;
using System.Text;

namespace OwnS3.IntegrationTests.Api;

// Права (глава 05 §3): репрезентативный набор ролей — reader/writer/admin.
// Том на класс (IClassFixture); мутационный кейс — на уникальном бакете СВОЕГО
// кейса, немутационные — на никогда не создаваемом «bucket».
public sealed class AccessScenarios(OwnS3AppFactory factory) : IClassFixture<OwnS3AppFactory>
{
    private OwnS3TestClient NewClient() => new(factory.CreateClient());

    [Fact]
    public async Task Reader_GetObject_PassesRights_ReachesStorage()
    {
        // Arrange / Act: бакет не создавался — 404 от Storage (права пройдены)
        var response = await NewClient().SendSignedAsync("GET", "/bucket/key",
            credentials: OwnS3TestClient.Reader());

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await RoutingScenarios.ErrorXmlAsync(response)).Should().Be("NoSuchBucket");
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
    public async Task Writer_PutObject_PassesRights_ReachesStorage()
    {
        // Arrange / Act: бакета нет — 404 от Storage
        var response = await NewClient().SendSignedAsync("PUT", "/bucket/key",
            credentials: OwnS3TestClient.Writer(), body: Encoding.UTF8.GetBytes("x"));

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await RoutingScenarios.ErrorXmlAsync(response)).Should().Be("NoSuchBucket");
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
    public async Task Admin_CreateBucket_PassesRights_CreatesBucket()
    {
        // Arrange / Act: уникальный бакет СВОЕГО кейса (ограничение 8)
        var response = await NewClient().SendSignedAsync("PUT", "/b-admin",
            credentials: OwnS3TestClient.Admin());

        // Assert: право admin — бакет реально создан на томе
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        Directory.Exists(Path.Combine(factory.TempVolumeDir, "b-admin")).Should().BeTrue();
    }

    [Theory]
    [InlineData("/bucket/key?uploadId=u")]        // ListParts
    [InlineData("/bucket?uploads")]               // ListMultipartUploads
    public async Task Reader_MultipartListings_PassRights_ReachStorage(string pathAndQuery)
    {
        // Arrange / Act: read-only допуск к листингам загрузок — право есть;
        // фильтр «своих» применяется к данным загрузок (канон 05 §3, t38)
        var response = await NewClient().SendSignedAsync("GET", pathAndQuery,
            credentials: OwnS3TestClient.Reader());

        // Assert: право пройдено, запрос дошёл до Storage — бакета нет (404)
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }
}
