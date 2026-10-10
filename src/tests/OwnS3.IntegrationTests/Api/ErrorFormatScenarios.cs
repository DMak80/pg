using System.Net;
using System.Xml.Linq;

namespace OwnS3.IntegrationTests.Api;

// Формат ошибок (глава 03 §5): структура XML, RequestId/HostId/Resource,
// HEAD-ошибки без тела, x-amz-request-id в каждом ответе.
[Collection(OwnS3TestCollection.Name)]
public sealed class ErrorFormatScenarios(OwnS3AppFactory factory)
{
    private OwnS3TestClient NewClient() => new(factory.CreateClient());

    [Fact]
    public async Task ErrorXml_ContainsAllFiveElements()
    {
        // Arrange / Act: анонимный GET → AccessDenied
        var response = await NewClient().SendAnonymousAsync("GET", "/bucket/key");
        var xml = XDocument.Parse(await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken));

        // Assert: все 5 элементов присутствуют
        var root = xml.Root!;
        root.Name.LocalName.Should().Be("Error");
        root.Element("Code")!.Value.Should().Be("AccessDenied");
        root.Element("Message")!.Value.Should().NotBeNullOrEmpty();
        root.Element("Resource")!.Value.Should().Be("/bucket/key");
        root.Element("RequestId")!.Value.Should().NotBeNullOrEmpty();
        root.Element("HostId")!.Value.Should().Be("owns3-test");
    }

    [Fact]
    public async Task RequestId_IsGuid_AndMatchesHeader()
    {
        // Arrange / Act
        var response = await NewClient().SendAnonymousAsync("GET", "/bucket/key");
        var header = response.Headers.GetValues("x-amz-request-id").Single();
        var xml = XDocument.Parse(await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken));
        var bodyRequestId = xml.Root!.Element("RequestId")!.Value;

        // Assert: валидный GUID, равен заголовку
        Guid.TryParse(header, out _).Should().BeTrue();
        bodyRequestId.Should().Be(header);
    }

    [Fact]
    public async Task EveryResponse_CarriesRequestIdHeader()
    {
        // Arrange: и успех (GetBucketLocation), и ошибка
        var client = NewClient();

        // Act
        var success = await client.SendSignedAsync("GET", "/bucket?location");
        var error = await client.SendAnonymousAsync("GET", "/bucket/key");

        // Assert
        success.Headers.Should().Contain(h => h.Key == "x-amz-request-id");
        error.Headers.Should().Contain(h => h.Key == "x-amz-request-id");
    }

    [Fact]
    public async Task HeadError_HasNoBody()
    {
        // Arrange: анонимный HEAD → 403 (глава 03 §5: HEAD — только статус)
        // Act
        var response = await NewClient().SendAnonymousAsync("HEAD", "/bucket/key");

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.Forbidden);
        (await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).Should().BeEmpty();
    }
}
