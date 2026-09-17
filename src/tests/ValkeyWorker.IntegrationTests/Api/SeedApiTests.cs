using System.Net;
using System.Text;
using FluentAssertions;
using Xunit;

namespace ValkeyWorker.IntegrationTests.Api;

// Seed-эндпоинт (spec §4.7): 201 при первой наливке, 200 {"seeded":false}
// при повторе (идемпотентность по живому config), тело без секретов.
[Collection(ValkeyApiCollection.Name)]
public class SeedApiTests(ValkeyApiFixture fx)
{
    [Fact]
    public async Task Seed_201Потом200SeededFalse_Идемпотентен()
    {
        // Arrange: свой demo-ключ чистим до теста (общий etcd фикстуры).
        await fx.Etcd.Gateway.DeleteAsync(fx.Etcd.Endpoint, "/valkey/clusters/demo/",
            prefix: true, TestContext.Current.CancellationToken);
        using var client = fx.Factory.CreateClient();

        // Act 1: первая наливка.
        using var first = await client.PostAsync("/api/seed/demo", null,
            TestContext.Current.CancellationToken);
        var firstBody = await first.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        // Act 2: повтор.
        using var second = await client.PostAsync("/api/seed/demo", null,
            TestContext.Current.CancellationToken);
        var secondBody = await second.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);

        // Assert
        first.StatusCode.Should().Be(HttpStatusCode.Created, firstBody);
        firstBody.Should().Contain("\"seeded\":true");
        AssertNoSecrets(firstBody);
        second.StatusCode.Should().Be(HttpStatusCode.OK, secondBody);
        secondBody.Should().Contain("\"seeded\":false");
        var config = await fx.Etcd.Gateway.GetAsync(
            fx.Etcd.Endpoint, "/valkey/clusters/demo/config", TestContext.Current.CancellationToken);
        config.Value!.Value.Should().Contain("536870912").And.Contain("NOT_INITIALIZED");
        var resources = await fx.Etcd.Gateway.GetAsync(
            fx.Etcd.Endpoint, "/valkey/clusters/demo/nodes/node1/resources", TestContext.Current.CancellationToken);
        resources.Value!.Value.Should().Be("""{"cpu":"1","mem":"1Gi","disk":"10Gi"}""");

        // Cleanup: свой demo-префикс (соседям чисто).
        await fx.Etcd.Gateway.DeleteAsync(fx.Etcd.Endpoint, "/valkey/clusters/demo/",
            prefix: true, TestContext.Current.CancellationToken);
    }

    // §9.2: тело без ключей *password* и 32-символьных значений.
    private static void AssertNoSecrets(string body)
        => body.Should().NotContain("password");
}
