using System.Net;
using System.Net.Http.Json;
using FluentAssertions;
using S3Worker.IntegrationTests.Etcd;
using Xunit;

namespace S3Worker.IntegrationTests.Api;

// POST /api/s3/clusters/{c}/sync-probe (arch/24 §1.1, spec §8.1.9/§3.7, t33):
// 404 кластера нет; 409 не-Active; 202 + put-if-absent заявки /s3worker/probes/<C>;
// повтор при живой заявке — 202 без перезаписи (requested_by первого сохраняется).
[Collection(S3ApiCollection.Name)]
public class SyncProbeApiTests(S3ApiFixture fx)
{
    private HttpClient Client => fx.Factory.CreateClient();

    private static string Cluster(string name) => $"{name}{Guid.NewGuid().ToString("N")[..6]}";

    // Сид канонического config через Gateway: вариант Active (без state) и
    // NOT_INITIALIZED (с state — не-Active по гварду хендлера).
    private async Task SeedConfigAsync(string cluster, bool active)
    {
        var value = active
            ? """{"platform":"owns3","nodes":2,"secret_profile":"backups-main","created_unix":1760156800}"""
            : """{"platform":"owns3","nodes":2,"secret_profile":"backups-main","created_unix":1760156800,"state":"NOT_INITIALIZED"}""";
        var put = await fx.Etcd.Gateway.PutAsync(
            fx.Etcd.Endpoint, $"/s3/clusters/{cluster}/config", value, null, TestContext.Current.CancellationToken);
        put.IsSuccess.Should().BeTrue(put.Error?.Message);
    }

    [Fact]
    public async Task SyncProbe_NoCluster_404()
    {
        // Arrange: имя без config в etcd.
        var cluster = Cluster("absent");
        using var client = Client;

        // Act
        using var response = await client.PostAsync($"/api/s3/clusters/{cluster}/sync-probe",
            null, TestContext.Current.CancellationToken);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.NotFound, body);
    }

    [Fact]
    public async Task SyncProbe_NotActive_409()
    {
        // Arrange: config с state (NOT_INITIALIZED — зона A).
        var cluster = Cluster("init");
        await SeedConfigAsync(cluster, active: false);
        using var client = Client;

        // Act
        using var response = await client.PostAsync($"/api/s3/clusters/{cluster}/sync-probe",
            null, TestContext.Current.CancellationToken);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);

        // Assert: 409; заявки нет.
        response.StatusCode.Should().Be(HttpStatusCode.Conflict, body);
        var probe = await fx.Etcd.Gateway.GetAsync(
            fx.Etcd.Endpoint, $"/s3worker/probes/{cluster}", TestContext.Current.CancellationToken);
        probe.Value.Should().BeNull();
    }

    [Fact]
    public async Task SyncProbe_202AndPutIfAbsent()
    {
        // Arrange: Active-config (без state).
        var cluster = Cluster("probe");
        await SeedConfigAsync(cluster, active: true);
        using var client = Client;
        using var request = new HttpRequestMessage(HttpMethod.Post, $"/api/s3/clusters/{cluster}/sync-probe");
        request.Headers.TryAddWithoutValidation("X-Requested-By", "e2e-op");

        // Act
        using var response = await client.SendAsync(request, TestContext.Current.CancellationToken);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);

        // Assert: 202; ключ заявки — {"requested_unix":<не 0>,"requested_by":"e2e-op"}.
        response.StatusCode.Should().Be(HttpStatusCode.Accepted, body);
        var probe = await fx.Etcd.Gateway.GetAsync(
            fx.Etcd.Endpoint, $"/s3worker/probes/{cluster}", TestContext.Current.CancellationToken);
        probe.Value.Should().NotBeNull();
        probe.Value!.Value.Should().Contain("\"requested_by\":\"e2e-op\"");
        var requestedUnix = System.Text.Json.JsonDocument.Parse(probe.Value.Value)
            .RootElement.GetProperty("requested_unix").GetInt64();
        requestedUnix.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task SyncProbe_RepeatWhileAlive_202NoOverwrite()
    {
        // Arrange: Active-config; первый заявитель e2e-1.
        var cluster = Cluster("again");
        await SeedConfigAsync(cluster, active: true);
        using var client = Client;
        using var first = new HttpRequestMessage(HttpMethod.Post, $"/api/s3/clusters/{cluster}/sync-probe");
        first.Headers.TryAddWithoutValidation("X-Requested-By", "e2e-1");

        // Act: первая заявка и повтор другим заявителем.
        using var firstResponse = await client.SendAsync(first, TestContext.Current.CancellationToken);
        firstResponse.StatusCode.Should().Be(HttpStatusCode.Accepted);
        var seeded = await fx.Etcd.Gateway.GetAsync(
            fx.Etcd.Endpoint, $"/s3worker/probes/{cluster}", TestContext.Current.CancellationToken);
        seeded.Value.Should().NotBeNull();
        var valueBefore = seeded.Value!.Value;
        using var second = new HttpRequestMessage(HttpMethod.Post, $"/api/s3/clusters/{cluster}/sync-probe");
        second.Headers.TryAddWithoutValidation("X-Requested-By", "e2e-2");
        using var secondResponse = await client.SendAsync(second, TestContext.Current.CancellationToken);

        // Assert: 202 без перезаписи — значение ключа НЕ изменилось.
        secondResponse.StatusCode.Should().Be(HttpStatusCode.Accepted);
        var after = await fx.Etcd.Gateway.GetAsync(
            fx.Etcd.Endpoint, $"/s3worker/probes/{cluster}", TestContext.Current.CancellationToken);
        after.Value!.Value.Should().Be(valueBefore);
        after.Value.Value.Should().Contain("\"requested_by\":\"e2e-1\"");
    }
}
