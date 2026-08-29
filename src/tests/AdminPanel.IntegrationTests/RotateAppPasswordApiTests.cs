using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using AdminPanel.Core;
using AdminPanel.Etcd;
using FluentAssertions;
using Xunit;

namespace AdminPanel.IntegrationTests;

// POST /api/clusters/{cluster}/app-password/rotate против реального etcd (arch/02 §9.8):
// клэйм-txn заявки /pgworker/rotations/<C>, 409 на живую заявку/не-Active, 404/503.
[Collection("api")]
public class RotateAppPasswordApiTests(AuthWebFactory factory, EtcdContainerFixture fixture)
    : IClassFixture<EtcdContainerFixture>
{
    private readonly AuthWebFactory _factory = factory;

    private void SetLiveSnapshot(string cluster)
    {
        var etcd = new EtcdStatus(
            true,
            [new EtcdEndpoint(fixture.Endpoint, true, 1, "3.5.21", null, null, null, null, [])],
            [], [], fixture.Endpoint, false, _factory.Time.GetUtcNow(), 0);
        _factory.Snapshot = InspectionSnapshots.Fixture(_factory.Time.GetUtcNow()) with
        {
            Etcd = etcd,
            Clusters =
            [
                new ClusterInfo(cluster, cluster, 6, 1755900000, ClusterState.Active,
                [
                    new ShardInfo("s1", $"host=s1a port=5432 dbname={cluster} user=bucket_admin",
                        ["s1a"], 5432, cluster, "bucket_admin", 2, null,
                        [new NodeInfo("s1a", "RUNNING")], null),
                ],
                [.. Enumerable.Range(0, 6).Select(i => new BucketInfo(i, "s1", BucketState.Active, null))],
                []),
            ],
        };
    }

    private async Task<HttpClient> LoginAsync() => await ApiTestLogin.LoginAsync(_factory);

    private async Task SeedAsync(params (string Key, string Value)[] kvs)
    {
        foreach (var (key, value) in kvs)
            await EtcdSeed.PutAsync(fixture.Endpoint, key, value, TestContext.Current.CancellationToken);
    }

    private async Task<string?> ReadKeyAsync(string key)
    {
        var gateway = EtcdTestHarness.NewGateway();
        var range = await gateway.RangeAsync(fixture.Endpoint, key, TestContext.Current.CancellationToken);
        return range.Value.FirstOrDefault(kv => kv.Key == key)?.Value;
    }

    private async Task SeedActiveConfigAsync(string cluster)
        => await SeedAsync(($"/clusters/{cluster}/config",
            $$"""{"buckets":6,"dbname":"{{cluster}}","created_unix":1755900000}"""));

    [Fact]
    public async Task Rotate_ActiveCluster_ClaimsTicketWithAudit()
    {
        // Arrange — Active-кластер в снапшоте и в etcd; заявки нет
        const string cluster = "rot1";
        SetLiveSnapshot(cluster);
        await SeedActiveConfigAsync(cluster);
        using var client = await LoginAsync();

        // Act
        var response = await client.PostAsync($"/api/clusters/{cluster}/app-password/rotate", null, TestContext.Current.CancellationToken);

        // Assert — 201 с телом; заявка в etcd с аудполями панели (§9.8 п.3)
        response.StatusCode.Should().Be(HttpStatusCode.Created);
        var dto = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        dto.GetProperty("cluster").GetString().Should().Be(cluster);
        dto.GetProperty("requestedBy").GetString().Should().Be("admin");
        var ticket = await ReadKeyAsync($"/pgworker/rotations/{cluster}");
        ticket.Should().NotBeNull();
        ticket.Should().Contain("admin").And.Contain("requested_unix");
    }

    [Fact]
    public async Task Rotate_LiveTicket_Conflict()
    {
        // Arrange — заявка уже стоит (повтор до исполнения → 409, §9.8 п.2)
        const string cluster = "rot2";
        SetLiveSnapshot(cluster);
        await SeedActiveConfigAsync(cluster);
        await SeedAsync(($"/pgworker/rotations/{cluster}",
            """{"requested_unix":1755900100,"requested_by":"someone"}"""));
        using var client = await LoginAsync();

        // Act
        var response = await client.PostAsync($"/api/clusters/{cluster}/app-password/rotate", null, TestContext.Current.CancellationToken);

        // Assert — 409, значение заявки НЕ перезаписано
        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
        (await ReadKeyAsync($"/pgworker/rotations/{cluster}"))
            .Should().Contain("someone");
    }

    [Fact]
    public async Task Rotate_NotActiveCluster_Conflict()
    {
        // Arrange — config с state=NOT_INITIALIZED (§9.8 п.1)
        const string cluster = "rot3";
        SetLiveSnapshot(cluster);
        await SeedAsync(($"/clusters/{cluster}/config",
            $$"""{"buckets":6,"dbname":"{{cluster}}","created_unix":1755900000,"state":"NOT_INITIALIZED"}"""));
        using var client = await LoginAsync();

        // Act
        var response = await client.PostAsync($"/api/clusters/{cluster}/app-password/rotate", null, TestContext.Current.CancellationToken);

        // Assert — 409, заявки нет
        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
        (await ReadKeyAsync($"/pgworker/rotations/{cluster}")).Should().BeNull();
    }

    [Fact]
    public async Task Rotate_UnknownCluster_NotFound()
    {
        // Arrange — имени нет в etcd (404 по §9.8 п.1)
        SetLiveSnapshot("rot4");
        using var client = await LoginAsync();

        // Act
        var response = await client.PostAsync("/api/clusters/nosuch/app-password/rotate", null, TestContext.Current.CancellationToken);

        // Assert — 404
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Rotate_NoSnapshot_ServiceUnavailable()
    {
        // Arrange — снапшота нет (etcd недоступен) → 503
        _factory.Snapshot = null;
        using var client = await LoginAsync();

        // Act
        var response = await client.PostAsync("/api/clusters/rot5/app-password/rotate", null, TestContext.Current.CancellationToken);

        // Assert — 503
        response.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
    }
}
