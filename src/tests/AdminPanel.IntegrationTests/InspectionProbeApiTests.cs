using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using AdminPanel.Core;
using AdminPanel.Etcd;
using FluentAssertions;
using Xunit;

namespace AdminPanel.IntegrationTests;

// Живой путь «etcd-сид → refresher(+состояние проб) → API» (spec §9.3): клейка —
// перенос снапшота в TestSnapshotStore фабрики (прецедент t04 §3.17).
[Collection("api")]
public class InspectionProbeApiTests(AuthWebFactory factory, EtcdContainerFixture fixture)
    : IClassFixture<EtcdContainerFixture>
{
    private readonly AuthWebFactory _factory = factory;

    private async Task<EtcdSnapshot> RefreshedAsync(ProbeState? probes)
    {
        var store = new SnapshotStore();
        var probeStore = new SettableProbeStateStore { Current = probes };
        var refresher = EtcdTestHarness.NewRefresher(store, probeStore, fixture.Endpoint);
        (await refresher.RefreshOnceAsync(CancellationToken.None)).IsSuccess.Should().BeTrue();
        return store.Current!;
    }

    [Fact]
    public async Task LiveEtcd_ProbeStateEnriches_HaAndClusterApi()
    {
        // Arrange: проб-состояние с member-обогащением demo-s1 и runtime demo/s1.
        var at = DateTimeOffset.UtcNow;
        var probes = new ProbeState(
            at,
            [],
            new Dictionary<string, HaMemberProbe>
            {
                ["demo-s1/s1a"] = new("master", "running", 1L, 0L, at, null),
                ["demo-s1/s1b"] = new("replica", "streaming", 2L, 4096L, at, null),
            },
            new Dictionary<string, ShardRuntime>
            {
                ["demo/s1"] = new(
                    "s1",
                    [],
                    [new StandbyInfo("s1b", "10.0.0.2", "streaming", "sync", 0L)],
                    [],
                    [.. Enumerable.Range(0, 16).Where(i => i % 2 == 0).Select(i => $"bucket_{i}")],
                    false,
                    null),
            });
        _factory.Snapshot = await RefreshedAsync(probes);
        using var client = await ApiTestLogin.LoginAsync(_factory);

        // Act
        var haList = await client.GetAsync("/api/ha", TestContext.Current.CancellationToken);
        var haDetails = await client.GetAsync("/api/ha/demo-s1", TestContext.Current.CancellationToken);
        var cluster = await client.GetAsync("/api/clusters/demo", TestContext.Current.CancellationToken);
        var failed = await client.GetAsync("/api/alerts?kind=probe-failed", TestContext.Current.CancellationToken);

        // Assert: timeline/lag видны в API; runtime шарда не null; probe-failed пуст.
        haList.StatusCode.Should().Be(HttpStatusCode.OK);
        var list = await haList.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        list.GetArrayLength().Should().Be(2); // demo-s1 + demo-s2 сида
        var details = await haDetails.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        var s1b = details.GetProperty("members")[1];
        s1b.GetProperty("timeline").GetInt64().Should().Be(2L);
        s1b.GetProperty("lagBytes").GetInt64().Should().Be(4096L);
        s1b.GetProperty("probeAtUtc").GetString().Should().NotBeNullOrEmpty();
        var clusterDto = await cluster.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        var runtime = clusterDto.GetProperty("shards")[0].GetProperty("runtime");
        runtime.ValueKind.Should().NotBe(JsonValueKind.Null);
        runtime.GetProperty("standbiesSync").GetInt32().Should().Be(1);
        runtime.GetProperty("bucketSchemas").GetArrayLength().Should().Be(8); // чётные = routing s1 (8/8)
        var failedList = await failed.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        failedList.GetArrayLength().Should().Be(0);
    }

    [Fact]
    public async Task LiveEtcd_FailedProbe_ProducesProbeFailedAlert()
    {
        // Arrange: patroni-проба члена упала (детали ошибки — в details алерта).
        var at = DateTimeOffset.UtcNow;
        var probes = new ProbeState(
            at,
            [new ProbeResult("demo-s1/s1a", "patroni", false, 2.0, "connection refused", at)],
            new Dictionary<string, HaMemberProbe>
            {
                ["demo-s1/s1a"] = new(null, null, null, null, at, "connection refused"),
            },
            new Dictionary<string, ShardRuntime>());
        _factory.Snapshot = await RefreshedAsync(probes);
        using var client = await ApiTestLogin.LoginAsync(_factory);

        // Act
        using var response = await client.GetAsync("/api/alerts", TestContext.Current.CancellationToken);

        // Assert: info-алерт probe-failed с kind в target; ha-member-not-streaming
        // по упавшей пробе не вычисляется (spec §3.13/§3.14).
        var alerts = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        var probeAlert = alerts.EnumerateArray().Single(a =>
            a.GetProperty("id").GetString() == "probe-failed:patroni:demo-s1/s1a");
        probeAlert.GetProperty("severity").GetString().Should().Be("info");
        alerts.EnumerateArray().Should().NotContain(a => a.GetProperty("kind").GetString() == "ha-member-not-streaming");
    }
}
