using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using AdminPanel.Core;
using AdminPanel.Etcd;
using FluentAssertions;
using Microsoft.AspNetCore.Mvc.Testing;
using Xunit;

namespace AdminPanel.IntegrationTests;

// Управляемое хранилище фабрики "api": тест ставит снапшот сам (spec §3.16).
public sealed class TestSnapshotStore : ISnapshotStore
{
    public EtcdSnapshot? Current { get; set; }

    public void Replace(EtcdSnapshot snapshot) => Current = snapshot;
}

// Логин в общем хосте фабрики: свежее окно rate-limiter'а + cookie в клиенте.
internal static class ApiTestLogin
{
    public static async Task<HttpClient> LoginAsync(AuthWebFactory factory)
    {
        factory.Time.Utc += TimeSpan.FromSeconds(61);
        var client = factory.CreateClient();
        var login = await client.PostAsJsonAsync(
            "/api/auth/login",
            new { username = "admin", password = "adminpw" },
            TestContext.Current.CancellationToken);
        login.StatusCode.Should().Be(HttpStatusCode.NoContent);
        return client;
    }
}

// Фикстурный снапшот HTTP-тестов (spec §9.1): 1 живой + 1 мёртвый endpoint,
// member-лидер, alarm NOSPACE, 1 critical + 2 warning алерта.
internal static class InspectionSnapshots
{
    public static EtcdSnapshot Fixture(DateTimeOffset builtAt)
    {
        var etcd = new EtcdStatus(
            true,
            [
                new EtcdEndpoint("http://etcd1:2379", true, 4.2, "3.5.21", 20480, 42, 17, 3, []),
                new EtcdEndpoint("http://etcd2:2379", false, null, null, null, null, null, null, ["connection refused"]),
            ],
            [new EtcdMember(42, "etcd1", ["http://etcd1:2380"], ["http://etcd1:2379"])],
            [new EtcdAlarm(42, EtcdAlarmType.NoSpace)],
            "http://etcd1:2379",
            false,
            builtAt,
            0);
        return new EtcdSnapshot(
            builtAt,
            etcd,
            [],
            [],
            [],
            [],
            [
                new Alert("etcd-alarm:42:nospace", AlertSeverity.Critical, "etcd-alarm", "42:nospace",
                    "тревога etcd NOSPACE на member 42", new Dictionary<string, string> { ["memberId"] = "42" }, null),
                new Alert("etcd-endpoint-down:http://etcd2:2379", AlertSeverity.Warning, "etcd-endpoint-down",
                    "http://etcd2:2379", "endpoint etcd недоступен", new Dictionary<string, string> { ["errors"] = "connection refused" }, null),
                new Alert("key-malformed:/x", AlertSeverity.Warning, "key-malformed", "/x",
                    "ключ не разобран", null, null),
            ],
            [],
            0);
    }

    // Кластерный снапшот HTTP-тестов (spec §9): Fixture + кластер demo — 2 шарда (s2 без master),
    // бакеты 0..15 (у 4 — дыра), SYNCING −30 c / FROZEN −10 c / ABORTING −5 c, 2 heals.
    public static EtcdSnapshot Clustered(DateTimeOffset builtAt, DateTimeOffset now)
    {
        var unix = now.ToUnixTimeSeconds();
        var cluster = new ClusterInfo(
            "demo", "demo", 16, 1755800000, ClusterState.Active,
            [
                new ShardInfo("s1", "host=s1a,s1b port=5432 dbname=demo user=postgres",
                    ["s1a", "s1b"], 5432, "demo", "postgres", 1, "s1a:5432", [], null),
                new ShardInfo("s2", "host=s2a,s2b port=5432 dbname=demo user=postgres",
                    ["s2a", "s2b"], 5432, "demo", "postgres", 1, null, [], null),
            ],
            [.. Enumerable.Range(0, 16).Select(i => i switch
            {
                1 => new BucketInfo(1, "s1", BucketState.Syncing,
                    new MoveInfo("s1", "s2", unix - 130, unix - 30, "copy", null)),
                2 => new BucketInfo(2, "s1", BucketState.Frozen,
                    new MoveInfo("s1", "s2", unix - 70, unix - 10, "cutover-wait", null)),
                3 => new BucketInfo(3, "s2", BucketState.Aborting,
                    new MoveInfo("s2", "s1", unix - 45, unix - 5, "cleanup", "receiver went away")),
                4 => new BucketInfo(4, null, BucketState.Active, null),
                _ => new BucketInfo(i, i % 2 == 0 ? "s1" : "s2", BucketState.Active, null),
            })],
            [
                new HealRecord("bucket_5", "s2", "s1", "restore-heal", unix - 3600),
                new HealRecord("bucket_9", "s1", "s2", "restore-heal", unix - 7200),
            ]);
        // t08 spec §8: реестр /cluster/nodes/ — 2 ноды, у второй адрес пуст.
        return Fixture(builtAt) with
        {
            Clusters = [cluster],
            StandNodes = [new StandNode("node1", "10.0.0.5"), new StandNode("node2", null)],
        };
    }

    // HA-фикстура HTTP-тестов (spec §9.2): demo-s1 с пробами, other-scope unmatched
    // с упавшей пробой; alerts — руками из Fixture (движок тут не работает).
    public static EtcdSnapshot Ha(DateTimeOffset builtAt, DateTimeOffset now)
    {
        var scopes = new List<AdminPanel.Core.HaScope>
        {
            new("demo-s1", "demo", "s1", true, "s1a", 738273634528L, true, null, null, null,
                [
                    new HaMember("s1a", "s1a", 5432, "master", "running", 1L, 0L, now, null),
                    new HaMember("s1b", "s1b", 5432, "replica", "streaming", 1L, 17L * 1024 * 1024, now, null),
                ],
                "{\"ttl\":5,\"loop_wait\":2}"),
            new("other-scope", null, null, false, null, null, false, null, null, null,
                [new HaMember("n1", "n1", 5432, "replica", "stopped", null, null, now, "connection refused")],
                null),
        };
        return Fixture(builtAt) with { HaScopes = scopes };
    }
}

// HTTP-контракт инспекционных эндпоинтов: 401/503/200/400/фильтры (spec §9.1).
[Collection("api")]
public class InspectionApiTests
{
    private readonly AuthWebFactory _factory;

    public InspectionApiTests(AuthWebFactory factory) => _factory = factory;

    private Task<HttpClient> LoginAsync() => ApiTestLogin.LoginAsync(_factory);

    private async Task<JsonElement> GetJsonAsync(HttpClient client, string url)
    {
        using var response = await client.GetAsync(url, TestContext.Current.CancellationToken);
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        return await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Endpoints_WithoutCookie_Return401()
    {
        // Arrange
        using var client = _factory.CreateClient(new WebApplicationFactoryClientOptions { AllowAutoRedirect = false });

        // Act
        var overview = await client.GetAsync("/api/overview", TestContext.Current.CancellationToken);
        var status = await client.GetAsync("/api/etcd/status", TestContext.Current.CancellationToken);
        var alerts = await client.GetAsync("/api/alerts", TestContext.Current.CancellationToken);

        // Assert: default-deny guard закрыл новые эндпоинты без правок auth.
        overview.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
        status.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
        alerts.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task Endpoints_NoSnapshot_Return503ProblemDetails()
    {
        // Arrange: до первого тика снапшота нет (t03 §3.13).
        _factory.Snapshot = null;
        using var client = await LoginAsync();

        // Act
        var overview = await client.GetAsync("/api/overview", TestContext.Current.CancellationToken);
        var status = await client.GetAsync("/api/etcd/status", TestContext.Current.CancellationToken);
        var alerts = await client.GetAsync("/api/alerts", TestContext.Current.CancellationToken);
        var clustersList = await client.GetAsync("/api/clusters", TestContext.Current.CancellationToken);
        var clusterDetails = await client.GetAsync("/api/clusters/demo", TestContext.Current.CancellationToken);

        // Assert: 503 ProblemDetails на всех эндпоинтах (spec §9.1).
        overview.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
        overview.Content.Headers.ContentType!.MediaType.Should().Be("application/problem+json");
        var body = await overview.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        body.GetProperty("title").GetString().Should().Be("Snapshot not ready");
        status.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
        alerts.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
        clustersList.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
        clusterDetails.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
    }

    [Fact]
    public async Task Overview_WithSnapshot_ReturnsDto()
    {
        // Arrange: сначала логин (сдвиг окна лимитера), затем снапшот по текущему времени
        // фабрики → возраст 0 (spec §9.1).
        using var client = await LoginAsync();
        _factory.Snapshot = InspectionSnapshots.Clustered(_factory.Time.Utc, _factory.Time.Utc);

        // Act
        var dto = await GetJsonAsync(client, "/api/overview");

        // Assert
        dto.GetProperty("alertsCritical").GetInt32().Should().Be(1);
        dto.GetProperty("alertsWarning").GetInt32().Should().Be(2);
        dto.GetProperty("stale").GetBoolean().Should().BeFalse();
        dto.GetProperty("snapshotAgeMs").GetInt64().Should().Be(0);
        var etcd = dto.GetProperty("etcd");
        etcd.GetProperty("reachable").GetBoolean().Should().BeTrue();
        etcd.GetProperty("endpointsOk").GetInt32().Should().Be(1);
        etcd.GetProperty("endpointsTotal").GetInt32().Should().Be(2);
        var clusters = dto.GetProperty("clusters");
        clusters.GetArrayLength().Should().Be(1);
        clusters[0].GetProperty("name").GetString().Should().Be("demo");
        clusters[0].GetProperty("shards").GetInt32().Should().Be(2);
        clusters[0].GetProperty("buckets").GetInt32().Should().Be(16);
        clusters[0].GetProperty("activeMoves").GetInt32().Should().Be(3);
        clusters[0].GetProperty("masterlessShards").GetInt32().Should().Be(1);
        dto.GetProperty("activeMoves").GetArrayLength().Should().Be(3);
    }

    [Fact]
    public async Task Overview_StaleSnapshot_StaleTrue()
    {
        // Arrange: сначала логин, затем снапшот возрастом 12 c > порога 3×3 c.
        using var client = await LoginAsync();
        _factory.Snapshot = InspectionSnapshots.Fixture(_factory.Time.Utc - TimeSpan.FromSeconds(12));

        // Act
        var dto = await GetJsonAsync(client, "/api/overview");

        // Assert
        dto.GetProperty("stale").GetBoolean().Should().BeTrue();
        dto.GetProperty("snapshotAgeMs").GetInt64().Should().Be(12000);
    }

    [Fact]
    public async Task EtcdStatus_WithSnapshot_ReturnsEndpointsMembersAlarms()
    {
        // Arrange
        _factory.Snapshot = InspectionSnapshots.Fixture(_factory.Time.Utc);
        using var client = await LoginAsync();

        // Act
        var dto = await GetJsonAsync(client, "/api/etcd/status");

        // Assert
        var endpoints = dto.GetProperty("endpoints");
        endpoints.GetArrayLength().Should().Be(2);
        var first = endpoints[0];
        first.GetProperty("url").GetString().Should().Be("http://etcd1:2379");
        first.GetProperty("reachable").GetBoolean().Should().BeTrue();
        first.GetProperty("active").GetBoolean().Should().BeTrue();
        first.GetProperty("version").GetString().Should().Be("3.5.21");
        first.GetProperty("leaderMemberId").GetString().Should().Be("42");
        first.GetProperty("raftTerm").GetInt64().Should().Be(3);
        endpoints[1].GetProperty("active").GetBoolean().Should().BeFalse();
        endpoints[1].GetProperty("errors")[0].GetString().Should().Be("connection refused");
        var member = dto.GetProperty("members")[0];
        member.GetProperty("id").GetString().Should().Be("42");
        member.GetProperty("name").GetString().Should().Be("etcd1");
        member.GetProperty("isLeader").GetBoolean().Should().BeTrue();
        var alarm = dto.GetProperty("alarms")[0];
        alarm.GetProperty("memberId").GetString().Should().Be("42");
        alarm.GetProperty("type").GetString().Should().Be("nospace");
        dto.GetProperty("quorumSuspected").GetBoolean().Should().BeFalse();
        dto.GetProperty("lastRefreshUtc").GetString().Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task Alerts_WithSnapshot_ReturnAllSorted()
    {
        // Arrange
        _factory.Snapshot = InspectionSnapshots.Fixture(_factory.Time.Utc);
        using var client = await LoginAsync();

        // Act
        var alerts = await GetJsonAsync(client, "/api/alerts");

        // Assert: severity desc, внутри уровня — kind (Ordinal); sinceUnix null виден как null.
        alerts.GetArrayLength().Should().Be(3);
        alerts[0].GetProperty("id").GetString().Should().Be("etcd-alarm:42:nospace");
        alerts[0].GetProperty("severity").GetString().Should().Be("critical");
        alerts[1].GetProperty("kind").GetString().Should().Be("etcd-endpoint-down");
        alerts[2].GetProperty("kind").GetString().Should().Be("key-malformed");
        alerts[0].GetProperty("sinceUnix").ValueKind.Should().Be(JsonValueKind.Null);
    }

    [Fact]
    public async Task Alerts_SeverityFilter_ReturnsOnlyMatching()
    {
        // Arrange
        _factory.Snapshot = InspectionSnapshots.Fixture(_factory.Time.Utc);
        using var client = await LoginAsync();

        // Act
        var critical = await GetJsonAsync(client, "/api/alerts?severity=critical");
        var warning = await GetJsonAsync(client, "/api/alerts?severity=warning");

        // Assert
        critical.GetArrayLength().Should().Be(1);
        critical[0].GetProperty("severity").GetString().Should().Be("critical");
        warning.GetArrayLength().Should().Be(2);
    }

    [Fact]
    public async Task Alerts_KindFilter_ReturnsOnlyMatching()
    {
        // Arrange
        _factory.Snapshot = InspectionSnapshots.Fixture(_factory.Time.Utc);
        using var client = await LoginAsync();

        // Act
        var alerts = await GetJsonAsync(client, "/api/alerts?kind=etcd-endpoint-down");

        // Assert
        alerts.GetArrayLength().Should().Be(1);
        alerts[0].GetProperty("kind").GetString().Should().Be("etcd-endpoint-down");
    }

    [Fact]
    public async Task Alerts_UnknownKind_ReturnsEmpty200()
    {
        // Arrange
        _factory.Snapshot = InspectionSnapshots.Fixture(_factory.Time.Utc);
        using var client = await LoginAsync();

        // Act
        var alerts = await GetJsonAsync(client, "/api/alerts?kind=nope");

        // Assert: kind'ы эволюционируют между задачами — пустой список, не 400 (spec §3.13).
        alerts.GetArrayLength().Should().Be(0);
    }

    [Fact]
    public async Task Alerts_InvalidSeverity_Returns400ProblemDetails()
    {
        // Arrange
        _factory.Snapshot = InspectionSnapshots.Fixture(_factory.Time.Utc);
        using var client = await LoginAsync();

        // Act
        var response = await client.GetAsync("/api/alerts?severity=bogus", TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        response.Content.Headers.ContentType!.MediaType.Should().Be("application/problem+json");
    }

    [Fact]
    public async Task Alerts_BothFilters_Combine()
    {
        // Arrange
        _factory.Snapshot = InspectionSnapshots.Fixture(_factory.Time.Utc);
        using var client = await LoginAsync();

        // Act
        var alerts = await GetJsonAsync(client, "/api/alerts?severity=warning&kind=key-malformed");

        // Assert
        alerts.GetArrayLength().Should().Be(1);
        alerts[0].GetProperty("kind").GetString().Should().Be("key-malformed");
    }
}

// Путь данных «живой etcd → API» (spec §3.17): реальный refresher (EtcdTestHarness t03 + AlertEngine)
// строит снапшот против контейнера, снапшот переносится в TestSnapshotStore хоста.
// Только НЕмутирующие проверки чистого сида: мутирующий сценарий — отдельный класс ниже
// (порядок выполнения тестов xunit не гарантирован).
[Collection("api")]
public class InspectionEtcdApiTests(AuthWebFactory factory, EtcdContainerFixture fixture)
    : IClassFixture<EtcdContainerFixture>
{
    private readonly AuthWebFactory _factory = factory;

    [Fact]
    public async Task LiveEtcd_InspectionEndpoints_ReflectRealSnapshot()
    {
        // Arrange
        var store = new SnapshotStore();
        var refresher = EtcdTestHarness.NewRefresher(store, fixture.Endpoint);
        (await refresher.RefreshOnceAsync(CancellationToken.None)).IsSuccess.Should().BeTrue();
        _factory.Snapshot = store.Current;
        using var client = await ApiTestLogin.LoginAsync(_factory);

        // Act
        using var status = await client.GetAsync("/api/etcd/status", TestContext.Current.CancellationToken);
        var overview = await client.GetAsync("/api/overview", TestContext.Current.CancellationToken);
        var alerts = await client.GetAsync("/api/alerts", TestContext.Current.CancellationToken);
        using var clustersList = await client.GetAsync("/api/clusters", TestContext.Current.CancellationToken);
        using var details = await client.GetAsync("/api/clusters/demo", TestContext.Current.CancellationToken);

        // Assert: etcd жив; 5 move-алертов протухшего сида demo (spec §3.15); кластеры отдают данные.
        status.StatusCode.Should().Be(HttpStatusCode.OK);
        var etcd = await status.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        etcd.GetProperty("endpoints")[0].GetProperty("version").GetString().Should().Be("3.5.21");
        etcd.GetProperty("members")[0].GetProperty("name").GetString().Should().Be("test");
        var overviewDto = await overview.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        overviewDto.GetProperty("etcd").GetProperty("reachable").GetBoolean().Should().BeTrue();
        overviewDto.GetProperty("etcd").GetProperty("endpointsOk").GetInt32().Should().Be(1);
        var alertList = await alerts.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        string.Join("|", alertList.EnumerateArray().Select(a => a.GetProperty("id").GetString()))
            .Should().Be("move-frozen-long:demo/bucket_11|move-aborting:demo/bucket_7|move-stale:demo/bucket_11|move-stale:demo/bucket_3|move-stale:demo/bucket_7");
        clustersList.StatusCode.Should().Be(HttpStatusCode.OK);
        var summaries = await clustersList.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        summaries.GetArrayLength().Should().Be(1);
        summaries[0].GetProperty("shardsWithMaster").GetInt32().Should().Be(2); // оба master сида живы
        var detailsDto = await details.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        detailsDto.GetProperty("buckets").GetArrayLength().Should().Be(16);
        detailsDto.GetProperty("buckets")[3].GetProperty("state").GetString().Should().Be("SYNCING");
        detailsDto.GetProperty("buckets")[3].GetProperty("move").GetProperty("target").GetString().Should().Be("s2");

        // t06: HA-эндпоинты против живого сида (без проб — обогащение только через стор, §3.1).
        using var haList = await client.GetAsync("/api/ha", TestContext.Current.CancellationToken);
        var haScopes = await haList.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        haScopes.GetArrayLength().Should().Be(2);
        haScopes[0].GetProperty("scope").GetString().Should().Be("demo-s1");
        haScopes[0].GetProperty("leaderName").GetString().Should().Be("s1a");
        using var haDetails = await client.GetAsync("/api/ha/demo-s1", TestContext.Current.CancellationToken);
        var haDto = await haDetails.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        haDto.GetProperty("members").GetArrayLength().Should().Be(2);
        haDto.GetProperty("members")[0].GetProperty("timeline").ValueKind.Should().Be(JsonValueKind.Null);
    }

}

// Сценарий с мутацией сида (kv/put без удаления — необратим для контейнера): отдельный класс
// со СВОИМ контейнером (IClassFixture — экземпляр на класс), чтобы строгий «чистый» тест выше
// не зависел от порядка выполнения (прецедент t03 — EtcdFailureTests с собственным fixture).
[Collection("api")]
public class InspectionSeededAnomaliesApiTests(AuthWebFactory factory, EtcdContainerFixture fixture)
    : IClassFixture<EtcdContainerFixture>
{
    private readonly AuthWebFactory _factory = factory;

    [Fact]
    public async Task LiveEtcd_SeededAnomalies_ProduceAlerts()
    {
        // Arrange: аномалии засеяны ДО первого тика → previous = null → sinceUnix null (spec §3.4, §9.2).
        var store = new SnapshotStore();
        var refresher = EtcdTestHarness.NewRefresher(store, fixture.Endpoint);
        await EtcdSeed.PutAsync(
            fixture.Endpoint, "/clusters/demo/buckets/status/bucket_1", "not json", CancellationToken.None);
        await EtcdSeed.PutAsync(
            fixture.Endpoint, "/clusters/ghost/shards/g1/dsn", "host=g1 port=5432", CancellationToken.None);
        (await refresher.RefreshOnceAsync(CancellationToken.None)).IsSuccess.Should().BeTrue();
        _factory.Snapshot = store.Current;
        using var client = await ApiTestLogin.LoginAsync(_factory);

        // Act
        using var response = await client.GetAsync("/api/alerts", TestContext.Current.CancellationToken);

        // Assert: 5 move-алертов сида demo + shard-no-master:ghost/g1 (dsn-шард ghost без master —
        // живое покрытие P11-правила, сид не сужается; spec §9.3) + cluster-incomplete:ghost
        // + key-malformed битого ключа; порядок severity → kind → target (Ordinal);
        // sinceUnix null — первое наблюдение (spec §3.4).
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var alerts = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        alerts.GetArrayLength().Should().Be(8);
        string.Join("|", alerts.EnumerateArray().Select(a => a.GetProperty("id").GetString()))
            .Should().Be("move-frozen-long:demo/bucket_11|shard-no-master:ghost/g1|cluster-incomplete:ghost|key-malformed:/clusters/demo/buckets/status/bucket_1|move-aborting:demo/bucket_7|move-stale:demo/bucket_11|move-stale:demo/bucket_3|move-stale:demo/bucket_7");
        alerts[1].GetProperty("kind").GetString().Should().Be("shard-no-master");
        alerts[1].GetProperty("target").GetString().Should().Be("ghost/g1");
        alerts[2].GetProperty("target").GetString().Should().Be("ghost");
        alerts[3].GetProperty("sinceUnix").ValueKind.Should().Be(JsonValueKind.Null);
    }
}
