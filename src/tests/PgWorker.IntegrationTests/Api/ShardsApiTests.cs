using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using PgWorker.IntegrationTests.Etcd;
using Xunit;

namespace PgWorker.IntegrationTests.Api;

// POST /api/clusters/{c}/shards + DELETE /api/clusters/{c}/shards/{x} (task
// etcd-via-worker-api): порт панельных ShardsApiTests на WAF-хост воркера.
[Collection(NonE2eCollection.Name)]
public class ShardsApiTests(PgApiFixture fixture)
{
    private HttpClient Client => fixture.Factory.CreateClient();

    private EtcdFixture Etcd => fixture.Etcd;

    // AAA: add-shard Active-кластеру — 201 с именем shard<max+1> и пакетом
    // ключей (replicas, nodes NOT_INITIALIZED, request_*).
    [Fact]
    public async Task AddShard_ActiveCluster_201ShardMaxPlusOne()
    {
        // Arrange — кластер 4×2 в каноне после provisioning (config без state;
        // strict выключен — механика max+1 не про strict-гвард, t06)
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "scale", buckets: 4, shards: 2, syncStrict: false);
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await Client.PostAsJsonAsync("/api/clusters/scale/shards",
            new { replicas = 1, requestCpu = 0.5, requestMem = 8, requestDisk = 100 }, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.Created);
        var body = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        body.GetProperty("name").GetString().Should().Be("shard3");
        body.GetProperty("state").GetString().Should().Be("NOT_INITIALIZED");
        (await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/clusters/scale/shards/shard3/replicas", ct))
            .Value!.Value.Should().Be("1");
        (await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/clusters/scale/shards/shard3/nodes/shard3a/state", ct))
            .Value!.Value.Should().Be("NOT_INITIALIZED");
        (await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/service/scale-shard3/request_cpu", ct))
            .Value!.Value.Should().Be("0.5");
    }

    // AAA: кластер не Active (NOT_INITIALIZED) — 409, ключи не пишутся.
    [Fact]
    public async Task AddShard_ClusterNotActive_409()
    {
        // Arrange
        var ct = TestContext.Current.CancellationToken;
        await Etcd.Gateway.PutAsync(Etcd.Endpoint, "/clusters/init/config",
            """{"buckets":4,"dbname":"init","created_unix":1756000000,"state":"NOT_INITIALIZED"}""", null, ct);

        // Act
        var resp = await Client.PostAsJsonAsync("/api/clusters/init/shards",
            new { replicas = 1, requestCpu = 0.5, requestMem = 8, requestDisk = 100 }, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.Conflict);
        var problem = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        problem.GetProperty("title").GetString().Should().Be("Shard add rejected");
        (await Etcd.Gateway.RangeAsync(Etcd.Endpoint, "/clusters/init/shards/shard1/replicas", ct))
            .Value.Should().BeEmpty();
    }

    // AAA: недодекларация (replicas без nodes) — повтор вычислит ТО ЖЕ имя и
    // проиграет клэйм → 409 (молча создать «другой» шард повтор не может).
    [Fact]
    public async Task AddShard_NameTakenByOrphanReplicas_409()
    {
        // Arrange — shard3 «существует» только replicas-ключом (не anchored);
        // strict выключен — кейс про 409-механику клэйма, не про strict-гвард (t06)
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "orphan", buckets: 4, shards: 2, syncStrict: false);
        var ct = TestContext.Current.CancellationToken;
        await Etcd.Gateway.PutAsync(Etcd.Endpoint, "/clusters/orphan/shards/shard3/replicas",
            "1", null, ct);

        // Act
        var resp = await Client.PostAsJsonAsync("/api/clusters/orphan/shards",
            new { replicas = 1, requestCpu = 0.5, requestMem = 8, requestDisk = 100 }, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.Conflict);
        var problem = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        problem.GetProperty("title").GetString().Should().Be("Shard add rejected");
    }

    // AAA: демонтаж шарда без бакетов — 204 + маркер TO_REMOVE; повтор идемпотентен.
    [Fact]
    public async Task DeleteShard_EmptyShard_204AndIdempotent()
    {
        // Arrange — все бакеты кластера 4×2 перекладываем на shard1, shard2 пуст
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "del", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;
        for (var i = 0; i < 4; i++)
            await Etcd.Gateway.PutAsync(Etcd.Endpoint, $"/clusters/del/buckets/routing/bucket_{i}",
                "shard1", null, ct);

        // Act
        var resp = await Client.DeleteAsync("/api/clusters/del/shards/shard2", ct);
        var repeat = await Client.DeleteAsync("/api/clusters/del/shards/shard2", ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.NoContent);
        repeat.StatusCode.Should().Be(HttpStatusCode.NoContent);
        (await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/clusters/del/shards/shard2/state", ct))
            .Value!.Value.Should().Be("TO_REMOVE");
    }

    // AAA: на шарде есть бакеты (routing) — 409 с подсказкой перевезти.
    [Fact]
    public async Task DeleteShard_BucketsOnShard_409()
    {
        // Arrange — 4×2: bucket 2,3 на shard2
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "busy", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await Client.DeleteAsync("/api/clusters/busy/shards/shard2", ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.Conflict);
        var problem = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        problem.GetProperty("title").GetString().Should().Be("Shard remove rejected");
        problem.GetProperty("detail").GetString().Should().Contain("бакетов");
    }

    // AAA (t06, spec §6.6): strict-кластер + replicas=1 → 400 errors.replicas.
    [Fact]
    public async Task AddShard_StrictCluster_RejectsSingleReplica()
    {
        // Arrange — сид: config с "synchronous_mode_strict":true.
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "str1", buckets: 4, shards: 2,
            replicas: 2, syncStrict: true);
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await Client.PostAsJsonAsync("/api/clusters/str1/shards",
            new { replicas = 1, requestCpu = 0.5, requestMem = 8, requestDisk = 100 }, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        var problem = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        problem.GetProperty("errors").GetProperty("replicas")
            .GetArrayLength().Should().BeGreaterThan(0);
    }

    // AAA (t06, spec §6.6 — легаси, решение №3): config БЕЗ поля + replicas=1
    // → 400 — гвард работает по умолчанию, а не только при явном true.
    [Fact]
    public async Task AddShard_LegacyConfigWithoutStrictField_AlsoRejectsSingleReplica()
    {
        // Arrange — сид со strict=false + перезапись config легаси-JSON (без поля).
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "leg1", buckets: 4, shards: 2,
            replicas: 2, syncStrict: false);
        var ct = TestContext.Current.CancellationToken;
        await Etcd.Gateway.PutAsync(Etcd.Endpoint, "/clusters/leg1/config",
            """{"buckets":4,"dbname":"leg1","created_unix":1756000000}""", null, ct);

        // Act
        var resp = await Client.PostAsJsonAsync("/api/clusters/leg1/shards",
            new { replicas = 1, requestCpu = 0.5, requestMem = 8, requestDisk = 100 }, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        var problem = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        problem.GetProperty("errors").GetProperty("replicas")
            .GetArrayLength().Should().BeGreaterThan(0);
    }

    // AAA (t06): strict=false → 201 (выключенный strict — без изменений).
    [Fact]
    public async Task AddShard_StrictOffCluster_AcceptsSingleReplica()
    {
        // Arrange — сид: "synchronous_mode_strict":false.
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "loose1", buckets: 4, shards: 2,
            replicas: 2, syncStrict: false);
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await Client.PostAsJsonAsync("/api/clusters/loose1/shards",
            new { replicas = 1, requestCpu = 0.5, requestMem = 8, requestDisk = 100 }, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.Created);
    }
}
