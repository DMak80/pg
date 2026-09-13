using System.Net;
using System.Net.Http.Json;
using System.Text;
using PgWorker.IntegrationTests.Etcd;
using Xunit;

namespace PgWorker.IntegrationTests.Api;

// POST /api/clusters/{c}/shards/{x}/restore (t05, spec Ф4/AC5): 202 + PLANNED-
// ключ в etcd; гварды confirm/RFC3339/Active/шард/дубль/source-пара/тело.
[Collection(PgApiCollection.Name)]
public class RestoreApiTests(PgApiFixture fixture)
{
    private HttpClient Client => fixture.Factory.CreateClient();

    private EtcdFixture Etcd => fixture.Etcd;

    private async Task<string?> GetRestoreKeyAsync(string cluster, string shard)
    {
        var stored = await Etcd.Gateway.GetAsync(
            Etcd.Endpoint, $"/pgworker/backups/{cluster}/{shard}/restore/", TestContext.Current.CancellationToken);
        return stored.Value?.Value;
    }

    // AAA: confirm=имя шарда → 202; etcd-ключ PLANNED с target=latest и requested_by
    [Fact]
    public async Task Restore_202_и_PLANNED_ключ_в_etcd()
    {
        // Arrange — активный кластер с shard1
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "rs", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await Client.PostAsJsonAsync("/api/clusters/rs/shards/shard1/restore", new
        {
            confirm = "shard1",
            requested_by = "operator",
        }, ct);

        // Assert — 202 и ключ канона
        resp.StatusCode.Should().Be(HttpStatusCode.Accepted);
        var raw = await GetRestoreKeyAsync("rs", "shard1");
        raw.Should().NotBeNull();
        raw.Should().Contain("\"state\":\"PLANNED\"")
            .And.Contain("\"target\":\"latest\"")
            .And.Contain("\"requested_by\":\"operator\"")
            .And.Contain("\"node\":\"shard1a\"")
            .And.Contain("\"source\":\"rs/shard1\"");
    }

    // AAA: confirm=чужое → 400 с ожидаемым именем; ключа нет
    [Fact]
    public async Task Restore_confirm_мисматч_400()
    {
        // Arrange
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "rsc", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await Client.PostAsJsonAsync("/api/clusters/rsc/shards/shard1/restore", new
        {
            confirm = "shard2",
        }, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        var text = await resp.Content.ReadAsStringAsync(ct);
        text.Should().Contain("shard1");
        (await GetRestoreKeyAsync("rsc", "shard1")).Should().BeNull();
    }

    // AAA: target_time не RFC3339 → 400
    [Fact]
    public async Task Restore_target_time_не_RFC3339_400()
    {
        // Arrange
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "rst", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await Client.PostAsJsonAsync("/api/clusters/rst/shards/shard1/restore", new
        {
            confirm = "shard1",
            target_time = "yesterday-ish",
        }, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await GetRestoreKeyAsync("rst", "shard1")).Should().BeNull();
    }

    // AAA: активная заявка уже есть → 409, значение не перезаписано
    [Fact]
    public async Task Restore_активная_заявка_409()
    {
        // Arrange — сид PLANNED-ключа (RUNNING тоже активен, но PLANNED — наш путь)
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "rsa", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;
        var seeded =
            """{"state":"PLANNED","backup_id":"","source":"rsa/shard1","target":"latest","node":"shard1a","requested_unix":1760000000,"requested_by":"operator"}""";
        await Etcd.Gateway.PutAsync(Etcd.Endpoint,
            "/pgworker/backups/rsa/shard1/restore/20260911120000Z", seeded, null, ct);

        // Act
        var resp = await Client.PostAsJsonAsync("/api/clusters/rsa/shards/shard1/restore", new
        {
            confirm = "shard1",
        }, ct);

        // Assert — 409; ключ не перезаписан (прежнему id, прежнее значение)
        resp.StatusCode.Should().Be(HttpStatusCode.Conflict);
        var raw = await GetRestoreKeyAsync("rsa", "shard1");
        raw.Should().Be(seeded);
    }

    // AAA: кластер не Active (state в config) → 409
    [Fact]
    public async Task Restore_кластер_не_Active_409()
    {
        // Arrange — config с state (не Active-канон)
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "rsn", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;
        await Etcd.Gateway.PutAsync(Etcd.Endpoint, "/clusters/rsn/config",
            """{"buckets":4,"dbname":"rsn","created_unix":1756000000,"state":"TO_REMOVE"}""", null, ct);

        // Act
        var resp = await Client.PostAsJsonAsync("/api/clusters/rsn/shards/shard1/restore", new
        {
            confirm = "shard1",
        }, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.Conflict);
        (await GetRestoreKeyAsync("rsn", "shard1")).Should().BeNull();
    }

    // AAA: шард не заявлен → 404
    [Fact]
    public async Task Restore_шард_не_заявлен_404()
    {
        // Arrange
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "rsh", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await Client.PostAsJsonAsync("/api/clusters/rsh/shards/shard9/restore", new
        {
            confirm = "shard9",
        }, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    // AAA: кластера нет → 404
    [Fact]
    public async Task Restore_кластер_нет_404()
    {
        // Arrange — пустой etcd

        // Act
        var resp = await Client.PostAsJsonAsync("/api/clusters/nosuch/shards/shard1/restore", new
        {
            confirm = "shard1",
        }, TestContext.Current.CancellationToken);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    // AAA: source_cluster без source_shard → 400 с errors по полям
    [Fact]
    public async Task Restore_source_cluster_без_source_shard_400()
    {
        // Arrange
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "rsv", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await Client.PostAsJsonAsync("/api/clusters/rsv/shards/shard1/restore", new
        {
            confirm = "shard1",
            source_cluster = "other",
        }, ct);

        // Assert — 400 + errors.source_shard
        resp.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        var text = await resp.Content.ReadAsStringAsync(ct);
        text.Should().Contain("source_shard");
        (await GetRestoreKeyAsync("rsv", "shard1")).Should().BeNull();
    }

    // AAA: без тела → 400
    [Fact]
    public async Task Restore_без_тела_400()
    {
        // Arrange — активный кластер
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "rsb", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await Client.PostAsync("/api/clusters/rsb/shards/shard1/restore",
            content: null, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await GetRestoreKeyAsync("rsb", "shard1")).Should().BeNull();
    }

    // AAA: target_time RFC3339 → ключ с time:<значение>; пустой body JSON без
    // confirm — 400 (confirm обязателен)
    [Fact]
    public async Task Restore_target_time_RFC3339_попадает_в_ключ()
    {
        // Arrange
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "rsp", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await Client.PostAsync("/api/clusters/rsp/shards/shard1/restore",
            new StringContent(
                """{"confirm":"shard1","target_time":"2026-09-11T10:00:00Z"}""",
                Encoding.UTF8, "application/json"), ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.Accepted);
        var raw = await GetRestoreKeyAsync("rsp", "shard1");
        raw.Should().NotBeNull();
        raw.Should().Contain("\"target\":\"time:2026-09-11T10:00:00Z\"");
    }
}
