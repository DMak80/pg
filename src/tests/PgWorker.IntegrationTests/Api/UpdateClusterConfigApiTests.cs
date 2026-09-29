using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using PgWorker.IntegrationTests.Etcd;
using Xunit;

namespace PgWorker.IntegrationTests.Api;

// PUT /api/clusters/{c}/config (t06, spec §6.5, 02 §9.10) на WAF-хосте воркера
// с реальным etcd: 204 и поле в etcd; идемпотентность (повтор 204, mod_revision
// не растёт); включение при replicas=1 → 400; не-Active → 409; нет кластера →
// 404; битое тело (без поля) → 400. Гонка mod_revision → 503 верифицируется
// юнит-формой (UpdateClusterConfigHandlerTests, Задача 8 шаг 3б — примечание
// в плане t06): инъекция конкурентной записи внутри одного вызова на реальном
// etcd детерминированно невоспроизводима.
[Collection(PgApiCollection.Name)]
public class UpdateClusterConfigApiTests(PgApiFixture fixture)
{
    private HttpClient Client => fixture.Factory.CreateClient();

    private EtcdFixture Etcd => fixture.Etcd;

    private async Task<string> ConfigModRevisionAsync(string cluster)
    {
        var kv = await Etcd.Gateway.GetAsync(Etcd.Endpoint, $"/clusters/{cluster}/config",
            TestContext.Current.CancellationToken);
        return kv.Value!.ModRevision.ToString();
    }

    // AAA: PUT выключения — 204; поле в etcd = false; прочие поля config
    // (buckets/dbname/created_unix/state) перенесены без изменений.
    [Fact]
    public async Task PutClusterConfig_204_FieldInEtcd_OthersUntouched()
    {
        // Arrange — Active-кластер (config без state), replicas=2 (включение валидно).
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "cfg1", buckets: 4, shards: 2, replicas: 2);
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await Client.PutAsJsonAsync("/api/clusters/cfg1/config",
            new { synchronousModeStrict = false }, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.NoContent);
        var config = (await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/clusters/cfg1/config", ct))
            .Value!.Value;
        config.Should().Contain("\"synchronous_mode_strict\":false");
        config.Should().Contain("\"buckets\":4").And.Contain("\"dbname\":\"cfg1\"")
            .And.Contain("\"created_unix\":1756000000");
        config.Should().NotContain("state"); // Active — поле state не появилось
    }

    // AAA: идемпотентность — повтор с тем же значением: 204 без записи
    // (mod_revision config не растёт).
    [Fact]
    public async Task PutClusterConfig_SameValue_IdempotentNoWrite()
    {
        // Arrange
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "cfg2", buckets: 4, shards: 2, replicas: 2);
        var ct = TestContext.Current.CancellationToken;
        var first = await Client.PutAsJsonAsync("/api/clusters/cfg2/config",
            new { synchronousModeStrict = false }, ct);
        first.StatusCode.Should().Be(HttpStatusCode.NoContent);
        var revisionBefore = await ConfigModRevisionAsync("cfg2");

        // Act
        var second = await Client.PutAsJsonAsync("/api/clusters/cfg2/config",
            new { synchronousModeStrict = false }, ct);

        // Assert
        second.StatusCode.Should().Be(HttpStatusCode.NoContent);
        (await ConfigModRevisionAsync("cfg2")).Should().Be(revisionBefore, "204 без записи");
    }

    // AAA: включение strict при шардe replicas=1 → 400 errors.syncStrict.
    [Fact]
    public async Task PutClusterConfig_EnableWithSingleReplicaShard_400()
    {
        // Arrange — дефолтный сид: replicas=1 (strict=false в сид-конфиге, t06).
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "cfg3", buckets: 4, shards: 2,
            replicas: 1, syncStrict: false);
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await Client.PutAsJsonAsync("/api/clusters/cfg3/config",
            new { synchronousModeStrict = true }, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        var problem = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        problem.GetProperty("errors").GetProperty("syncStrict")
            .GetArrayLength().Should().BeGreaterThan(0);
    }

    // AAA: не-Active (NOT_INITIALIZED) → 409.
    [Fact]
    public async Task PutClusterConfig_NotInitialized_409()
    {
        // Arrange
        var ct = TestContext.Current.CancellationToken;
        await Etcd.Gateway.PutAsync(Etcd.Endpoint, "/clusters/cfg4/config",
            """{"buckets":4,"dbname":"cfg4","created_unix":1756000000,"state":"NOT_INITIALIZED"}""",
            null, ct);

        // Act
        var resp = await Client.PutAsJsonAsync("/api/clusters/cfg4/config",
            new { synchronousModeStrict = false }, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.Conflict);
        var problem = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        problem.GetProperty("title").GetString().Should().Be("Cluster not active");
    }

    // AAA: кластера нет → 404.
    [Fact]
    public async Task PutClusterConfig_UnknownCluster_404()
    {
        // Arrange / Act
        var resp = await Client.PutAsJsonAsync("/api/clusters/nosuch/config",
            new { synchronousModeStrict = false }, TestContext.Current.CancellationToken);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    // AAA: битое тело (поле отсутствует) → 400 (выключение «по умолчанию»
    // недопустимо — 02 §9.10).
    [Fact]
    public async Task PutClusterConfig_MissingField_400()
    {
        // Arrange / Act — пустое JSON-тело: поле не биндится.
        var resp = await Client.PutAsJsonAsync("/api/clusters/cfg5/config",
            new { }, TestContext.Current.CancellationToken);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        var problem = await resp.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        problem.GetProperty("errors").GetProperty("synchronousModeStrict")
            .GetArrayLength().Should().BeGreaterThan(0);
    }
}
