using PgWorker.Etcd.Client;
using PgWorker.IntegrationTests.Docker;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// E2E per-node app_params (spec §7.2/§7.3): provisioning обеспечивает ключ
// каждой ноды дефолтом; значение стабильно между тиками; ручная правка не
// перезаписывается (put-if-absent, миграция надзора).
[Collection(E2eCollection.Name)]
public class E2eAppParamsScenarios(E2eFixture fixture)
{
    private const string Cluster = "appparams";

    private string Endpoint => fixture.EtcdEndpoint;

    private EtcdGateway G => fixture.Gateway;

    [Fact]
    public async Task AppParams_Provisioning_AllNodesGetDefaultAndStable()
    {
        // Arrange — сид NOT_INITIALIZED без app_params-ключей (генерирует PgWorker)
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await SeedClusterAsync(Cluster);
        await using var app = await fixture.StartHostAsync("appparams", ct: ct);

        // Act — ждать SQL-фазы обоих шардов (dsn записан)
        var provisioned = await E2eFixture.WaitForAsync(async () =>
            await GetOrNullAsync($"/clusters/{Cluster}/shards/shard1/dsn") is not null
            && await GetOrNullAsync($"/clusters/{Cluster}/shards/shard2/dsn") is not null,
            TimeSpan.FromSeconds(360), ct);

        // Assert 1 (критерий 2): у КАЖДОЙ ноды обоих шардов app_params = дефолт
        // конфига ("sslmode=require"), без user/password в значении.
        provisioned.Should().BeTrue("provisioning дошёл до SQL-фазы (dsn обоих шардов)");
        foreach (var shard in new[] { "shard1", "shard2" })
            foreach (var node in new[] { "a", "b" })
            {
                var kv = await GetOrNullAsync($"/clusters/{Cluster}/shards/{shard}/nodes/{shard}{node}/app_params");
                kv.Should().NotBeNull($"app_params узла {shard}{node} обеспечен");
                kv!.Value.Should().Be("sslmode=require");
                kv.Value.Should().NotContainAny(["user=", "password="], "клиентские параметры не входят (spec §3.1)");
            }

        // Assert 2 (критерий 2): стабильность между тиками + ручная правка жива
        // (миграция/ensure не перезаписывают существующее — put-if-absent).
        await G.PutAsync(Endpoint,
            $"/clusters/{Cluster}/shards/shard1/nodes/shard1a/app_params",
            "sslmode=verify-full", null, ct);
        await Task.Delay(TimeSpan.FromSeconds(5), ct); // ≥2 тика scan
        (await GetOrNullAsync($"/clusters/{Cluster}/shards/shard1/nodes/shard1a/app_params"))!.Value
            .Should().Be("sslmode=verify-full", "ручное значение не перезаписано (spec §3.1)");
        (await GetOrNullAsync($"/clusters/{Cluster}/shards/shard1/nodes/shard1b/app_params"))!.Value
            .Should().Be("sslmode=require", "прочие ноды стабильны");
    }

    // Сид кластера в стиле панели (копия E2eAppSecretScenarios.SeedClusterAsync).
    private async Task SeedClusterAsync(string cluster)
    {
        var ct = TestContext.Current.CancellationToken;
        var config = $$"""
            {"buckets":2,"dbname":"{{cluster}}","created_unix":1755800000,"state":"NOT_INITIALIZED","bucket_admin_password":"{{E2eFixture.BucketAdminPassword}}"}
            """;
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/config", config, null, ct);
        foreach (var shard in new[] { "shard1", "shard2" })
        {
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/replicas", "2", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/nodes/{shard}a/state", "NOT_INITIALIZED", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/nodes/{shard}b/state", "NOT_INITIALIZED", null, ct);
        }

        for (var i = 0; i < 2; i++)
        {
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/buckets/routing/bucket_{i}", $"shard{i + 1}", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/buckets/status/bucket_{i}",
                """{"state":"NOT_INITIALIZED"}""", null, ct);
        }
    }

    private async Task<Kv?> GetOrNullAsync(string key)
        => (await G.GetAsync(Endpoint, key, TestContext.Current.CancellationToken)).Value;
}
