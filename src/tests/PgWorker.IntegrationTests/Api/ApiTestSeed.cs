using PgWorker.IntegrationTests.Etcd;

namespace PgWorker.IntegrationTests.Api;

// Сид-декларации API-тестов (task etcd-via-worker-api): прямые put в fixture-gateway
// (образец панельных integration-тестов, канонические значения arch/02 §9.1).
internal static class ApiTestSeed
{
    /// <summary>Кластер в каноне ПОСЛЕ provisioning (config без state = Active).</summary>
    public static async Task SeedActiveClusterAsync(
        EtcdFixture etcd, string name, int buckets = 4, int shards = 2, int replicas = 1)
    {
        var ct = TestContext.Current.CancellationToken;
        var gw = etcd.Gateway;
        await gw.PutAsync(etcd.Endpoint, $"/clusters/{name}/config",
            $$"""{"buckets":{{buckets}},"dbname":"{{name}}","created_unix":1756000000}""", null, ct);
        for (var s = 1; s <= shards; s++)
        {
            await gw.PutAsync(etcd.Endpoint, $"/clusters/{name}/shards/shard{s}/replicas",
                replicas.ToString(), null, ct);
            for (var r = 0; r < replicas; r++)
                await gw.PutAsync(etcd.Endpoint,
                    $"/clusters/{name}/shards/shard{s}/nodes/shard{s}{(char)('a' + r)}/state",
                    "RUNNING", null, ct);
        }

        // routing — непрерывные блоки (канон arch/02 §9.1.1)
        for (var i = 0; i < buckets; i++)
        {
            var owner = $"shard{(2 * i + 1) * shards / (2 * buckets) + 1}";
            await gw.PutAsync(etcd.Endpoint, $"/clusters/{name}/buckets/routing/bucket_{i}",
                owner, null, ct);
        }
    }

    /// <summary>Заявка в очереди переездов /pgworker/moves/&lt;C&gt;/bucket_&lt;N&gt;.</summary>
    public static Task SeedMoveTicketAsync(
        EtcdFixture etcd, string cluster, int bucket, string to, long unix = 100, string by = "seed")
        => etcd.Gateway.PutAsync(etcd.Endpoint, $"/pgworker/moves/{cluster}/bucket_{bucket}",
            $$"""{"op":"move","to":"{{to}}","requested_unix":{{unix}},"requested_by":"{{by}}"}""",
            null, TestContext.Current.CancellationToken);
}
