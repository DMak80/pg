using System.Text;
using System.Text.Json;

namespace AdminPanel.IntegrationTests;

// Сид контроль-плейна demo (arch/04 §2.2) — те же значения, что в EtcdFixtures/*.json;
// скрипт seed.sh dev-станда (t10) обязан использовать те же (spec §3.16).
public static class EtcdSeed
{
    public static readonly IReadOnlyList<(string Key, string Value)> Demo =
    [
        ("/clusters/demo/config", "{\"buckets\":16,\"dbname\":\"demo\",\"created_unix\":1755800000}"),
        ("/clusters/demo/shards/s1/dsn", "host=s1a,s1b port=5432 dbname=demo user=postgres"),
        ("/clusters/demo/shards/s1/replicas", "1"),
        ("/clusters/demo/shards/s1/master", "s1a:5432"),
        ("/clusters/demo/shards/s2/dsn", "host=s2a,s2b port=5432 dbname=demo user=postgres"),
        ("/clusters/demo/shards/s2/replicas", "1"),
        ("/clusters/demo/shards/s2/master", "s2a:5432"),
        ("/clusters/demo/buckets/routing/bucket_0", "s1"),
        ("/clusters/demo/buckets/routing/bucket_1", "s2"),
        ("/clusters/demo/buckets/routing/bucket_2", "s1"),
        ("/clusters/demo/buckets/routing/bucket_3", "s1"),
        ("/clusters/demo/buckets/routing/bucket_4", "s1"),
        ("/clusters/demo/buckets/routing/bucket_5", "s2"),
        ("/clusters/demo/buckets/routing/bucket_6", "s1"),
        ("/clusters/demo/buckets/routing/bucket_7", "s2"),
        ("/clusters/demo/buckets/routing/bucket_8", "s1"),
        ("/clusters/demo/buckets/routing/bucket_9", "s2"),
        ("/clusters/demo/buckets/routing/bucket_10", "s1"),
        ("/clusters/demo/buckets/routing/bucket_11", "s1"),
        ("/clusters/demo/buckets/routing/bucket_12", "s1"),
        ("/clusters/demo/buckets/routing/bucket_13", "s2"),
        ("/clusters/demo/buckets/routing/bucket_14", "s1"),
        ("/clusters/demo/buckets/routing/bucket_15", "s2"),
        ("/clusters/demo/buckets/status/bucket_3", "{\"bucket\":\"bucket_3\",\"state\":\"SYNCING\",\"owner\":\"s1\",\"target\":\"s2\",\"started_unix\":1755900000,\"updated_unix\":1755900600,\"phase\":\"copy\"}"),
        ("/clusters/demo/buckets/status/bucket_7", "{\"bucket\":\"bucket_7\",\"state\":\"ABORTING\",\"owner\":\"s2\",\"target\":\"s1\",\"started_unix\":1755800000,\"updated_unix\":1755800500,\"phase\":\"cleanup\",\"last_error\":\"receiver went away\"}"),
        ("/clusters/demo/buckets/status/bucket_11", "{\"bucket\":\"bucket_11\",\"state\":\"FROZEN\",\"owner\":\"s1\",\"target\":\"s2\",\"started_unix\":1755700000,\"updated_unix\":1755700200,\"phase\":\"cutover-wait\"}"),
        ("/clusters/demo/heals/bucket_5", "{\"bucket\":\"bucket_5\",\"was\":\"s2\",\"now\":\"s1\",\"reason\":\"restore-heal\",\"ts\":1755600000}"),
        ("/service/demo-s1/leader", "{\"name\":\"s1a\"}"),
        ("/service/demo-s1/members/s1a", "{\"name\":\"s1a\",\"conn_url\":\"s1a:5432\",\"role\":\"master\",\"state\":\"running\",\"timeline\":1,\"lag\":0}"),
        ("/service/demo-s1/members/s1b", "{\"name\":\"s1b\",\"conn_url\":\"s1b:5432\",\"role\":\"replica\",\"state\":\"streaming\",\"timeline\":1,\"lag\":0}"),
        ("/service/demo-s1/optime/leader", "738273634528"),
        ("/service/demo-s1/initialize", "738273612345678"),
        ("/service/demo-s1/config", "{\"ttl\":5,\"loop_wait\":2,\"retry_timeout\":3}"),
        ("/service/demo-s2/leader", "{\"name\":\"s2a\"}"),
        ("/service/demo-s2/members/s2a", "{\"name\":\"s2a\",\"conn_url\":\"s2a:5432\",\"role\":\"master\",\"state\":\"running\",\"timeline\":1,\"lag\":0}"),
        ("/service/demo-s2/members/s2b", "{\"name\":\"s2b\",\"conn_url\":\"s2b:5432\",\"role\":\"replica\",\"state\":\"streaming\",\"timeline\":1,\"lag\":0}"),
        ("/service/demo-s2/optime/leader", "738273634001"),
        ("/service/demo-s2/initialize", "738273611234567"),
        ("/service/demo-s2/config", "{\"ttl\":5,\"loop_wait\":2,\"retry_timeout\":3}"),
        ("/cluster/nodes/s1a", "172.28.0.11"),
        ("/cluster/nodes/s1b", "172.28.0.12"),
        ("/cluster/nodes/s2a", "172.28.0.21"),
        ("/cluster/nodes/s2b", "172.28.0.22"),
    ];

    // Запись одного ключа тем же транспортом, что читает панель (kv/put; тест — не панель).
    public static async Task PutAsync(string endpoint, string key, string value, CancellationToken ct)
    {
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(3) };
        var body = JsonSerializer.Serialize(new
        {
            key = Convert.ToBase64String(Encoding.UTF8.GetBytes(key)),
            value = Convert.ToBase64String(Encoding.UTF8.GetBytes(value)),
        });
        using var response = await http.PostAsync(
            endpoint + "/v3/kv/put",
            new StringContent(body, Encoding.UTF8, "application/json"),
            ct);
        response.EnsureSuccessStatusCode();
    }

    public static async Task SeedAsync(string endpoint, CancellationToken ct)
    {
        foreach (var (key, value) in Demo)
            await PutAsync(endpoint, key, value, ct);
    }
}
