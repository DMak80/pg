using KafkaWorker.IntegrationTests.Etcd;

namespace KafkaWorker.IntegrationTests.Api;

// Сид-декларации API-тестов kafka-домена (task etcd-via-worker-api): прямые
// put в fixture-gateway (порт панельного KafkaCommandHarness.SeedActiveCluster,
// канонические значения arch/02 §10.2 / arch/15 §2).
internal static class KafkaApiTestSeed
{
    /// <summary>Active-кластер: config без state (arch/15 §2.1) + N брокеров
    /// (controller у первых трёх, как сид панели) + endpoints + app-креды.</summary>
    public static async Task SeedActiveClusterAsync(
        EtcdFixture etcd, string cluster, int brokers = 3)
    {
        var ct = TestContext.Current.CancellationToken;
        var gw = etcd.Gateway;
        // Чистый кластер на каждый сид: etcd общий на класс, ключи прошлых
        // тестов (например broker4 от DELETE-сценария) ломали следующий сид
        // «N брокеров» — POST нового брокера отвечал конфликтом имени.
        await gw.DeleteAsync(etcd.Endpoint, $"/kafka/clusters/{cluster}/", prefix: true, ct);
        await gw.PutAsync(etcd.Endpoint, $"/kafka/clusters/{cluster}/config",
            $$"""{"brokers":{{brokers}},"replication_factor":3,"min_insync_replicas":2,"default_partitions":12,"default_retention_ms":604800000,"created_unix":1756500000}""",
            null, ct);
        for (var k = 1; k <= brokers; k++)
        {
            await gw.PutAsync(etcd.Endpoint, $"/kafka/clusters/{cluster}/brokers/broker{k}/state",
                "RUNNING", null, ct);
            await gw.PutAsync(etcd.Endpoint, $"/kafka/clusters/{cluster}/brokers/broker{k}/role",
                k <= Math.Min(3, brokers) ? "controller" : "broker", null, ct);
            await gw.PutAsync(etcd.Endpoint, $"/kafka/clusters/{cluster}/brokers/broker{k}/resources",
                """{"cpu":"2","mem":"4Gi","disk":"40Gi"}""", null, ct);
        }

        await gw.PutAsync(etcd.Endpoint, $"/kafka/clusters/{cluster}/endpoints",
            "h1:16001,h1:16002,h1:16003", null, ct);
        await gw.PutAsync(etcd.Endpoint, $"/kafka/clusters/{cluster}/app_user", "app", null, ct);
        await gw.PutAsync(etcd.Endpoint, $"/kafka/clusters/{cluster}/app_password",
            "OldPassword0123456789abcdef", null, ct);
    }

    /// <summary>Факт-ключ топика (arch/15 §3): partitions/RF/configs/desired/missing.</summary>
    public static Task SeedTopicKeyAsync(
        EtcdFixture etcd, string cluster, string topic, int partitions,
        string? desiredJson = null, bool missing = false)
        => etcd.Gateway.PutAsync(etcd.Endpoint, $"/kafka/clusters/{cluster}/topics/{topic}",
            "{\"partitions\":" + partitions
            + ",\"replication_factor\":3"
            + ",\"configs\":{\"retention.ms\":\"604800000\"}"
            + (desiredJson is null ? "" : "," + desiredJson)
            + ",\"synced_unix\":1756500900"
            + ",\"missing\":" + (missing ? "true" : "false") + "}",
            null, TestContext.Current.CancellationToken);

    /// <summary>Живая lifecycle-заявка topics/<t>/desired.&lt;op&gt; (arch/15 §3.1).</summary>
    public static Task SeedLifecycleTicketAsync(
        EtcdFixture etcd, string cluster, string topic, string op,
        long unix = 1756501200, string by = "seed")
        => etcd.Gateway.PutAsync(etcd.Endpoint,
            $"/kafka/clusters/{cluster}/topics/{topic}/desired.{op}",
            $$"""{"requested_unix":{{unix}},"requested_by":"{{by}}"}""",
            null, TestContext.Current.CancellationToken);
}
