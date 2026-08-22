using AdminPanel.Core;

namespace AdminPanel.UnitTests;

// Сборка EtcdSnapshot-фикстур для тестов алертов/мапперов/хендлеров (spec §10):
// healthy-базис и модификации through with.
internal static class TestSnapshots
{
    // Здоровый снапшот: 3 живых endpoints, полный кластер demo, без алертов/ошибок.
    public static EtcdSnapshot Healthy(DateTimeOffset builtAt) => new(
        builtAt,
        HealthyEtcd(builtAt),
        [FullCluster()],
        [],
        [],
        [],
        [],
        [],
        0);

    // Все endpoints живые; alive < total — хвост мёртвый с ошибкой транспорта.
    public static EtcdStatus HealthyEtcd(DateTimeOffset at, int alive = 3, int total = 3) => new(
        alive > 0,
        [.. Enumerable.Range(0, total).Select(i => new EtcdEndpoint(
            $"http://etcd{i + 1}:2379",
            i < alive,
            i < alive ? 3 + i : null,
            i < alive ? "3.5.21" : null,
            i < alive ? 20480 : null,
            i < alive ? 42 : null,
            i < alive ? 17 : null,
            i < alive ? 3 : null,
            i < alive ? [] : ["connection refused"]))],
        [new EtcdMember(42, "etcd1", ["http://etcd1:2380"], ["http://etcd1:2379"])],
        [],
        "http://etcd1:2379",
        false,
        at,
        0);

    // Полный кластер (config есть): Incomplete = false.
    public static ClusterInfo FullCluster() => new(
        "demo", "demo", 16, 1755800000,
        [new ShardInfo(
            "s1", "host=s1a,s1b port=5432 dbname=demo user=postgres",
            ["s1a", "s1b"], 5432, "demo", "postgres", 1, "s1a:5432", null)],
        [.. Enumerable.Range(0, 16).Select(i => new BucketInfo(i, "s1", BucketState.Active, null))],
        []);

    // Кластер без config-ключа: Incomplete = true (t03 §3.6).
    public static ClusterInfo GhostCluster() => new("ghost", null, 0, null, [], [], []);
}
