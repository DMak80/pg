using AdminPanel.Core;

namespace AdminPanel.UnitTests;

// Сборка EtcdSnapshot-фикстур для тестов алертов/мапперов/хендлеров (spec §10):
// healthy-базис и модификации through with.
internal static class TestSnapshots
{
    // Здоровый снапшот: 3 живых endpoints, полный кластер demo, живой ключ API
    // воркера (канон task etcd-via-worker-api: здоровая система = воркер жив),
    // без алертов/ошибок.
    public static EtcdSnapshot Healthy(DateTimeOffset builtAt) => new(
        builtAt,
        HealthyEtcd(builtAt),
        [FullCluster()],
        [],
        [],
        [],
        [new WorkerEndpoint("w1", "http://pgworker:8080", builtAt.ToUnixTimeSeconds())],
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
        "demo", "demo", 16, 1755800000, ClusterState.Active,
        [new ShardInfo(
            "s1", "host=s1a,s1b port=5432 dbname=demo user=postgres",
            ["s1a", "s1b"], 5432, "demo", "postgres", 1, "s1a:5432", [], null)],
        [.. Enumerable.Range(0, 16).Select(i => new BucketInfo(i, "s1", BucketState.Active, null))],
        []);

    // Кластер без config-ключа: Incomplete = true (t03 §3.6).
    public static ClusterInfo GhostCluster() => new("ghost", null, 0, null, ClusterState.Active, [], [], []);

    // Кластер с динамикой переездов и аномалиями (spec §10.5): 2 шарда (s2 — без master),
    // бакеты 0..15 (routing s1/s2, у 4 — дыра), 3 статус-ключа относительно now, 2 heals.
    public static ClusterInfo MovingCluster(DateTimeOffset now)
    {
        var unix = now.ToUnixTimeSeconds();
        return new ClusterInfo(
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
    }

    // HA-фикстуры t06: matched-скоп с пробами членов и unmatched без лидера (spec §10).
    public static HaScope HaScopeDemo(DateTimeOffset now) => new(
        "demo-s1", "demo", "s1", true, "s1a", 738273634528L, true, null, null, null,
        [
            new HaMember("s1a", "s1a", 5432, "master", "running", 1L, 0L, now, null, null),
            new HaMember("s1b", "s1b", 5432, "replica", "streaming", 1L, 17L * 1024 * 1024, now, null, null),
        ],
        "{\"ttl\":5,\"loop_wait\":2}");

    public static HaScope UnmatchedNoLeader(DateTimeOffset now) => new(
        "other-scope", null, null, false, null, null, false, null, null, null,
        [new HaMember("n1", "n1", 5432, "replica", "stopped", null, null, now, "connection refused", null)],
        null);

    // Снапшот с HA-скопами (без runtime — SQL-часть добавляется через with).
    public static EtcdSnapshot WithHaScopes(DateTimeOffset now) => Healthy(now) with
    {
        HaScopes = [HaScopeDemo(now), UnmatchedNoLeader(now)],
    };

    // Runtime шарда для SQL-правил: слот с лагом/риском, standby async (spec §10.1).
    public static ShardRuntime ShardRuntimeOf(string shard) => new(
        shard,
        [new ReplicationSlotInfo("move_bucket_3", "logical", true, "lost", 512L * 1024 * 1024, 100L)],
        [new StandbyInfo("s1b", "10.0.0.2", "streaming", "async", 100L)],
        [],
        [.. Enumerable.Range(0, 16).Select(i => $"bucket_{i}")],
        false,
        null);
}
