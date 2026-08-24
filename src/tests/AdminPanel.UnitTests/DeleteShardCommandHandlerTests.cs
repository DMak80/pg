using AdminPanel.Api.Operations;
using AdminPanel.Core;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Client;
using AdminPanel.Infrastructure;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Хендлер демонтажа шарда: config → replicas → пред-проверки guard'ов по
// снапшоту (Д4) → идемпотентный PUT маркера TO_REMOVE (arch/02 §9.6, t06).
public class DeleteShardCommandHandlerTests
{
    private const string Endpoint = "http://etcd:2379";
    private const string ConfigKey = "/clusters/shop/config";
    private const string ReplicasKey = "/clusters/shop/shards/shard1/replicas";
    private const string MarkerKey = "/clusters/shop/shards/shard1/state";

    // FakeGateway: как в тестах добавления шарда (различение 503/404 по Failed-range).
    private sealed class FakeGateway : IEtcdGateway
    {
        public List<Kv> All = [];
        public readonly List<(string Key, string Value)> Puts = [];
        public Func<string, bool>? FailRangeByPrefix;

        public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
        {
            if (FailRangeByPrefix?.Invoke(prefix) == true)
                return Task.FromResult(Result<IReadOnlyList<Kv>>.Failed(
                    new InvalidOperationException($"range failed: {prefix}")));
            return Task.FromResult(Result<IReadOnlyList<Kv>>.Success(
                [.. All.Where(kv => kv.Key.StartsWith(prefix, StringComparison.Ordinal))]));
        }

        public Task<Result<EtcdStatusPayload>> StatusAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<EtcdStatusPayload>.Success(new(null, null, null, null, null)));

        public Task<Result<IReadOnlyList<EtcdMember>>> MemberListAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<EtcdMember>>.Success([]));

        public Task<Result<IReadOnlyList<EtcdAlarm>> > AlarmAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<EtcdAlarm>>.Success([]));

        public Task<Result<TxnResult>> TxnAsync(
            string endpoint, IReadOnlyList<TxnCompare> compares, IReadOnlyList<KvPut> puts, CancellationToken ct)
            => Task.FromResult(Result<TxnResult>.Success(new(true)));

        public Task<Result> PutAsync(string endpoint, string key, string value, CancellationToken ct)
        {
            Puts.Add((key, value));
            return Task.FromResult(Result.Success());
        }

        public Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
            => Task.FromResult(Result.Success());
    }

    // Кластер shop в снапшоте: 2 шарда RUNNING, 6 бакетов — ВСЕ на shard2
    // (shard1 пуст — база для счастливого пути маркера).
    private static ClusterInfo ShopCluster() => new(
        "shop", "shop", 6, 1755900000, ClusterState.Active,
        [
            new ShardInfo("shard1", "host=shard1a port=5432 dbname=shop user=bucket_admin",
                ["shard1a"], 5432, "shop", "bucket_admin", 2, "shard1a:5432",
                [new NodeInfo("shard1a", "RUNNING"), new NodeInfo("shard1b", "RUNNING")], null),
            new ShardInfo("shard2", "host=shard2a port=5432 dbname=shop user=bucket_admin",
                ["shard2a"], 5432, "shop", "bucket_admin", 2, "shard2a:5432",
                [new NodeInfo("shard2a", "RUNNING"), new NodeInfo("shard2b", "RUNNING")], null),
        ],
        [.. Enumerable.Range(0, 6).Select(i => new BucketInfo(i, "shard2", BucketState.Active, null))],
        []);

    // Handler + снапшот с кластером cluster (по умолчанию shop) + Active etcd.
    private static (DeleteShardCommandHandler Handler, FakeGateway Gateway, SnapshotStore Store) NewHandler(
        ClusterInfo? cluster = null)
    {
        var gateway = new FakeGateway();
        var store = new SnapshotStore();
        store.Replace(TestSnapshots.Healthy(DateTimeOffset.UnixEpoch) with
        {
            Etcd = new EtcdStatus(
                true, [new EtcdEndpoint(Endpoint, true, 1, "3.5.21", null, null, null, null, [])],
                [], [], Endpoint, false, DateTimeOffset.UnixEpoch, 0),
            Clusters = [cluster ?? ShopCluster()],
        });
        return (new DeleteShardCommandHandler(store, gateway), gateway, store);
    }

    // Сид etcd: config Active + replicas обоих шардов.
    private static void SeedActive(FakeGateway gateway)
    {
        gateway.All =
        [
            new Kv(ConfigKey, """{"buckets":6,"dbname":"shop","created_unix":1755900000}""", 1),
            new Kv(ReplicasKey, "2", 2),
            new Kv("/clusters/shop/shards/shard2/replicas", "2", 3),
        ];
    }

    [Fact]
    public async Task Handle_NoClusterConfig_Returns404()
    {
        // Arrange: config-ключа нет (§9.6 п.2)
        var (handler, gateway, _) = NewHandler();
        gateway.All = [new Kv(ReplicasKey, "2", 1)];

        // Act
        var result = await handler.Handle(new DeleteShardCommand("shop", "shard1"), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<ClusterNotFoundException>();
        gateway.Puts.Should().BeEmpty();
    }

    [Fact]
    public async Task Handle_ConfigReadFails_ReturnsEtcdError503Not404()
    {
        // Arrange: сбой чтения config → исходная ошибка (503), НЕ ClusterNotFoundException
        var (handler, gateway, _) = NewHandler();
        SeedActive(gateway);
        gateway.FailRangeByPrefix = prefix => prefix == ConfigKey;

        // Act
        var result = await handler.Handle(new DeleteShardCommand("shop", "shard1"), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<InvalidOperationException>();
        gateway.Puts.Should().BeEmpty();
    }

    [Theory]
    [InlineData("NOT_INITIALIZED")]
    [InlineData("TO_REMOVE")]
    public async Task Handle_ClusterNotActive_ReturnsClusterNotActive(string state)
    {
        // Arrange: состояние кластера проверяется до записи (§9.6 п.2)
        var (handler, gateway, _) = NewHandler();
        gateway.All =
        [
            new Kv(ConfigKey, $$"""{"buckets":6,"dbname":"shop","created_unix":1755900000,"state":"{{state}}"}""", 1),
        ];

        // Act
        var result = await handler.Handle(new DeleteShardCommand("shop", "shard1"), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<ClusterNotActiveException>();
        gateway.Puts.Should().BeEmpty();
    }

    [Fact]
    public async Task Handle_NoReplicasKey_ReturnsShardNotFound()
    {
        // Arrange: шард не заявлен — replicas-ключа нет (§9.6 п.3)
        var (handler, gateway, _) = NewHandler();
        gateway.All = [new Kv(ConfigKey, """{"buckets":6,"dbname":"shop"}""", 1)];

        // Act
        var result = await handler.Handle(new DeleteShardCommand("shop", "ghost"), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<ShardNotFoundException>();
        gateway.Puts.Should().BeEmpty();
    }

    [Fact]
    public async Task Handle_ReplicasReadFails_ReturnsEtcdError503Not404()
    {
        // Arrange: сбой чтения replicas-ключа → 503, НЕ ShardNotFoundException
        var (handler, gateway, _) = NewHandler();
        SeedActive(gateway);
        gateway.FailRangeByPrefix = prefix => prefix == ReplicasKey;

        // Act
        var result = await handler.Handle(new DeleteShardCommand("shop", "shard1"), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<InvalidOperationException>();
        gateway.Puts.Should().BeEmpty();
    }

    [Fact]
    public async Task Handle_RoutingOnShard_Returns409WithBucketCount()
    {
        // Arrange: все 6 бакетов на shard2 → демонтаж shard2 блокирован (Д4)
        var (handler, gateway, _) = NewHandler();
        SeedActive(gateway);

        // Act
        var result = await handler.Handle(new DeleteShardCommand("shop", "shard2"), CancellationToken.None);

        // Assert: 409 с числом бакетов и подсказкой; маркер НЕ писался
        var error = result.Error.Should().BeOfType<ShardRemoveBlockedException>().Subject;
        error.Message.Should().Contain("6").And.Contain("перевезите");
        gateway.Puts.Should().BeEmpty();
    }

    [Fact]
    public async Task Handle_UnfinishedMoveTargetShard_Returns409()
    {
        // Arrange: бакет SYNCING с Move.Target=shard1 (routing на shard2) —
        // шард — цель незавершённого переезда (зеркало G4, «owner ИЛИ target»)
        var (handler, gateway, _) = NewHandler(cluster: ShopCluster() with
        {
            Buckets =
            [
                new BucketInfo(0, "shard2", BucketState.Syncing,
                    new MoveInfo("shard2", "shard1", 1, 2, "copy", null)),
                .. Enumerable.Range(1, 5).Select(i => new BucketInfo(i, "shard2", BucketState.Active, null)),
            ],
        });
        SeedActive(gateway);

        // Act
        var result = await handler.Handle(new DeleteShardCommand("shop", "shard1"), CancellationToken.None);

        // Assert
        var error = result.Error.Should().BeOfType<ShardRemoveBlockedException>().Subject;
        error.Message.Should().Contain("незавершённый переезд");
        gateway.Puts.Should().BeEmpty();
    }

    [Fact]
    public async Task Handle_UnfinishedMoveFlippedOwnerShard_Returns409()
    {
        // Arrange: «flip прошёл, статус завис»: routing-owner = shard2, статус
        // жив с Move.Owner = shard1 → пред-проверка смотрит и статус-owner
        var (handler, gateway, _) = NewHandler(cluster: ShopCluster() with
        {
            Buckets =
            [
                new BucketInfo(0, "shard2", BucketState.Syncing,
                    new MoveInfo("shard1", "shard2", 1, 2, "flip", null)),
                .. Enumerable.Range(1, 5).Select(i => new BucketInfo(i, "shard2", BucketState.Active, null)),
            ],
        });
        SeedActive(gateway);

        // Act
        var result = await handler.Handle(new DeleteShardCommand("shop", "shard1"), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<ShardRemoveBlockedException>()
            .Which.Message.Should().Contain("незавершённый переезд");
        gateway.Puts.Should().BeEmpty();
    }

    [Fact]
    public async Task Handle_SingleShardCluster_Returns409LastShard()
    {
        // Arrange: шард один в кластере — G7 (§9.6 п.4)
        var (handler, gateway, _) = NewHandler(cluster: ShopCluster() with
        {
            Shards = [ShopCluster().Shards[0]],
        });
        SeedActive(gateway);

        // Act
        var result = await handler.Handle(new DeleteShardCommand("shop", "shard1"), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<ShardRemoveBlockedException>()
            .Which.Message.Should().Contain("последний шард");
        gateway.Puts.Should().BeEmpty();
    }

    [Fact]
    public async Task Handle_QuarantinedNode_Returns409Quarantine()
    {
        // Arrange: нода shard1b в карантине после эвакуации (§9.6 п.4)
        var (handler, gateway, _) = NewHandler(cluster: ShopCluster() with
        {
            Shards =
            [
                ShopCluster().Shards[0] with
                {
                    Nodes = [new NodeInfo("shard1a", "RUNNING"), new NodeInfo("shard1b", "QUARANTINED")],
                },
                ShopCluster().Shards[1],
            ],
        });
        SeedActive(gateway);

        // Act
        var result = await handler.Handle(new DeleteShardCommand("shop", "shard1"), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<ShardRemoveBlockedException>()
            .Which.Message.Should().Contain("карантин");
        gateway.Puts.Should().BeEmpty();
    }

    [Fact]
    public async Task Handle_AlreadyToRemove_IdempotentSuccessNoWrite()
    {
        // Arrange: маркер уже стоит — 204 без записи (§9.6 п.5)
        var (handler, gateway, _) = NewHandler();
        SeedActive(gateway);
        gateway.All.Add(new Kv(MarkerKey, "TO_REMOVE", 4));

        // Act
        var result = await handler.Handle(new DeleteShardCommand("shop", "shard1"), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be(new ShardDeletedDto("shop", "shard1", "TO_REMOVE"));
        gateway.Puts.Should().BeEmpty();
    }

    [Fact]
    public async Task Handle_MarkerReadFails_ReturnsEtcdError_NoPut()
    {
        // Arrange: сбой чтения state-ключа → 503 — не идемпотентный успех и
        // не PUT поверх нечитанного состояния (§9.6 п.5)
        var (handler, gateway, _) = NewHandler();
        SeedActive(gateway);
        gateway.FailRangeByPrefix = prefix => prefix == MarkerKey;

        // Act
        var result = await handler.Handle(new DeleteShardCommand("shop", "shard1"), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<InvalidOperationException>();
        gateway.Puts.Should().BeEmpty();
    }

    [Fact]
    public async Task Handle_Success_PutsMarkerKey()
    {
        // Arrange: пустой шард Active-кластера (§9.6 п.5)
        var (handler, gateway, _) = NewHandler();
        SeedActive(gateway);

        // Act
        var result = await handler.Handle(new DeleteShardCommand("shop", "shard1"), CancellationToken.None);

        // Assert: единственный PUT — маркер TO_REMOVE
        result.IsSuccess.Should().BeTrue();
        gateway.Puts.Should().ContainSingle().Which.Should().Be((MarkerKey, "TO_REMOVE"));
    }

    [Fact]
    public async Task Handle_SnapshotLag_Returns503PrecheckUnavailable()
    {
        // Arrange: кластер в etcd есть, в снапшоте нет — пред-проверки невозможны
        var (handler, gateway, store) = NewHandler(cluster: TestSnapshots.FullCluster());
        SeedActive(gateway);

        // Act
        var result = await handler.Handle(new DeleteShardCommand("shop", "shard1"), CancellationToken.None);

        // Assert: 503 «повторите запрос» (следующий тик снапшота подхватит)
        result.Error.Should().BeOfType<ShardPrecheckUnavailableException>();
        gateway.Puts.Should().BeEmpty();
    }
}
