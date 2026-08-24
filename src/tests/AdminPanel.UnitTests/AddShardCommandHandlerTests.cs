using AdminPanel.Api.Operations;
using AdminPanel.Core;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Client;
using AdminPanel.Etcd.Writing;
using AdminPanel.Infrastructure;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Хендлер добавления шарда: валидация → config (Active only) → имя shard<max+1>
// → клэйм-txn → пакет PUT → компенсация (arch/02 §9.5, t06).
public class AddShardCommandHandlerTests
{
    private const string Endpoint = "http://etcd:2379";
    private const string ConfigKey = "/clusters/shop/config";

    // FakeGateway: All — общий пул kv (range = StartsWith); инъекции отказов
    // по префиксу (чтение) и по ключу (PUT) — различаем 503/404/компенсацию.
    private sealed class FakeGateway : IEtcdGateway
    {
        public List<Kv> All = [];
        public readonly List<(string Key, string Value)> Puts = [];
        public readonly List<(string Key, bool Prefix)> Deletes = [];
        public readonly List<(IReadOnlyList<TxnCompare> Compares, IReadOnlyList<KvPut> Puts)> Txns = [];
        public bool SucceedTxn = true;
        public Func<string, bool>? FailRangeByPrefix;
        public Func<string, bool>? FailPutWhen;

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

        public Task<Result<IReadOnlyList<EtcdAlarm>>> AlarmAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<EtcdAlarm>>.Success([]));

        public Task<Result<TxnResult>> TxnAsync(
            string endpoint, IReadOnlyList<TxnCompare> compares, IReadOnlyList<KvPut> puts, CancellationToken ct)
        {
            Txns.Add((compares, puts));
            return Task.FromResult(Result<TxnResult>.Success(new(SucceedTxn)));
        }

        public Task<Result> PutAsync(string endpoint, string key, string value, CancellationToken ct)
        {
            Puts.Add((key, value));
            return Task.FromResult(FailPutWhen?.Invoke(key) == true
                ? Result.Failed(new InvalidOperationException($"put failed: {key}"))
                : Result.Success());
        }

        public Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
        {
            Deletes.Add((keyOrPrefix, prefix));
            return Task.FromResult(Result.Success());
        }
    }

    private static (AddShardCommandHandler Handler, FakeGateway Gateway) NewHandler()
    {
        var gateway = new FakeGateway();
        var store = new SnapshotStore();
        // Healthy-базис + активный endpoint (ActiveEndpoint решает, куда писать).
        store.Replace(TestSnapshots.Healthy(DateTimeOffset.UnixEpoch) with
        {
            Etcd = new EtcdStatus(
                true, [new EtcdEndpoint(Endpoint, true, 1, "3.5.21", null, null, null, null, [])],
                [], [], Endpoint, false, DateTimeOffset.UnixEpoch, 0),
        });
        return (new AddShardCommandHandler(store, gateway), gateway);
    }

    // Сид Active-кластера shop: config без state, shard1/shard2 с replicas + nodes
    // (nodes/dsn «закрепляют» шард — см. вычисление max в handler).
    private static void SeedActiveCluster(FakeGateway gateway)
    {
        gateway.All =
        [
            new Kv(ConfigKey, """{"buckets":6,"dbname":"shop","created_unix":1755900000}""", 1),
            new Kv("/clusters/shop/shards/shard1/replicas", "2", 2),
            new Kv("/clusters/shop/shards/shard1/nodes/shard1a/state", "RUNNING", 3),
            new Kv("/clusters/shop/shards/shard2/replicas", "2", 4),
            new Kv("/clusters/shop/shards/shard2/nodes/shard2a/state", "RUNNING", 5),
        ];
    }

    [Fact]
    public async Task Handle_InvalidRequest_ReturnsValidationErrors()
    {
        // Arrange: replicas=27 — вне границ §9.3 (replicas=0 НЕ годится: handler
        // подставляет дефолт 2 ДО валидации; отдельный кейс ниже)
        var (handler, gateway) = NewHandler();
        SeedActiveCluster(gateway);

        // Act
        var result = await handler.Handle(
            new AddShardCommand("shop", new AddShardRequest(27, 2, 8, 100)), CancellationToken.None);

        // Assert: 400-ошибка с errors по полю; в etcd не ходили
        var error = result.Error.Should().BeOfType<AddShardValidationException>().Subject;
        error.Errors.Should().Contain(e => e.Field == "replicas");
        gateway.Txns.Should().BeEmpty();
        gateway.Puts.Should().BeEmpty();
    }

    [Fact]
    public async Task Handle_NoConfigKey_Returns404ClusterNotFound()
    {
        // Arrange: config-ключа нет — кластер не существует (§9.5 п.1)
        var (handler, gateway) = NewHandler();
        gateway.All = [new Kv("/clusters/shop/shards/shard1/replicas", "2", 1)];

        // Act
        var result = await handler.Handle(
            new AddShardCommand("shop", new AddShardRequest(2, 2, 8, 100)), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<ClusterNotFoundException>();
        gateway.Txns.Should().BeEmpty();
    }

    [Fact]
    public async Task Handle_ConfigReadFails_ReturnsEtcdError503Not404()
    {
        // Arrange: RangeAsync по config-ключу Failed → та же ошибка наверх
        // (эндпоинт даст 503 «Etcd write failed», НЕ ClusterNotFoundException/404)
        var (handler, gateway) = NewHandler();
        SeedActiveCluster(gateway);
        gateway.FailRangeByPrefix = prefix => prefix == ConfigKey;

        // Act
        var result = await handler.Handle(
            new AddShardCommand("shop", new AddShardRequest(2, 2, 8, 100)), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<InvalidOperationException>();
        gateway.Txns.Should().BeEmpty();
    }

    [Theory]
    [InlineData("NOT_INITIALIZED")]
    [InlineData("TO_REMOVE")]
    public async Task Handle_ClusterNotActive_ReturnsClusterNotActive(string state)
    {
        // Arrange: NOT_INITIALIZED → «дождитесь инициализации»; TO_REMOVE →
        // «кластер удаляется» (§9.5 п.1: Active only, состояние до записи)
        var (handler, gateway) = NewHandler();
        gateway.All =
        [
            new Kv(ConfigKey, $$"""{"buckets":6,"dbname":"shop","created_unix":1755900000,"state":"{{state}}"}""", 1),
        ];

        // Act
        var result = await handler.Handle(
            new AddShardCommand("shop", new AddShardRequest(2, 2, 8, 100)), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<ClusterNotActiveException>();
        gateway.Txns.Should().BeEmpty();
    }

    [Fact]
    public async Task Handle_ComputesShardNameMaxPlusOne()
    {
        // Arrange: существующие shard1/shard2 → имя shard3 (§9.5 п.2)
        var (handler, gateway) = NewHandler();
        SeedActiveCluster(gateway);

        // Act
        var result = await handler.Handle(
            new AddShardCommand("shop", new AddShardRequest(2, 2, 8, 100)), CancellationToken.None);

        // Assert: клэйм-txn на replicas-ключ нового имени; пакет — nodes + request_*
        result.IsSuccess.Should().BeTrue();
        var (compares, txnPuts) = gateway.Txns.Should().ContainSingle().Subject;
        compares.Should().ContainSingle().Which.Key.Should().Be("/clusters/shop/shards/shard3/replicas");
        txnPuts.Should().ContainSingle().Which.Should().Be(
            new KvPut("/clusters/shop/shards/shard3/replicas", "2"));
        gateway.Puts.Select(p => p.Key).Should().BeEquivalentTo(
        [
            "/clusters/shop/shards/shard3/nodes/shard3a/state",
            "/clusters/shop/shards/shard3/nodes/shard3b/state",
            "/service/shop-shard3/request_cpu",
            "/service/shop-shard3/request_mem",
            "/service/shop-shard3/request_disk",
        ]);
    }

    [Fact]
    public async Task Handle_ClaimTxnLost_ReturnsShardNameTaken()
    {
        // Arrange: конкурентный POST занял имя — compare не сошёлся (§9.5 п.2)
        var (handler, gateway) = NewHandler();
        SeedActiveCluster(gateway);
        gateway.SucceedTxn = false;

        // Act
        var result = await handler.Handle(
            new AddShardCommand("shop", new AddShardRequest(2, 2, 8, 100)), CancellationToken.None);

        // Assert: 409; пакет PUT не выполнялся
        result.Error.Should().BeOfType<ShardNameTakenException>();
        gateway.Puts.Should().BeEmpty();
    }

    [Fact]
    public async Task Handle_PutFailsMiddle_Compensates()
    {
        // Arrange: PUT падает на 2-м ключе пакета (nodes/shard3b/state) (§9.5 п.4)
        var (handler, gateway) = NewHandler();
        SeedActiveCluster(gateway);
        gateway.FailPutWhen = key => key.EndsWith("shard3b/state", StringComparison.Ordinal);

        // Act
        var result = await handler.Handle(
            new AddShardCommand("shop", new AddShardRequest(2, 2, 8, 100)), CancellationToken.None);

        // Assert: компенсация — del prefix shards/shard3/ + точечные del request_*
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<InvalidOperationException>();
        gateway.Deletes.Should().Contain(("/clusters/shop/shards/shard3/", true));
        gateway.Deletes.Should().Contain(d => d.Key.StartsWith("/service/shop-shard3/request_", StringComparison.Ordinal) && !d.Prefix);
        gateway.Deletes.Count(d => !d.Prefix).Should().Be(3);
    }

    [Fact]
    public async Task Handle_Success_ReturnsCanonicalDto()
    {
        // Arrange: полный путь — 201-DTO с каноническими строками (§6.1)
        var (handler, gateway) = NewHandler();
        SeedActiveCluster(gateway);

        // Act
        var result = await handler.Handle(
            new AddShardCommand("shop", new AddShardRequest(2, 0.5m, 8, 100)), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be(new ShardAddedDto(
            "shop", "shard3", 2, "0.5", "8Gi", "100Gi", "NOT_INITIALIZED"));
        gateway.Deletes.Should().BeEmpty();
    }

    [Fact]
    public async Task Handle_ReplicasZeroDefaultsToTwo()
    {
        // Arrange: replicas=0 = поле отсутствовало в JSON → дефолт 2 (§6.1/§9.3)
        var (handler, gateway) = NewHandler();
        SeedActiveCluster(gateway);

        // Act
        var result = await handler.Handle(
            new AddShardCommand("shop", new AddShardRequest(0, 2, 8, 100)), CancellationToken.None);

        // Assert: клэйм и DTO с replicas "2" — валидацию дефолт не ломает
        result.IsSuccess.Should().BeTrue();
        result.Value.Replicas.Should().Be(2);
        gateway.Txns.Single().Puts.Should().ContainSingle().Which.Value.Should().Be("2");
    }
}
