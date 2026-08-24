using System.Text.Json;
using AdminPanel.Api.Operations;
using AdminPanel.Core;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Client;
using AdminPanel.Infrastructure;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Хендлер постановки заявок на переезды: валидация → config → guard'ы по снапшоту →
// очередь напрямую → txn-клэйм per key; сбой без компенсации (arch/02 §9.7, spec §4.3).
public class MoveBucketsCommandHandlerTests
{
    private const string Endpoint = "http://etcd:2379";
    private static readonly DateTimeOffset Now = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    // FakeGateway — копия образца AddShardCommandHandlerTests (пул kv + счётчики txn).
    private sealed class FakeGateway : IEtcdGateway
    {
        public List<Kv> All = [];
        public readonly List<(IReadOnlyList<TxnCompare> Compares, IReadOnlyList<KvPut> Puts)> Txns = [];
        public bool SucceedTxn = true;
        public Func<string, bool>? FailRangeByPrefix;
        public Func<string, bool>? FailTxnWhen;

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
            return Task.FromResult(FailTxnWhen?.Invoke(puts[0].Key) == true
                ? Result<TxnResult>.Failed(new InvalidOperationException($"txn failed: {puts[0].Key}"))
                : Result<TxnResult>.Success(new(SucceedTxn)));
        }

        public Task<Result> PutAsync(string endpoint, string key, string value, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
            => Task.FromResult(Result.Success());
    }

    // Снапшот: Active-кластер shop, 2 шарда, 6 бакетов (0,2,4 — shard1; 1,3,5 — shard2).
    private static ClusterInfo ShopCluster() => new(
        "shop", "shop", 6, 1755900000, ClusterState.Active,
        [
            new ShardInfo("shard1", "", [], null, null, null, 2, null, [], null),
            new ShardInfo("shard2", "", [], null, null, null, 2, null, [], null),
        ],
        [.. Enumerable.Range(0, 6).Select(i => new BucketInfo(i, i % 2 == 0 ? "shard1" : "shard2", BucketState.Active, null))],
        []);

    private static (MoveBucketsCommandHandler Handler, FakeGateway Gateway) NewHandler(ClusterInfo? cluster = null)
    {
        var gateway = new FakeGateway();
        var store = new SnapshotStore();
        var time = new FixedTimeProvider { Utc = Now };
        store.Replace(TestSnapshots.Healthy(Now) with
        {
            Etcd = new EtcdStatus(
                true, [new EtcdEndpoint(Endpoint, true, 1, "3.5.21", null, null, null, null, [])],
                [], [], Endpoint, false, Now, 0),
            Clusters = [cluster ?? ShopCluster()],
        });
        return (new MoveBucketsCommandHandler(store, gateway, time), gateway);
    }

    // etcd-сид: config Active-кластера shop.
    private static void Seed(FakeGateway gateway)
    {
        gateway.All =
        [
            new Kv("/clusters/shop/config", """{"buckets":6,"dbname":"shop","created_unix":1755900000}""", 1),
        ];
    }

    private static MoveBucketsCommand Command(string from = "shard1", string to = "shard2",
        int[]? buckets = null, string by = "admin") =>
        new("shop", from, to, buckets ?? [0, 2, 4], by);

    [Fact]
    public async Task Handle_EmptyOrDuplicateBuckets_Returns400()
    {
        // Arrange / Act
        var (handler, gateway) = NewHandler();
        Seed(gateway);
        var empty = await handler.Handle(Command(buckets: []), CancellationToken.None);
        var dup = await handler.Handle(Command(buckets: [0, 0]), CancellationToken.None);

        // Assert: errors по полю buckets; в etcd не ходили
        empty.Error.Should().BeOfType<MoveBucketsValidationException>();
        dup.Error.Should().BeOfType<MoveBucketsValidationException>();
        gateway.Txns.Should().BeEmpty();
    }

    [Fact]
    public async Task Handle_NullFieldsBody_Returns400WithFieldErrors()
    {
        // Arrange: тело {} или {"from":null,"to":null} — JSON-биндинг даёт null-поля
        // (non-nullable аннотации STJ не проверяет); валидатор обязан вернуть
        // ошибки по полям, а не упасть NRE на сравнении from==to.
        var (handler, gateway) = NewHandler();
        Seed(gateway);

        // Act
        var result = await handler.Handle(
            new MoveBucketsCommand("shop", null!, null!, null!, "admin"), CancellationToken.None);

        // Assert: 400-путь — MoveBucketsValidationException с errors по from/to/buckets; в etcd не ходили
        var error = result.Error.Should().BeOfType<MoveBucketsValidationException>().Subject;
        error.Errors.Select(e => e.Field).Should().BeEquivalentTo(["from", "to", "buckets"]);
        gateway.Txns.Should().BeEmpty();
    }

    [Fact]
    public async Task Handle_FromEqualsTo_Returns400()
    {
        // Arrange / Act
        var (handler, gateway) = NewHandler();
        Seed(gateway);
        var result = await handler.Handle(Command(from: "shard1", to: "shard1"), CancellationToken.None);

        // Assert
        var error = result.Error.Should().BeOfType<MoveBucketsValidationException>().Subject;
        error.Errors.Should().Contain(e => e.Field == "to");
    }

    [Fact]
    public async Task Handle_NoConfig_Returns404()
    {
        // Arrange: config-ключа нет
        var (handler, _) = NewHandler();
        // Act
        var result = await handler.Handle(Command(), CancellationToken.None);
        // Assert
        result.Error.Should().BeOfType<ClusterNotFoundException>();
    }

    [Fact]
    public async Task Handle_NotActiveCluster_Returns409()
    {
        // Arrange
        var (handler, gateway) = NewHandler();
        gateway.All = [new Kv("/clusters/shop/config",
            """{"buckets":6,"dbname":"shop","state":"TO_REMOVE"}""", 1)];
        // Act
        var result = await handler.Handle(Command(), CancellationToken.None);
        // Assert
        result.Error.Should().BeOfType<ClusterNotActiveException>();
    }

    [Fact]
    public async Task Handle_NonSharded_Returns409()
    {
        // Arrange: 1 бакет и единственный шард (arch/03 §2)
        var (handler, gateway) = NewHandler(new ClusterInfo("shop", "shop", 1, null, ClusterState.Active,
            [new ShardInfo("shard1", "", [], null, null, null, 2, null, [], null)],
            [new BucketInfo(0, "shard1", BucketState.Active, null)], []));
        Seed(gateway);
        // Act
        var result = await handler.Handle(Command(), CancellationToken.None);
        // Assert
        result.Error.Should().BeOfType<NonShardedClusterException>();
    }

    [Fact]
    public async Task Handle_UnknownShard_Returns404()
    {
        // Arrange / Act
        var (handler, gateway) = NewHandler();
        Seed(gateway);
        var result = await handler.Handle(Command(to: "shard9"), CancellationToken.None);
        // Assert
        result.Error.Should().BeOfType<ShardNotFoundException>();
    }

    [Fact]
    public async Task Handle_TargetToRemove_Returns409()
    {
        // Arrange: приёмник в демонтаже (Д9)
        var (handler, gateway) = NewHandler(ShopCluster() with
        {
            Shards =
            [
                new ShardInfo("shard1", "", [], null, null, null, 2, null, [], null),
                new ShardInfo("shard2", "", [], null, null, null, 2, null, [], null, ShardState.ToRemove),
            ],
        });
        Seed(gateway);
        // Act
        var result = await handler.Handle(Command(), CancellationToken.None);
        // Assert
        result.Error.Should().BeOfType<MoveTargetRemovingException>();
        gateway.Txns.Should().BeEmpty();
    }

    [Fact]
    public async Task Handle_BucketNotOnSource_Returns409()
    {
        // Arrange: бакет 1 принадлежит shard2, везём «с shard1»
        var (handler, gateway) = NewHandler();
        Seed(gateway);
        // Act
        var result = await handler.Handle(Command(buckets: [0, 1]), CancellationToken.None);
        // Assert: сообщение называет фактического владельца
        result.Error.Should().BeOfType<BucketNotOnSourceException>()
            .Which.Message.Should().Contain("shard2");
    }

    [Fact]
    public async Task Handle_SyncingBucket_Returns409()
    {
        // Arrange: бакет 2 в незавершённом переезде (статус-ключ)
        var cluster = ShopCluster() with
        {
            Buckets =
            [
                .. Enumerable.Range(0, 6).Select(i => new BucketInfo(
                    i, i % 2 == 0 ? "shard1" : "shard2",
                    i == 2 ? BucketState.Syncing : BucketState.Active,
                    i == 2 ? new MoveInfo("shard1", "shard2", 1, 2, "copy", null) : null)),
            ],
        };
        var (handler, gateway) = NewHandler(cluster);
        Seed(gateway);
        // Act
        var result = await handler.Handle(Command(), CancellationToken.None);
        // Assert
        result.Error.Should().BeOfType<BucketNotOnSourceException>()
            .Which.Message.Should().Contain("SYNCING");
    }

    [Fact]
    public async Task Handle_ConflictingTicket_Returns409BeforeWrites()
    {
        // Arrange: на bucket_0 уже стоит ИНАЯ заявка (op=move, to=shard3) (Д7)
        var (handler, gateway) = NewHandler();
        gateway.All =
        [
            new Kv("/clusters/shop/config", """{"buckets":6,"dbname":"shop"}""", 1),
            new Kv("/pgworker/moves/shop/bucket_0",
                """{"op":"move","to":"shard3","requested_unix":10,"requested_by":"etcdctl"}""", 2),
        ];
        // Act
        var result = await handler.Handle(Command(), CancellationToken.None);
        // Assert: отказ до любых txn; бакет назван
        result.Error.Should().BeOfType<MoveRequestConflictException>()
            .Which.Message.Should().Contain("bucket_0");
        gateway.Txns.Should().BeEmpty();
    }

    [Fact]
    public async Task Handle_IdenticalTicket_GoesToSkippedWithoutTxn()
    {
        // Arrange: на bucket_0 уже стоит ТА ЖЕ заявка move→shard2 (Д6)
        var (handler, gateway) = NewHandler();
        gateway.All =
        [
            new Kv("/clusters/shop/config", """{"buckets":6,"dbname":"shop"}""", 1),
            new Kv("/pgworker/moves/shop/bucket_0",
                """{"op":"move","to":"shard2","requested_unix":1755850000,"requested_by":"ops"}""", 2),
        ];
        // Act
        var result = await handler.Handle(Command(), CancellationToken.None);
        // Assert: bucket_0 skipped, остальные 2 — txn с base = maxUnix+1 > now (Д2)
        result.IsSuccess.Should().BeTrue();
        result.Value.Skipped.Should().BeEquivalentTo([0]);
        result.Value.Queued.Should().BeEquivalentTo([2, 4]);
        var unixes = gateway.Txns.Select(t => ParseUnix(t.Puts[0].Value)).ToList();
        unixes.Should().BeInAscendingOrder().And.OnlyContain(u => u >= 1755850001L);
    }

    [Fact]
    public async Task Handle_Success_QueuesAscendingUnixWithCanonicalBody()
    {
        // Arrange: очередь пуста → base = now (FixedTimeProvider)
        var (handler, gateway) = NewHandler();
        Seed(gateway);
        // Act
        var result = await handler.Handle(Command(by: "ops"), CancellationToken.None);
        // Assert: по возрастанию id, requested_unix = base+0/+1/+2 (Д2/Д3)
        result.IsSuccess.Should().BeTrue();
        // record-равенство списков — по ссылкам; сверяем значения рекурсивно
        result.Value.Should().BeEquivalentTo(
            new MovesQueuedDto("shop", "shard1", "shard2", [0, 2, 4], []));
        gateway.Txns.Select(t => t.Puts[0].Key).Should().BeEquivalentTo(
        [
            "/pgworker/moves/shop/bucket_0",
            "/pgworker/moves/shop/bucket_2",
            "/pgworker/moves/shop/bucket_4",
        ]);
        gateway.Txns.Select(t => t.Puts[0].Value).Should().BeEquivalentTo(
        [
            """{"op":"move","to":"shard2","requested_unix":""" + Now.ToUnixTimeSeconds() + ""","requested_by":"ops"}""",
            """{"op":"move","to":"shard2","requested_unix":""" + (Now.ToUnixTimeSeconds() + 1) + ""","requested_by":"ops"}""",
            """{"op":"move","to":"shard2","requested_unix":""" + (Now.ToUnixTimeSeconds() + 2) + ""","requested_by":"ops"}""",
        ]);
        gateway.Txns.Should().OnlyContain(t => t.Compares.Count == 1
            && t.Compares[0].Key.StartsWith("/pgworker/moves/shop/bucket_") && t.Compares[0].Version == 0);
    }

    [Fact]
    public async Task Handle_OrderedByAscendingIdRegardlessOfRequestBody()
    {
        // Arrange: массив в обратном порядке — обработка всё равно по id (Д3)
        var (handler, gateway) = NewHandler();
        Seed(gateway);
        // Act
        await handler.Handle(Command(buckets: [4, 2, 0]), CancellationToken.None);
        // Assert
        gateway.Txns.Select(t => ParseBucket(t.Puts[0].Key)).Should().BeInAscendingOrder();
    }

    [Fact]
    public async Task Handle_ClaimLost_Returns409()
    {
        // Arrange: txn-compare не сошёлся — конкурентная заявка (Д4)
        var (handler, gateway) = NewHandler();
        Seed(gateway);
        gateway.SucceedTxn = false;
        // Act
        var result = await handler.Handle(Command(), CancellationToken.None);
        // Assert
        result.Error.Should().BeOfType<MoveClaimLostException>();
    }

    [Fact]
    public async Task Handle_TxnEtcdFailsMiddle_NoCompensation()
    {
        // Arrange: etcd-сбой на 2-й заявке → 503, поставленные НЕ откатываем (Д5)
        var (handler, gateway) = NewHandler();
        Seed(gateway);
        gateway.FailTxnWhen = key => key.EndsWith("bucket_2", StringComparison.Ordinal);
        // Act
        var result = await handler.Handle(Command(), CancellationToken.None);
        // Assert: ошибка наверх; первая заявка (bucket_0) осталась поставленной
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<InvalidOperationException>();
        gateway.Txns.Should().HaveCount(2);
    }

    [Fact]
    public async Task Handle_ConfigReadFails_ReturnsEtcdError()
    {
        // Arrange: чтение config по prefix-ключу не удалось → 503-путь
        var (handler, gateway) = NewHandler();
        gateway.FailRangeByPrefix = p => p == "/clusters/shop/config";
        // Act
        var result = await handler.Handle(Command(), CancellationToken.None);
        // Assert
        result.Error.Should().BeOfType<InvalidOperationException>();
    }

    // requested_unix из JSON-тела заявки.
    private static long ParseUnix(string body)
    {
        using var doc = JsonDocument.Parse(body);
        return doc.RootElement.GetProperty("requested_unix").GetInt64();
    }

    private static int ParseBucket(string key) => int.Parse(key.Split('/')[^1]["bucket_".Length..]);
}
