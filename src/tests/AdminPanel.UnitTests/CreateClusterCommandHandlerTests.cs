using AdminPanel.Api.Operations;
using AdminPanel.Core;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Client;
using AdminPanel.Etcd.Writing;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Хендлер создания: клэйм-txn → пакет PUT → компенсация при сбое (arch/02 §9.2).
public class CreateClusterCommandHandlerTests
{
    private const string Endpoint = "http://etcd:2379";

    private sealed class FakeGateway : IEtcdGateway
    {
        public bool TxnSucceeded = true;
        public int FailPutAtIndex = -1;             // -1 = пакет проходит целиком
        public readonly List<string> Puts = [];
        public readonly List<string> DeletedPrefixes = [];
        public readonly List<string> DeletedKeys = [];

        public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<Kv>>.Success([]));

        public Task<Result<EtcdStatusPayload>> StatusAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<EtcdStatusPayload>.Success(new(null, null, null, null, null)));

        public Task<Result<IReadOnlyList<EtcdMember>>> MemberListAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<EtcdMember>>.Success([]));

        public Task<Result<IReadOnlyList<EtcdAlarm>>> AlarmAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<EtcdAlarm>>.Success([]));

        public Task<Result<TxnResult>> TxnAsync(
            string endpoint, IReadOnlyList<TxnCompare> compares, IReadOnlyList<KvPut> puts, CancellationToken ct)
            => Task.FromResult(TxnSucceeded
                ? Result<TxnResult>.Success(new(true))
                : Result<TxnResult>.Success(new(false)));

        public Task<Result> PutAsync(string endpoint, string key, string value, CancellationToken ct)
        {
            Puts.Add(key);
            return Task.FromResult(Puts.Count - 1 == FailPutAtIndex
                ? Result.Failed(new InvalidOperationException("put failed"))
                : Result.Success());
        }

        public Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
        {
            if (prefix)
                DeletedPrefixes.Add(keyOrPrefix);
            else
                DeletedKeys.Add(keyOrPrefix);
            return Task.FromResult(Result.Success());
        }
    }

    private static (CreateClusterCommandHandler Handler, FakeGateway Gateway, SnapshotStore Store) NewHandler()
    {
        var gateway = new FakeGateway();
        var store = new SnapshotStore();
        // Healthy-базис + один живой endpoint: ActiveEndpoint решает, куда пишет хендлер.
        store.Replace(TestSnapshots.Healthy(DateTimeOffset.UnixEpoch) with
        {
            Etcd = new EtcdStatus(
                true, [new EtcdEndpoint(Endpoint, true, 1, "3.5.21", null, null, null, null, [])],
                [], [], Endpoint, false, DateTimeOffset.UnixEpoch, 0),
        });
        return (new CreateClusterCommandHandler(store, gateway, TimeProvider.System), gateway, store);
    }

    private static CreateClusterCommand Command() => new(new("shop", 4, 2, 2, 0.5m, 8, 100));

    [Fact]
    public async Task Handle_ValidRequest_ClaimsThenPutsAndReturnsDto()
    {
        // Arrange
        var (handler, gateway, _) = NewHandler();

        // Act
        var result = await handler.Handle(Command(), CancellationToken.None);

        // Assert: DTO с каноническими строками и state NOT_INITIALIZED; пакет = план минус config
        result.IsSuccess.Should().BeTrue();
        result.Value.Name.Should().Be("shop");
        result.Value.State.Should().Be("NOT_INITIALIZED");
        result.Value.Sharded.Should().BeTrue(); // legacy-запрос без sharded = true
        result.Value.RequestCpu.Should().Be("0.5");
        result.Value.RequestMem.Should().Be("8Gi");
        result.Value.RequestDisk.Should().Be("100Gi");
        gateway.Puts.Should().HaveCountGreaterThan(0);
        gateway.Puts.Should().NotContain("/clusters/shop/config"); // конфиг — в txn-клэйме
        gateway.DeletedPrefixes.Should().BeEmpty();
    }

    [Fact]
    public async Task Handle_ClaimFailed_ReturnsAlreadyExists()
    {
        // Arrange
        var (handler, gateway, _) = NewHandler();
        gateway.TxnSucceeded = false;

        // Act
        var result = await handler.Handle(Command(), CancellationToken.None);

        // Assert: ничего не писано после несошедшегося compare
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<ClusterAlreadyExistsException>();
        gateway.Puts.Should().BeEmpty();
    }

    [Fact]
    public async Task Handle_InvalidRequest_ReturnsValidationErrors()
    {
        // Arrange
        var (handler, _, _) = NewHandler();

        // Act: шардов больше бакетов
        var result = await handler.Handle(new CreateClusterCommand(new("shop", 1, 2, 2, 1m, 1, 1)), CancellationToken.None);

        // Assert: до etcd дело не дошло
        result.Error.Should().BeOfType<CreateClusterValidationException>()
            .Which.Errors.Should().NotBeEmpty();
    }

    [Fact]
    public async Task Handle_NoSnapshot_ReturnsWriteUnavailable()
    {
        // Arrange: снапшота нет — активный endpoint неизвестен (spec t12 §8.12)
        var gateway = new FakeGateway();
        var handler = new CreateClusterCommandHandler(new SnapshotStore(), gateway, TimeProvider.System);

        // Act
        var result = await handler.Handle(Command(), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<EtcdWriteUnavailableException>();
        gateway.Puts.Should().BeEmpty();
    }

    [Fact]
    public async Task Handle_PutFailsMidway_CompensatesClusterPrefixAndRequestKeys()
    {
        // Arrange
        var (handler, gateway, _) = NewHandler();
        gateway.FailPutAtIndex = 2;

        // Act
        var result = await handler.Handle(Command(), CancellationToken.None);

        // Assert: отказ исходной ошибки + компенсация — префикс кластера и ТОЧЕЧНЫЕ request_*
        result.IsSuccess.Should().BeFalse();
        gateway.DeletedPrefixes.Should().ContainSingle().Which.Should().Be("/clusters/shop/");
        gateway.DeletedKeys.Should().BeEquivalentTo(
        [
            "/service/shop-shard1/request_cpu", "/service/shop-shard1/request_mem", "/service/shop-shard1/request_disk",
            "/service/shop-shard2/request_cpu", "/service/shop-shard2/request_mem", "/service/shop-shard2/request_disk",
        ]);
    }

    [Fact]
    public async Task Handle_SingleCluster_ReturnsDegenerateDto()
    {
        // Arrange: нешардированная — buckets/shards не переданы (0) и не важны
        var (handler, gateway, _) = NewHandler();

        // Act
        var result = await handler.Handle(new CreateClusterCommand(
            new("solo", 0, 0, 2, 0.5m, 8, 100, Sharded: false)), CancellationToken.None);

        // Assert: DTO вырожденный — sharded=false, 1/1; ключи только solo-shard1
        result.IsSuccess.Should().BeTrue();
        result.Value.Sharded.Should().BeFalse();
        result.Value.BucketsCount.Should().Be(1);
        result.Value.ShardsTotal.Should().Be(1);
        gateway.Puts.Where(k => k.Contains("shard2")).Should().BeEmpty();
    }

    [Fact]
    public async Task Handle_ShardedFalseWithGarbage_NormalizesAndSucceeds()
    {
        // Arrange: sharded=false + мусорные buckets/shards — игнорируются
        var (handler, _, _) = NewHandler();

        // Act
        var result = await handler.Handle(new CreateClusterCommand(
            new("solo2", 99999, -3, 2, 1m, 8, 100, Sharded: false)), CancellationToken.None);

        // Assert: не 400-валидация, а успешная вырожденная запись
        result.IsSuccess.Should().BeTrue();
        result.Value.BucketsCount.Should().Be(1);
        result.Value.ShardsTotal.Should().Be(1);
    }
}
