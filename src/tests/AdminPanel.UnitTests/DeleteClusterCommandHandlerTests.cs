using AdminPanel.Api.Operations;
using AdminPanel.Core;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Client;
using AdminPanel.Infrastructure;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Хендлер удаления: чтение config → идемпотентность / перезапись state=TO_REMOVE
// (arch/02 §9.4). FakeGateway подменяет только Range/Put — путь команды.
public class DeleteClusterCommandHandlerTests
{
    private const string Endpoint = "http://etcd:2379";
    private const string ConfigKey = "/clusters/shop/config";

    private sealed class FakeGateway : IEtcdGateway
    {
        public IReadOnlyList<Kv> RangeKvs = [];
        public readonly List<(string Key, string Value)> Puts = [];
        public int Reads;
        public bool FailPut;

        public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
        {
            Reads++;
            return Task.FromResult(Result<IReadOnlyList<Kv>>.Success(RangeKvs));
        }

        public Task<Result<EtcdStatusPayload>> StatusAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<EtcdStatusPayload>.Success(new(null, null, null, null, null)));

        public Task<Result<IReadOnlyList<EtcdMember>>> MemberListAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<EtcdMember>>.Success([]));

        public Task<Result<IReadOnlyList<EtcdAlarm>>> AlarmAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<EtcdAlarm>>.Success([]));

        public Task<Result<TxnResult>> TxnAsync(
            string endpoint, IReadOnlyList<TxnCompare> compares, IReadOnlyList<KvPut> puts, CancellationToken ct)
            => Task.FromResult(Result<TxnResult>.Success(new(true)));

        public Task<Result> PutAsync(string endpoint, string key, string value, CancellationToken ct)
        {
            Puts.Add((key, value));
            return Task.FromResult(FailPut
                ? Result.Failed(new InvalidOperationException("put failed"))
                : Result.Success());
        }

        public Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
            => Task.FromResult(Result.Success());
    }

    private static (DeleteClusterCommandHandler Handler, FakeGateway Gateway, SnapshotStore Store) NewHandler()
    {
        var gateway = new FakeGateway();
        var store = new SnapshotStore();
        // Healthy-базис + активный endpoint — как в тестах создания (ActiveEndpoint решает, куда писать).
        store.Replace(TestSnapshots.Healthy(DateTimeOffset.UnixEpoch) with
        {
            Etcd = new EtcdStatus(
                true, [new EtcdEndpoint(Endpoint, true, 1, "3.5.21", null, null, null, null, [])],
                [], [], Endpoint, false, DateTimeOffset.UnixEpoch, 0),
        });
        return (new DeleteClusterCommandHandler(store, gateway), gateway, store);
    }

    private static void SetConfig(FakeGateway gateway, string value)
        => gateway.RangeKvs = [new Kv(ConfigKey, value, 1)];

    [Fact]
    public async Task Handle_ExistingCluster_RewritesConfigWithToRemoveState()
    {
        // Arrange: созданный панелью кластер — state NOT_INITIALIZED (§9.1)
        var (handler, gateway, _) = NewHandler();
        SetConfig(gateway, """{"buckets":4,"dbname":"shop","created_unix":1755900000,"state":"NOT_INITIALIZED"}""");

        // Act
        var result = await handler.Handle(new DeleteClusterCommand("shop"), CancellationToken.None);

        // Assert: один PUT ровно в config — канонический набор полей с state=TO_REMOVE,
        // buckets/dbname/created_unix сохранены (§9.4)
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be(new ClusterDeletedDto("shop", "TO_REMOVE"));
        gateway.Puts.Should().ContainSingle().Which.Should().Be(
            (ConfigKey, """{"buckets":4,"dbname":"shop","created_unix":1755900000,"state":"TO_REMOVE"}"""));
    }

    [Fact]
    public async Task Handle_AlreadyToRemove_IdempotentSuccessWithoutWrite()
    {
        // Arrange: повторный DELETE кластера в TO_REMOVE (§9.4)
        var (handler, gateway, _) = NewHandler();
        SetConfig(gateway, """{"buckets":4,"dbname":"shop","created_unix":1755900000,"state":"TO_REMOVE"}""");

        // Act
        var result = await handler.Handle(new DeleteClusterCommand("shop"), CancellationToken.None);

        // Assert: успех без записи — перестановка одного и того же значения не нужна
        result.IsSuccess.Should().BeTrue();
        result.Value.State.Should().Be("TO_REMOVE");
        gateway.Puts.Should().BeEmpty();
    }

    [Fact]
    public async Task Handle_OldConfigWithoutCreatedUnix_RewrittenWithoutIt()
    {
        // Arrange: config старого init-кластера без created_unix (§2.1) и без state
        var (handler, gateway, _) = NewHandler();
        SetConfig(gateway, """{"buckets":16,"dbname":"shop"}""");

        // Act
        var result = await handler.Handle(new DeleteClusterCommand("shop"), CancellationToken.None);

        // Assert: created_unix не добавляется, state=TO_REMOVE появился
        result.IsSuccess.Should().BeTrue();
        gateway.Puts.Should().ContainSingle().Which.Value.Should().Be(
            """{"buckets":16,"dbname":"shop","state":"TO_REMOVE"}""");
    }

    [Fact]
    public async Task Handle_MissingConfig_ReturnsNotFound()
    {
        // Arrange: config-ключа нет — кластер не существует (§9.4 п.3)
        var (handler, gateway, _) = NewHandler();

        // Act
        var result = await handler.Handle(new DeleteClusterCommand("ghost"), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<ClusterNotFoundException>();
        gateway.Puts.Should().BeEmpty();
    }

    [Fact]
    public async Task Handle_InvalidName_ReturnsNotFoundWithoutEtcd()
    {
        // Arrange: имя не проходит паттерн §9.3 — панель такое создать не могла
        var (handler, gateway, _) = NewHandler();

        // Act
        var result = await handler.Handle(new DeleteClusterCommand("Bad-Name"), CancellationToken.None);

        // Assert: 404 без похода в etcd
        result.Error.Should().BeOfType<ClusterNotFoundException>();
        gateway.Reads.Should().Be(0);
        gateway.Puts.Should().BeEmpty();
    }

    [Fact]
    public async Task Handle_NoSnapshot_ReturnsWriteUnavailable()
    {
        // Arrange: снапшота нет — активный endpoint неизвестен (как при создании, §9.2)
        var gateway = new FakeGateway();
        var handler = new DeleteClusterCommandHandler(new SnapshotStore(), gateway);

        // Act
        var result = await handler.Handle(new DeleteClusterCommand("shop"), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<EtcdWriteUnavailableException>();
        gateway.Puts.Should().BeEmpty();
    }

    [Fact]
    public async Task Handle_BrokenConfig_ReturnsInvalidConfig()
    {
        // Arrange: config не парсится — 503 «битый config» (03 §1.2)
        var (handler, gateway, _) = NewHandler();
        SetConfig(gateway, "{oops");

        // Act
        var result = await handler.Handle(new DeleteClusterCommand("shop"), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<InvalidClusterConfigException>();
        gateway.Puts.Should().BeEmpty();
    }

    [Fact]
    public async Task Handle_ConfigWithoutRequiredFields_ReturnsInvalidConfig()
    {
        // Arrange: config без buckets/dbname — перезапись невозможна (§9.4 п.5)
        var (handler, gateway, _) = NewHandler();
        SetConfig(gateway, """{"state":"NOT_INITIALIZED"}""");

        // Act
        var result = await handler.Handle(new DeleteClusterCommand("shop"), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<InvalidClusterConfigException>();
        gateway.Puts.Should().BeEmpty();
    }

    [Fact]
    public async Task Handle_PutFails_PropagatesError()
    {
        // Arrange: etcd отказал в записи
        var (handler, gateway, _) = NewHandler();
        SetConfig(gateway, """{"buckets":4,"dbname":"shop","created_unix":1755900000,"state":"NOT_INITIALIZED"}""");
        gateway.FailPut = true;

        // Act
        var result = await handler.Handle(new DeleteClusterCommand("shop"), CancellationToken.None);

        // Assert: исходная ошибка наверх (без ретраев — повтор = новый DELETE)
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<InvalidOperationException>();
    }
}
