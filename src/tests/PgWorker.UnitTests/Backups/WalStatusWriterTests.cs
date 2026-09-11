using FluentAssertions;
using PgWorker.Backups;
using PgWorker.Etcd.Client;
using PgWorker.Core;
using PgWorker.Etcd.Parsing;
using PgWorker.UnitTests.Api;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Статус-райтер WAL-потока: JSON 1:1 с каркасным форматом t01 (парсер читает без
// parseErrors), идемпотентность — безделье не пишет; failover по endpoints.
public class WalStatusWriterTests
{
    private static readonly WalStatusWriter Writer = new(
        new FakeEtcdGateway(), ["http://test"]);

    private static WalStreamState Sample(WalStreamStatus state = WalStreamStatus.Active) => new(
        state, "pgw_bkp_c1_shard1", "shard1a",
        "000000010000000000000001", "000000010000000000000005",
        "000000010000000000000005", 1757500000, 3, null);

    [Fact]
    public void ToJson_формат_канона_snake_case()
    {
        // Arrange / Act
        var json = WalStatusWriter.ToJson(Sample());

        // Assert — поля ключа arch/19 §4, значения каноническими строками
        json.Should().Contain("\"state\":\"ACTIVE\"")
            .And.Contain("\"slot\":\"pgw_bkp_c1_shard1\"")
            .And.Contain("\"master_node\":\"shard1a\"")
            .And.Contain("\"chain_start_segment\":\"000000010000000000000001\"")
            .And.Contain("\"last_received_segment\":\"000000010000000000000005\"")
            .And.Contain("\"last_uploaded_segment\":\"000000010000000000000005\"")
            .And.Contain("\"last_uploaded_unix\":1757500000")
            .And.Contain("\"lag_segments\":3");
        json.Should().NotContain("\"Error\""); // camelCase нет
    }

    [Fact]
    public void ToJson_error_null_опускается_degraded_несет_error()
    {
        // Arrange
        var degraded = Sample(WalStreamStatus.Degraded) with { Error = "дыра WAL-цепочки: ожидался X" };

        // Act
        var active = WalStatusWriter.ToJson(Sample());
        var degradedJson = WalStatusWriter.ToJson(degraded);

        // Assert
        active.Should().NotContain("\"error\"");
        degradedJson.Should().Contain("\"error\":\"дыра WAL-цепочки: ожидался X\"");
    }

    [Fact]
    public async Task WriteIfChanged_новый_ключ_пишется_и_читается_парсером_t01()
    {
        // Arrange
        var gateway = new FakeEtcdGateway();
        var writer = new WalStatusWriter(gateway, ["http://test"]);

        // Act
        var written = await writer.WriteIfChangedAsync("c1", "shard1", Sample(), ct: TestContext.Current.CancellationToken);
        var read = await writer.ReadAsync("c1", "shard1", ct: TestContext.Current.CancellationToken);

        // Assert
        written.IsSuccess.Should().BeTrue();
        read.Value.Should().BeEquivalentTo(Sample());
    }

    [Fact]
    public async Task WriteIfChanged_повтор_без_изменений_не_пишет()
    {
        // Arrange
        var gateway = new FakeEtcdGateway();
        var writer = new WalStatusWriter(gateway, ["http://test"]);
        await writer.WriteIfChangedAsync("c1", "shard1", Sample(), ct: TestContext.Current.CancellationToken);

        // Act — то же значение
        var second = await writer.WriteIfChangedAsync("c1", "shard1", Sample(), ct: TestContext.Current.CancellationToken);

        // Assert — идемпотентность: успех без второй Put-мутации
        second.IsSuccess.Should().BeTrue();
        gateway.Store["/pgworker/backups/c1/shard1/wal"].Should().Be(WalStatusWriter.ToJson(Sample()));
    }

    [Fact]
    public async Task WriteIfChanged_изменение_state_перезаписывает()
    {
        // Arrange
        var gateway = new FakeEtcdGateway();
        var writer = new WalStatusWriter(gateway, ["http://test"]);
        await writer.WriteIfChangedAsync("c1", "shard1", Sample(), ct: TestContext.Current.CancellationToken);

        // Act
        var stopped = Sample(WalStreamStatus.Stopped);
        await writer.WriteIfChangedAsync("c1", "shard1", stopped, ct: TestContext.Current.CancellationToken);

        // Assert
        (await writer.ReadAsync("c1", "shard1", ct: TestContext.Current.CancellationToken))
            .Value!.State.Should().Be(WalStreamStatus.Stopped);
    }

    [Fact]
    public async Task ReadAsync_нет_ключа_null()
    {
        // Arrange / Act
        var read = await Writer.ReadAsync("nope", "shardX", ct: TestContext.Current.CancellationToken);

        // Assert — нет ключа = агент не поднимался, НЕ ошибка
        read.IsSuccess.Should().BeTrue();
        read.Value.Should().BeNull();
    }

    [Fact]
    public async Task WriteIfChanged_отказ_первого_endpoint_failover_на_второй()
    {
        // Arrange — «плохой» endpoint отвечает отказом, «хороший» — живой
        // (ревью Ф4-2 №4: отказ endpoints[0] не должен ронять запись при живых остальных)
        var good = new FakeEtcdGateway();
        var writer = new WalStatusWriter(
            new FirstEndpointFailsGateway(good, "http://bad"),
            ["http://bad", "http://good"]);

        // Act
        var written = await writer.WriteIfChangedAsync("c1", "shard1", Sample(), ct: TestContext.Current.CancellationToken);
        var read = await new WalStatusWriter(good, ["http://good"])
            .ReadAsync("c1", "shard1", ct: TestContext.Current.CancellationToken);

        // Assert — запись прошла через живой endpoint
        written.IsSuccess.Should().BeTrue();
        read.Value.Should().BeEquivalentTo(Sample());
    }

    // Фейк с отказом одного endpoint: все операции на badEndpoint → Failed,
    // остальные — делегируются inner (паттерн failover-тестов).
    private sealed class FirstEndpointFailsGateway(IEtcdGateway inner, string badEndpoint) : IEtcdGateway
    {
        private Result<T> Fail<T>(string endpoint) =>
            Result<T>.Failed(new ApplicationException($"endpoint {endpoint} недоступен"));

        public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
            => endpoint == badEndpoint
                ? Task.FromResult(Fail<IReadOnlyList<Kv>>(endpoint))
                : inner.RangeAsync(endpoint, prefix, ct);

        public Task<Result<Kv?>> GetAsync(string endpoint, string key, CancellationToken ct)
            => endpoint == badEndpoint
                ? Task.FromResult(Fail<Kv?>(endpoint))
                : inner.GetAsync(endpoint, key, ct);

        public Task<Result> PutAsync(string endpoint, string key, string value, long? lease, CancellationToken ct)
            => endpoint == badEndpoint
                ? Task.FromResult(Result.Failed(new ApplicationException($"endpoint {endpoint} недоступен")))
                : inner.PutAsync(endpoint, key, value, lease, ct);

        public Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
            => endpoint == badEndpoint
                ? Task.FromResult(Result.Failed(new ApplicationException($"endpoint {endpoint} недоступен")))
                : inner.DeleteAsync(endpoint, keyOrPrefix, prefix, ct);

        public Task<Result<TxnResult>> TxnAsync(string endpoint, TxnRequest req, CancellationToken ct)
            => endpoint == badEndpoint
                ? Task.FromResult(Fail<TxnResult>(endpoint))
                : inner.TxnAsync(endpoint, req, ct);

        public Task<Result<long>> LeaseGrantAsync(string endpoint, int ttlSec, CancellationToken ct)
            => endpoint == badEndpoint
                ? Task.FromResult(Fail<long>(endpoint))
                : inner.LeaseGrantAsync(endpoint, ttlSec, ct);

        public Task<Result> LeaseRevokeAsync(string endpoint, long lease, CancellationToken ct)
            => endpoint == badEndpoint
                ? Task.FromResult(Result.Failed(new ApplicationException($"endpoint {endpoint} недоступен")))
                : inner.LeaseRevokeAsync(endpoint, lease, ct);

        public Task<Result> LeaseKeepaliveAsync(string endpoint, long lease, CancellationToken ct)
            => endpoint == badEndpoint
                ? Task.FromResult(Result.Failed(new ApplicationException($"endpoint {endpoint} недоступен")))
                : inner.LeaseKeepaliveAsync(endpoint, lease, ct);

        public Task<Result<byte[]>> SnapshotSaveAsync(string endpoint, CancellationToken ct)
            => endpoint == badEndpoint
                ? Task.FromResult(Fail<byte[]>(endpoint))
                : inner.SnapshotSaveAsync(endpoint, ct);

        public Task<Result<long>> StatusAsync(string endpoint, CancellationToken ct)
            => endpoint == badEndpoint
                ? Task.FromResult(Fail<long>(endpoint))
                : inner.StatusAsync(endpoint, ct);

        public Task<Result> CompactAsync(string endpoint, long revision, CancellationToken ct)
            => endpoint == badEndpoint
                ? Task.FromResult(Result.Failed(new ApplicationException($"endpoint {endpoint} недоступен")))
                : inner.CompactAsync(endpoint, revision, ct);

        public Task<Result> DefragmentAsync(string endpoint, CancellationToken ct)
            => endpoint == badEndpoint
                ? Task.FromResult(Result.Failed(new ApplicationException($"endpoint {endpoint} недоступен")))
                : inner.DefragmentAsync(endpoint, ct);
    }
}
