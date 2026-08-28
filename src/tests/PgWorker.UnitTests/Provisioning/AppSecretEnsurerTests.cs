using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Etcd.Client;
using PgWorker.Provisioning.Processes;
using Xunit;

namespace PgWorker.UnitTests.Provisioning;

// Ensure per-cluster app-секрета (spec §3.1/§4.1): put-if-absent, идемпотентность,
// частичные состояния, failover txn по endpoints.
public class AppSecretEnsurerTests
{
    private const string Ep = "http://etcd:2379";

    private static AppSecretEnsurer Sut(Fakes.FakeEtcd etcd) => new(etcd, [Ep]);

    [Fact]
    public async Task Ensure_NoKeys_GeneratesBoth()
    {
        // Arrange — пустой etcd
        var etcd = new Fakes.FakeEtcd();

        // Act
        var result = await Sut(etcd).EnsureAsync("shop", CancellationToken.None);

        // Assert — оба ключа созданы, креды возвращены
        result.IsSuccess.Should().BeTrue();
        result.Value.User.Should().Be("app");
        result.Value.Password.Should().MatchRegex("^[A-Za-z0-9]{32}$");
        etcd.Store["/clusters/shop/app_user"].Value.Should().Be("app");
        etcd.Store["/clusters/shop/app_password"].Value.Should().Be(result.Value.Password);
    }

    [Fact]
    public async Task Ensure_ExistingKeys_ReturnsAndDoesNotRegenerate()
    {
        // Arrange — ключи уже есть (повторный тик/re-run)
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/clusters/shop/app_user", "app");
        etcd.Seed("/clusters/shop/app_password", "OldPassword0000000000000000000A");

        // Act
        var result = await Sut(etcd).EnsureAsync("shop", CancellationToken.None);

        // Assert — значение не перегенерировано (идемпотентность, spec §2.5)
        result.Value.Password.Should().Be("OldPassword0000000000000000000A");
        etcd.Store["/clusters/shop/app_password"].Value.Should().Be("OldPassword0000000000000000000A");
        etcd.Txns.Should().BeEmpty("существующие ключи не переписываются txn");
    }

    [Fact]
    public async Task Ensure_PartialKeys_PutsOnlyMissing()
    {
        // Arrange — только app_user (битое состояние)
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/clusters/shop/app_user", "app");

        // Act
        var result = await Sut(etcd).EnsureAsync("shop", CancellationToken.None);

        // Assert — дописан только пароль; user не тронут
        result.IsSuccess.Should().BeTrue();
        etcd.Store["/clusters/shop/app_user"].Value.Should().Be("app");
        etcd.Store["/clusters/shop/app_password"].Value.Should().Be(result.Value.Password);
    }

    [Fact]
    public async Task Ensure_TxnFailsOnFirstEndpoint_FailoverToNext()
    {
        // Arrange — txn падает на первом endpoint (транспортный сбой), второй жив;
        // чтение (GetAsync) живо на обоих
        var etcd = new Fakes.FakeEtcd();
        var flaky = new FailFirstEndpointTxn(etcd);
        var sut = new AppSecretEnsurer(flaky, ["http://e1:2379", "http://e2:2379"]);

        // Act
        var result = await sut.EnsureAsync("shop", CancellationToken.None);

        // Assert — txn повторён на втором endpoint, ключи созданы
        // (failover-паттерн ReadAsync: ошибочный endpoint → следующий)
        result.IsSuccess.Should().BeTrue();
        etcd.Store["/clusters/shop/app_user"].Value.Should().Be("app");
        etcd.Store["/clusters/shop/app_password"].Value.Should().Be(result.Value.Password);
    }

    // Декоратор шлюза: TxnAsync возвращает Failed на первом endpoint,
    // остальное делегирует внутреннему FakeEtcd.
    private sealed class FailFirstEndpointTxn(Fakes.FakeEtcd inner) : IEtcdGateway
    {
        public Task<Result<TxnResult>> TxnAsync(string endpoint, TxnRequest req, CancellationToken ct)
            => endpoint == "http://e1:2379"
                ? Task.FromResult(Result<TxnResult>.Failed(new ApplicationException("endpoint down")))
                : inner.TxnAsync(endpoint, req, ct);

        public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
            => inner.RangeAsync(endpoint, prefix, ct);

        public Task<Result<Kv?>> GetAsync(string endpoint, string key, CancellationToken ct)
            => inner.GetAsync(endpoint, key, ct);

        public Task<Result> PutAsync(string endpoint, string key, string value, long? lease, CancellationToken ct)
            => inner.PutAsync(endpoint, key, value, lease, ct);

        public Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
            => inner.DeleteAsync(endpoint, keyOrPrefix, prefix, ct);

        public Task<Result<long>> LeaseGrantAsync(string endpoint, int ttlSec, CancellationToken ct)
            => inner.LeaseGrantAsync(endpoint, ttlSec, ct);

        public Task<Result> LeaseRevokeAsync(string endpoint, long lease, CancellationToken ct)
            => inner.LeaseRevokeAsync(endpoint, lease, ct);

        public Task<Result> LeaseKeepaliveAsync(string endpoint, long lease, CancellationToken ct)
            => inner.LeaseKeepaliveAsync(endpoint, lease, ct);

        public Task<Result<byte[]>> SnapshotSaveAsync(string endpoint, CancellationToken ct)
            => inner.SnapshotSaveAsync(endpoint, ct);

        public Task<Result<long>> StatusAsync(string endpoint, CancellationToken ct)
            => inner.StatusAsync(endpoint, ct);

        public Task<Result> CompactAsync(string endpoint, long revision, CancellationToken ct)
            => inner.CompactAsync(endpoint, revision, ct);

        public Task<Result> DefragmentAsync(string endpoint, CancellationToken ct)
            => inner.DefragmentAsync(endpoint, ct);
    }
}
