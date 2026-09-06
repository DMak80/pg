using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Etcd.Client;
using PgWorker.Provisioning.Processes;
using Xunit;

namespace PgWorker.UnitTests.Provisioning;

// Ensure per-cluster тройки кредов app/mover/bucket_admin (t02 §3.1, arch/14 §4):
// put-if-absent пяти ключей, идемпотентность, частичные состояния, failover txn.
public class ClusterSecretEnsurerTests
{
    private const string Ep = "http://etcd:2379";

    private static readonly ClusterConfig Config =
        new("shop", 4, "shop", null, ClusterState.Active, null, null);

    private static ClusterSecretEnsurer Sut(Fakes.FakeEtcd etcd) => new(etcd, [Ep]);

    [Fact]
    public async Task Ensure_NoKeys_GeneratesAllFive()
    {
        // Arrange — пустой etcd
        var etcd = new Fakes.FakeEtcd();

        // Act
        var result = await Sut(etcd).EnsureAsync("shop", Config, CancellationToken.None);

        // Assert — пять ключей созданы, тройка возвращена
        result.IsSuccess.Should().BeTrue();
        result.Value.App.User.Should().Be("app");
        result.Value.App.Password.Should().MatchRegex("^[A-Za-z0-9]{32}$");
        result.Value.MoverPassword.Should().MatchRegex("^[A-Za-z0-9]{32}$");
        result.Value.BucketAdmin.User.Should().Be("bucket_admin");
        result.Value.BucketAdmin.Password.Should().MatchRegex("^[A-Za-z0-9]{32}$");
        etcd.Store["/clusters/shop/app_user"].Value.Should().Be("app");
        etcd.Store["/clusters/shop/app_password"].Value.Should().Be(result.Value.App.Password);
        etcd.Store["/clusters/shop/mover_password"].Value.Should().Be(result.Value.MoverPassword);
        etcd.Store["/clusters/shop/bucket_admin_user"].Value.Should().Be("bucket_admin");
        etcd.Store["/clusters/shop/bucket_admin_password"].Value.Should().Be(result.Value.BucketAdmin.Password);
    }

    [Fact]
    public async Task Ensure_ConfigHasBucketAdmin_UsesConfigValues()
    {
        // Arrange — config с per-cluster bucket_admin (панель задала при создании)
        var etcd = new Fakes.FakeEtcd();
        var config = Config with
        {
            BucketAdminUser = "ba_user",
            BucketAdminPassword = "FromConfigPass0000000000000000000A",
        };

        // Act
        var result = await Sut(etcd).EnsureAsync("shop", config, CancellationToken.None);

        // Assert — bucket_admin-ключи из config (не сгенерированы)
        result.IsSuccess.Should().BeTrue();
        etcd.Store["/clusters/shop/bucket_admin_user"].Value.Should().Be("ba_user");
        etcd.Store["/clusters/shop/bucket_admin_password"].Value.Should().Be("FromConfigPass0000000000000000000A");
        result.Value.BucketAdmin.User.Should().Be("ba_user");
        result.Value.BucketAdmin.Password.Should().Be("FromConfigPass0000000000000000000A");
    }

    [Fact]
    public async Task Ensure_ExistingKeys_ReturnsAndDoesNotRegenerate()
    {
        // Arrange — все пять ключей уже есть (повторный тик/re-run)
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/clusters/shop/app_user", "app");
        etcd.Seed("/clusters/shop/app_password", "OldPassword0000000000000000000A");
        etcd.Seed("/clusters/shop/mover_password", "OldMoverPass0000000000000000000A");
        etcd.Seed("/clusters/shop/bucket_admin_user", "ba_user");
        etcd.Seed("/clusters/shop/bucket_admin_password", "OldBaPass000000000000000000000A");

        // Act
        var result = await Sut(etcd).EnsureAsync("shop", Config, CancellationToken.None);

        // Assert — значения не перегенерированы (идемпотентность, spec §2.5)
        result.IsSuccess.Should().BeTrue();
        result.Value.App.Password.Should().Be("OldPassword0000000000000000000A");
        result.Value.MoverPassword.Should().Be("OldMoverPass0000000000000000000A");
        result.Value.BucketAdmin.User.Should().Be("ba_user");
        result.Value.BucketAdmin.Password.Should().Be("OldBaPass000000000000000000000A");
        etcd.Txns.Should().BeEmpty("существующие ключи не переписываются txn");
    }

    [Fact]
    public async Task Ensure_PartialKeys_PutsOnlyMissing()
    {
        // Arrange — app_user и mover_password есть; недостающие добираются
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/clusters/shop/app_user", "app");
        etcd.Seed("/clusters/shop/mover_password", "OldMoverPass0000000000000000000A");

        // Act
        var result = await Sut(etcd).EnsureAsync("shop", Config, CancellationToken.None);

        // Assert — существующие не тронуты, недостающие добраны
        result.IsSuccess.Should().BeTrue();
        etcd.Store["/clusters/shop/app_user"].Value.Should().Be("app");
        etcd.Store["/clusters/shop/mover_password"].Value.Should().Be("OldMoverPass0000000000000000000A");
        result.Value.MoverPassword.Should().Be("OldMoverPass0000000000000000000A");
        etcd.Store["/clusters/shop/app_password"].Value.Should().Be(result.Value.App.Password);
        etcd.Store["/clusters/shop/bucket_admin_password"].Value.Should().Be(result.Value.BucketAdmin.Password);
    }

    [Fact]
    public async Task Ensure_TxnFailsOnFirstEndpoint_FailoverToNext()
    {
        // Arrange — txn падает на первом endpoint (транспортный сбой), второй жив;
        // чтение (GetAsync) живо на обоих
        var etcd = new Fakes.FakeEtcd();
        var flaky = new FailFirstEndpointTxn(etcd);
        var sut = new ClusterSecretEnsurer(flaky, ["http://e1:2379", "http://e2:2379"]);

        // Act
        var result = await sut.EnsureAsync("shop", Config, CancellationToken.None);

        // Assert — txn повторён на втором endpoint, ключи созданы
        // (failover-паттерн ReadAsync: ошибочный endpoint → следующий)
        result.IsSuccess.Should().BeTrue();
        etcd.Store["/clusters/shop/app_user"].Value.Should().Be("app");
        etcd.Store["/clusters/shop/app_password"].Value.Should().Be(result.Value.App.Password);
        etcd.Store["/clusters/shop/mover_password"].Value.Should().Be(result.Value.MoverPassword);
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
