using PgWorker.Core.Model;
using PgWorker.Provisioning.Processes;
using Xunit;

namespace PgWorker.UnitTests.Provisioning;

// Ensure per-cluster app-секрета (spec §3.1/§4.1): put-if-absent, идемпотентность,
// частичные состояния.
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
}
