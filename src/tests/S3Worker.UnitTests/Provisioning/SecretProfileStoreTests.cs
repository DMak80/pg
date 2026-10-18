using FluentAssertions;
using S3Worker.Provisioning;
using Xunit;

namespace S3Worker.UnitTests.Provisioning;

// Env-профили кредов нод (arch/23 §6): полный профиль → env контейнера;
// отсутствие/неполнота → диагностики БЕЗ значений; полупары → fail-fast.
public class SecretProfileStoreTests
{
    private const string RootUser = "s3-root";
    private const string RootPassword = "root-secret-value";
    private const string AccessKey = "AKIA-DEMO";
    private const string SecretKeyValue = "sk-demo-value";

    private static SecretProfileStore Store(
        Dictionary<string, string?> values, IEnumerable<string>? names = null)
        => new(name => name is not null && values.TryGetValue(name, out var v) ? v : null, names ?? values.Keys);

    private static Dictionary<string, string?> FullProfile(string profile = "backups-main") => new()
    {
        [$"S3W_NODE_SECRETS__{profile}__ROOT_USER"] = RootUser,
        [$"S3W_NODE_SECRETS__{profile}__ROOT_PASSWORD"] = RootPassword,
        [$"S3W_NODE_SECRETS__{profile}__ACCESS_KEYS__0__ACCESSKEY"] = AccessKey,
        [$"S3W_NODE_SECRETS__{profile}__ACCESS_KEYS__0__SECRETKEY"] = SecretKeyValue,
        [$"S3W_NODE_SECRETS__{profile}__ACCESS_KEYS__0__POLICY"] = "read-write",
        [$"S3W_NODE_SECRETS__{profile}__ACCESS_KEYS__1__ACCESSKEY"] = "ro-key",
        [$"S3W_NODE_SECRETS__{profile}__ACCESS_KEYS__1__SECRETKEY"] = "ro-secret",
        [$"S3W_NODE_SECRETS__{profile}__ACCESS_KEYS__1__POLICY"] = "read-only",
    };

    [Fact]
    public void FullProfile_TryGetAndToContainerEnv()
    {
        // Arrange: профиль backups-main: root-пара + ключи 0..1.
        var store = Store(FullProfile());

        // Act
        var ok = store.TryGet("backups-main", out var secrets, out var diagnostic);

        // Assert: профиль читается; env ноды — точный формат OWNS3_*.
        ok.Should().BeTrue();
        diagnostic.Should().BeNull();
        secrets!.RootUser.Should().Be(RootUser);
        var env = secrets.ToContainerEnv();
        env["OWNS3_ROOT_USER"].Should().Be(RootUser);
        env["OWNS3_ROOT_PASSWORD"].Should().Be(RootPassword);
        env["OWNS3_ACCESS_KEYS__0__ACCESSKEY"].Should().Be(AccessKey);
        env["OWNS3_ACCESS_KEYS__0__SECRETKEY"].Should().Be(SecretKeyValue);
        env["OWNS3_ACCESS_KEYS__0__POLICY"].Should().Be("read-write");
        env["OWNS3_ACCESS_KEYS__1__POLICY"].Should().Be("read-only");
    }

    [Fact]
    public void RootPairOnly_ValidWithoutKeys()
    {
        // Arrange: access keys опциональны.
        var store = Store(new Dictionary<string, string?>
        {
            ["S3W_NODE_SECRETS__p1__ROOT_USER"] = RootUser,
            ["S3W_NODE_SECRETS__p1__ROOT_PASSWORD"] = RootPassword,
        });

        // Act
        var ok = store.TryGet("p1", out var secrets, out _);

        // Assert
        ok.Should().BeTrue();
        secrets!.AccessKeys.Should().BeEmpty();
        secrets.ToContainerEnv().Should().HaveCount(2);
    }

    [Fact]
    public void ProfileMissing_DiagnosticWithNamesList()
    {
        // Arrange: есть другой профиль.
        var store = Store(FullProfile("other"));

        // Act
        var ok = store.TryGet("backups-main", out var secrets, out var diagnostic);

        // Assert: отказ; перечень — ТОЛЬКО имена.
        ok.Should().BeFalse();
        secrets.Should().BeNull();
        diagnostic.Should().Contain("other").And.NotContain(RootPassword);
    }

    [Fact]
    public void HalfRootPair_ValidateEnvPairsReturnsProfile()
    {
        // Arrange: ROOT_USER есть, ROOT_PASSWORD нет.
        var store = Store(new Dictionary<string, string?>
        {
            ["S3W_NODE_SECRETS__p1__ROOT_USER"] = RootUser,
        });

        // Act
        var broken = store.ValidateEnvPairs();

        // Assert: fail-fast старта (arch/24 §8).
        broken.Should().BeEquivalentTo(["p1"]);
    }

    [Fact]
    public void AccessKeyWithoutTriple_ProfileInvalid()
    {
        // Arrange: ключ 0 без SECRETKEY/POLICY — неполный профиль (permanent P1).
        var store = Store(new Dictionary<string, string?>
        {
            ["S3W_NODE_SECRETS__p1__ROOT_USER"] = RootUser,
            ["S3W_NODE_SECRETS__p1__ROOT_PASSWORD"] = RootPassword,
            ["S3W_NODE_SECRETS__p1__ACCESS_KEYS__0__ACCESSKEY"] = AccessKey,
        });

        // Act
        var ok = store.TryGet("p1", out var secrets, out var diagnostic);

        // Assert: отказ с диагностикой без значений.
        ok.Should().BeFalse();
        secrets.Should().BeNull();
        diagnostic.Should().Contain("incomplete access key #0").And.NotContain(AccessKey);
    }

    [Fact]
    public void PolicyOutsideAllowed_ProfileInvalid()
    {
        // Arrange: policy ≠ read-write|read-only.
        var store = Store(new Dictionary<string, string?>
        {
            ["S3W_NODE_SECRETS__p1__ROOT_USER"] = RootUser,
            ["S3W_NODE_SECRETS__p1__ROOT_PASSWORD"] = RootPassword,
            ["S3W_NODE_SECRETS__p1__ACCESS_KEYS__0__ACCESSKEY"] = AccessKey,
            ["S3W_NODE_SECRETS__p1__ACCESS_KEYS__0__SECRETKEY"] = SecretKeyValue,
            ["S3W_NODE_SECRETS__p1__ACCESS_KEYS__0__POLICY"] = "admin",
        });

        // Act
        var ok = store.TryGet("p1", out var secrets, out _);

        // Assert
        ok.Should().BeFalse();
        secrets.Should().BeNull();
    }

    [Fact]
    public void DiagnosticsWithoutValues()
    {
        // Arrange: полупара + неполный ключ — обе диагностики в одном сторе.
        var values = new Dictionary<string, string?>
        {
            ["S3W_NODE_SECRETS__half__ROOT_USER"] = RootUser,
            ["S3W_NODE_SECRETS__short__ROOT_USER"] = RootUser,
            ["S3W_NODE_SECRETS__short__ROOT_PASSWORD"] = RootPassword,
            ["S3W_NODE_SECRETS__short__ACCESS_KEYS__0__ACCESSKEY"] = AccessKey,
        };
        var store = Store(values);

        // Act
        store.TryGet("short", out _, out var shortDiagnostic);
        var broken = store.ValidateEnvPairs();

        // Assert: значения секретов нигде не всплывают.
        shortDiagnostic.Should().NotContain(RootPassword).And.NotContain(AccessKey);
        broken.Should().BeEquivalentTo(["half", "short"]);
    }

    [Fact]
    public void ProfileNames_ListOfTwo()
    {
        // Arrange: два профиля в env.
        var store = Store(FullProfile("a"), FullProfile("b").Keys.Concat(FullProfile("a").Keys));

        // Act
        var names = store.ProfileNames;

        // Assert
        names.Should().BeEquivalentTo(["a", "b"]);
    }
}
