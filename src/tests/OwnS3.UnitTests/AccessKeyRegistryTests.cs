using OwnS3.App.Access;
using OwnS3.App;

namespace OwnS3.UnitTests;

// Реестр учёток (arch/owns3/05 §1–2): root из опций → admin; статические ключи →
// роли; неизвестный ключ → null; неизвестная policy → fail-fast построения.
public sealed class AccessKeyRegistryTests
{
    private static OwnS3Options Options(params (string Ak, string Sk, string Policy)[] keys) => new()
    {
        Root = new RootOptions { User = "root", Password = "rootpassword" },
        AccessKeys = [.. keys.Select(k => new AccessKeyOptions { AccessKey = k.Ak, SecretKey = k.Sk, Policy = k.Policy })],
    };

    [Fact]
    public void Registry_RootKey_HasAdminPolicy()
    {
        // Arrange / Act
        var registry = new AccessKeyRegistry(Options());

        // Assert: root аутентифицируется как обычный ключ admin (глава 05 §1)
        var record = registry.Find("root");
        record.Should().NotBeNull();
        record!.Policy.Should().Be(AccessPolicy.Admin);
        record.SecretKey.Should().Be("rootpassword");
    }

    [Fact]
    public void Registry_StaticKeys_MapToPolicies()
    {
        // Arrange / Act
        var registry = new AccessKeyRegistry(Options(
            ("reader", "reader-secret", "read-only"),
            ("writer", "writer-secret", "read-write"),
            ("admin", "admin-secret", "admin")));

        // Assert
        registry.Find("reader")!.Policy.Should().Be(AccessPolicy.ReadOnly);
        registry.Find("writer")!.Policy.Should().Be(AccessPolicy.ReadWrite);
        registry.Find("admin")!.Policy.Should().Be(AccessPolicy.Admin);
    }

    [Fact]
    public void Registry_UnknownKey_ReturnsNull()
    {
        // Arrange / Act
        var registry = new AccessKeyRegistry(Options(("reader", "reader-secret", "read-only")));

        // Assert
        registry.Find("nobody").Should().BeNull();
    }

    [Fact]
    public void Registry_UnknownPolicy_FailsFast()
    {
        // Arrange: policy вне {read-only, read-write, admin}
        // Act
        var act = () => new AccessKeyRegistry(Options(("bad", "bad-secret", "full-access")));

        // Assert: отказ построения реестра (fail-fast конфигурации)
        act.Should().Throw<ArgumentException>();
    }
}
