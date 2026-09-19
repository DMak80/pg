using FluentAssertions;
using ValkeyWorker.Provisioning.Processes;
using Xunit;

namespace ValkeyWorker.UnitTests.Provisioning;

// Сборка args контейнера (arch/21 §2, spec §4.5.1): канонический порядок
// флагов valkey-server из декларации и кредов — детерминизм сверки V3.
public class NodeArgsBuilderTests
{
    // AAA: канонический набор флагов valkey-server из декларации и кредов (arch/21 §2)
    [Fact]
    public void Build_канонический_набор_флагов()
    {
        // Arrange
        long maxmemory = 536870912;
        var policy = "allkeys-lru";

        // Act
        var args = NodeArgsBuilder.Build(maxmemory, policy, "ADMINPWD32SYMBOLSxxxxxxxxxxx", "APPPWD32SYMBOLSxxxxxxxxxxxxx");

        // Assert — порядок флагов канонический (детерминизм сверки V3), хвост TLS (t06)
        args.Should().Equal(
            "valkey-server",
            "--user", "default", "off",
            "--user", "admin", "on", ">ADMINPWD32SYMBOLSxxxxxxxxxxx", "~*", "+@all",
            "--user", "app", "on", ">APPPWD32SYMBOLSxxxxxxxxxxxxx", "~*", "+@read", "+@write",
            "--maxmemory", "536870912",
            "--maxmemory-policy", "allkeys-lru",
            "--save", "",
            "--appendonly", "no",
            "--tls-port", "6379", "--port", "0",
            "--tls-cert-file", "/tls/node.crt",
            "--tls-key-file", "/tls/node.key",
            "--tls-ca-cert-file", "/tls/ca.pem",
            "--tls-auth-clients", "no",
            "--tls-replication", "no");
    }

    [Fact]
    public void Build_ПустойАргументSave_РовноОдинПустойЭлемент()
    {
        // Arrange: persistence off (arch/21 §2) — `--save ""`.
        // Act
        var args = NodeArgsBuilder.Build(1, "noeviction", "a", "b");

        // Assert: РОВНО один пустой элемент после --save (не отсутствие и не ""-литерал).
        args.Should().Contain("--save").And.Contain("");
        args.Count(a => a.Length == 0).Should().Be(1);
        args.Should().NotContain("\"\"");
    }

    [Fact]
    public void Build_РазныеПаролиИлиMaxmemory_РазныеArgs()
    {
        // Arrange: два набора входов.
        var first = NodeArgsBuilder.Build(536870912, "allkeys-lru", "pwd1pwd1pwd1pwd1pwd1pwd1pwd1", "apppwdapppwdapppwdapppwdapppwd");
        var second = NodeArgsBuilder.Build(1073741824, "allkeys-lfu", "pwd2pwd2pwd2pwd2pwd2pwd2pwd2", "apppwdapppwdapppwdapppwdapppwd");

        // Assert: args — детерминированная функция входов (пересоздание собирает актуальные).
        first.Should().NotEqual(second);
    }

    // AAA: TLS-хвост канон arch/21 §2 (t06) — TLS-порт = контейнерный 6379,
    // plain закрыт, серты из /tls, клиенты без сертов (ACL).
    [Fact]
    public void Build_IncludesCanonicalTlsFlags()
    {
        // Arrange / Act
        var args = NodeArgsBuilder.Build(536870912, "allkeys-lru", "adm", "app").ToArray();
        // Assert
        args.Should().Contain("--tls-port");
        args[Array.IndexOf(args, "--tls-port") + 1].Should().Be("6379");
        args.Should().Contain("--port");
        args[Array.IndexOf(args, "--port") + 1].Should().Be("0");
        args.Skip(args.Length - 14).Should().Equal(new[]
        {
            "--tls-port", "6379", "--port", "0",
            "--tls-cert-file", "/tls/node.crt",
            "--tls-key-file", "/tls/node.key",
            "--tls-ca-cert-file", "/tls/ca.pem",
            "--tls-auth-clients", "no",
            "--tls-replication", "no",
        });
    }
}
