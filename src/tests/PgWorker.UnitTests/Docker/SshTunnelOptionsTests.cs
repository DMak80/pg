using System.Security.Cryptography;
using System.Text;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using PgWorker.Docker.Engine;
using Xunit;

namespace PgWorker.UnitTests.Docker;

// SSH-туннель к Engine API (arch/14 §2.2.1, t03): env-биндинги, fingerprint-семантика
// (pin задан — строгое сравнение; не задан — TOFU-accept c признаком warning, R14),
// целевой адрес форварда — чистые функции без сети.
public class SshTunnelOptionsTests
{
    [Fact]
    public void ApplyEnvOverrides_SshKeysMapped()
    {
        // Arrange
        var env = new Dictionary<string, string>
        {
            ["PGW_DOCKER_SSH_KEY_PATH"] = "/secrets/id_pgworker",
            ["PGW_DOCKER_SSH_FINGERPRINT"] = "SHA256:abcdef",
        };
        var config = new ConfigurationManager();

        // Act
        SshTunnelOptions.ApplyEnvOverrides(config, key => env.GetValueOrDefault(key));

        // Assert
        config["PgWorker:Docker:Ssh:KeyPath"].Should().Be("/secrets/id_pgworker");
        config["PgWorker:Docker:Ssh:FingerprintSha256"].Should().Be("SHA256:abcdef");
        SshTunnelOptions.EnvBindings.Should().HaveCount(3);
    }

    [Fact]
    public void DecideHostKeyTrust_ExpectedPinSet_StrictComparison()
    {
        // Arrange: произвольные host-key данные + ожидаемый pin = их SHA-256.
        var hostKey = Encoding.ASCII.GetBytes("host-key-blob");
        var sha = Convert.ToBase64String(SHA256.HashData(hostKey)).TrimEnd('=');

        // Act / Assert: точное совпадение (в форматах с префиксом и без) — доверие;
        // посторонний pin — отказ; TOFU-флага нет.
        SshTunnelOptions.DecideHostKeyTrust(hostKey, "SHA256:" + sha, out var tofu1).Should().BeTrue();
        SshTunnelOptions.DecideHostKeyTrust(hostKey, sha, out _).Should().BeTrue();
        SshTunnelOptions.DecideHostKeyTrust(hostKey, "SHA256:AAAA", out var tofu2).Should().BeFalse();
        tofu1.Should().BeFalse();
        tofu2.Should().BeFalse();
    }

    [Fact]
    public void DecideHostKeyTrust_NoPin_TofuAcceptWithWarning()
    {
        // Arrange: pin не задан (PGW_DOCKER_SSH_FINGERPRINT пуст).
        // Act
        var trust = SshTunnelOptions.DecideHostKeyTrust("blob"u8.ToArray(), null, out var tofu);

        // Assert: принимаем (TOFU), но семантика требует warning-лога у вызывающего.
        trust.Should().BeTrue();
        tofu.Should().BeTrue();
    }

    [Fact]
    public void KeyMaterial_PemOrPathFallback()
    {
        // Arrange: PEM-значение приоритетнее пути (дуализм env-секретов).
        var opts = new SshTunnelOptions { KeyPem = "-----BEGIN PRIVATE KEY-----", KeyPath = "/nonexistent" };

        // Act / Assert: наличный ключ без сети — только факт выбора источника
        // (метод SshHostConnection.ReadKeyMaterial вынесен как internal static).
        SshHostConnection.ReadKeyMaterial(opts).Should().Be("-----BEGIN PRIVATE KEY-----");
        SshHostConnection.ReadKeyMaterial(new SshTunnelOptions { KeyPem = null, KeyPath = null })
            .Should().BeNull();
    }

    [Fact]
    public void TunnelTarget_DefaultsAndCustom_Validated()
    {
        // Arrange: дефолты канона (loopback демона, 2376 c --tlsverify) и кастом.
        var custom = new SshTunnelOptions { RemoteDaemonHost = "dock-internal", RemoteDaemonPort = 2375 };

        // Act / Assert: target-вычисление без сети (spec §5.5) — дефолты/кастом.
        new SshTunnelOptions().TunnelTarget().Should().Be(("127.0.0.1", 2376));
        custom.TunnelTarget().Should().Be(("dock-internal", 2375));
    }

    [Theory]
    [InlineData("", 2376)]           // пустой хост
    [InlineData("127.0.0.1", 0)]     // порт вне диапазона
    [InlineData("127.0.0.1", 65536)] // порт вне диапазона
    public void TunnelTarget_Invalid_FailFast(string host, int port)
    {
        // Arrange: некорректная цель форварда.
        var opts = new SshTunnelOptions { RemoteDaemonHost = host, RemoteDaemonPort = port };

        // Act / Assert: конфигурационная ошибка — при создании туннеля, не в рантайме тика.
        Assert.Throws<ApplicationException>(() => opts.TunnelTarget());
    }
}
