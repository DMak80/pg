using System.Net;
using System.Security.Cryptography;
using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using FluentAssertions;
using PgWorker.Docker.Engine;
using Xunit;

namespace PgWorker.IntegrationTests.Docker;

// SSH-туннель к Engine API (spec §5.5/§8.2, t03): sshd-контейнер c key-аутентификацией
// и socat (TCP-LISTEN:2376 → unix:docker.sock); DockerEngineFactory на
// ssh://testuser@localhost:<mapped> — Ping/ListContainers через ForwardedPortLocal.
// Fingerprint-pin: корректный pin — подключается; неверный — отказ host-key.
public class SshTunnelEngineTests
{
    private static SshTunnelOptions Options(string keyPem, string? fingerprint = null) => new()
    {
        KeyPem = keyPem,
        RemoteDaemonHost = "127.0.0.1",
        RemoteDaemonPort = 2376,
        FingerprintSha256 = fingerprint,
    };

    private static async Task<IContainer> StartSshdAsync(string authorizedKeys)
    {
        var keysDir = Path.Combine(Path.GetTempPath(), $"pgw-sshd-{Guid.NewGuid():N}");
        Directory.CreateDirectory(keysDir);
        await File.WriteAllTextAsync(Path.Combine(keysDir, "authorized_keys"), authorizedKeys + "\n");
        return new ContainerBuilder("alpine:3.20")
            // Явное имя pgw-t-*: зачистка серии по --filter name=pgw-.
            .WithName("pgw-t-sshd")
            .WithCommand("sh", "-c", string.Join(" && ",
            [
                "apk add --no-cache openssh socat >/dev/null",
                "ssh-keygen -A",
                "adduser -D testuser",
                // adduser -D блокирует аккаунт («!» в shadow): OpenSSH 9 без PAM
                // отвергает pubkey даже на locked-аккаунте — «*» = без пароля, не locked.
                "sed -i 's/^testuser:!/testuser:*/' /etc/shadow",
                // Дефолт alpine-конфига запрещает форвардинг — direct-tcpip SSH.NET
                // получает «administratively prohibited».
                "sed -i 's/^AllowTcpForwarding no/AllowTcpForwarding yes/' /etc/ssh/sshd_config",
                "mkdir -p /home/testuser/.ssh",
                "cp /keys/authorized_keys /home/testuser/.ssh/authorized_keys",
                "chown -R testuser:testuser /home/testuser/.ssh",
                "chmod 600 /home/testuser/.ssh/authorized_keys",
                "/usr/sbin/sshd -e -p 2222",
                // bind=127.0.0.1: без него socat слушает IPv6-wildcard — direct-tcpip
                // sshd на IPv4-loopback получает connection reset.
                "socat TCP-LISTEN:2376,bind=127.0.0.1,fork,reuseaddr UNIX-CONNECT:/var/run/docker.sock",
            ]))
            .WithBindMount("/var/run/docker.sock", "/var/run/docker.sock")
            // destination — каталог (testcontainers кладёт файл внутрь):
            // /keys/authorized_keys.
            .WithResourceMapping(Path.Combine(keysDir, "authorized_keys"), "/keys")
            .WithPortBinding(2222, assignRandomHostPort: true)
            .WithPortBinding(2376, assignRandomHostPort: true)
            // Готовность ДО первого Connect: apk add идёт секундами — без wait
            // первый SSH-хендшейк ловит connection refused (флаки). Оба порта:
            // 2222 (sshd) и 2376 (socat — цель форварда туннеля); busybox netstat.
            .WithWaitStrategy(Wait.ForUnixContainer().UntilCommandIsCompleted(
                "netstat -tln | grep -q ':2222' && netstat -tln | grep -q ':2376'",
                w => w.WithTimeout(TimeSpan.FromSeconds(90)))) // ≤ 100 c: падаем быстро
            .Build();
        // NOTE: keysDir не удалять до DisposeAsync контейнера (bind source).
    }

    [Fact]
    public async Task SshTunnel_PingListContainers_ThroughForward()
    {
        // Arrange: RSA-ключпар фикстуры → authorized_keys (openssh-формат);
        // sshd+socat контейнер; endpoint ssh://testuser@localhost:<mapped-2222>.
        DockerTrait.SkipIfUnavailable();
        using var rsa = RSA.Create(2048);
        var keyPem = rsa.ExportPkcs8PrivateKeyPem();
        var authorizedKeys = OpenSshPublicKey(rsa, "pgw-test");
        await using var sshd = await StartSshdAsync(authorizedKeys);
        await sshd.StartAsync(TestContext.Current.CancellationToken);
        var sshPort = sshd.GetMappedPublicPort(2222);
        var endpoint = $"ssh://testuser@localhost:{sshPort}";

        var factory = new DockerEngineFactory(ssh: Options(keyPem));
        await using var engine = factory.Create(endpoint, hostAlias: "local");

        // Act: Ping + ListContainers через туннель (форвард → socat → docker.sock).
        var ct = TestContext.Current.CancellationToken;
        var ping = await ResultProbe(async () => await engine.PingAsync(ct), TimeSpan.FromSeconds(20));
        ping.IsSuccess.Should().BeTrue($"SSH-туннель пробрасывает Engine API; error: {ping.Error}");
        (await engine.ListContainersAsync("pgw-ssh-test", all: false, ct)).IsSuccess.Should().BeTrue();
    }

    [Fact]
    public async Task SshTunnel_WrongFingerprintPin_Refused()
    {
        // Arrange: живой sshd, но pin заведомо неверный.
        DockerTrait.SkipIfUnavailable();
        using var rsa = RSA.Create(2048);
        await using var sshd = await StartSshdAsync(OpenSshPublicKey(rsa, "pgw-test"));
        await sshd.StartAsync(TestContext.Current.CancellationToken);
        var endpoint = $"ssh://testuser@localhost:{sshd.GetMappedPublicPort(2222)}";

        // Act / Assert: host-key отвергнут — создание туннеля падает (spec §8.2).
        var factory = new DockerEngineFactory(ssh: Options(rsa.ExportPkcs8PrivateKeyPem(), "SHA256:AAAAAAAAAAAAAAAAAAAAAA=="));
        Assert.ThrowsAny<Exception>(() => factory.Create(endpoint)).Message.Should().NotBeNull();
    }

    [Fact]
    public async Task SshTunnel_CorrectFingerprintPin_Connects()
    {
        // Arrange: первый туннель без pin читает фактический fingerprint (TOFU),
        // второй — с этим pin: подключение обязано пройти (строгая семантика).
        DockerTrait.SkipIfUnavailable();
        using var rsa = RSA.Create(2048);
        var keyPem = rsa.ExportPkcs8PrivateKeyPem();
        await using var sshd = await StartSshdAsync(OpenSshPublicKey(rsa, "pgw-test"));
        await sshd.StartAsync(TestContext.Current.CancellationToken);
        var endpoint = $"ssh://testuser@localhost:{sshd.GetMappedPublicPort(2222)}";

        var probe = new SshHostConnection(
            EndpointScheme.Parse(endpoint), Options(keyPem));
        try
        {
            var pinned = probe.FingerprintSha256;
            pinned.Should().NotBeNull("TOFU-подключение фиксирует fingerprint");

            // Act: фабрика с pin.
            var factory = new DockerEngineFactory(ssh: Options(keyPem, pinned));
            await using var engine = factory.Create(endpoint);
            (await engine.PingAsync(TestContext.Current.CancellationToken))
                .IsSuccess.Should().BeTrue("корректный pin не мешает туннелю");
        }
        finally
        {
            await probe.DisposeAsync();
        }
    }

    // socat стартует сразу за sshd, но форвард живёт на удалённой стороне:
    // первый Ping может словить отказ соединения через туннель — короткий ретрай
    // с бюджетом (transient-толерантность первого тика, spec §2).
    private static async Task<PgWorker.Core.Result> ResultProbe(
        Func<Task<PgWorker.Core.Result>> probe, TimeSpan budget)
    {
        var deadline = DateTimeOffset.UtcNow + budget;
        while (true)
        {
            var result = await probe();
            if (result.IsSuccess || DateTimeOffset.UtcNow >= deadline)
                return result;
            await Task.Delay(500, TestContext.Current.CancellationToken);
        }
    }

    // ssh-rsa public-строка из RSA-параметров (blob: "ssh-rsa" + mpint e + mpint n,
    // каждый элемент — length-prefixed big-endian; неположительное число — с ведущим 0).
    private static string OpenSshPublicKey(RSA rsa, string comment)
    {
        var p = rsa.ExportParameters(false);

        static byte[] Len(byte[] b)
        {
            var prefix = BitConverter.GetBytes(IPAddress.HostToNetworkOrder(b.Length));
            return [.. prefix, .. b];
        }

        static byte[] Mpint(byte[] v)
        {
            var padded = (v[0] & 0x80) != 0 ? [0, .. v] : v;
            return Len(padded);
        }

        var name = Len(Encoding.ASCII.GetBytes("ssh-rsa"));
        var blob = name
            .Concat(Mpint(p.Exponent!))
            .Concat(Mpint(p.Modulus!))
            .ToArray();
        return $"ssh-rsa {Convert.ToBase64String(blob)} {comment}";
    }
}
