using System.Net;
using DotNet.Testcontainers.Builders;
using FluentAssertions;
using PgWorker.Core.Model;
using PgWorker.Docker.Engine;
using Xunit;
using static PgWorker.IntegrationTests.Docker.EngineProxyTestPki;

namespace PgWorker.IntegrationTests.Docker;

// TLS к Engine API (spec §5.5/§8.2, t03): nginx stream-прокси (listen … ssl →
// unix:/var/run/docker.sock) с сертами фикстуры — DockerEngineFactory на
// tcp://localhost:<mapped> + DockerTlsOptions выполняет Ping/ListContainers и
// create/start/delete одноразового контейнера. Порт — динамический.
public class TlsEngineProxyTests
{
    [Fact]
    public async Task TlsProxy_PingList_CreateDelete_ThroughTls()
    {
        // Arrange: per-install docker-CA + серверный (SAN localhost/127.0.0.1) +
        // клиентский серты; nginx.conf stream-прокси; порт — случайный хост-порт.
        DockerTrait.SkipIfUnavailable();
        var (caPem, caKeyPem) = GenerateCa();
        var (serverCertPem, serverKeyPem) = Issue(caPem, caKeyPem, "docker-host", ["localhost"], IPAddress.Loopback);
        var (clientCertPem, clientKeyPem) = Issue(caPem, caKeyPem, "pgworker", ["pgworker"], null);
        var certsDir = Path.Combine(Path.GetTempPath(), $"pgw-tls-proxy-{Guid.NewGuid():N}");
        Directory.CreateDirectory(certsDir);
        await File.WriteAllTextAsync(Path.Combine(certsDir, "server.crt"), serverCertPem, TestContext.Current.CancellationToken);
        await File.WriteAllTextAsync(Path.Combine(certsDir, "server.key"), serverKeyPem, TestContext.Current.CancellationToken);
        // user root: worker открывает смонтированный docker.sock (владелец root);
        // stream-модуль в официальном образе статический — load_module не нужен.
        const string nginxConf = """
            user root;
            worker_processes 1;
            events { worker_connections 64; }
            stream {
              server {
                listen 2376 ssl;
                ssl_certificate /etc/nginx/certs/server.crt;
                ssl_certificate_key /etc/nginx/certs/server.key;
                proxy_pass unix:/var/run/docker.sock;
              }
            }
            """;
        var confPath = Path.Combine(certsDir, "nginx.conf");
        await File.WriteAllTextAsync(confPath, nginxConf, TestContext.Current.CancellationToken);

        var container = new ContainerBuilder("nginx:alpine")
            // Явное имя pgw-t-*: зачистка серии по --filter name=pgw- (случайные
            // имена testcontainers по фильтрам nginx/alpine не матчатся).
            .WithName("pgw-t-tlsproxy")
            .WithResourceMapping(confPath, "/etc/nginx")
            .WithResourceMapping(Path.Combine(certsDir, "server.crt"), "/etc/nginx/certs")
            .WithResourceMapping(Path.Combine(certsDir, "server.key"), "/etc/nginx/certs")
            .WithBindMount("/var/run/docker.sock", "/var/run/docker.sock")
            .WithPortBinding(2376, assignRandomHostPort: true)
            .Build();
        await container.StartAsync(TestContext.Current.CancellationToken);
        var port = container.GetMappedPublicPort(2376);

        var factory = new DockerEngineFactory(new DockerTlsOptions
        {
            CaPem = caPem, ClientCertPem = clientCertPem, ClientKeyPem = clientKeyPem,
        });
        await using var engine = factory.Create($"tcp://localhost:{port}", hostAlias: "local");

        // Act 1: транспорт жив — Ping + ListContainers.
        (await engine.PingAsync(TestContext.Current.CancellationToken)).IsSuccess.Should().BeTrue("TLS-прокси к Engine API жив");
        var listed = await engine.ListContainersAsync("pgw-tls-proxy-test", all: false, TestContext.Current.CancellationToken);
        listed.IsSuccess.Should().BeTrue();

        // Act 2 / Assert 2: полный цикл одноразового контейнера через TLS.
        var name = "pgw-tls-proxy-test-" + Guid.NewGuid().ToString("N")[..6];
        var spec = new ContainerSpec(
            "alpine:3.20", new Dictionary<string, string>(), "", "", [], name, null, null, null,
            Cmd: ["sleep", "5"]);
        (await engine.CreateContainerAsync(spec, name, TestContext.Current.CancellationToken)).IsSuccess.Should().BeTrue();
        (await engine.StartContainerAsync(name, TestContext.Current.CancellationToken)).IsSuccess.Should().BeTrue();
        (await engine.RemoveContainerAsync(name, force: true, TestContext.Current.CancellationToken)).IsSuccess.Should().BeTrue();

        // Cleanup: сертификаты фикстуры + контейнер.
        await container.DisposeAsync();
        Directory.Delete(certsDir, recursive: true);
    }
}
