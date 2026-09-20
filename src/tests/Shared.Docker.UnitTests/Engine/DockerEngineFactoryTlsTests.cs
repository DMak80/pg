using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using FluentAssertions;
using Shared.Docker;
using Xunit;

namespace Shared.Docker.UnitTests.Engine;

// TLS к Engine API (arch/14 §2.2.1, t03): сборка handler'а с клиентским сертом
// и доверием docker-CA, fail-fast частичной конфигурации, unix:// игнорирует
// TLS, plaintext tcp:// без TLS остаётся рабочим (R15).
public class DockerEngineFactoryTlsTests
{
    [Fact]
    public void Factory_TcpWithTls_ClientCertAndChainCallbackSet()
    {
        // Arrange: фикстурная docker-CA + клиентская пара (локальный TestPki).
        var (caPem, caKeyPem) = TestPki.GenerateCa();
        var (certPem, keyPem) = TestPki.Issue(caPem, caKeyPem, "pgworker");
        var factory = new DockerEngineFactory(new DockerTlsOptions
        {
            CaPem = caPem, ClientCertPem = certPem, ClientKeyPem = keyPem,
        });

        // Act: транспортный handler tcp-эндпоинта.
        var handler = factory.CreateHandler("tcp://host1:2376") as SocketsHttpHandler;

        // Assert: клиентский серт подан, колбэк доверия цепочки установлен
        // (паттерн WorkerTlsHandlerTests).
        handler.Should().NotBeNull();
        handler!.SslOptions.ClientCertificates.Should().NotBeNull();
        handler.SslOptions.ClientCertificates!.Count.Should().BePositive();
        handler.SslOptions.RemoteCertificateValidationCallback.Should().NotBeNull();
    }

    [Fact]
    public void Factory_PartialTlsConfig_FailFast()
    {
        // Arrange: CA задан, клиентская пара — нет (частичная конфигурация).
        var (caPem, _) = TestPki.GenerateCa();

        // Act / Assert: ошибка старта фабрики (spec §5.1), а не молчаливый plaintext.
        var ex = Assert.Throws<ApplicationException>(() =>
            new DockerEngineFactory(new DockerTlsOptions { CaPem = caPem }));
        ex.Message.Should().Contain("PgWorker:Docker:Tls");
    }

    [Fact]
    public void Factory_NoTls_PlainTcpHandlerWithoutSslOptions()
    {
        // Arrange / Act: фабрика без TLS-конфигурации (dev/тесты, R15).
        var handler = new DockerEngineFactory().CreateHandler("tcp://host1:2375") as SocketsHttpHandler;

        // Assert: plaintext-путь не сломан.
        handler!.SslOptions.ClientCertificates.Should().BeNull();
    }

    [Fact]
    public void Factory_UnixEndpoint_TlsIgnored()
    {
        // Arrange: TLS задан, но endpoint — unix-сокет.
        var (caPem, caKeyPem) = TestPki.GenerateCa();
        var (certPem, keyPem) = TestPki.Issue(caPem, caKeyPem, "pgworker");
        var factory = new DockerEngineFactory(new DockerTlsOptions
        {
            CaPem = caPem, ClientCertPem = certPem, ClientKeyPem = keyPem,
        });

        // Act
        var handler = factory.CreateHandler("unix:///var/run/docker.sock") as SocketsHttpHandler;

        // Assert: unix-транспорт — без TLS (сокет локальный, arch/14 §2.2).
        handler!.SslOptions.ClientCertificates.Should().BeNull();
    }

    // Локальный PKI-хелпер: CertificateRequest + RSA-2048 (паттерн TestPki из
    // AdminPanel.UnitTests/Workers/WorkerTlsHandlerTests.cs:64-100).
    private static class TestPki
    {
        public static (string CaPem, string CaKeyPem) GenerateCa()
        {
            using var rsa = RSA.Create(2048);
            var request = new CertificateRequest(
                "CN=test-docker-ca", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
            using var ca = request.CreateSelfSigned(
                DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(1));
            return (ca.ExportCertificatePem(), ca.GetRSAPrivateKey()!.ExportPkcs8PrivateKeyPem());
        }

        public static (string CertPem, string KeyPem) Issue(string caPem, string caKeyPem, string commonName)
        {
            using var caCert = X509Certificate2.CreateFromPem(caPem);
            using var caKey = RSA.Create();
            caKey.ImportFromPem(caKeyPem);
            using var caWithKey = caCert.CopyWithPrivateKey(caKey);
            using var rsa = RSA.Create(2048);
            var request = new CertificateRequest(
                $"CN={commonName}", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
                [new Oid("1.3.6.1.5.5.7.3.1"), new Oid("1.3.6.1.5.5.7.3.2")], critical: false));
            var notAfter = DateTimeOffset.UtcNow.AddYears(1);
            if (notAfter > caWithKey.NotAfter)
                notAfter = caWithKey.NotAfter; // кламп к CA: UtcNow+1y мог уйти за срок CA
            using var cert = request.Create(
                caWithKey, DateTimeOffset.UtcNow.AddDays(-1), notAfter,
                RandomNumberGenerator.GetBytes(16));
            return (cert.ExportCertificatePem(), rsa.ExportPkcs8PrivateKeyPem());
        }
    }
}

// Канон-суперсет t07 (§4.6.1): инспекция endpoint'а с параметром контейнерного
// порта (kfw: 9094, vwk: 6379) — порт-биндинг из одного инспекта, Running из
// State.Running.
public class InspectNodeEndpointTests
{
    private sealed class FakeHandler(Func<HttpRequestMessage, HttpResponseMessage> responder) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
            => Task.FromResult(responder(request));
    }

    private static HttpResponseMessage Json(string body) => new()
    {
        StatusCode = HttpStatusCode.OK,
        Content = new StringContent(body, Encoding.UTF8, "application/json"),
    };

    // Инспект контейнера с биндингами обоих клиентских портов (kfw/vwk).
    private const string InspectBody = """
        {"Id":"abc","HostConfig":{"PortBindings":{"9094/tcp":[{"HostIp":"0.0.0.0","HostPort":"19094"}],
          "6379/tcp":[{"HostIp":"0.0.0.0","HostPort":"16379"}]}},
         "State":{"Running":true,"Status":"running"}}
        """;

    [Fact]
    public async Task InspectNodeEndpoint_PortParameter_ResolvesBinding()
    {
        // Arrange — один инспект несёт биндинги 9094/tcp и 6379/tcp
        var handler = new FakeHandler(_ => Json(InspectBody));

        // Act: kfw-порт
        var kfw = await NewEngine(handler).InspectNodeEndpointAsync("node1", 9094, CancellationToken.None);
        // Act: vwk-порт
        var vwk = await NewEngine(handler).InspectNodeEndpointAsync("node1", 6379, CancellationToken.None);

        // Assert: каждый вызов вернул порт СВОЕГО биндинга; Running — из State.Running
        kfw.IsSuccess.Should().BeTrue();
        kfw.Value.Should().NotBeNull();
        kfw.Value!.ClientHostPort.Should().Be(19094);
        kfw.Value.Running.Should().BeTrue();
        vwk.IsSuccess.Should().BeTrue();
        vwk.Value.Should().NotBeNull();
        vwk.Value!.ClientHostPort.Should().Be(16379);
        vwk.Value.Running.Should().BeTrue();
    }

    [Fact]
    public async Task InspectNodeEndpoint_NotRunning_FromState()
    {
        // Arrange — контейнер есть, но остановлен: PortBindings персистят
        const string stopped = """
            {"Id":"abc","HostConfig":{"PortBindings":{"6379/tcp":[{"HostIp":"0.0.0.0","HostPort":"16379"}]}},
             "State":{"Running":false,"Status":"exited"}}
            """;
        var handler = new FakeHandler(_ => Json(stopped));

        // Act
        var result = await NewEngine(handler).InspectNodeEndpointAsync("node1", 6379, CancellationToken.None);

        // Assert: endpoint-факт есть, Running=false (vwk-семантика надзора)
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().NotBeNull();
        result.Value!.ClientHostPort.Should().Be(16379);
        result.Value.Running.Should().BeFalse();
    }

    private static DockerEngine NewEngine(FakeHandler handler)
        => new(new HttpClient(handler) { BaseAddress = new Uri("http://docker") }, hostAlias: null);
}
