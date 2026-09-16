using System.Net.Sockets;
using System.Net.Security;
using System.Text.Json;
using FluentAssertions;
using ValkeyWorker.App.Api;
using Microsoft.Extensions.Logging;
using Xunit;

namespace ValkeyWorker.IntegrationTests.Api;

// Чтение ключа /workers/api_tls/valkeyworker при старте (arch/21 §1.1,
// порт WorkerApiCertStartupTests kfw): Found/Missing/Unreachable/Broken +
// применение в ConfigureMtls (серт грани из ключа; fail-fast на битый;
// env-фоллбек VWK_API_TLS_*).
[Collection(ValkeyApiCollection.Name)]
public class WorkerApiCertStartupTests(ValkeyApiFixture fx)
{
    private const string Worker = "valkeyworker";

    private string Endpoints => fx.Etcd.Endpoint;

    private async Task PutKeyAsync(string json)
        => await fx.Etcd.Gateway.PutAsync(
            Endpoints, $"/workers/api_tls/{Worker}", json, null, TestContext.Current.CancellationToken);

    // Предочистка/посточистка ключа серта: тесты живут в общем etcd фикстуры —
    // чистим свой ключ при любом исходе.
    private async Task DeleteKeyAsync()
        => await fx.Etcd.Gateway.DeleteAsync(
            Endpoints, $"/workers/api_tls/{Worker}", prefix: false, TestContext.Current.CancellationToken);

    private (string CaPem, string CaKeyPem, string CertPem, string KeyPem) IssuePair()
    {
        var (caPem, caKeyPem) = TestPki.GenerateCa("cert-startup");
        var (certPem, keyPem) = TestPki.IssueCertificate(caPem, caKeyPem, $"{Worker}-api");
        return (caPem, caKeyPem, certPem, keyPem);
    }

    [Fact]
    public async Task Read_КлючаНет_Missing()
    {
        // Arrange: ключа нет (чистим свой ключ фикстуры).
        await DeleteKeyAsync();
        // Act
        var read = await WorkerApiCertReader.ReadAsync([Endpoints], Worker, TestContext.Current.CancellationToken);
        // Assert: env-фоллбек.
        read.Status.Should().Be(ManagedCertStatus.Missing);
    }

    [Fact]
    public async Task Read_ВалиднаяПара_Found()
    {
        // Arrange
        await DeleteKeyAsync();
        var (_, _, certPem, keyPem) = IssuePair();
        await PutKeyAsync(JsonSerializer.Serialize(new { cert_pem = certPem, key_pem = keyPem }));
        try
        {
            // Act
            var read = await WorkerApiCertReader.ReadAsync([Endpoints], Worker, TestContext.Current.CancellationToken);
            // Assert
            read.Status.Should().Be(ManagedCertStatus.Found);
            read.CertPem.Should().Be(certPem);
        }
        finally
        {
            await DeleteKeyAsync();
        }
    }

    [Fact]
    public async Task Read_БитыйJson_Broken()
    {
        // Arrange: JSON-мусор — fail-fast-ветка старта.
        await DeleteKeyAsync();
        await PutKeyAsync("{not-json");
        try
        {
            // Act
            var read = await WorkerApiCertReader.ReadAsync([Endpoints], Worker, TestContext.Current.CancellationToken);
            // Assert
            read.Status.Should().Be(ManagedCertStatus.Broken);
            read.Error.Should().NotBeNullOrEmpty();
        }
        finally
        {
            await DeleteKeyAsync();
        }
    }

    [Fact]
    public async Task Read_МёртвыйEndpoint_Unreachable()
    {
        // Arrange: ключ чист; мёртвый порт (зонд свободного → никто не слушает).
        await DeleteKeyAsync();
        var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        // Act
        var read = await WorkerApiCertReader.ReadAsync([$"http://localhost:{port}"], Worker,
            TestContext.Current.CancellationToken);
        // Assert: env-фоллбек с warning при живом env-серте.
        read.Status.Should().Be(ManagedCertStatus.Unreachable);
    }

    [Fact]
    public async Task ConfigureMtls_ManagedFound_СертИзEtcd()
    {
        // Arrange: валидная пара в etcd; CA пары — CA клиентского серта
        // (грань mTLS: клиент обязан предъявить серт этой CA).
        await DeleteKeyAsync();
        var (caPem, caKeyPem, certPem, keyPem) = IssuePair();
        await PutKeyAsync(JsonSerializer.Serialize(new { cert_pem = certPem, key_pem = keyPem }));
        try
        {
            var managed = await WorkerApiCertReader.ReadAsync([Endpoints], Worker,
                TestContext.Current.CancellationToken);
            managed.Status.Should().Be(ManagedCertStatus.Found);

            // Act: конфигурация mTLS с управляемым сертом (тот же код Program.cs).
            // Изоляция от env фабрик (AllowInsecureHttp=true процесса): in-memory
            // оверрайд сильнее env-провайдера — грань обязана подняться на mTLS.
            var builder = Microsoft.AspNetCore.Builder.WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.Configuration["ValkeyWorker:Api:Tls:AllowInsecureHttp"] = "false";
            // CA клиентских сертов (та же API-CA — для проверки цепочки на хендшейке).
            builder.Configuration["ValkeyWorker:Api:Tls:ClientCaPem"] = caPem;
            var setup = TlsEndpoints.ConfigureMtls(builder, 0, managed);
            await using var app = builder.Build();

            // Assert: источник etcd-ключ, thumbprint серта пары.
            setup.Source.Should().Be("etcd:/workers/api_tls/valkeyworker");
            var expected = Convert.ToHexString(
                System.Security.Cryptography.SHA256.HashData(
                    System.Security.Cryptography.X509Certificates.X509Certificate2
                        .CreateFromPem(certPem, keyPem).RawData)).ToLowerInvariant();
            var thumbprint = Convert.ToHexString(
                System.Security.Cryptography.SHA256.HashData(setup.ServerCert!.RawData)).ToLowerInvariant();
            thumbprint.Should().Be(expected);
        }
        finally
        {
            await DeleteKeyAsync();
        }
    }
}
