using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using AdminPanel.Probes.Valkey;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests.ProbesValkey;

// Мини-PKI юнит-тестов панели (копия TlsTestServer воркера, свой экземпляр —
// t06): CA per-тест + серверный серт SAN=127.0.0.1; канон-режим AUTH→+OK,
// PING→+PONG поверх SslStream.
internal static class TlsTestServer
{
    public static (string CaPem, string CaKeyPem) CreateCa(string commonName)
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            $"CN={commonName}", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(
            new X509BasicConstraintsExtension(true, false, 0, true));
        using var ca = request.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(5));
        return (PemCert(ca), PemKey(rsa));
    }

    public static (string CertPem, string KeyPem) IssueServerCertificate(
        string caPem, string caKeyPem, string commonName, string? dnsSan = null, IPAddress? ipSan = null)
    {
        using var ca = X509Certificate2.CreateFromPem(caPem, caKeyPem);
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            $"CN={commonName}", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var san = new SubjectAlternativeNameBuilder();
        if (dnsSan is not null)
            san.AddDnsName(dnsSan);
        if (ipSan is not null)
            san.AddIpAddress(ipSan);
        request.CertificateExtensions.Add(san.Build());
        using var cert = request.Create(ca, DateTimeOffset.UtcNow.AddDays(-1),
            DateTimeOffset.UtcNow.AddYears(3), RandomNumberGenerator.GetBytes(16));
        var withKey = cert.CopyWithPrivateKey(rsa);
        return (PemCert(withKey), PemKey(rsa));
    }

    private static string PemCert(X509Certificate2 cert)
        => cert.ExportCertificatePem().ReplaceLineEndings("\n").TrimEnd() + "\n";

    private static string PemKey(RSA rsa)
        => rsa.ExportPkcs8PrivateKeyPem().ReplaceLineEndings("\n").TrimEnd() + "\n";
}

// Фейк-TLS-сервер пробы: TcpListener (динамический порт) + SslStream + RESP
// (AUTH→+OK, PING→+PONG).
internal sealed class TlsProbeServer : IAsyncDisposable
{
    private readonly TcpListener _listener;
    private readonly X509Certificate2 _serverCertificate;
    private readonly CancellationTokenSource _cts = new();

    private TlsProbeServer(X509Certificate2 certificate)
    {
        _serverCertificate = certificate;
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
    }

    public int Port { get; }

    public static TlsProbeServer Start(string certPem, string keyPem)
    {
        var cert = X509Certificate2.CreateFromPem(certPem, keyPem);
        // SslStream требует серт с ключом в том же объекте — Pkcs12-round-trip.
        var pfx = cert.Export(X509ContentType.Pkcs12);
        return new TlsProbeServer(X509CertificateLoader.LoadPkcs12(pfx, null));
    }

    // Обслужить ОДНО соединение; отказ клиента (чужой CA/битый PEM) — норма.
    public async Task HandleOneAsync()
    {
        Socket socket;
        try
        {
            socket = await _listener.AcceptSocketAsync(_cts.Token);
        }
        catch (OperationCanceledException)
        {
            return;
        }

        try
        {
            using (socket)
            await using (var ns = new NetworkStream(socket, ownsSocket: false))
            {
                using var ssl = new SslStream(ns, leaveInnerStreamOpen: false);
                await ssl.AuthenticateAsServerAsync(_serverCertificate, clientCertificateRequired: false,
                    System.Security.Authentication.SslProtocols.None, checkCertificateRevocation: false);
                while (true)
                {
                    var command = await ReadCommandAsync(ssl);
                    if (command is null)
                        return;
                    var reply = command[0].ToUpperInvariant() switch
                    {
                        "PING" => "+PONG\r\n",
                        _ => "+OK\r\n",
                    };
                    await ssl.WriteAsync(Encoding.UTF8.GetBytes(reply));
                    await ssl.FlushAsync();
                }
            }
        }
        catch (Exception)
        {
            // клиент ушёл/битый кадр — соединение закрыто
        }
    }

    private static async Task<string[]?> ReadCommandAsync(SslStream stream)
    {
        var header = await ReadLineAsync(stream);
        if (header is null || !header.StartsWith('*'))
            return null;
        var count = int.Parse(header[1..]);
        var args = new string[count];
        for (var i = 0; i < count; i++)
        {
            var bulk = await ReadLineAsync(stream);
            if (bulk is null || !bulk.StartsWith('$'))
                return null;
            var size = int.Parse(bulk[1..]);
            var bytes = new byte[size];
            await stream.ReadExactlyAsync(bytes);
            args[i] = Encoding.UTF8.GetString(bytes);
            var crlf = new byte[2];
            await stream.ReadExactlyAsync(crlf);
        }

        return args;
    }

    private static async Task<string?> ReadLineAsync(SslStream stream)
    {
        var sb = new StringBuilder();
        while (true)
        {
            var b = new byte[1];
            var read = await stream.ReadAsync(b);
            if (read == 0)
                return sb.Length == 0 ? null : sb.ToString();
            if (b[0] == '\r')
            {
                var lf = new byte[1];
                await stream.ReadExactlyAsync(lf);
                return sb.ToString();
            }

            sb.Append((char)b[0]);
        }
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        _listener.Stop();
        _serverCertificate.Dispose();
        await ValueTask.CompletedTask;
    }
}

// PING-проба панели (t06 TLS): доверие per-cluster CA (CustomRootTrust + SAN
// endpoint-хоста); чужой CA / битый ca_pem / отсутствие ca_pem — Failed.
public sealed class ValkeyConnectionTests
{
    private static readonly (string CaPem, string CaKeyPem) Ca = TlsTestServer.CreateCa("probe-tests-ca");

    private static (TlsProbeServer Server, Task Handle) StartServer(string caPem, string caKey)
    {
        var (cert, key) = TlsTestServer.IssueServerCertificate(caPem, caKey, "node1", ipSan: IPAddress.Parse("127.0.0.1"));
        var server = TlsProbeServer.Start(cert, key);
        return (server, server.HandleOneAsync());
    }

    private static ValkeyProbeTarget Target(int port, string? caPem = null)
        => new("127.0.0.1", port, "admin", "secret", caPem);

    // AAA: доверенный CA — TLS-хендшейк, AUTH+PONG → успех.
    [Fact]
    public async Task Ping_TlsTrustedCa_Pong()
    {
        // Arrange — сервер с сертом от CA; клиент доверяет тот же CA
        var (server, handle) = StartServer(Ca.CaPem, Ca.CaKeyPem);
        await using (server)
        {
            var client = new ValkeyConnection();

            // Act
            var result = await client.PingAsync(
                Target(server.Port, Ca.CaPem), TimeSpan.FromSeconds(3), CancellationToken.None);
            await handle;

            // Assert
            result.IsSuccess.Should().BeTrue(result.Error?.Message);
        }
    }

    // AAA: серт сервера чужого CA — аутентификация отвергнута → Failed.
    [Fact]
    public async Task Ping_TlsForeignCa_Failed()
    {
        // Arrange — сервер подписан CA2, клиент доверяет CA1
        var (ca2Pem, ca2Key) = TlsTestServer.CreateCa("ca2");
        var (server, handle) = StartServer(ca2Pem, ca2Key);
        await using (server)
        {
            var client = new ValkeyConnection();

            // Act
            var result = await client.PingAsync(
                Target(server.Port, Ca.CaPem), TimeSpan.FromSeconds(3), CancellationToken.None);
            await handle;

            // Assert
            result.IsSuccess.Should().BeFalse();
        }
    }

    // AAA: ca_pem нет — TLS-проба невозможна, Failed с диагнозом (без сети).
    [Fact]
    public async Task Ping_NoCaPem_FailedWithMessage()
    {
        // Arrange
        var client = new ValkeyConnection();

        // Act
        var result = await client.PingAsync(
            Target(59999), TimeSpan.FromSeconds(1), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error!.Message.Should().Contain("ca_pem").And.Contain("TLS");
    }

    // AAA: битый PEM ca_pem — Failed с указанием ключа (толерантность как у кредов).
    [Fact]
    public async Task Ping_BrokenCaPem_Failed()
    {
        // Arrange
        var client = new ValkeyConnection();

        // Act
        var result = await client.PingAsync(
            Target(59999, "not a pem"), TimeSpan.FromSeconds(1), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error!.Message.Should().Contain("ca_pem");
    }

    // AAA: SAN не покрывает endpoint-хост — отказ валидации.
    [Fact]
    public async Task Ping_SanMismatch_Failed()
    {
        // Arrange — серт с DNS-SAN "other.host", endpoint 127.0.0.1
        var (cert, key) = TlsTestServer.IssueServerCertificate(Ca.CaPem, Ca.CaKeyPem, "node1", dnsSan: "other.host");
        await using var server = TlsProbeServer.Start(cert, key);
        var handle = server.HandleOneAsync();
        var client = new ValkeyConnection();

        // Act
        var result = await client.PingAsync(
            Target(server.Port, Ca.CaPem), TimeSpan.FromSeconds(3), CancellationToken.None);
        await handle;

        // Assert
        result.IsSuccess.Should().BeFalse();
    }

    // AAA: свободный порт — сетевой отказ → Failed (не исключение).
    [Fact]
    public async Task Ping_Refused_Fails()
    {
        // Arrange
        using var probe = new TcpListener(IPAddress.Loopback, 0);
        probe.Start();
        var port = ((IPEndPoint)probe.LocalEndpoint).Port;
        probe.Stop();
        var client = new ValkeyConnection();

        // Act
        var result = await client.PingAsync(
            Target(port, Ca.CaPem), TimeSpan.FromSeconds(2), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().NotBeNull();
    }
}
