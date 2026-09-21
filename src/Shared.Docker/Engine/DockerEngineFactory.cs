using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Logging;
using Shared.Tls;

namespace Shared.Docker;

// Фабрика движков (arch/14 §2.2/§2.2.1, t03): endpoint "unix:///var/run/docker.sock"
// | "tcp://host[:2375]" (+TLS при заданном DockerTlsOptions) | "ssh://[user@]host[:22]"
// (туннель — SshTunnelOptions). API-версия закреплена v1.44 (docker >= 23).
// Общая фабрика трёх воркеров (t07): SSH/TLS — опции, доступные всем доменам
// (kfw/vwk `new DockerEngineFactory()` валиден — опции optional).
public class DockerEngineFactory : IAsyncDisposable
{
    private readonly DockerTlsMaterial? _tls;
    private readonly SshTunnelOptions? _ssh;
    private readonly ILogger? _logger;
    private readonly ILoggerFactory? _loggerFactory;
    private readonly Dictionary<string, SshHostConnection> _tunnels = new();
    private readonly object _tunnelsLock = new();

    // Fail-fast здесь (а не в тике): частичная TLS-конфигурация — ошибка старта.
    public DockerEngineFactory(
        DockerTlsOptions? tls = null,
        SshTunnelOptions? ssh = null,
        ILogger? logger = null,
        ILoggerFactory? loggerFactory = null)
    {
        _ssh = ssh;
        _logger = logger ?? loggerFactory?.CreateLogger<DockerEngineFactory>();
        _loggerFactory = loggerFactory;
        _tls = tls is null ? null : DockerTlsMaterial.Load(tls);
    }

    // Транспортный handler: unix → ConnectCallback с UnixDomainSocketEndPoint;
    // tcp → TLS (клиентский серт + цепочка против docker-CA), если сконфигурирован.
    internal HttpMessageHandler CreateHandler(string endpoint)
    {
        var scheme = EndpointScheme.Parse(endpoint);
        var sockets = new SocketsHttpHandler
        {
            // docker-прокси держит соединения — не рвём их агрессивно
            PooledConnectionLifetime = TimeSpan.FromMinutes(5),
        };
        if (scheme.Scheme == EndpointScheme.Unix)
        {
            var socketPath = scheme.Host;
            sockets.ConnectCallback = async (context, ct) =>
            {
                var socket = new System.Net.Sockets.Socket(
                    System.Net.Sockets.AddressFamily.Unix, System.Net.Sockets.SocketType.Stream,
                    System.Net.Sockets.ProtocolType.Unspecified);
                try
                {
                    await socket.ConnectAsync(new System.Net.Sockets.UnixDomainSocketEndPoint(socketPath), ct);
                    return new System.Net.Sockets.NetworkStream(socket, ownsSocket: true);
                }
                catch
                {
                    socket.Dispose();
                    throw;
                }
            };
        }
        else if (_tls is not null)
        {
            // tcp (+TLS поверх ssh-туннеля — endpoint уже tcp://127.0.0.1:<bound>)
            sockets.SslOptions.ClientCertificates = new X509CertificateCollection { _tls.ClientCert };
            sockets.SslOptions.RemoteCertificateValidationCallback =
                (_, certificate, _, _) => DockerTlsMaterial.ValidateChain(certificate, _tls.Ca);
        }
        else if (scheme.Scheme == EndpointScheme.Tcp)
        {
            // R15: plaintext tcp — только dev/тесты/локальные стенды; канон прода —
            // 2376+mTLS или ssh (arch/14 §2.2.1).
            _logger?.LogWarning(
                "Engine API {Endpoint} без TLS (plaintext tcp; канон прода — tcp://:2376 mTLS или ssh://, arch/14 §2.2.1)",
                endpoint);
        }

        return sockets;
    }

    // hostAlias — имя docker-хоста для BusyPorts plain-режима (swarm: null).
    public virtual IDockerEngine Create(string endpoint, string? hostAlias = null)
    {
        var scheme = EndpointScheme.Parse(endpoint);
        if (scheme.Scheme == EndpointScheme.Ssh)
            endpoint = $"tcp://127.0.0.1:{TunnelFor(endpoint, scheme).BoundPort}";

        var parsedEndpoint = EndpointScheme.Parse(endpoint);
        var baseAddress = scheme.Scheme == EndpointScheme.Unix
            ? "http://localhost" // фиктивный хост: соединение уходит в unix-сокет через ConnectCallback
            : (_tls is not null
                ? $"https://{parsedEndpoint.Host}:{parsedEndpoint.Port}"
                : $"http://{parsedEndpoint.Host}:{parsedEndpoint.Port}"); // HttpClient не понимает tcp://
        var httpClient = new HttpClient(CreateHandler(endpoint)) { BaseAddress = new Uri(baseAddress) };
        return new DockerEngine(httpClient, hostAlias);
    }

    // кэш туннелей по endpoint: подключённый — переиспользуем; разорванный —
    // reconnect с бэкоффом (EnsureConnected бросает transient-ошибку на тик).
    private SshHostConnection TunnelFor(string endpoint, EndpointScheme scheme)
    {
        lock (_tunnelsLock)
        {
            if (_tunnels.TryGetValue(endpoint, out var existing))
            {
                existing.EnsureConnected();
                return existing;
            }

            var tunnel = new SshHostConnection(scheme, _ssh ?? new SshTunnelOptions(),
                _loggerFactory?.CreateLogger<SshHostConnection>());
            _tunnels[endpoint] = tunnel;
            return tunnel;
        }
    }

    public async ValueTask DisposeAsync()
    {
        SshHostConnection[] tunnels;
        lock (_tunnelsLock)
        {
            tunnels = [.. _tunnels.Values];
            _tunnels.Clear();
        }

        foreach (var tunnel in tunnels)
            await tunnel.DisposeAsync();
    }
}

// Загруженный TLS-материал фабрики: живёт время жизни фабрики (валидация цепочки
// вызывается на КАЖДОМ хендшейке — без using; паттерн WorkerTlsHandler.Build).
internal sealed class DockerTlsMaterial
{
    public required X509Certificate2 ClientCert { get; init; }

    public required X509Certificate2 Ca { get; init; }

    // PEM с файловым fallback; частичная конфигурация → ApplicationException.
    public static DockerTlsMaterial Load(DockerTlsOptions tls)
    {
        var caPem = tls.CaPem ?? TlsMaterial.ReadPemFile(tls.CaPath);
        var certPem = tls.ClientCertPem ?? TlsMaterial.ReadPemFile(tls.ClientCertPath);
        var keyPem = tls.ClientKeyPem ?? TlsMaterial.ReadPemFile(tls.ClientKeyPath);
        if (caPem is null || certPem is null || keyPem is null)
            throw new ApplicationException(
                "PgWorker:Docker:Tls: частичная TLS-конфигурация — нужны CA+CERT+KEY "
                + "(env PGW_DOCKER_TLS_{CA,CERT,KEY}[_PATH], arch/14 §2.2.1)");

        // PFX round-trip: ключ CreateFromPem эфемерный — macOS SslStream требует
        // ре-импорт (паттерн WorkerTlsHandler.Build).
        var clientCert = TlsMaterial.LoadPemPair(certPem, keyPem);
        return new DockerTlsMaterial
        {
            ClientCert = clientCert,
            Ca = TlsMaterial.LoadPem(caPem),
        };
    }

    // Цепочка серверного серта демона против per-install docker-CA — обёртка
    // над общим TlsChain.ValidateChain (t08: тело в Shared.Tls).
    public static bool ValidateChain(X509Certificate? certificate, X509Certificate2 ca)
        => TlsChain.ValidateChain(certificate, ca);
}
