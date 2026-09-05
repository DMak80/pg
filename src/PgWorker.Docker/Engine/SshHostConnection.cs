using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Renci.SshNet;

namespace PgWorker.Docker.Engine;

// SSH-туннель к Engine API (arch/14 §2.2.1, t03, О3): одна SshClient-сессия
// (key-аутентификация, fingerprint-pin/TOFU) + ForwardedPortLocal
// ("127.0.0.1":0 → RemoteDaemonHost:RemoteDaemonPort); фактический bound-порт
// отдаётся фабрике — Engine-клиент ходит на него как на обычный tcp:// (+TLS
// сверху при заданном Docker:Tls — канон daemon --tlsverify). Штатные вызовы
// движка о туннеле не знают. Разрыв — transient: EnsureConnected() переподключает
// с бэкоффом, тики healthz/надзора честно видят хост недоступным.
public sealed class SshHostConnection : IAsyncDisposable
{
    private const int ReconnectBackoffSec = 5;

    private readonly SshClient _client;
    private readonly ForwardedPortLocal _port;
    private readonly ILogger _logger;
    private readonly string _endpoint;
    private readonly string? _fingerprint;
    private readonly bool[] _tofuWarned = [false]; // warning единожды на хост
    private DateTimeOffset _lastAttempt = DateTimeOffset.MinValue;

    public int BoundPort { get; }

    // fingerprint последнего хендшейка (диагностика TOFU; тест pin-кейса).
    public string? FingerprintSha256 { get; private set; }

    public bool IsConnected => _client.IsConnected && _port.IsStarted;

    public SshHostConnection(EndpointScheme scheme, SshTunnelOptions options, ILogger? logger = null)
    {
        _endpoint = $"ssh://{scheme.User}@{scheme.Host}:{scheme.Port}";
        _logger = logger ?? NullLogger.Instance;
        var keyPem = ReadKeyMaterial(options)
            ?? throw new ApplicationException(
                $"PgWorker:Docker:Ssh: ключ не задан (env PGW_DOCKER_SSH_KEY[_PATH]) для {_endpoint}, arch/14 §2.2.1");
        _fingerprint = options.FingerprintSha256;
        var keyFile = new PrivateKeyFile(new MemoryStream(Encoding.UTF8.GetBytes(keyPem)));
        var user = scheme.User ?? "root"; // дефолт docker-хостов: операторские демоны
        var connection = new ConnectionInfo(scheme.Host, scheme.Port, user, new PrivateKeyAuthenticationMethod(user, keyFile))
        {
            Timeout = TimeSpan.FromSeconds(Math.Max(1, options.ConnectTimeoutSec)),
        };
        _client = new SshClient(connection) { KeepAliveInterval = TimeSpan.FromSeconds(Math.Max(1, options.KeepAliveSec)) };
        _client.HostKeyReceived += (_, e) =>
        {
            var trust = SshTunnelOptions.DecideHostKeyTrust(e.HostKey, _fingerprint, out var tofu);
            FingerprintSha256 = "SHA256:" + Convert.ToBase64String(SHA256.HashData(e.HostKey)).TrimEnd('=');
            if (trust && tofu && !_tofuWarned[0])
            {
                _tofuWarned[0] = true;
                _logger.LogWarning(
                    "SSH host-key {Endpoint} принят по TOFU (fingerprint {Fingerprint}); закрепите PGW_DOCKER_SSH_FINGERPRINT (R14)",
                    _endpoint, FingerprintSha256);
            }

            e.CanTrust = trust;
        };

        var (targetHost, targetPort) = options.TunnelTarget(); // target — валидированная чистая функция (юнит-тест)
        _port = new ForwardedPortLocal("127.0.0.1", 0, targetHost, checked((uint)targetPort));
        // Порядок важен: сессия (host-key → fingerprint) — затем регистрация и
        // старт форварда (SSH.NET 2026: AddForwardedPort требует открытой сессии).
        Connect();
        _client.AddForwardedPort(_port);
        _port.Start(); // bound-порт выделяется здесь (порт 0 → фактический)
        BoundPort = (int)_port.BoundPort;
    }

    // Connect/reconnect с бэкоффом: подряд идущие попытки не чаще
    // ReconnectBackoffSec (иначе — ApplicationException-transient на тик).
    public void EnsureConnected()
    {
        if (IsConnected)
            return;
        if (DateTimeOffset.UtcNow - _lastAttempt < TimeSpan.FromSeconds(ReconnectBackoffSec))
            throw new ApplicationException($"SSH-туннель {_endpoint} разорван — повторная попытка после бэкоффа (transient)");
        Connect();
    }

    private void Connect()
    {
        _lastAttempt = DateTimeOffset.UtcNow;
        try
        {
            _client.Connect();
        }
        catch (Exception e)
        {
            throw new ApplicationException($"SSH-подключение {_endpoint} не удалось (transient): {e.Message}", e);
        }
    }

    // PEM с файловым fallback (PKCS#8/OpenSSL — формат PrivateKeyFile SSH.NET).
    internal static string? ReadKeyMaterial(SshTunnelOptions options)
    {
        if (!string.IsNullOrWhiteSpace(options.KeyPem))
            return options.KeyPem;
        return options.KeyPath is not null && File.Exists(options.KeyPath)
            ? File.ReadAllText(options.KeyPath).Trim()
            : null;
    }

    public ValueTask DisposeAsync()
    {
        try
        {
            if (_port.IsStarted)
                _port.Stop();
            _port.Dispose();
            if (_client.IsConnected)
                _client.Disconnect();
            _client.Dispose();
        }
        catch (Exception e)
        {
            _logger.LogWarning("закрытие SSH-туннеля {Endpoint}: {Message}", _endpoint, e.Message);
        }

        return ValueTask.CompletedTask;
    }
}
