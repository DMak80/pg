using System.Security.Cryptography;
using Microsoft.Extensions.Configuration;

namespace PgWorker.Docker.Engine;

// SSH-туннель к Engine API (arch/14 §2.2.1, t03): worker-managed
// ForwardedPortLocal → RemoteDaemonHost:RemoteDaemonPort. key-аутентификация
// (пароли вне канона); fingerprint-pin опционален (без него TOFU+warning, R14).
public sealed class SshTunnelOptions
{
    // env-секреты → конфиг-дерево (паттерн WorkerTlsHandler.EnvBindings).
    public static readonly (string Env, string Key)[] EnvBindings =
    [
        ("PGW_DOCKER_SSH_KEY", "PgWorker:Docker:Ssh:KeyPem"),
        ("PGW_DOCKER_SSH_KEY_PATH", "PgWorker:Docker:Ssh:KeyPath"),
        ("PGW_DOCKER_SSH_FINGERPRINT", "PgWorker:Docker:Ssh:FingerprintSha256"),
    ];

    /// <summary>PEM приватного ключа PKCS#8/OpenSSL RSA (или KEY_PATH файл).</summary>
    public string? KeyPem { get; set; }

    public string? KeyPath { get; set; }

    /// <summary>Адрес daemon-порта НА удалённом хосте (loopback демона).</summary>
    public string RemoteDaemonHost { get; set; } = "127.0.0.1";

    /// <summary>Порт daemon-порта на удалённом хосте (канон: 2376 c --tlsverify).</summary>
    public int RemoteDaemonPort { get; set; } = 2376;

    /// <summary>SHA-256 fingerprint хост-ключа (ssh-keygen-формат, с/без «SHA256:»);
    /// null — TOFU-accept + warning (R14).</summary>
    public string? FingerprintSha256 { get; set; }

    /// <summary>Keepalive SSH-сессии, сек.</summary>
    public int KeepAliveSec { get; set; } = 15;

    /// <summary>Бюджет подключения/аутентификации, сек.</summary>
    public int ConnectTimeoutSec { get; set; } = 10;

    public static void ApplyEnvOverrides(ConfigurationManager configuration, Func<string, string?>? getenv = null)
    {
        getenv ??= Environment.GetEnvironmentVariable;
        foreach (var (env, key) in EnvBindings)
        {
            var value = getenv(env);
            if (!string.IsNullOrWhiteSpace(value))
                configuration[key] = value;
        }
    }

    // Цель форварда на удалённом хосте (чистая функция — юнит-тесты без сети,
    // spec §5.5 «target-вычисление туннеля»): валидация host/порта.
    public (string Host, int Port) TunnelTarget()
    {
        if (string.IsNullOrWhiteSpace(RemoteDaemonHost) || RemoteDaemonPort is < 1 or > 65535)
            throw new ApplicationException(
                $"PgWorker:Docker:Ssh: некорректная цель туннеля {RemoteDaemonHost}:{RemoteDaemonPort} (arch/14 §2.2.1)");
        return (RemoteDaemonHost, RemoteDaemonPort);
    }

    // Семантика host-key (юнит-тестируема без сети): pin задан — строгое
    // сравнение SHA-256 (нормализация префикса/паддинга); не задан — TOFU-accept
    // (trustByTofu=true — вызывающий логирует warning единожды на хост).
    public static bool DecideHostKeyTrust(byte[] hostKeyData, string? expectedSha256, out bool trustByTofu)
    {
        trustByTofu = false;
        var actual = Convert.ToBase64String(SHA256.HashData(hostKeyData)).TrimEnd('=');
        if (expectedSha256 is not { Length: > 0 })
        {
            trustByTofu = true;
            return true;
        }

        var expected = expectedSha256.Trim();
        if (expected.StartsWith("SHA256:", StringComparison.Ordinal))
            expected = expected["SHA256:".Length..];
        expected = expected.TrimEnd('=');
        return string.Equals(actual, expected, StringComparison.Ordinal);
    }
}
