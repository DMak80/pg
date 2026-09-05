using Microsoft.Extensions.Configuration;

namespace PgWorker.Docker.Engine;

// TLS к Docker Engine API (arch/14 §2.2.1, t03): per-install docker-CA + клиентская
// пара воркера (deploy/tls/gen-docker.sh). PEM-дуализм env-секретов — значение или
// _PATH-файл. Частичная конфигурация — fail-fast фабрики (DockerEngineFactory).
public sealed class DockerTlsOptions
{
    // env-секреты → конфиг-дерево: PEM-значения и _PATH-файлы (паттерн WorkerTlsHandler.EnvBindings).
    public static readonly (string Env, string Key)[] EnvBindings =
    [
        ("PGW_DOCKER_TLS_CA", "PgWorker:Docker:Tls:CaPem"),
        ("PGW_DOCKER_TLS_CERT", "PgWorker:Docker:Tls:ClientCertPem"),
        ("PGW_DOCKER_TLS_KEY", "PgWorker:Docker:Tls:ClientKeyPem"),
        ("PGW_DOCKER_TLS_CA_PATH", "PgWorker:Docker:Tls:CaPath"),
        ("PGW_DOCKER_TLS_CERT_PATH", "PgWorker:Docker:Tls:ClientCertPath"),
        ("PGW_DOCKER_TLS_KEY_PATH", "PgWorker:Docker:Tls:ClientKeyPath"),
    ];

    /// <summary>PEM per-install docker-CA (или CA_PATH файл).</summary>
    public string? CaPem { get; set; }

    public string? CaPath { get; set; }

    /// <summary>PEM клиентского серта воркера (или CERT_PATH файл).</summary>
    public string? ClientCertPem { get; set; }

    public string? ClientCertPath { get; set; }

    /// <summary>PEM приватного ключа PKCS#8 (или KEY_PATH файл).</summary>
    public string? ClientKeyPem { get; set; }

    public string? ClientKeyPath { get; set; }

    // Перенос env → конфиг; getenv-инъекция — для юнит-теста (без окружения).
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
}
