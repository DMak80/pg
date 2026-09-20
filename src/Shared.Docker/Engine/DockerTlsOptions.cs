namespace Shared.Docker;

// TLS к Docker Engine API (arch/14 §2.2.1, t03): per-install docker-CA + клиентская
// пара воркера (deploy/tls/gen-docker.sh). PEM-дуализм env-секретов — значение или
// _PATH-файл. Частичная конфигурация — fail-fast фабрики (DockerEngineFactory).
// env-биндинги PGW_DOCKER_TLS_* — pg-специфика, живут в PgWorker.App (t07).
public sealed class DockerTlsOptions
{
    /// <summary>PEM per-install docker-CA (или CA_PATH файл).</summary>
    public string? CaPem { get; set; }

    public string? CaPath { get; set; }

    /// <summary>PEM клиентского серта воркера (или CERT_PATH файл).</summary>
    public string? ClientCertPem { get; set; }

    public string? ClientCertPath { get; set; }

    /// <summary>PEM приватного ключа PKCS#8 (или KEY_PATH файл).</summary>
    public string? ClientKeyPem { get; set; }

    public string? ClientKeyPath { get; set; }
}
