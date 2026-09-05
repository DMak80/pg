using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Etcd.Workers;

/// <summary>
/// Настройки обращений панели в API воркеров (секция AdminPanel:Workers).
/// Оба воркера — mTLS клиентским сертом (t03, arch/02 §2.3.2): X-Api-Key удалён.
/// </summary>
[Config("AdminPanel:Workers")]
public sealed class WorkerApiOptions
{
    /// <summary>mTLS обращений в API ОБОИХ воркеров (t03-pg): единый клиентский
    /// серт per-install API-CA + ServerCA.</summary>
    public WorkerTlsOptions WorkerTls { get; set; } = new();

    /// <summary>Таймаут одного HTTP-вызова (failover перебирает следующие ключи).</summary>
    public int TimeoutSec { get; set; } = 10;

    /// <summary>Опрос /healthz живых инстансов PgWorker (spec D4): выключатель.</summary>
    public bool HealthEnabled { get; set; } = true;

    /// <summary>Интервал опроса /healthz, сек (spec D4; &lt;= 0 — дефолт 15).</summary>
    public int HealthIntervalSec { get; set; } = 15;
}

/// <summary>
/// mTLS обращений в API ОБОИХ воркеров (arch/02 §2.3.2, t03): единый клиентский
/// серт per-install API-CA + ServerCA для доверия серверу; env
/// WORKERS_PANEL_TLS_{CERT,KEY,SERVER_CA}[_PATH] (таблица WorkerTlsHandler.EnvBindings).
/// </summary>
public sealed class WorkerTlsOptions
{
    /// <summary>PEM клиентского серта панели (или *_PATH файл).</summary>
    public string? ClientCertPem { get; set; }

    public string? ClientCertPath { get; set; }

    /// <summary>PEM приватного ключа клиента PKCS#8 (или *_PATH файл).</summary>
    public string? ClientKeyPem { get; set; }

    public string? ClientKeyPath { get; set; }

    /// <summary>PEM CA серверных сертов воркеров (доверие серверу).</summary>
    public string? ServerCaPem { get; set; }

    public string? ServerCaPath { get; set; }
}
