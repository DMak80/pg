using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Etcd.Workers;

/// <summary>
/// Настройки обращений панели в API воркеров (секция AdminPanel:Workers).
/// PgWorker — X-Api-Key; KafkaWorker — mTLS клиентским сертом (t03, arch/02 §2.3.2).
/// </summary>
[Config("AdminPanel:Workers")]
public sealed class WorkerApiOptions
{
    /// <summary>X-Api-Key для API PgWorker (env-секрет стенда; пусто — не шлётся).</summary>
    public string? PgApiKey { get; set; }

    /// <summary>mTLS-конфигурация обращений в KafkaWorker (t03; pg-ключ не трогается).</summary>
    public KafkaTlsOptions KafkaTls { get; set; } = new();

    /// <summary>Таймаут одного HTTP-вызова (failover перебирает следующие ключи).</summary>
    public int TimeoutSec { get; set; } = 10;

    /// <summary>Опрос /healthz живых инстансов PgWorker (spec D4): выключатель.</summary>
    public bool HealthEnabled { get; set; } = true;

    /// <summary>Интервал опроса /healthz, сек (spec D4; &lt;= 0 — дефолт 15).</summary>
    public int HealthIntervalSec { get; set; } = 15;
}

/// <summary>
/// mTLS для API KafkaWorker (arch/02 §2.3.2, t03): клиентский серт+ключ per-install
/// API-CA и ServerCA для доверия серверу; env KFW_PANEL_TLS_{CERT,KEY,SERVER_CA}[_PATH]
/// (таблица WorkerTlsHandler.EnvBindings).
/// </summary>
public sealed class KafkaTlsOptions
{
    /// <summary>PEM клиентского серта панели (или *_PATH файл).</summary>
    public string? ClientCertPem { get; set; }

    public string? ClientCertPath { get; set; }

    /// <summary>PEM приватного ключа клиента PKCS#8 (или *_PATH файл).</summary>
    public string? ClientKeyPem { get; set; }

    public string? ClientKeyPath { get; set; }

    /// <summary>PEM CA серверного серта KafkaWorker (доверие серверу).</summary>
    public string? ServerCaPem { get; set; }

    public string? ServerCaPath { get; set; }
}