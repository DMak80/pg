using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Etcd.Workers;

/// <summary>
/// Настройки обращений панели в API воркеров (секция AdminPanel:Workers).
/// Ключи — по воркерам (X-Api-Key), на стенде пусто (доверенная docker-сеть).
/// </summary>
[Config("AdminPanel:Workers")]
public sealed class WorkerApiOptions
{
    /// <summary>X-Api-Key для API PgWorker (env-секрет стенда; пусто — не шлётся).</summary>
    public string? PgApiKey { get; set; }

    /// <summary>X-Api-Key для API KafkaWorker (env-секрет стенда; пусто — не шлётся).</summary>
    public string? KafkaApiKey { get; set; }

    /// <summary>Таймаут одного HTTP-вызова (failover перебирает следующие ключи).</summary>
    public int TimeoutSec { get; set; } = 10;
}
