namespace AdminPanel.Etcd.Workers;

// Ответ API воркера: статус + сырое тело (ProblemDetails проксируется как есть).
public sealed record WorkerApiResult(int StatusCode, string? Body);

/// <summary>
/// Живых ключей api нет / все URL недоступны → 503-ветка панели.
/// </summary>
public sealed class WorkerApiUnavailableException(string worker)
    : Exception($"API воркера {worker} недоступен: живых ключей доступа нет или все инстансы не отвечают");

/// <summary>
/// HTTP-клиент к API воркеров (arch/01 §1: панель — прокси мутаций, etcd читает).
/// </summary>
public interface IWorkerApiGateway
{
    /// <summary>
    /// worker: "pgworker" | "kafkaworker"; path — "/api/clusters" и т.п.; body — DTO запроса.
    /// requestedBy — имя оператора сессии панели: шлюз шлёт его заголовком
    /// X-Requested-By на ВСЕХ мутациях (сквозная идентичность оператора,
    /// spec §3.7 — значения etcd не меняются при переходе на прокси);
    /// null → заголовок не шлётся (воркерский fallback "api").
    /// </summary>
    Task<WorkerApiResult> SendAsync(
        string worker, HttpMethod method, string path, object? body, string? requestedBy, CancellationToken ct);
}
