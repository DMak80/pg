namespace AdminPanel.Etcd.Workers;

// Ответ API воркера: статус + сырое тело (ProblemDetails проксируется как есть).
public sealed record WorkerApiResult(int StatusCode, string? Body);

/// <summary>
/// Живых ключей api нет / все URL недоступны → 503-ветка панели.
/// </summary>
public sealed class WorkerApiUnavailableException(string worker)
    : Exception($"API воркера {worker} недоступен: живых ключей доступа нет или все инстансы не отвечают");

/// <summary>
/// Per-instance итог broadcast-вызова (spec §3.3 п.2): Response — любой
/// HTTP-ответ инстанса; Error — сетевой сбой/таймаут этого URL.
/// </summary>
public sealed record WorkerApiInstanceResult(string Instance, WorkerApiResult? Response, string? Error);

/// <summary>
/// HTTP-клиент к API воркеров (arch/01 §1: панель — прокси мутаций, etcd читает).
/// </summary>
public interface IWorkerApiGateway
{
    /// <summary>
    /// Обход ВСЕХ живых endpoints (рестарт, spec §4.4): без failover — каждый
    /// инстанс получает запрос независимо от остальных; per-instance итог.
    /// Живых ключей нет → WorkerApiUnavailableException.
    /// </summary>
    Task<IReadOnlyList<WorkerApiInstanceResult>> SendAllAsync(
        string worker, HttpMethod method, string path, object? body, string? requestedBy, CancellationToken ct);

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
