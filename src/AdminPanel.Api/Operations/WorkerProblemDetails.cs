using System.Text.Json;
using AdminPanel.Etcd.Workers;

namespace AdminPanel.Api.Operations;

/// <summary>
/// Ошибка API воркера: статус + сырое тело ProblemDetails (проксируется панели
/// как есть — UI-контракт не меняется, arch/03 §1). Модуль отдаёт
/// Results.Text(Body, "application/problem+json", StatusCode).
/// </summary>
public sealed class WorkerProblemDetails(int statusCode, string body)
    : Exception($"worker API responded {statusCode}")
{
    public int StatusCode { get; } = statusCode;

    public string Body { get; } = body;

    public static WorkerProblemDetails From(WorkerApiResult resp)
        => new(resp.StatusCode, resp.Body ?? "");
}

/// <summary>
/// Общий скелет прокси-вызова (task etcd-via-worker-api): 2xx → десериализация
/// DTO; иной статус → WorkerProblemDetails (тело воркера как есть); недоступность
/// API (живых ключей нет/все URL молчат) → Failed(WorkerApiUnavailableException)
/// — модуль панели отвечает 503 «API воркера недоступен».
/// </summary>
internal static class WorkerProxy
{
    // Minimal API воркера сериализует Web-camelCase — так же читаем.
    public static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    public static async ValueTask<Result<T>> SendAsync<T>(
        IWorkerApiGateway api, string worker, HttpMethod method, string path,
        object? body, string? requestedBy, CancellationToken ct) where T : class
    {
        WorkerApiResult resp;
        try
        {
            resp = await api.SendAsync(worker, method, path, body, requestedBy, ct);
        }
        catch (WorkerApiUnavailableException e)
        {
            return Result<T>.Failed(e);
        }

        if (resp.StatusCode is >= 200 and < 300)
        {
            // 204-мутации (DELETE) отвечают без тела — DTO модуль не использует.
            var dto = string.IsNullOrEmpty(resp.Body)
                ? default
                : JsonSerializer.Deserialize<T>(resp.Body, Json);
            return Result<T>.Success(dto!);
        }

        return Result<T>.Failed(WorkerProblemDetails.From(resp));
    }

    // Результат прокси с исходным кодом воркера (для зеркалирования 202|204/201|200).
    // Объявляется на уровне namespace AdminPanel.Api.Operations (НЕ внутри WorkerProxy —
    // команды Operations.S3 ссылаются на него без квалификации).
    public static async ValueTask<Result<WorkerRaw<T>>> SendWithStatusAsync<T>(
        IWorkerApiGateway api, string worker, HttpMethod method, string path,
        object? body, string? requestedBy, CancellationToken ct) where T : class
    {
        WorkerApiResult resp;
        try
        {
            resp = await api.SendAsync(worker, method, path, body, requestedBy, ct);
        }
        catch (WorkerApiUnavailableException e)
        {
            return Result<WorkerRaw<T>>.Failed(e);
        }

        if (resp.StatusCode is >= 200 and < 300)
        {
            // DELETE- и deny/allow-мутации различают 202 (записано) и 204 (already),
            // ротация — 201/200: Success несёт исходный код, модуль зеркалит 1:1.
            var dto = string.IsNullOrEmpty(resp.Body)
                ? default
                : JsonSerializer.Deserialize<T>(resp.Body, Json);
            return Result<WorkerRaw<T>>.Success(new WorkerRaw<T>(resp.StatusCode, dto));
        }

        return Result<WorkerRaw<T>>.Failed(WorkerProblemDetails.From(resp));
    }
}

/// <summary>
/// Прокси-ответ с исходным статусом воркера (t34, arch/02 §12.2): Value — DTO
/// или null (204 без тела).
/// </summary>
public sealed record WorkerRaw<T>(int StatusCode, T? Value);
