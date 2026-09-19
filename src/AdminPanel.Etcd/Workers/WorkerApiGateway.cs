using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using AdminPanel.Core;
using Microsoft.Extensions.Options;

namespace AdminPanel.Etcd.Workers;

/// <summary>
/// Шлюз в API воркеров: URL резолвит по живым lease-ключам из снапшота
/// (/pgworker/api/ — EtcdSnapshot, /kafkaworker/api/ — KafkaSnapshot,
/// arch/02 §2.3.1/§2.3.2). Сетевой сбой/таймаут одного URL → следующий ключ
/// (failover); ответ получен (любой статус) → результат с телом как есть;
/// живых нет/все молчат → WorkerApiUnavailableException (503 панели).
/// Аутентификация — mTLS клиентским сертом (WorkerTlsHandler); X-Api-Key
/// удалён для ОБОИХ воркеров (t03).
/// </summary>
public sealed class WorkerApiGateway(
    IOptions<WorkerApiOptions> options,
    IHttpClientFactory factory,
    ISnapshotStore pgStore,
    IKafkaSnapshotStore kafkaStore,
    IValkeySnapshotStore valkeyStore) : IWorkerApiGateway
{
    /// <summary>Имя именованного HttpClient в фабрике (ModuleExtensions.AddEtcd).</summary>
    public const string HttpClientName = "workers";

    private const string JsonContentType = "application/json";

    public async Task<WorkerApiResult> SendAsync(
        string worker, HttpMethod method, string path, object? body, string? requestedBy, CancellationToken ct)
    {
        var endpoints = ResolveEndpoints(worker)
            ?? throw new WorkerApiUnavailableException(worker);

        // Живые ключи детерминированы: сортировка по InstanceId — стабильный
        // порядок failover (не зависит от порядка ответа etcd).
        var ordered = endpoints.OrderBy(e => e.InstanceId, StringComparer.Ordinal).ToArray();
        if (ordered.Length == 0)
            throw new WorkerApiUnavailableException(worker);

        using var client = factory.CreateClient(HttpClientName);
        var seconds = options.Value.TimeoutSec;
        if (seconds > 0)
            client.Timeout = TimeSpan.FromSeconds(seconds);

        foreach (var endpoint in ordered)
        {
            try
            {
                using var response = await SendCoreAsync(client, endpoint, method, path, body, requestedBy, ct);
                var responseBody = await response.Content.ReadAsStringAsync(ct);
                return new WorkerApiResult((int)response.StatusCode, responseBody);
            }
            catch (HttpRequestException)
            {
                // сетевой сбой этого URL — следующий живой ключ
            }
            catch (TaskCanceledException) when (!ct.IsCancellationRequested)
            {
                // таймаут этого URL — следующий живой ключ (отмена Caller'а — проброс)
            }
        }

        throw new WorkerApiUnavailableException(worker);
    }

    /// <summary>
    /// Broadcast на ВСЕ живые endpoints (рестарт, spec §4.4): без failover —
    /// сбой одного инстанса не мешает остальным; per-instance итог.
    /// </summary>
    public async Task<IReadOnlyList<WorkerApiInstanceResult>> SendAllAsync(
        string worker, HttpMethod method, string path, object? body, string? requestedBy, CancellationToken ct)
    {
        var endpoints = ResolveEndpoints(worker)
            ?? throw new WorkerApiUnavailableException(worker);
        var ordered = endpoints.OrderBy(e => e.InstanceId, StringComparer.Ordinal).ToArray();
        if (ordered.Length == 0)
            throw new WorkerApiUnavailableException(worker);

        using var client = factory.CreateClient(HttpClientName);
        var seconds = options.Value.TimeoutSec;
        if (seconds > 0)
            client.Timeout = TimeSpan.FromSeconds(seconds);

        var results = new List<WorkerApiInstanceResult>();
        foreach (var endpoint in ordered)
        {
            try
            {
                using var response = await SendCoreAsync(client, endpoint, method, path, body, requestedBy, ct);
                results.Add(new(endpoint.InstanceId, new WorkerApiResult((int)response.StatusCode, null), null));
            }
            catch (HttpRequestException e)
            {
                results.Add(new(endpoint.InstanceId, null, e.Message)); // сетевой сбой этого URL — остальные продолжаются
            }
            catch (TaskCanceledException e) when (!ct.IsCancellationRequested)
            {
                results.Add(new(endpoint.InstanceId, null, $"таймаут: {e.Message}"));
            }
        }

        return results;
    }

    // Общий запрос-блок failover- и broadcast-обходов: сборка запроса + отправка.
    private static async Task<HttpResponseMessage> SendCoreAsync(
        HttpClient client, WorkerEndpoint endpoint, HttpMethod method, string path,
        object? body, string? requestedBy, CancellationToken ct)
    {
        var request = new HttpRequestMessage(method, new Uri(new Uri(endpoint.Url), path));
        if (requestedBy is not null)
            request.Headers.Add("X-Requested-By", requestedBy);
        if (body is not null)
        {
            request.Content = new StringContent(JsonSerializer.Serialize(body), Encoding.UTF8);
            request.Content.Headers.ContentType = new MediaTypeHeaderValue(JsonContentType);
        }

        return await client.SendAsync(request, ct);
    }

    // Ключи доступа из снапшота соответствующего воркера (Task 11; valkey — t03).
    private IReadOnlyList<WorkerEndpoint>? ResolveEndpoints(string worker) => worker switch
    {
        "pgworker" => pgStore.Current?.PgWorkerEndpoints,
        "kafkaworker" => kafkaStore.Current?.WorkerEndpoints,
        "valkeyworker" => valkeyStore.Current?.WorkerEndpoints,
        _ => throw new ArgumentOutOfRangeException(nameof(worker), worker, "ожидался pgworker|kafkaworker|valkeyworker"),
    };
}
