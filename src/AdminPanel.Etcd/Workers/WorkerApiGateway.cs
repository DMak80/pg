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
    IKafkaSnapshotStore kafkaStore) : IWorkerApiGateway
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
            using var request = new HttpRequestMessage(method, new Uri(new Uri(endpoint.Url), path));
            if (requestedBy is not null)
                request.Headers.Add("X-Requested-By", requestedBy);
            if (body is not null)
            {
                request.Content = new StringContent(JsonSerializer.Serialize(body), Encoding.UTF8);
                request.Content.Headers.ContentType = new MediaTypeHeaderValue(JsonContentType);
            }

            try
            {
                using var response = await client.SendAsync(request, ct);
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

    // Ключи доступа из снапшота соответствующего воркера (Task 11).
    private IReadOnlyList<WorkerEndpoint>? ResolveEndpoints(string worker) => worker switch
    {
        "pgworker" => pgStore.Current?.PgWorkerEndpoints,
        "kafkaworker" => kafkaStore.Current?.WorkerEndpoints,
        _ => throw new ArgumentOutOfRangeException(nameof(worker), worker, "ожидался pgworker|kafkaworker"),
    };
}
