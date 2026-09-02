using AdminPanel.Core;
using AdminPanel.Infrastructure.DI;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AdminPanel.Etcd.Workers;

// Тик опроса /healthz живых инстансов PgWorker (spec §3.4 D4; arch/adminpanel/02
// §2.3.1): 200 → Healthy, 503 → Degraded, сетевой сбой/таймаут → Unreachable
// (lease жив — панель «недавно видела» воркера). /healthz не под X-Api-Key.
[InjectAsSingleton(typeof(IHostedService))]
public sealed class WorkerHealthPoller(
    ISnapshotReader snapshotReader,
    IWorkerHealthStore store,
    IKafkaSnapshotReader kafkaSnapshotReader,
    IKafkaWorkerHealthStore kafkaStore,
    IHttpClientFactory factory,
    IOptions<WorkerApiOptions> options,
    TimeProvider time,
    ILogger<WorkerHealthPoller> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var value = options.Value;
        if (!value.HealthEnabled)
        {
            logger.LogInformation("AdminPanel:Workers:HealthEnabled=false — опрос /healthz не запускается");
            return;
        }

        var seconds = value.HealthIntervalSec > 0 ? value.HealthIntervalSec : 15;
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(seconds));
        do
        {
            await RunOnceAsync(stoppingToken);
        }
        while (await timer.WaitForNextTickAsync(stoppingToken));
    }

    // Ядро тика — публично для unit-тестов (прецедент RefreshOnceAsync).
    public async Task RunOnceAsync(CancellationToken ct)
    {
        var endpoints = snapshotReader.Current?.PgWorkerEndpoints ?? [];
        var at = time.GetUtcNow();
        var results = await Task.WhenAll(endpoints.Select(e => ProbeAsync(e, at, ct)));
        store.Replace([.. results.OrderBy(r => r.InstanceId, StringComparer.Ordinal)]);

        // KafkaWorker-инстансы (t09; arch/adminpanel/02 §2.3.2): тот же тик/клиент/
        // семантика — 200 → Healthy, 503 → Degraded, сетевой сбой → Unreachable;
        // /healthz не под X-Api-Key (ApiKeyMiddleware проверяет только /api).
        var kafkaEndpoints = kafkaSnapshotReader.Current?.WorkerEndpoints ?? [];
        var kafkaAt = time.GetUtcNow();
        var kafkaResults = await Task.WhenAll(kafkaEndpoints.Select(e => ProbeAsync(e, kafkaAt, ct)));
        kafkaStore.Replace([.. kafkaResults.OrderBy(r => r.InstanceId, StringComparer.Ordinal)]);
    }

    private async Task<WorkerHealth> ProbeAsync(WorkerEndpoint endpoint, DateTimeOffset at, CancellationToken ct)
    {
        using var client = factory.CreateClient(WorkerApiGateway.HttpClientName);
        var timeout = options.Value.TimeoutSec;
        if (timeout > 0)
            client.Timeout = TimeSpan.FromSeconds(timeout);
        try
        {
            using var response = await client.GetAsync(new Uri(new Uri(endpoint.Url), "/healthz"), ct);
            var status = response.IsSuccessStatusCode
                ? WorkerHealthStatus.Healthy
                : WorkerHealthStatus.Degraded; // 503 и прочие — процесс жив, но нездоров
            return new WorkerHealth(endpoint.InstanceId, endpoint.Url, status, at,
                status == WorkerHealthStatus.Healthy ? null : $"HTTP {(int)response.StatusCode}");
        }
        catch (Exception e) when (e is HttpRequestException or TaskCanceledException)
        {
            return new WorkerHealth(endpoint.InstanceId, endpoint.Url, WorkerHealthStatus.Unreachable, at, e.Message);
        }
    }
}
