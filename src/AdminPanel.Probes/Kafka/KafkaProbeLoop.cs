using AdminPanel.Core;
using AdminPanel.Core.Kafka;
using AdminPanel.Etcd;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AdminPanel.Probes.Kafka;

// Стор состояния kafka-проб (pg-аналог IProbeStateStore): писатель один —
// KafkaProbeLoop; читают KafkaSnapshotRefresher (Probes) и инспекция (live).
public interface IKafkaProbeStore
{
    KafkaProbeState? Current { get; }

    void Replace(KafkaProbeState state);
}

public sealed class KafkaProbeStore : IKafkaProbeStore
{
    private volatile KafkaProbeState? _current;

    public KafkaProbeState? Current => _current;

    public void Replace(KafkaProbeState state) => _current = state;
}

// Результат пробы одного кластера: live-данные + ProbeResult (kind "kafka").
internal sealed record KafkaProbeOutcome(KafkaClusterLive? Live, ProbeResult Result);

/// <summary>
/// Фоновый тик kafka-пробы (план B6): per-кластерный DescribeCluster по
/// endpoints из etcd (через HostMap) с SASL из internal-стора кредов; пароль
/// в результаты не попадает. Ошибка пробы не роняет etcd-часть панели.
/// </summary>
public sealed class KafkaProbeLoop(
    IKafkaSnapshotReader snapshotReader,
    IKafkaSecretsStore secrets,
    IKafkaProbeClient client,
    IKafkaProbeStore store,
    IOptions<KafkaProbeOptions> kafkaOptions,
    IOptions<ProbesOptions> probesOptions,
    TimeProvider time,
    ILogger<KafkaProbeLoop> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!kafkaOptions.Value.Enabled)
        {
            logger.LogInformation("AdminPanel:Probes:Kafka: проба выключена — тик не запускается");
            return;
        }

        var seconds = kafkaOptions.Value.IntervalSeconds;
        if (seconds <= 0)
        {
            logger.LogWarning("AdminPanel:Probes:Kafka:IntervalSeconds <= 0 — использую 15 c");
            seconds = 15;
        }

        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(seconds));
        do
        {
            try
            {
                await RunOnceAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }
        while (await timer.WaitForNextTickAsync(stoppingToken));
    }

    // Ядро тика — публично для unit-тестов без хоста.
    public async Task RunOnceAsync(CancellationToken ct)
    {
        var at = time.GetUtcNow();
        var snapshot = snapshotReader.Current;
        if (snapshot is null)
            return; // etcd-снапшота ещё нет — пробать нечего

        var results = new List<ProbeResult>();
        var live = new Dictionary<string, KafkaClusterLive>();

        // Цели: Active-кластеры с endpoints (поднятые); без кредов — ошибка пробы.
        foreach (var cluster in snapshot.Clusters.Where(c =>
                     c.State == KafkaClusterState.Active && !string.IsNullOrEmpty(c.Endpoints)))
        {
            var outcome = await ProbeAsync(cluster, at, ct);
            results.Add(outcome.Result);
            if (outcome.Live is not null)
                live[cluster.Name] = outcome.Live;
        }

        results.Sort((a, b) => string.CompareOrdinal(a.Target, b.Target));
        store.Replace(new KafkaProbeState(at, results, live));
    }

    private async Task<KafkaProbeOutcome> ProbeAsync(
        KafkaClusterInfo cluster, DateTimeOffset at, CancellationToken ct)
    {
        var timeout = TimeSpan.FromSeconds(
            kafkaOptions.Value.TimeoutSeconds > 0 ? kafkaOptions.Value.TimeoutSeconds : 3);

        // Bootstrap: каждый адрес endpoints через HostMap (стенд: advertised
        // host.docker.internal:<port> → localhost:<port> — симметрия A2/A13).
        var hostMap = probesOptions.Value.HostMap;
        var bootstrap = string.Join(",", cluster.Endpoints!.Split(',')
            .Select(address =>
            {
                var parts = address.Split(':');
                return parts.Length == 2 && int.TryParse(parts[1], out var port)
                    ? HostMapResolver.Resolve(hostMap, parts[0], port)
                    : address;
            }));

        if (!secrets.Current.TryGetValue(cluster.Name, out var creds))
            return new KafkaProbeOutcome(null, new ProbeResult(
                cluster.Name, "kafka", false, null,
                "нет app-кредов в etcd (кластер не поднят или ensure не выполнен)", at));

        var view = await client.DescribeClusterAsync(bootstrap, creds.User, creds.Password, timeout, ct);
        if (!view.IsSuccess)
        {
            // Пароль (creds) в ошибку не попадает — только bootstrap-адрес.
            return new KafkaProbeOutcome(null, new ProbeResult(
                cluster.Name, "kafka", false, null, view.Error!.Message, at));
        }

        var brokers = view.Value.Brokers
            .Select(b => new KafkaBrokerLive(b.Id, b.Host, b.Id == view.Value.ControllerId))
            .ToList();
        return new KafkaProbeOutcome(
            new KafkaClusterLive(cluster.Name, at, brokers),
            new ProbeResult(cluster.Name, "kafka", true, null, null, at));
    }
}
