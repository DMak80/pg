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

// Backoff недоступного кластера (t11): сколько проб подряд упало, когда
// разрешена следующая и текст последней ошибки (живёт в ProbeResult.skip-тика).
internal sealed record ClusterBackoffState(
    int ConsecutiveFailures,
    DateTimeOffset NextAttemptUtc,
    string LastError);

/// <summary>
/// Фоновый тик kafka-пробы (план B6): per-кластерный DescribeCluster по
/// endpoints из etcd (через HostMap) с admin-кредами/CA из internal-стора
/// (t03: SASL_SSL/PLAIN, arch/15 §5); пароль в результаты не попадает.
/// Ошибка пробы не роняет etcd-часть панели.
/// </summary>
public sealed class KafkaProbeLoop(
    IKafkaSnapshotReader snapshotReader,
    IKafkaSecretsStore secrets,
    IKafkaProbeClient client,
    IKafkaProbeStore store,
    IOptions<KafkaProbeOptions> kafkaOptions,
    IOptions<ProbesOptions> probesOptions,
    TimeProvider time,
    ILogger<KafkaProbeLoop> logger,
    IKafkaProbeRuntimeClient? runtimeClient = null) : BackgroundService
{
    // Backoff per-кластер (t11): писатель один — тик loop; success сбрасывает.
    private readonly Dictionary<string, ClusterBackoffState> _backoff = [];

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
        var targets = snapshot.Clusters.Where(c =>
            c.State == KafkaClusterState.Active && !string.IsNullOrEmpty(c.Endpoints)).ToList();

        foreach (var cluster in targets)
        {
            // Backoff недоступного кластера (t11): окно не истекло — тик
            // пропускается, последняя ошибка остаётся в состоянии с пометкой
            // (кластер не мерцает, мёртвые endpoints не штурмуются каждые 15 c).
            if (_backoff.TryGetValue(cluster.Name, out var backoff)
                && at < backoff.NextAttemptUtc)
            {
                results.Add(new ProbeResult(
                    cluster.Name, "kafka", false, null,
                    $"{backoff.LastError}; backoff {backoff.ConsecutiveFailures} неудач подряд, следующая проба ~{backoff.NextAttemptUtc:HH:mm:ss}Z",
                    at));
                continue;
            }

            var outcome = await ProbeAsync(cluster, at, ct);
            results.Add(outcome.Result);
            if (outcome.Live is not null)
                live[cluster.Name] = outcome.Live;
        }

        // Кластеры исчезли из etcd — backoff-состояние не копится.
        var targetNames = targets.Select(c => c.Name).ToHashSet();
        foreach (var gone in _backoff.Keys.Where(name => !targetNames.Contains(name)).ToList())
            _backoff.Remove(gone);

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
                "нет admin-кредов/CA кластера в etcd (премиграционный кластер или ensure не выполнен)", at));

        var view = await client.DescribeClusterAsync(
            bootstrap, creds.AdminUser, creds.AdminPassword, creds.CaPem, timeout, ct);
        if (!view.IsSuccess)
        {
            // Пароль (creds) в ошибку не попадает — только bootstrap-адрес.
            TrackFailure(cluster.Name, view.Error!.Message, at);
            return new KafkaProbeOutcome(null, new ProbeResult(
                cluster.Name, "kafka", false, null, view.Error!.Message, at));
        }

        // Живой брокер — backoff снят (t11), runtime-уровень можно звать.
        _backoff.Remove(cluster.Name);

        var brokers = view.Value.Brokers
            .Select(b => new KafkaBrokerLive(b.Id, b.Host, b.Id == view.Value.ControllerId))
            .ToList();

        // Runtime-уровень (волна C): топики (USR по ISR) + группы с лагами.
        // Ошибка runtime НЕ роняет брокерскую часть пробы: live-брокеры живы,
        // топики/группы просто недоступны (в DTO их не будет).
        IReadOnlyList<KafkaTopicRuntime>? topics = null;
        IReadOnlyList<KafkaGroupInfo>? groups = null;
        if (runtimeClient is not null)
        {
            var runtime = await ProbeRuntimeAsync(cluster.Name, bootstrap, creds, timeout, ct);
            topics = runtime.Topics;
            groups = runtime.Groups;
        }

        return new KafkaProbeOutcome(
            new KafkaClusterLive(cluster.Name, at, brokers, topics, groups),
            new ProbeResult(cluster.Name, "kafka", true, null, null, at));
    }

    // Неудачная проба брокеров: растит счётчик и раздвигает окно повтора
    // (15 c → 60 c → 300 c) — мёртвый кластер не штурмуется каждый тик (t11).
    private void TrackFailure(string cluster, string error, DateTimeOffset at)
    {
        var failures = (_backoff.TryGetValue(cluster, out var state)
            ? state.ConsecutiveFailures : 0) + 1;
        var interval = TimeSpan.FromSeconds(
            kafkaOptions.Value.IntervalSeconds > 0 ? kafkaOptions.Value.IntervalSeconds : 15);
        var delay = BackoffAfter(failures, interval);
        _backoff[cluster] = new ClusterBackoffState(failures, at + delay, error);
        logger.LogDebug(
            "AdminPanel:Probes:Kafka: {Cluster} — {Failures} неудач подряд, следующая проба через {Delay}",
            cluster, failures, delay);
    }

    // Окно после N-й подряд неудачной пробы: первая — обычный тик (интервал),
    // вторая → 60 c, дальше 300 c (t11: 15 → 60 → 300, сброс при успехе).
    internal static TimeSpan BackoffAfter(int consecutiveFailures, TimeSpan interval)
        => consecutiveFailures switch
        {
            <= 1 => interval,
            2 => TimeSpan.FromSeconds(60),
            _ => TimeSpan.FromSeconds(300),
        };

    // Топики и группы с лагами (план C3): describe→чистый расчёт KafkaGroupLag;
    // частичный отказ — недостающие данные опускаются (null).
    private async Task<(IReadOnlyList<KafkaTopicRuntime>? Topics, IReadOnlyList<KafkaGroupInfo>? Groups)>
        ProbeRuntimeAsync(
            string cluster, string bootstrap, KafkaClusterSecrets creds, TimeSpan timeout, CancellationToken ct)
    {
        IReadOnlyList<KafkaTopicRuntime>? topics = null;
        IReadOnlyList<KafkaGroupInfo>? groups = null;

        var topicsView = await runtimeClient!.DescribeTopicsAsync(bootstrap, creds.AdminUser, creds.AdminPassword, creds.CaPem, timeout, ct);
        if (topicsView.IsSuccess)
            topics = [.. topicsView.Value.Select(t => new KafkaTopicRuntime(
                t.Topic, t.Partitions, (short?)t.ReplicationFactor, t.UnderReplicatedPartitions))];

        var groupIds = await runtimeClient.ListGroupsAsync(bootstrap, creds.AdminUser, creds.AdminPassword, creds.CaPem, timeout, ct);
        if (groupIds.IsSuccess && groupIds.Value.Count > 0)
        {
            var details = await runtimeClient.DescribeGroupsAsync(
                bootstrap, creds.AdminUser, creds.AdminPassword, creds.CaPem, groupIds.Value, timeout, ct);
            if (details.IsSuccess)
            {
                var end = new Dictionary<(string Topic, int Partition), long>();
                var found = new List<KafkaGroupInfo>();
                foreach (var detail in details.Value)
                {
                    // Лаг — по COMMITTED-оффсетам группы (не по живому assignment:
                    // умерший консьюмер оставляет committed и отставание — это
                    // ровно то, что должен подсветить мониторинг/алерт).
                    var committed = await runtimeClient.CommittedAsync(
                        bootstrap, creds.AdminUser, creds.AdminPassword, creds.CaPem, detail.Group, [], timeout, ct);
                    if (!committed.IsSuccess)
                        continue; // оффсеты недоступны — группа не показана в этом тике

                    if (committed.Value.Count == 0)
                    {
                        found.Add(new KafkaGroupInfo(detail.Group, detail.State, detail.Members, 0));
                        continue;
                    }

                    var missing = committed.Value.Keys.Where(p => !end.ContainsKey(p)).ToList();
                    if (missing.Count > 0)
                    {
                        var fetched = await runtimeClient.EndOffsetsAsync(
                            bootstrap, creds.AdminUser, creds.AdminPassword, creds.CaPem, missing, timeout, ct);
                        if (!fetched.IsSuccess)
                            continue;

                        foreach (var pair in fetched.Value)
                            end[pair.Key] = pair.Value;
                    }

                    var groupEnd = end.Where(p => committed.Value.ContainsKey(p.Key))
                        .ToDictionary(p => p.Key, p => p.Value);
                    found.Add(new KafkaGroupInfo(
                        detail.Group,
                        detail.State,
                        detail.Members,
                        KafkaGroupLag.Total(groupEnd, committed.Value)));
                }

                groups = [.. found.OrderBy(g => g.TotalLag, Comparer<long>.Create((x, y) => y.CompareTo(x)))
                    .ThenBy(g => g.Group, StringComparer.Ordinal)];
            }
        }

        return (topics, groups);
    }
}
