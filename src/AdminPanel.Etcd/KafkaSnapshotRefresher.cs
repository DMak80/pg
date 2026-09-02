using AdminPanel.Core;
using AdminPanel.Core.Kafka;
using AdminPanel.Core.Kafka.KafkaAlerting;
using AdminPanel.Etcd.Client;
using AdminPanel.Etcd.Parsing;
using AdminPanel.Infrastructure;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AdminPanel.Etcd;

// Единственный писатель kafka-снапшота (арх/02 §10): тик RefreshIntervalSeconds,
// range /kafka/clusters/ + /kafkaworker/rotations/ на активном endpoint
// (sticky + failover, опции общие с pg-циклом EtcdOptions). Транспортный провал
// любого чтения роняет тик: прежние данные, EtcdReachable=false, счётчик отказов.
// Регистрация — явно в ModuleExtensions.AddKafka().
public sealed class KafkaSnapshotRefresher(
    IEtcdGateway gateway,
    IKafkaAlertEngine alertEngine,
    IKafkaSnapshotStore store,
    IKafkaSecretsStore secretsStore,
    IOptions<EtcdOptions> etcdOptions,
    IOptions<KafkaPanelOptions> kafkaOptions,
    TimeProvider time,
    ILogger<KafkaSnapshotRefresher> logger,
    IKafkaWorkerHealthStore workerHealthStore,
    IKafkaProbeReader? probeReader = null) : BackgroundService
{
    private string? _activeEndpoint;
    private bool _endpointsWarned;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var seconds = kafkaOptions.Value.RefreshIntervalSeconds;
        if (seconds <= 0)
        {
            logger.LogWarning("AdminPanel:Kafka:RefreshIntervalSeconds <= 0 — использую 3 c");
            seconds = 3;
        }

        // Первый тик сразу: панель набирает данные со старта (симметрия pg-цикла).
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(seconds));
        do
        {
            try
            {
                await RefreshOnceAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }
        while (await timer.WaitForNextTickAsync(stoppingToken));
    }

    // Ядро одного тика — публично для unit/integration-тестов без хоста.
    public async Task<Result> RefreshOnceAsync(CancellationToken ct)
    {
        var endpoints = etcdOptions.Value.Endpoints.Where(IsValidEndpoint).ToArray();
        if (!_endpointsWarned && endpoints.Length == 0)
        {
            logger.LogWarning("AdminPanel:Etcd:Endpoints не задан или невалиден — kafka-данные недоступны");
            _endpointsWarned = true;
        }

        var now = time.GetUtcNow();
        var previous = store.Current;

        if (endpoints.Length == 0)
            return FailTick(previous, now, "AdminPanel:Etcd:Endpoints не задан или невалиден");

        // Активный — sticky с прошлого тика, иначе первый по списку.
        var active = _activeEndpoint is not null && endpoints.Contains(_activeEndpoint)
            ? _activeEndpoint
            : endpoints[0];

        var clustersKv = await RangeWithFailoverAsync(endpoints, active, Prefixes.Clusters, ct);
        var rotationsKv = await RangeWithFailoverAsync(endpoints, active, Prefixes.Rotations, ct);
        var rebalancesKv = await RangeWithFailoverAsync(endpoints, active, Prefixes.Rebalances, ct);
        var reassignmentsKv = await RangeWithFailoverAsync(endpoints, active, Prefixes.Reassignments, ct);
        var workerApiKv = await RangeWithFailoverAsync(endpoints, active, Prefixes.WorkerApi, ct);
        if (!clustersKv.IsSuccess || !rotationsKv.IsSuccess
            || !rebalancesKv.IsSuccess || !reassignmentsKv.IsSuccess || !workerApiKv.IsSuccess)
            return FailTick(previous, now, "KV-чтения etcd не удались");

        _activeEndpoint = active;

        // Парсеры → модель → алерты → атомарная замена (механика pg-цикла §4).
        var clusters = KafkaParser.ParseClusters(clustersKv.Value);
        var rotations = KafkaParser.ParseRotations(rotationsKv.Value);
        var rebalances = KafkaParser.ParseRebalances(rebalancesKv.Value);
        var reassignments = KafkaParser.ParseReassignments(reassignmentsKv.Value);
        var workerApi = WorkerEndpointsParser.Parse(workerApiKv.Value);

        // SASL-креды для проб (B6): в модель кластера НЕ попадают (arch/02 §10.1) —
        // отдельный internal-словарь стора.
        secretsStore.Replace(ReadSecrets(clustersKv.Value));

        var built = new KafkaSnapshot(
            now,
            EtcdReachable: true,
            ConsecutiveFailures: 0,
            MergeRuntime(clusters.Clusters, probeReader?.Current?.Clusters),
            rotations.Tickets,
            rebalances.Tickets,
            reassignments.Progress,
            workerApi.Endpoints,
            workerHealthStore.Current ?? [], // health-проб воркера вносит успешный тик (t09; arch/02 §2.3.2)
            previous?.Probes ?? [],       // пробы переживают отказ etcd (симметрия pg spec §4.3)
            Alerts: [],
            [.. clusters.Errors, .. rotations.Errors, .. rebalances.Errors, .. reassignments.Errors,
                .. workerApi.Errors],
            clusters.UnknownKeyCount);

        store.Replace(built with { Alerts = alertEngine.Evaluate(built, previous) });
        return Result.Success();
    }

    // Мердж live-данных проб (волна C): USR топиков и группы с лагами — в
    // runtime-поля кластеров; пробы молчат о кластере — etcd-данные как есть.
    private static IReadOnlyList<KafkaClusterInfo> MergeRuntime(
        IReadOnlyList<KafkaClusterInfo> clusters,
        IReadOnlyDictionary<string, KafkaClusterLive>? live)
    {
        if (live is null || live.Count == 0)
            return clusters;

        return [.. clusters.Select(c =>
        {
            if (!live.TryGetValue(c.Name, out var clusterLive))
                return c; // проба не знает кластер — runtime-полей нет

            var runtimeTopics = clusterLive.Topics is { Count: > 0 }
                ? c.Topics.Select(t => t with
                {
                    UnderReplicatedPartitions = clusterLive.Topics
                        .FirstOrDefault(lt => string.Equals(lt.Topic, t.Name, StringComparison.Ordinal))
                        ?.UnderReplicatedPartitions,
                }).ToList()
                : c.Topics;
            return c with { Topics = runtimeTopics, Groups = clusterLive.Groups };
        })];
    }

    // Failover: один проход по endpoints по кругу от активного (pg-механика §4).
    private async Task<Result<IReadOnlyList<Kv>>> RangeWithFailoverAsync(
        string[] endpoints,
        string active,
        string prefix,
        CancellationToken ct)
    {
        var start = Array.IndexOf(endpoints, active);
        Exception? last = null;
        for (var i = 0; i < endpoints.Length; i++)
        {
            var endpoint = endpoints[(start + i) % endpoints.Length];
            var result = await gateway.RangeAsync(endpoint, prefix, ct);
            if (result.IsSuccess)
            {
                _activeEndpoint = endpoint;
                return result;
            }

            last = result.Error!;
        }

        return Result<IReadOnlyList<Kv>>.Failed(new EtcdUnreachableException(
            $"все endpoints не ответили на range {prefix}: {last?.Message}"));
    }

    // Отказ тика: прежние данные/BuiltAtUtc, Reachable=false, счётчик растёт.
    private Result FailTick(KafkaSnapshot? previous, DateTimeOffset now, string reason)
    {
        var error = Result.Failed(new EtcdUnreachableException(reason));
        var failed = previous
            ?? new KafkaSnapshot(now, EtcdReachable: false, ConsecutiveFailures: 0,
                [], [], [], [], [], [], [], [], [], 0);
        failed = failed with
        {
            EtcdReachable = false,
            ConsecutiveFailures = failed.ConsecutiveFailures + 1,
            // WorkerHealth НЕ мерджится из стора на отказном тике (t09; spec §3.4,
            // симметрия pg): переносится из previous автоматически — свежие пробы
            // вносит только успешный тик; алерт worker-unhealthy загорается первым
            // успешным тиком refresher'а после возвращения etcd.
        };

        // Алерты пересчитываются и на отказном тике (pg-семантика §4).
        store.Replace(failed with { Alerts = alertEngine.Evaluate(failed, previous) });
        return error;
    }

    // Креды /kafka/clusters/<C>/{app_user,app_password} (expected-skip парсера).
    private static IReadOnlyDictionary<string, KafkaClusterSecrets> ReadSecrets(IReadOnlyList<Kv> kvs)
    {
        var users = new Dictionary<string, string>();
        var passwords = new Dictionary<string, string>();
        foreach (var kv in kvs)
        {
            // "/kafka/clusters/<C>/app_user" → ["", "kafka", "clusters", <C>, "app_user"]
            var segments = kv.Key.Split('/');
            if (segments.Length != 5)
                continue;
            switch (segments[4])
            {
                case "app_user":
                    users[segments[3]] = kv.Value;
                    break;
                case "app_password":
                    passwords[segments[3]] = kv.Value;
                    break;
            }
        }

        var secrets = new Dictionary<string, KafkaClusterSecrets>();
        foreach (var (cluster, password) in passwords)
            if (users.TryGetValue(cluster, out var user))
                secrets[cluster] = new KafkaClusterSecrets(cluster, user, password);
        return secrets;
    }

    private static bool IsValidEndpoint(string endpoint)
        => Uri.TryCreate(endpoint, UriKind.Absolute, out var uri)
            && uri.Scheme is "http" or "https"
            && !string.IsNullOrEmpty(uri.Host);

    private static class Prefixes
    {
        public const string Clusters = "/kafka/clusters/";
        public const string Rotations = "/kafkaworker/rotations/";
        public const string Rebalances = "/kafkaworker/rebalances/";
        public const string Reassignments = "/kafkaworker/reassignments/";
        public const string WorkerApi = "/kafkaworker/api/";
    }
}
