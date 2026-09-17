using AdminPanel.Core;
using AdminPanel.Core.Valkey;
using AdminPanel.Core.Valkey.ValkeyAlerting;
using Shared.Etcd.Client;
using AdminPanel.Etcd.Parsing;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AdminPanel.Etcd;

// Единственный писатель valkey-снапшота (arch/02 §11): тик RefreshIntervalSeconds,
// range /valkey/clusters/ + /valkeyworker/rotations/ + /valkeyworker/api/ и
// точечный ключ /workers/api_tls/valkeyworker на активном endpoint (sticky +
// failover, опции общие с pg-циклом EtcdOptions). Транспортный провал любого
// чтения роняет тик: прежние данные, EtcdReachable=false, счётчик отказов.
// Регистрация — явно в ModuleExtensions.AddValkey().
public sealed class ValkeySnapshotRefresher(
    IEtcdGateway gateway,
    IValkeyAlertEngine alertEngine,
    IValkeySnapshotStore store,
    IValkeySecretsStore secretsStore,
    IOptions<EtcdOptions> etcdOptions,
    IOptions<ValkeyPanelOptions> valkeyOptions,
    TimeProvider time,
    ILogger<ValkeySnapshotRefresher> logger,
    AdminPanel.Etcd.Workers.IValkeyWorkerHealthStore workerHealthStore,
    IValkeyProbeReader? probeReader = null) : BackgroundService
{
    private string? _activeEndpoint;
    private bool _endpointsWarned;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var seconds = valkeyOptions.Value.RefreshIntervalSeconds;
        if (seconds <= 0)
        {
            logger.LogWarning("AdminPanel:Valkey:RefreshIntervalSeconds <= 0 — использую 3 c");
            seconds = 3;
        }

        // Первый тик сразу: панель набирает данные со старта (симметрия pg/kafka).
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
            logger.LogWarning("AdminPanel:Etcd:Endpoints не задан или невалиден — valkey-данные недоступны");
            _endpointsWarned = true;
        }

        var now = time.GetUtcNow();
        var previous = store.Current;

        if (endpoints.Length == 0)
            return FailTick(previous, now, "AdminPanel:Etcd:Endpoints не задан или невалиден");

        var active = _activeEndpoint is not null && endpoints.Contains(_activeEndpoint)
            ? _activeEndpoint
            : endpoints[0];

        var clustersKv = await RangeWithFailoverAsync(endpoints, active, Prefixes.Clusters, ct);
        var rotationsKv = await RangeWithFailoverAsync(endpoints, active, Prefixes.Rotations, ct);
        var workerApiKv = await RangeWithFailoverAsync(endpoints, active, Prefixes.WorkerApi, ct);
        var certKv = await RangeWithFailoverAsync(endpoints, active, Prefixes.WorkerApiCert, ct);
        if (!clustersKv.IsSuccess || !rotationsKv.IsSuccess || !workerApiKv.IsSuccess || !certKv.IsSuccess)
            return FailTick(previous, now, "KV-чтения etcd не удались");

        _activeEndpoint = active;

        var clusters = ValkeyParser.ParseClusters(clustersKv.Value);
        var rotations = ValkeyParser.ParseRotations(rotationsKv.Value);
        var workerApi = WorkerEndpointsParser.Parse(workerApiKv.Value);
        // Префикс-запрос точечный: ровно один ключ /workers/api_tls/valkeyworker.
        var certParsed = WorkerCertParser.Parse(Prefixes.WorkerApiCert, certKv.Value.FirstOrDefault());

        // Креды проб: admin-пара — internal-стор; в модель кластера не попадает
        // (arch/02 §11.1). Частичный набор — не ошибка (ensure воркера в процессе).
        secretsStore.Replace(ReadSecrets(clustersKv.Value));

        var rotationsByCluster = rotations.Tickets
            .GroupBy(t => t.Cluster, StringComparer.Ordinal)
            .ToDictionary(g => g.Key, g => g.First(), StringComparer.Ordinal);

        var built = new ValkeySnapshot(
            now,
            EtcdReachable: true,
            ConsecutiveFailures: 0,
            MergeProbes(
                [.. clusters.Clusters.Select(c => c with
                {
                    Rotation = rotationsByCluster.GetValueOrDefault(c.Name),
                })],
                probeReader?.Current),
            rotations.Tickets,
            workerApi.Endpoints,
            workerHealthStore.Current ?? [],   // health-проб воркера вносит успешный тик (t03; arch/02 §2.3.3)
            previous?.Probes ?? [],     // пробы переживают отказ etcd (симметрия pg/kafka)
            Alerts: [],
            [.. clusters.Errors, .. rotations.Errors, .. workerApi.Errors,
                .. WorkerCertParser.ErrorsOf(certParsed)],
            clusters.UnknownKeyCount,
            WorkerApiCert: certParsed.Cert);

        store.Replace(built with { Alerts = alertEngine.Evaluate(built, previous) });
        return Result.Success();
    }

    // Мердж live-проб: Live/ProbeError в ноды кластеров; проба молчит о кластере —
    // etcd-данные как есть (Live=null). Публичен для юнит-тестов модели (spec §5.1).
    public static IReadOnlyList<ValkeyClusterInfo> MergeProbes(
        IReadOnlyList<ValkeyClusterInfo> clusters,
        IReadOnlyList<ValkeyProbeResult>? probes)
    {
        if (probes is null || probes.Count == 0)
            return clusters;

        var byCluster = probes
            .GroupBy(p => p.Cluster, StringComparer.Ordinal)
            .ToDictionary(g => g.Key, g => g.First(), StringComparer.Ordinal);
        return [.. clusters.Select(c => byCluster.TryGetValue(c.Name, out var probe)
            ? c with
            {
                NodesList = [.. c.NodesList.Select(n =>
                    n.Name == probe.Node ? n with { Live = probe.Live, ProbeError = probe.Error } : n)],
            }
            : c)];
    }

    // Failover: один проход по endpoints по кругу от активного (pg-механика).
    private async Task<Result<IReadOnlyList<Kv>>> RangeWithFailoverAsync(
        string[] endpoints, string active, string prefix, CancellationToken ct)
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
    private Result FailTick(ValkeySnapshot? previous, DateTimeOffset now, string reason)
    {
        var error = Result.Failed(new EtcdUnreachableException(reason));
        var failed = previous
            ?? new ValkeySnapshot(now, EtcdReachable: false, ConsecutiveFailures: 0,
                [], [], [], [], [], [], [], 0);
        failed = failed with { EtcdReachable = false, ConsecutiveFailures = failed.ConsecutiveFailures + 1 };

        // Алерты пересчитываются и на отказном тике (pg-семантика §4).
        store.Replace(failed with { Alerts = alertEngine.Evaluate(failed, previous) });
        return error;
    }

    // Креды проб: "/valkey/clusters/<C>/admin_user|admin_password" → стор;
    // полный набор → запись, частичный — пропуск без ошибки (ensure воркера).
    private static IReadOnlyDictionary<string, ValkeyClusterSecrets> ReadSecrets(IReadOnlyList<Kv> kvs)
    {
        var users = new Dictionary<string, string>();
        var passwords = new Dictionary<string, string>();
        foreach (var kv in kvs)
        {
            // "/valkey/clusters/<C>/admin_user" → ["", "valkey", "clusters", <C>, "admin_user"]
            var segments = kv.Key.Split('/');
            if (segments.Length != 5)
                continue;
            switch (segments[4])
            {
                case "admin_user":
                    users[segments[3]] = kv.Value;
                    break;
                case "admin_password":
                    passwords[segments[3]] = kv.Value;
                    break;
            }
        }

        var secrets = new Dictionary<string, ValkeyClusterSecrets>();
        foreach (var cluster in users.Keys.Union(passwords.Keys).OrderBy(n => n, StringComparer.Ordinal))
        {
            var user = users.GetValueOrDefault(cluster) ?? string.Empty;
            var password = passwords.GetValueOrDefault(cluster) ?? string.Empty;
            if (user.Length == 0 || password.Length == 0)
                continue;

            secrets[cluster] = new ValkeyClusterSecrets(cluster, user, password);
        }

        return secrets;
    }

    private static bool IsValidEndpoint(string endpoint)
        => Uri.TryCreate(endpoint, UriKind.Absolute, out var uri)
           && uri.Scheme is "http" or "https"
           && !string.IsNullOrEmpty(uri.Host);

    private static class Prefixes
    {
        public const string Clusters = "/valkey/clusters/";
        public const string Rotations = "/valkeyworker/rotations/";
        public const string WorkerApi = "/valkeyworker/api/";
        public const string WorkerApiCert = "/workers/api_tls/valkeyworker";
    }
}
