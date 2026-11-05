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
// Применение только изменений — arch/02 §11/§5 (arch/synchronization/05-delta-application.md §S10): прочитанное
// идентично кэшу последнего успешно прочитанного состояния → тик без изменений:
// пересборки и алертного пересчёта нет, обновляются свежая доступностная
// часть и штамп свежести; тики не классифицируются (E11) — гейт только по
// дельте состояния. Регистрация — явно в ModuleExtensions.AddValkey().
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

    // Кэш последнего успешно прочитанного состояния (arch/02 §11, arch/synchronization/05-delta-application.md §S10):
    // тик сравнивает прочитанное с ним и пересобирает снапшот только при
    // фактическом изменении. Обновляется только на пересборке — отказной и
    // тик без изменений кэш не меняют (по построению кэш хранит успешно прочитанное).
    private (string Prefix, IReadOnlyList<Kv> Items)[]? _lastRead;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var seconds = valkeyOptions.Value.RefreshIntervalSeconds;
        if (seconds <= 0)
        {
            logger.LogWarning("AdminPanel:Valkey:RefreshIntervalSeconds <= 0 — using 3 s");
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
            logger.LogWarning("AdminPanel:Etcd:Endpoints is not set or invalid — valkey data unavailable");
            _endpointsWarned = true;
        }

        var now = time.GetUtcNow();
        var previous = store.Current;

        if (endpoints.Length == 0)
            return FailTick(previous, now, "AdminPanel:Etcd:Endpoints is not set or invalid");

        var active = _activeEndpoint is not null && endpoints.Contains(_activeEndpoint)
            ? _activeEndpoint
            : endpoints[0];

        // Чтения тика — ПАРАЛЛЕЛЬНО (образец pg-цикла SnapshotRefresher):
        // длина тика = max латентностей, а не сумма; частичный провал любого
        // чтения = отказ тика (механика без изменений).
        var clustersTask = RangeWithFailoverAsync(endpoints, active, Prefixes.Clusters, ct);
        var rotationsTask = RangeWithFailoverAsync(endpoints, active, Prefixes.Rotations, ct);
        var caRotationsTask = RangeWithFailoverAsync(endpoints, active, Prefixes.CaRotations, ct);
        var ticketOutcomesTask = RangeWithFailoverAsync(endpoints, active, Prefixes.TicketOutcomes, ct);
        var workerApiTask = RangeWithFailoverAsync(endpoints, active, Prefixes.WorkerApi, ct);
        var certTask = RangeWithFailoverAsync(endpoints, active, Prefixes.WorkerApiCert, ct);
        // u13/u14: work-журналы и координация valkey-домена (arch/valkey-clusters/03-coordination.md §1).
        var workTask = RangeWithFailoverAsync(endpoints, active, Prefixes.Work, ct);
        var leaderTask = RangeWithFailoverAsync(endpoints, active, Prefixes.Leader, ct);
        var claimsTask = RangeWithFailoverAsync(endpoints, active, Prefixes.Claims, ct);
        var instancesTask = RangeWithFailoverAsync(endpoints, active, Prefixes.Instances, ct);
        var clustersKv = await clustersTask;
        var rotationsKv = await rotationsTask;
        var caRotationsKv = await caRotationsTask;
        var ticketOutcomesKv = await ticketOutcomesTask;
        var workerApiKv = await workerApiTask;
        var certKv = await certTask;
        var workKv = await workTask;
        var leaderKv = await leaderTask;
        var claimsKv = await claimsTask;
        var instancesKv = await instancesTask;
        if (!clustersKv.IsSuccess || !rotationsKv.IsSuccess || !caRotationsKv.IsSuccess
            || !ticketOutcomesKv.IsSuccess || !workerApiKv.IsSuccess || !certKv.IsSuccess
            || !workKv.IsSuccess || !leaderKv.IsSuccess || !claimsKv.IsSuccess || !instancesKv.IsSuccess)
            return FailTick(previous, now, "etcd KV reads failed");

        _activeEndpoint = active;

        // Прочитанное тика — вход сравнения с кэшем (arch/02 §11, arch/synchronization/05-delta-application.md §S10):
        // списки KV всех наблюдаемых префиксов (Kv — record: ключ/значение/
        // mod_revision); member list/alarm/статусы endpoints домен не читает.
        (string Prefix, IReadOnlyList<Kv> Items)[] reads =
        [
            (Prefixes.Clusters, clustersKv.Value),
            (Prefixes.Rotations, rotationsKv.Value),
            (Prefixes.CaRotations, caRotationsKv.Value),
            (Prefixes.TicketOutcomes, ticketOutcomesKv.Value),
            (Prefixes.WorkerApi, workerApiKv.Value),
            (Prefixes.WorkerApiCert, certKv.Value),
            (Prefixes.Work, workKv.Value),
            (Prefixes.Leader, leaderKv.Value),
            (Prefixes.Claims, claimsKv.Value),
            (Prefixes.Instances, instancesKv.Value),
        ];

        // arch/02 §11/§5 (arch/synchronization/05-delta-application.md §S10): прочитанное идентично кэшу (последнему
        // успешно прочитанному состоянию из etcd) — тик без изменений: снапшот не
        // пересобирается, алертный пересчёт и перепись кредов проб не
        // выполняются; обновляются свежая доступностная часть (телеметрия
        // доступности не замерзает) и штамп свежести. Ветвление только по
        // факту совпадения с кэшем, без истории исходов тиков (E11):
        // совпадение после сбоя — тик без изменений, различие — пересборка.
        if (previous is ValkeySnapshot lastApplied
            && TickUnchanged(reads))
        {
            store.Replace(lastApplied with
            {
                BuiltAtUtc = now,
                EtcdReachable = true,
                ConsecutiveFailures = 0,
            });
            return Result.Success();
        }

        var clusters = ValkeyParser.ParseClusters(clustersKv.Value);
        var rotations = ValkeyParser.ParseRotations(rotationsKv.Value);
        var caRotations = ValkeyParser.ParseCaRotations(caRotationsKv.Value); 
        var ticketOutcomes = ValkeyParser.ParseTicketOutcomes(ticketOutcomesKv.Value); 
        var workerApi = WorkerEndpointsParser.Parse(workerApiKv.Value);
        // Префикс-запрос точечный: ровно один ключ /workers/api_tls/valkeyworker.
        var certParsed = WorkerCertParser.Parse(Prefixes.WorkerApiCert, certKv.Value.FirstOrDefault());
        // u13/u14: журналы и координация (факт-поля pg у домена отсутствуют).
        var workParsed = WorkJournalParser.Parse(workKv.Value);
        var coordinationParsed = CoordinationParser.Parse(leaderKv.Value.FirstOrDefault(), claimsKv.Value, instancesKv.Value);

        // Креды проб: admin-пара — internal-стор; в модель кластера не попадает
        // (arch/02 §11.1). Частичный набор — не ошибка (ensure воркера в процессе).
        secretsStore.Replace(ReadSecrets(clustersKv.Value));

        var rotationsByCluster = rotations.Tickets
            .GroupBy(t => t.Cluster, StringComparer.Ordinal)
            .ToDictionary(g => g.Key, g => g.First(), StringComparer.Ordinal);
        var caRotationsByCluster = caRotations.Tickets
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
                    CaRotation = caRotationsByCluster.GetValueOrDefault(c.Name),
                })],
                probeReader?.Current),
            rotations.Tickets,
            workerApi.Endpoints,
            workerHealthStore.Current ?? [],   // health-проб воркера вносит успешный тик (; arch/02 §2.3.3)
            previous?.Probes ?? [],     // пробы переживают отказ etcd (симметрия pg/kafka)
            Alerts: [],
            [.. clusters.Errors, .. rotations.Errors, .. caRotations.Errors, .. ticketOutcomes.Errors,
                .. workerApi.Errors, .. WorkerCertParser.ErrorsOf(certParsed),
                .. workParsed.Errors, .. coordinationParsed.Errors],
            clusters.UnknownKeyCount,
            CaRotations: caRotations.Tickets,
            TicketOutcomes: ticketOutcomes.Tickets,
            WorkerApiCert: certParsed.Cert,
            WorkJournals: workParsed.Items,
            Coordination: coordinationParsed.Coordination);

        store.Replace(built with { Alerts = alertEngine.Evaluate(built, previous) });
        RememberTick(reads);
        return Result.Success();
    }

    // Кэш последнего успешно прочитанного состояния: обновляется только на
    // пересборке — отказной и тик без изменений кэш не меняют (arch/02 §11, arch/synchronization/05-delta-application.md §S10).
    private void RememberTick((string Prefix, IReadOnlyList<Kv> Items)[] reads)
        => _lastRead = reads;

    // Сравнение прочитанного тика с кэшем (arch/02 §11, arch/synchronization/05-delta-application.md §S10): списки
    // KV наблюдаемых префиксов по значению. Кэш хранит последнее успешно
    // прочитанное состояние: обновляется только на пересборке, отказной тик
    // его не меняет по построению; тики не классифицируются (E11) — ветвление
    // только по факту совпадения (кэш не собран — первый тик → «изменено»).
    private bool TickUnchanged((string Prefix, IReadOnlyList<Kv> Items)[] reads)
    {
        if (_lastRead is null || reads.Length != _lastRead.Length)
            return false;
        for (var i = 0; i < reads.Length; i++)
            if (reads[i].Prefix != _lastRead[i].Prefix
                || !reads[i].Items.SequenceEqual(_lastRead[i].Items))
                return false;
        return true;
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
        var start = Math.Max(Array.IndexOf(endpoints, active), 0);
        var result = await EtcdEndpointFailover.CallAsync(endpoints, async (endpoint, token) =>
        {
            var range = await gateway.RangeAsync(endpoint, prefix, token);
            if (range.IsSuccess)
                _activeEndpoint = endpoint;
            return range;
        }, ct, start);
        return result.IsSuccess
            ? result
            : Result<IReadOnlyList<Kv>>.Failed(new EtcdUnreachableException(
                $"all endpoints failed to answer range {prefix}: {result.Error!.Message}"));
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

    // Креды проб: "/valkey/clusters/<C>/admin_user|admin_password|ca_pem" → стор;
    // полный кред-набор → запись (ca_pem — TLS-доверие пробы; ca без пары —
    // запись не создаётся: пробы невозможны без кредов), частичный — пропуск
    // без ошибки (ensure воркера/миграция в процессе).
    private static IReadOnlyDictionary<string, ValkeyClusterSecrets> ReadSecrets(IReadOnlyList<Kv> kvs)
    {
        var users = new Dictionary<string, string>();
        var passwords = new Dictionary<string, string>();
        var cas = new Dictionary<string, string>();
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
                case "ca_pem":
                    cas[segments[3]] = kv.Value;
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

            secrets[cluster] = new ValkeyClusterSecrets(
                cluster, user, password, cas.GetValueOrDefault(cluster));
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
        public const string CaRotations = "/valkeyworker/ca_rotations/"; 
        public const string TicketOutcomes = "/valkeyworker/ticket_outcomes/"; 
        public const string WorkerApi = "/valkeyworker/api/";
        public const string WorkerApiCert = "/workers/api_tls/valkeyworker";
        // u13/u14 (arch/valkey-clusters/03-coordination.md §1, adminpanel/02 §2.3.3).
        public const string Work = "/valkeyworker/work/";
        public const string Leader = "/valkeyworker/leader";
        public const string Claims = "/valkeyworker/claims/";
        public const string Instances = "/valkeyworker/instances/";
    }
}
