using AdminPanel.Core;
using AdminPanel.Core.S3;
using AdminPanel.Core.S3.S3Alerting;
using Shared.Etcd.Client;
using AdminPanel.Etcd.Parsing;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AdminPanel.Etcd;

// Единственный писатель s3-снапшота (arch/02 §12): тик RefreshIntervalSeconds,
// range /s3/clusters/ + /s3worker/rotations/ + /s3worker/api/ и точечный ключ
// /workers/api_tls/s3worker на активном endpoint (sticky + failover, опции
// общие с pg-циклом EtcdOptions). Транспортный провал любого чтения роняет тик:
// прежние данные, EtcdReachable=false, счётчик отказов. Применение только
// изменений — arch/02 §12/§5 (arch/synchronization/05-delta-application.md §S10): прочитанное идентично кэшу
// последнего успешно прочитанного состояния → тик без изменений: пересборки и
// алертного пересчёта нет, обновляются свежая доступностная часть и штамп
// свежести; тики не классифицируются (E11) — гейт только по дельте состояния.
// Регистрация — явно в ModuleExtensions.AddS3().
public sealed class S3SnapshotRefresher(
    IEtcdGateway gateway,
    IS3AlertEngine alertEngine,
    IS3SnapshotStore store,
    IOptions<EtcdOptions> etcdOptions,
    IOptions<S3PanelOptions> s3Options,
    TimeProvider time,
    ILogger<S3SnapshotRefresher> logger,
    AdminPanel.Etcd.Workers.IS3WorkerHealthStore workerHealthStore) : BackgroundService
{
    private string? _activeEndpoint;
    private bool _endpointsWarned;

    // Кэш последнего успешно прочитанного состояния (arch/02 §12, arch/synchronization/05-delta-application.md §S10):
    // тик сравнивает прочитанное с ним и пересобирает снапшот только при
    // фактическом изменении. Обновляется только на пересборке — отказной и
    // тик без изменений кэш не меняют (по построению кэш хранит успешно прочитанное).
    private (string Prefix, IReadOnlyList<Kv> Items)[]? _lastRead;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var seconds = s3Options.Value.RefreshIntervalSeconds;
        if (seconds <= 0)
        {
            logger.LogWarning("AdminPanel:S3:RefreshIntervalSeconds <= 0 — using 3 s");
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
            logger.LogWarning("AdminPanel:Etcd:Endpoints is not set or invalid — s3 data unavailable");
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
        var workerApiTask = RangeWithFailoverAsync(endpoints, active, Prefixes.WorkerApi, ct);
        var certTask = RangeWithFailoverAsync(endpoints, active, Prefixes.WorkerApiCert, ct);
        // u13/u14: work-журналы и координация s3-домена (arch/s3-clusters/08-coordination.md).
        var workTask = RangeWithFailoverAsync(endpoints, active, Prefixes.Work, ct);
        var leaderTask = RangeWithFailoverAsync(endpoints, active, Prefixes.Leader, ct);
        var claimsTask = RangeWithFailoverAsync(endpoints, active, Prefixes.Claims, ct);
        var instancesTask = RangeWithFailoverAsync(endpoints, active, Prefixes.Instances, ct);
        var clustersKv = await clustersTask;
        var rotationsKv = await rotationsTask;
        var workerApiKv = await workerApiTask;
        var certKv = await certTask;
        var workKv = await workTask;
        var leaderKv = await leaderTask;
        var claimsKv = await claimsTask;
        var instancesKv = await instancesTask;
        if (!clustersKv.IsSuccess || !rotationsKv.IsSuccess || !workerApiKv.IsSuccess || !certKv.IsSuccess
            || !workKv.IsSuccess || !leaderKv.IsSuccess || !claimsKv.IsSuccess || !instancesKv.IsSuccess)
            return FailTick(previous, now, "etcd KV reads failed");

        _activeEndpoint = active;

        // Прочитанное тика — вход сравнения с кэшем (arch/02 §12, arch/synchronization/05-delta-application.md §S10):
        // списки KV всех наблюдаемых префиксов (Kv — record: ключ/значение/
        // mod_revision); member list/alarm/статусы endpoints домен не читает.
        (string Prefix, IReadOnlyList<Kv> Items)[] reads =
        [
            (Prefixes.Clusters, clustersKv.Value),
            (Prefixes.Rotations, rotationsKv.Value),
            (Prefixes.WorkerApi, workerApiKv.Value),
            (Prefixes.WorkerApiCert, certKv.Value),
            (Prefixes.Work, workKv.Value),
            (Prefixes.Leader, leaderKv.Value),
            (Prefixes.Claims, claimsKv.Value),
            (Prefixes.Instances, instancesKv.Value),
        ];

        // arch/02 §12/§5 (arch/synchronization/05-delta-application.md §S10): прочитанное идентично кэшу (последнему
        // успешно прочитанному состоянию из etcd) — тик без изменений: снапшот не
        // пересобирается, алертный пересчёт не выполняется; обновляются свежая
        // доступностная часть (телеметрия доступности не замерзает) и штамп
        // свежести. Ветвление только по факту совпадения с кэшем, без истории
        // исходов тиков (E11): совпадение после сбоя — тик без изменений, различие —
        // пересборка.
        if (previous is S3Snapshot lastApplied
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

        var clusters = S3Parser.ParseClusters(clustersKv.Value);
        var rotations = S3Parser.ParseRotations(rotationsKv.Value);
        var workerApi = WorkerEndpointsParser.Parse(workerApiKv.Value);
        // Префикс-запрос точечный: ровно один ключ /workers/api_tls/s3worker.
        var certParsed = WorkerCertParser.Parse(Prefixes.WorkerApiCert, certKv.Value.FirstOrDefault());
        // u13/u14: журналы и координация (факт-поля pg у домена отсутствуют).
        var workParsed = WorkJournalParser.Parse(workKv.Value);
        var coordinationParsed = CoordinationParser.Parse(leaderKv.Value.FirstOrDefault(), claimsKv.Value, instancesKv.Value);

        var rotationsByCluster = rotations.Tickets
            .GroupBy(t => t.Cluster, StringComparer.Ordinal)
            .ToDictionary(g => g.Key, g => g.First(), StringComparer.Ordinal);

        var built = new S3Snapshot(
            now,
            EtcdReachable: true,
            ConsecutiveFailures: 0,
            [.. clusters.Clusters.Select(c => c with
            {
                Rotation = rotationsByCluster.GetValueOrDefault(c.Name),
            })],
            rotations.Tickets,
            workerApi.Endpoints,
            workerHealthStore.Current ?? [],   // health-проб воркера вносит успешный тик (arch/02 §2.3.4)
            Alerts: [],
            [.. clusters.Errors, .. rotations.Errors,
                .. workerApi.Errors, .. WorkerCertParser.ErrorsOf(certParsed),
                .. workParsed.Errors, .. coordinationParsed.Errors],
            clusters.UnknownKeyCount,
            WorkerApiCert: certParsed.Cert,
            WorkJournals: workParsed.Items,
            Coordination: coordinationParsed.Coordination);

        store.Replace(built with { Alerts = alertEngine.Evaluate(built, previous) });
        RememberTick(reads);
        return Result.Success();
    }

    // Кэш последнего успешно прочитанного состояния: обновляется только на
    // пересборке — отказной и тик без изменений кэш не меняют (arch/02 §12, arch/synchronization/05-delta-application.md §S10).
    private void RememberTick((string Prefix, IReadOnlyList<Kv> Items)[] reads)
        => _lastRead = reads;

    // Сравнение прочитанного тика с кэшем (arch/02 §12, arch/synchronization/05-delta-application.md §S10): списки
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
    private Result FailTick(S3Snapshot? previous, DateTimeOffset now, string reason)
    {
        var error = Result.Failed(new EtcdUnreachableException(reason));
        var failed = previous
            ?? new S3Snapshot(now, EtcdReachable: false, ConsecutiveFailures: 0,
                [], [], [], [], [], [], 0);
        failed = failed with { EtcdReachable = false, ConsecutiveFailures = failed.ConsecutiveFailures + 1 };

        // Алерты пересчитываются и на отказном тике (pg-семантика §4).
        store.Replace(failed with { Alerts = alertEngine.Evaluate(failed, previous) });
        return error;
    }

    private static bool IsValidEndpoint(string endpoint)
        => Uri.TryCreate(endpoint, UriKind.Absolute, out var uri)
           && uri.Scheme is "http" or "https"
           && !string.IsNullOrEmpty(uri.Host);

    private static class Prefixes
    {
        public const string Clusters = "/s3/clusters/";
        public const string Rotations = "/s3worker/rotations/";
        public const string WorkerApi = "/s3worker/api/";
        public const string WorkerApiCert = "/workers/api_tls/s3worker";
        // u13/u14 (arch/s3-clusters/08-coordination.md, adminpanel/02 §2.3.4).
        public const string Work = "/s3worker/work/";
        public const string Leader = "/s3worker/leader";
        public const string Claims = "/s3worker/claims/";
        public const string Instances = "/s3worker/instances/";
    }
}
