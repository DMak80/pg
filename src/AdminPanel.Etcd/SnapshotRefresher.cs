using System.Diagnostics;
using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using AdminPanel.Etcd.Client;
using AdminPanel.Etcd.Parsing;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.DI;
using AdminPanel.Infrastructure.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AdminPanel.Etcd;

// Единственный писатель снапшота (arch/01 §1): тик RefreshIntervalSeconds по arch/02 §4.
// Отказ тика = обновление только Etcd-части: данные и BuiltAtUtc прежние, возраст растёт (spec §3.9).
[InjectAsSingleton(typeof(IHostedService))]
public sealed class SnapshotRefresher(
    IEtcdGateway gateway,
    IAlertEngine alertEngine,
    ISnapshotStore store,
    IProbeStateStore probeStateStore,
    IOptions<EtcdOptions> options,
    TimeProvider time,
    ILogger<SnapshotRefresher> logger) : BackgroundService, IHealthCheckService
{
    private string? _activeEndpoint;
    private bool _inited;
    private bool _working;
    private Result _statusError = Result.Success();
    private bool _endpointsWarned;

    public bool Inited => _inited;

    public bool Working => _working;

    public Result StatusError => _statusError;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var seconds = options.Value.RefreshIntervalSeconds;
        if (seconds <= 0)
        {
            logger.LogWarning("AdminPanel:Etcd:RefreshIntervalSeconds <= 0 — использую 3 c");
            seconds = 3;
        }

        // Первый тик сразу: панель набирает данные со старта; далее по периоду (spec §7.2).
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(seconds));
        do
        {
            await RefreshOnceAsync(stoppingToken);
        }
        while (await timer.WaitForNextTickAsync(stoppingToken));
    }

    // Ядро одного тика — публично для unit/integration-тестов без хоста.
    public async Task<Result> RefreshOnceAsync(CancellationToken ct)
    {
        var endpoints = options.Value.Endpoints.Where(IsValidEndpoint).ToArray();
        if (!_endpointsWarned && endpoints.Length == 0)
        {
            // один warning на серии отказов — не шумим в логах каждым тиком (spec §3.12)
            logger.LogWarning("AdminPanel:Etcd:Endpoints не задан или невалиден — etcd-данные недоступны");
            _endpointsWarned = true;
        }

        var now = time.GetUtcNow();
        var previous = store.Current;

        if (endpoints.Length == 0)
            return FailTick(previous, [], now, "AdminPanel:Etcd:Endpoints не задан или невалиден");

        // 1. Статусы персонально по всем endpoints — параллельно (arch/02 §4 п.1).
        var statuses = await Task.WhenAll(endpoints.Select(e => StatusOfAsync(e, ct)));
        var alive = statuses.Where(s => s.Reachable).ToArray();
        if (alive.Length == 0)
            return FailTick(previous, statuses, now, "нет живых endpoints etcd");

        // 2. Активный: sticky с прошлого тика, иначе первый живой (arch/02 §4 п.1).
        var active = alive.FirstOrDefault(e => e.Url == _activeEndpoint) ?? alive[0];
        _activeEndpoint = active.Url;

        // 3. Чтения на активном — параллельно; транспортный провал → failover по кругу живых (spec §3.10).
        var clustersTask = WithFailoverAsync(alive, active, (ep, t) => gateway.RangeAsync(ep, Prefixes.Clusters, t), ct);
        var serviceTask = WithFailoverAsync(alive, active, (ep, t) => gateway.RangeAsync(ep, Prefixes.Service, t), ct);
        var nodesTask = WithFailoverAsync(alive, active, (ep, t) => gateway.RangeAsync(ep, Prefixes.Nodes, t), ct);
        var portAllocTask = WithFailoverAsync(alive, active, (ep, t) => gateway.RangeAsync(ep, Prefixes.PortAlloc, t), ct);
        var membersTask = WithFailoverAsync(alive, active, (ep, t) => gateway.MemberListAsync(ep, t), ct);
        var alarmsTask = WithFailoverAsync(alive, active, (ep, t) => gateway.AlarmAsync(ep, t), ct);

        var clustersKv = await clustersTask;
        var serviceKv = await serviceTask;
        var nodesKv = await nodesTask;
        var portAllocKv = await portAllocTask;
        var members = await membersTask;
        var alarms = await alarmsTask;

        // Частичный KV-провал = неполный снапшот: консервативно отказ тика, данные прежние
        // (уточнение к spec §7.2 п.5: пустой префикс — валидные данные, транспортный отказ — нет).
        if (!clustersKv.IsSuccess || !serviceKv.IsSuccess || !nodesKv.IsSuccess || !portAllocKv.IsSuccess)
            return FailTick(previous, statuses, now, "KV-чтения etcd не удались");

        // 4. Парсеры → модель (чистые функции, arch/02 §4 п.3).
        var clustersParsed = ClustersParser.Parse(clustersKv.Value);
        var serviceParsed = ServiceParser.Parse(
            serviceKv.Value, clustersParsed.Clusters, ServiceParser.ParsePortAlloc(portAllocKv.Value));
        var nodes = StandNodesParser.Parse(nodesKv.Value);

        // 5. Кворум-эвристика (spec §3.11) + мягкие метаданные member/alarm (ошибка не роняет тик).
        var quorumSuspected = alive.All(a => a.LeaderMemberId is not > 0 || a.RaftTerm is not > 0)
            || alive.Any(a => a.Errors.Any(IsRaftError));

        var metaErrors = new List<string>();
        if (!members.IsSuccess)
            metaErrors.Add("member/list: " + members.Error!.Message);
        if (!alarms.IsSuccess)
            metaErrors.Add("alarm: " + alarms.Error!.Message);
        EtcdEndpoint[] patchedStatuses = statuses;
        if (metaErrors.Count > 0)
        {
            patchedStatuses = [.. statuses.Select(s => s.Url == active.Url
                ? s with { Errors = [.. s.Errors, .. metaErrors] }
                : s)];
        }

        var etcd = new EtcdStatus(
            true,
            patchedStatuses,
            members.IsSuccess ? members.Value : [],
            alarms.IsSuccess ? alarms.Value : [],
            active.Url,
            quorumSuspected,
            now,
            0);

        // 6. Сборка + внесение проб (arch/02 §4 п.3) + алерты + атомарная замена
        // (arch/02 §4 п.4–5; Alerts на обоих путях тика, spec §5; spec §3.1).
        var built = ProbeEnricher.Apply(
            SnapshotBuilder.Build(
                time, clustersParsed, serviceParsed, nodes,
                etcd.Members, etcd.Alarms, etcd),
            probeStateStore.Current);
        store.Replace(built with
        {
            Alerts = alertEngine.Evaluate(built, previous, now, EffectiveIntervalSeconds()),
        });
        return Finish(Result.Success(), working: true);
    }

    private async Task<EtcdEndpoint> StatusOfAsync(string endpoint, CancellationToken ct)
    {
        var started = Stopwatch.GetTimestamp();
        var result = await gateway.StatusAsync(endpoint, ct);
        var latencyMs = Stopwatch.GetElapsedTime(started).TotalMilliseconds;
        return result.IsSuccess
            ? new EtcdEndpoint(
                endpoint, true, latencyMs, result.Value.Version, result.Value.DbSizeBytes,
                result.Value.LeaderMemberId, result.Value.RaftIndex, result.Value.RaftTerm, [])
            : new EtcdEndpoint(endpoint, false, null, null, null, null, null, null, [result.Error!.Message]);
    }

    // Failover: один проход по живым endpoints по кругу от активного; смена цели, не повтор
    // (retry внутри тика запрещён arch/02 §4; failover — §3.10).
    private static async Task<Result<T>> WithFailoverAsync<T>(
        EtcdEndpoint[] alive,
        EtcdEndpoint active,
        Func<string, CancellationToken, Task<Result<T>>> call,
        CancellationToken ct)
    {
        var start = Array.IndexOf(alive, active);
        Exception? last = null;
        for (var i = 0; i < alive.Length; i++)
        {
            var endpoint = alive[(start + i) % alive.Length];
            var result = await call(endpoint.Url, ct);
            if (result.IsSuccess)
                return result;
            last = result.Error!;
        }

        return Result<T>.Failed(new EtcdUnreachableException(
            $"все живые endpoints не ответили: {last?.Message}"));
    }

    // Отказ тика: свежий Etcd-статус + прежние данные/BuiltAtUtc (spec §3.9).
    private Result FailTick(
        EtcdSnapshot? previous,
        IReadOnlyList<EtcdEndpoint> statuses,
        DateTimeOffset now,
        string reason)
    {
        var error = Result.Failed(new EtcdUnreachableException(reason));
        var etcd = new EtcdStatus(
            false,
            statuses,
            previous?.Etcd.Members ?? [],
            previous?.Etcd.Alarms ?? [],
            null,
            previous?.Etcd.QuorumSuspected ?? false,
            now,
            (previous?.Etcd.ConsecutiveFailures ?? 0) + 1);
        var failed = new EtcdSnapshot(
            previous?.BuiltAtUtc ?? now,
            etcd,
            previous?.Clusters ?? [],
            previous?.HaScopes ?? [],
            previous?.StandNodes ?? [],
            previous?.Probes ?? [],   // t06: пробы — часть снапшота, отказ etcd их не теряет (spec §4.3)
            [],
            previous?.ParseErrors ?? [],
            previous?.UnknownKeyCount ?? 0);

        // Алерты вычисляются и на отказном тике: etcd-unreachable/snapshot-stale
        // живут именно здесь (spec §3.5); data-алерты пересчитываются по прежним данным.
        store.Replace(failed with
        {
            Alerts = alertEngine.Evaluate(failed, previous, now, EffectiveIntervalSeconds()),
        });
        return Finish(error, working: false);
    }

    private Result Finish(Result status, bool working)
    {
        _inited = true;
        _working = working;
        _statusError = status;
        return status;
    }

    private static bool IsValidEndpoint(string endpoint)
        => Uri.TryCreate(endpoint, UriKind.Absolute, out var uri)
            && uri.Scheme is "http" or "https"
            && !string.IsNullOrEmpty(uri.Host);

    // Эффективный интервал тика: RefreshIntervalSeconds или fallback 3 c (t03 §3.3);
    // тот же порог ×3 кормит snapshot-stale (spec §3.3).
    private double EffectiveIntervalSeconds()
    {
        var seconds = options.Value.RefreshIntervalSeconds;
        return seconds > 0 ? seconds : 3;
    }

    private static bool IsRaftError(string message)
        => message.Contains("raft", StringComparison.OrdinalIgnoreCase)
            || message.Contains("no leader", StringComparison.OrdinalIgnoreCase)
            || message.Contains("quorum", StringComparison.OrdinalIgnoreCase);

    private static class Prefixes
    {
        public const string Clusters = "/clusters/";
        public const string Service = "/service/";
        public const string Nodes = "/cluster/nodes/";
        public const string PortAlloc = "/pgworker/portalloc/";
    }
}
