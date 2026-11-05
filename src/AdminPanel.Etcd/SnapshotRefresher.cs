using System.Diagnostics;
using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using AdminPanel.Core.S3;
using Shared.Etcd.Client;
using AdminPanel.Etcd.Parsing;
using AdminPanel.Etcd.Workers;
using Shared.Core.DI;
using Shared.Core.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AdminPanel.Etcd;

// Единственный писатель снапшота (arch/01 §1): тик RefreshIntervalSeconds по arch/02 §4,
// применение только изменений — arch/02 §5 (arch/synchronization/05-delta-application.md §S10): чтение совпало с кэшем →
// пересборки и алертного пересчёта нет, обновляются только штампы свежести.
// Отказ тика = обновление только Etcd-части: данные и BuiltAtUtc прежние, возраст растёт (spec §3.9).
[InjectAsSingleton(typeof(IHostedService))]
public sealed class SnapshotRefresher(
    IEtcdGateway gateway,
    IAlertEngine alertEngine,
    ISnapshotStore store,
    IProbeStateStore probeStateStore,
    IWorkerHealthStore workerHealthStore,
    IS3InventoryStore inventoryStore,
    IEtcdWorkerHealthStore etcdWorkerHealthStore,
    IS3SnapshotReader s3SnapshotStore,
    IOptions<EtcdOptions> options,
    TimeProvider time,
    ILogger<SnapshotRefresher> logger) : BackgroundService, IHealthCheckService
{
    private string? _activeEndpoint;
    private bool _inited;
    private bool _working;
    private Result _statusError = Result.Success();
    private bool _endpointsWarned;

    // Кэш последнего применённого тика (arch/02 §5, arch/synchronization/05-delta-application.md §S10): тик сравнивает
    // прочитанное с ним и пересобирает снапшот только при фактическом изменении.
    // Members — проекцией: record несёт списки URL (reference-equality).
    private Dictionary<string, IReadOnlyList<Kv>>? _lastRead;
    private (ulong Id, string? Name, string PeerUrls, string ClientUrls, bool IsLearner)[]? _lastMemberFacts;
    private IReadOnlyList<EtcdAlarm>? _lastAlarms;
    private (string Url, bool Reachable, string? Version, ulong? LeaderMemberId, ulong? RaftTerm, string Errors)[]? _lastStatusFacts;

    public bool Inited => _inited;

    public bool Working => _working;

    public Result StatusError => _statusError;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var seconds = options.Value.RefreshIntervalSeconds;
        if (seconds <= 0)
        {
            logger.LogWarning("AdminPanel:Etcd:RefreshIntervalSeconds <= 0 — using 3 s");
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
            logger.LogWarning("AdminPanel:Etcd:Endpoints is not set or invalid — etcd data unavailable");
            _endpointsWarned = true;
        }

        var now = time.GetUtcNow();
        var previous = store.Current;

        if (endpoints.Length == 0)
            return FailTick(previous, [], now, "AdminPanel:Etcd:Endpoints is not set or invalid");

        // 1. Статусы персонально по всем endpoints — параллельно (arch/02 §4 п.1).
        var statuses = await Task.WhenAll(endpoints.Select(e => StatusOfAsync(e, ct)));
        var alive = statuses.Where(s => s.Reachable).ToArray();
        if (alive.Length == 0)
            return FailTick(previous, statuses, now, "no alive etcd endpoints");

        // 2. Активный: sticky с прошлого тика, иначе первый живой (arch/02 §4 п.1).
        var active = alive.FirstOrDefault(e => e.Url == _activeEndpoint) ?? alive[0];
        _activeEndpoint = active.Url;

        // 3. Чтения на активном — параллельно; транспортный провал → failover по кругу живых (spec §3.10).
        // URL живых endpoints — один массив на тик (не пересобирается в каждом WithFailoverAsync).
        var aliveUrls = alive.Select(e => e.Url).ToArray();
        var clustersTask = WithFailoverAsync(aliveUrls, active, (ep, t) => gateway.RangeAsync(ep, Prefixes.Clusters, t), ct);
        var serviceTask = WithFailoverAsync(aliveUrls, active, (ep, t) => gateway.RangeAsync(ep, Prefixes.Service, t), ct);
        var nodesTask = WithFailoverAsync(aliveUrls, active, (ep, t) => gateway.RangeAsync(ep, Prefixes.Nodes, t), ct);
        var portAllocTask = WithFailoverAsync(aliveUrls, active, (ep, t) => gateway.RangeAsync(ep, Prefixes.PortAlloc, t), ct);
        var movesTask = WithFailoverAsync(aliveUrls, active, (ep, t) => gateway.RangeAsync(ep, Prefixes.Moves, t), ct);
        var backupsTask = WithFailoverAsync(aliveUrls, active, (ep, t) => gateway.RangeAsync(ep, Prefixes.Backups, t), ct);
        var pgApiTask = WithFailoverAsync(aliveUrls, active, (ep, t) => gateway.RangeAsync(ep, Prefixes.PgWorkerApi, t), ct);
        var pgCertTask = WithFailoverAsync(aliveUrls, active, (ep, t) => gateway.RangeAsync(ep, Prefixes.WorkerApiCert, t), ct);
        var etcdSnapshotsTask = WithFailoverAsync(aliveUrls, active, (ep, t) => gateway.RangeAsync(ep, Prefixes.EtcdSnapshots, t), ct);
        var etcdConfigTask = WithFailoverAsync(aliveUrls, active, (ep, t) => gateway.RangeAsync(ep, Prefixes.EtcdConfig, t), ct);
        var etcdNodesTask = WithFailoverAsync(aliveUrls, active, (ep, t) => gateway.RangeAsync(ep, Prefixes.EtcdNodes, t), ct);
        var etcdwApiTask = WithFailoverAsync(aliveUrls, active, (ep, t) => gateway.RangeAsync(ep, Prefixes.EtcdWorkerApi, t), ct);
        var etcdwCertTask = WithFailoverAsync(aliveUrls, active, (ep, t) => gateway.RangeAsync(ep, Prefixes.EtcdWorkerCert, t), ct);
        var workTask = WithFailoverAsync(aliveUrls, active, (ep, t) => gateway.RangeAsync(ep, Prefixes.PgWorkerWork, t), ct);
        // u10–u14: стоп бэкапов (точечный range — образец WorkerApiCert),
        // эвакуации, живые заявки ротаций, координация leader/claims/instances.
        var backupsDisabledTask = WithFailoverAsync(aliveUrls, active, (ep, t) => gateway.RangeAsync(ep, Prefixes.BackupsDisabled, t), ct);
        var evacuationsTask = WithFailoverAsync(aliveUrls, active, (ep, t) => gateway.RangeAsync(ep, Prefixes.Evacuations, t), ct);
        var pgRotationsTask = WithFailoverAsync(aliveUrls, active, (ep, t) => gateway.RangeAsync(ep, Prefixes.PgRotations, t), ct);
        var leaderTask = WithFailoverAsync(aliveUrls, active, (ep, t) => gateway.RangeAsync(ep, Prefixes.Leader, t), ct);
        var claimsTask = WithFailoverAsync(aliveUrls, active, (ep, t) => gateway.RangeAsync(ep, Prefixes.Claims, t), ct);
        var instancesTask = WithFailoverAsync(aliveUrls, active, (ep, t) => gateway.RangeAsync(ep, Prefixes.Instances, t), ct);
        var membersTask = WithFailoverAsync(aliveUrls, active, (ep, t) => gateway.MemberListAsync(ep, t), ct);
        var alarmsTask = WithFailoverAsync(aliveUrls, active, (ep, t) => gateway.AlarmAsync(ep, t), ct);

        var clustersKv = await clustersTask;
        var serviceKv = await serviceTask;
        var nodesKv = await nodesTask;
        var portAllocKv = await portAllocTask;
        var movesKv = await movesTask;
        var backupsKv = await backupsTask;
        var pgApiKv = await pgApiTask;
        var workKv = await workTask;
        var etcdConfigKv = await etcdConfigTask;
        var etcdNodesKv = await etcdNodesTask;
        var etcdwApiKv = await etcdwApiTask;
        var etcdwCertKv = await etcdwCertTask;
        var members = await membersTask;
        var alarms = await alarmsTask;

        // Частичный KV-провал = неполный снапшот: консервативно отказ тика, данные прежние
        // (уточнение к spec §7.2 п.5: пустой префикс — валидные данные, транспортный отказ — нет).
        var pgCertKv = await pgCertTask;
        var etcdSnapshotsKv = await etcdSnapshotsTask;

        var backupsDisabledKv = await backupsDisabledTask;
        var evacuationsKv = await evacuationsTask;
        var pgRotationsKv = await pgRotationsTask;
        var leaderKv = await leaderTask;
        var claimsKv = await claimsTask;
        var instancesKv = await instancesTask;

        if (!clustersKv.IsSuccess || !serviceKv.IsSuccess || !nodesKv.IsSuccess
            || !portAllocKv.IsSuccess || !movesKv.IsSuccess || !backupsKv.IsSuccess
            || !pgApiKv.IsSuccess || !workKv.IsSuccess || !pgCertKv.IsSuccess
            || !etcdSnapshotsKv.IsSuccess || !etcdConfigKv.IsSuccess || !etcdNodesKv.IsSuccess
            || !etcdwApiKv.IsSuccess || !etcdwCertKv.IsSuccess
            || !backupsDisabledKv.IsSuccess
            || !evacuationsKv.IsSuccess || !pgRotationsKv.IsSuccess
            || !leaderKv.IsSuccess || !claimsKv.IsSuccess || !instancesKv.IsSuccess)
            return FailTick(previous, statuses, now, "etcd KV reads failed");

        // Прочитанное тика — вход сравнения с кэшем (arch/02 §5). Member/alarm
        // при мягком сбое применяются пустыми (прежнее поведение) — кэш зеркалит
        // применённое, а не запрошенное.
        (string Prefix, IReadOnlyList<Kv> Items)[] reads =
        [
            (Prefixes.Clusters, clustersKv.Value),
            (Prefixes.Service, serviceKv.Value),
            (Prefixes.Nodes, nodesKv.Value),
            (Prefixes.PortAlloc, portAllocKv.Value),
            (Prefixes.Moves, movesKv.Value),
            (Prefixes.Backups, backupsKv.Value),
            (Prefixes.PgWorkerApi, pgApiKv.Value),
            (Prefixes.WorkerApiCert, pgCertKv.Value),
            (Prefixes.EtcdSnapshots, etcdSnapshotsKv.Value),
            (Prefixes.EtcdConfig, etcdConfigKv.Value),
            (Prefixes.EtcdNodes, etcdNodesKv.Value),
            (Prefixes.EtcdWorkerApi, etcdwApiKv.Value),
            (Prefixes.EtcdWorkerCert, etcdwCertKv.Value),
            (Prefixes.PgWorkerWork, workKv.Value),
            (Prefixes.BackupsDisabled, backupsDisabledKv.Value),
            (Prefixes.Evacuations, evacuationsKv.Value),
            (Prefixes.PgRotations, pgRotationsKv.Value),
            (Prefixes.Leader, leaderKv.Value),
            (Prefixes.Claims, claimsKv.Value),
            (Prefixes.Instances, instancesKv.Value),
        ];
        IReadOnlyList<EtcdMember> appliedMembers = members.IsSuccess ? members.Value : [];
        IReadOnlyList<EtcdAlarm> appliedAlarms = alarms.IsSuccess ? alarms.Value : [];

        // Кворум-эвристика (spec §3.11) + мягкие метаданные member/alarm (ошибка не
        // роняет тик); свежая Etcd-часть нужна и тихому тику (телеметрия доступности).
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
            appliedMembers,
            appliedAlarms,
            active.Url,
            quorumSuspected,
            now,
            0);

        // arch/02 §5 (arch/synchronization/05-delta-application.md §S10): прочитанное идентично кэшу (последнему
        // успешно прочитанному состоянию из etcd) — тик без изменений: данные не
        // пересобираются, парсеры/AlertEngine/потребители не вызываются;
        // обновляются свежая Etcd-часть (телеметрия доступности не замерзает)
        // и штампы свежести. Ветвление только по факту совпадения с кэшем,
        // без истории исходов тиков: совпадение после сбоя — тик без изменений,
        // различие — пересборка.
        if (previous is EtcdSnapshot lastApplied
            && TickUnchanged(reads, appliedMembers, appliedAlarms, statuses))
        {
            store.Replace(lastApplied with
            {
                BuiltAtUtc = now,
                Etcd = etcd,
            });
            return Finish(Result.Success(), working: true);
        }

        // 4. Парсеры → модель (чистые функции, arch/02 §4 п.3).
        var clustersParsed = ClustersParser.Parse(clustersKv.Value);
        var serviceParsed = ServiceParser.Parse(
            serviceKv.Value, clustersParsed.Clusters, ServiceParser.ParsePortAlloc(portAllocKv.Value));
        var nodes = StandNodesParser.Parse(nodesKv.Value);
        var movesParsed = MovesQueueParser.Parse(movesKv.Value);
        var backupsParsed = BackupsParser.Parse(backupsKv.Value);
        var pgApiParsed = WorkerEndpointsParser.Parse(pgApiKv.Value);
        // Префикс-запрос точечный: ровно один ключ /workers/api_tls/pgworker.
        var pgCertParsed = WorkerCertParser.Parse(Prefixes.WorkerApiCert, pgCertKv.Value.FirstOrDefault());
        // Префикс-запрос точечный: ровно один ключ /etcdworker/snapshots
        // (паттерн WorkerApiCert; arch/etcdworker/00-etcd-contract.md).
        var etcdSnapshotsParsed = EtcdSnapshotsParser.Parse(etcdSnapshotsKv.Value.FirstOrDefault());
        // Домен EtcdWorker (adminpanel/02 §2.3.5): декларация + managed-узлы,
        // живые ключи /etcdworker/api/, целевой серт /workers/api_tls/etcdworker.
        var etcdDomainParsed = EtcdDomainParser.Parse(etcdConfigKv.Value.FirstOrDefault(), etcdNodesKv.Value);
        var etcdwApiParsed = WorkerEndpointsParser.Parse(etcdwApiKv.Value);
        var etcdwCertParsed = WorkerCertParser.Parse(Prefixes.EtcdWorkerCert, etcdwCertKv.Value.FirstOrDefault());
        var workParsed = WorkJournalParser.Parse(workKv.Value);
        // u10–u14: точечный ключ стопа бэкапов (образец WorkerApiCert),
        // эвакуации, живые заявки ротаций pg, координация pg-домена.
        var backupsDisabledParsed = BackupsDisabledParser.Parse(backupsDisabledKv.Value.FirstOrDefault());
        var evacuationsParsed = EvacuationsParser.Parse(evacuationsKv.Value);
        var pgRotationsParsed = RotationsParser.Parse(pgRotationsKv.Value);
        var coordinationParsed = CoordinationParser.Parse(leaderKv.Value.FirstOrDefault(), claimsKv.Value, instancesKv.Value);

        // 6. Сборка + внесение проб (arch/02 §4 п.3) + health воркеров (spec D4,
        // образец ProbeEnricher.Apply — poller пишет независимо от KV-тика) +
        // алерты + атомарная замена (arch/02 §4 п.4–5; Alerts на обоих путях тика, spec §5; spec §3.1).
        var builtBase = SnapshotBuilder.Build(
            time, clustersParsed, serviceParsed, nodes, movesParsed, backupsParsed, pgApiParsed, workParsed,
            etcd.Members, etcd.Alarms, etcd, pgCertParsed, etcdSnapshotsParsed,
            etcdDomainParsed, etcdwApiParsed, etcdwCertParsed,
            backupsDisabledParsed.Info,
            backupsParsed.Agents ?? [],
            evacuationsParsed,
            pgRotationsParsed,
            coordinationParsed.Coordination,
            coordinationParsed.Errors,
            backupsDisabledParsed.Error);
        var enrichedBase = ProbeEnricher.Apply(builtBase, probeStateStore.Current)
            with
        {
            WorkerHealth = workerHealthStore.Current ?? [],
            // live-S3-инвентарь — готовым состоянием из стора (по образцу
            // WorkerHealth: poller пишет независимо, KV-тик не блокируется).
            S3Storage = inventoryStore.Current,
 // health etcdworker-инстансов — готовым из стора общего
            // поллера (паттерн S3WorkerHealthStore); факт etcd-backup
            // S3-кластера — агрегатом из S3-домена (adminpanel/03 §4).
            EtcdWorkerHealth = etcdWorkerHealthStore.Current ?? [],
            EtcdBackupTarget = ResolveEtcdBackupTarget(s3SnapshotStore.Current),
        };
        store.Replace(enrichedBase with
        {
            Alerts = alertEngine.Evaluate(enrichedBase, previous, now, EffectiveIntervalSeconds()),
        });
        RememberTick(reads, appliedMembers, appliedAlarms, statuses);
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
        string[] aliveUrls,
        EtcdEndpoint active,
        Func<string, CancellationToken, Task<Result<T>>> call,
        CancellationToken ct)
    {
        var result = await EtcdEndpointFailover.CallAsync(
            aliveUrls, call, ct,
            Math.Max(Array.IndexOf(aliveUrls, active.Url), 0));
        return result.IsSuccess
            ? result
            : Result<T>.Failed(new EtcdUnreachableException(
                $"all alive endpoints failed to answer: {result.Error!.Message}"));
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
            previous?.MoveTickets ?? [],  // очередь заявок не теряется на отказном тике — как Clusters
            previous?.Backups ?? [],      // префикс бэкапов переживает отказный тик — как moves 
            previous?.PgWorkerEndpoints ?? [], // ключи доступа воркера переживают отказ etcd
            previous?.PgWorkerWork ?? [], // журналы процессов воркера переживают отказ etcd (R4)
            previous?.WorkerHealth ?? [], // health-пробы переживают отказ etcd (poller независим)
            previous?.Probes ?? [],   // пробы — часть снапшота, отказ etcd их не теряет (spec §4.3)
            [],
            previous?.ParseErrors ?? [],
            previous?.UnknownKeyCount ?? 0,
            previous?.BackupStorage, // ключ storage переживает отказный тик — как Backups 
            previous?.BackupOrphans, // реестр сирот переживает отказный тик — как storage 
            previous?.S3Storage, // S3-инвентарь переживает отказ etcd (как WorkerHealth)
            null, // WorkerApiCert — прежнее поведение отказного тика (серт не переносится)
            previous?.EtcdSnapshots, // статус DR-выгрузки слепков переживает отказный тик
            previous?.BackupsDisabled, // стоп бэкапов переживает отказный тик (u10)
            previous?.WalAgents ?? [], // приёмники WAL переживают отказный тик (u11)
            previous?.Evacuations ?? [], // эвакуации переживают отказный тик (u12)
            previous?.PgRotations ?? [], // живые заявки ротаций переживают отказный тик (u07)
            previous?.Coordination, // координация переживает отказный тик (u14)
            previous?.EtcdDomain, // домен EtcdWorker переживает отказный тик
            previous?.EtcdWorkerEndpoints, // «не читались» переживает отказный тик (null ≠ пусто)
            previous?.EtcdWorkerHealth ?? [], // health etcdworker — независимый поллер
            previous?.EtcdBackupTarget, // факт etcd-backup S3-кластера — из S3-домена
            previous?.EtcdWorkerApiCert); // целевой серт etcdworker (не переносится только при отсутствии)

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

    // Кэш последнего применённого тика: запоминается только на пересборке —
    // отказной и тик без изменений кэш не меняют (arch/02 §5).
    private void RememberTick(
        (string Prefix, IReadOnlyList<Kv> Items)[] reads,
        IReadOnlyList<EtcdMember> members,
        IReadOnlyList<EtcdAlarm> alarms,
        IReadOnlyList<EtcdEndpoint> statuses)
    {
        _lastRead = reads.ToDictionary(r => r.Prefix, r => r.Items);
        _lastMemberFacts = members.Select(MemberFact).ToArray();
        _lastAlarms = alarms;
        _lastStatusFacts = statuses.Select(StatusFact).ToArray();
    }

    // Факты статуса endpoint'а без изменчивых от тика к тику полей (latency/
    // dbSize/raftIndex — шум каждого тика): вход сравнения с кэшем (arch/02 §5).
    private static (string Url, bool Reachable, string? Version, ulong? LeaderMemberId, ulong? RaftTerm, string Errors) StatusFact(EtcdEndpoint e)
        => (e.Url, e.Reachable, e.Version, e.LeaderMemberId, e.RaftTerm, string.Join(";", e.Errors));

    // Проекция члена кластера (PeerUrls/ClientUrls — списки с reference-
    // equality, record их не сравнивает по значению).
    private static (ulong Id, string? Name, string PeerUrls, string ClientUrls, bool IsLearner) MemberFact(EtcdMember m)
        => (m.Id, m.Name, string.Join(",", m.PeerUrls), string.Join(",", m.ClientUrls), m.IsLearner);

    // Сравнение прочитанного тика с кэшем последнего применённого (arch/02 §5):
    // списки KV всех наблюдаемых префиксов (Kv — record: ключ/значение/
    // mod_revision) + member list + alarm + факты статусов. Кэш не собран
    // (первый тик / после отказа — Reachable=false у снапшота, проверяет
    // вызывающий) → «изменено».
    private bool TickUnchanged(
        (string Prefix, IReadOnlyList<Kv> Items)[] reads,
        IReadOnlyList<EtcdMember> members,
        IReadOnlyList<EtcdAlarm> alarms,
        IReadOnlyList<EtcdEndpoint> statuses)
    {
        if (_lastRead is null || _lastMemberFacts is null || _lastAlarms is null || _lastStatusFacts is null)
            return false;
        if (reads.Length != _lastRead.Count)
            return false;
        foreach (var (prefix, items) in reads)
            if (!_lastRead.TryGetValue(prefix, out var cached) || !cached.SequenceEqual(items))
                return false;
        return members.Select(MemberFact).SequenceEqual(_lastMemberFacts)
            && alarms.SequenceEqual(_lastAlarms)
            && statuses.Select(StatusFact).SequenceEqual(_lastStatusFacts);
    }

    private static bool IsValidEndpoint(string endpoint)
        => Uri.TryCreate(endpoint, UriKind.Absolute, out var uri)
            && uri.Scheme is "http" or "https"
            && !string.IsNullOrEmpty(uri.Host);

    // Эффективный интервал тика: RefreshIntervalSeconds или fallback 3 c (§3.3);
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
        public const string Moves = "/pgworker/moves/";
        public const string Backups = "/pgworker/backups/";
        public const string PgWorkerApi = "/pgworker/api/";
        public const string PgWorkerWork = "/pgworker/work/";
        public const string WorkerApiCert = "/workers/api_tls/pgworker";
        public const string EtcdSnapshots = "/etcdworker/snapshots"; // статус DR-слепков EtcdWorker (arch/etcdworker/00-etcd-contract.md)
        public const string EtcdConfig = "/etcd/config";             // декларация состава контура (02 §2.3.5)
        public const string EtcdNodes = "/etcd/nodes/";              // managed-узлы контура (02 §2.3.5)
        public const string EtcdWorkerApi = "/etcdworker/api/";      // живые ключи EtcdWorker (02 §2.3.5)
        public const string EtcdWorkerCert = "/workers/api_tls/etcdworker"; // целевой серт EtcdWorker (02 §9.9)
        // u10–u14 (adminpanel/02 §2.3.1): глобальный стоп бэкапов — ТОЧЕЧНЫЙ
        // range (ключ НЕ входит в /pgworker/backups/ — '-' < '/' байтово);
        // остальные — обычные range-чтения того же тика.
        public const string BackupsDisabled = "/pgworker/backups-disabled";
        public const string Evacuations = "/pgworker/evacuations/";
        public const string PgRotations = "/pgworker/rotations/";
        public const string Leader = "/pgworker/leader";
        public const string Claims = "/pgworker/claims/";
        public const string Instances = "/pgworker/instances/";
    }

    // Факт etcd-backup S3-кластера (adminpanel/03 §4): из S3-домена готовым
    // агрегатом (паттерн live-инвентаря S3Storage); null — домен ещё не читался
    // (алерт etcd-backup-cluster-missing молчит), Cluster=null — кластера нет.
    private static EtcdBackupTargetInfo? ResolveEtcdBackupTarget(S3Snapshot? s3)
        => s3 is { EtcdReachable: true }
            ? new(s3.Clusters
                .Where(c => c.EtcdBackup && c.State != S3ClusterState.ToRemove)
                .Select(c => c.Name)
                .FirstOrDefault())
            : null;
}
