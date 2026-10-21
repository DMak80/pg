using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using Microsoft.Extensions.Options;
using Shared.Core.HealthChecks;
using PgWorker.Core;
using PgWorker.Core.Model;
using Shared.Etcd.Client;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Processes;

namespace PgWorker.App.Loops;

/// <summary>
/// Главный цикл (задача 23; spec §6.2, arch/14 §4; образец — BusConsumerHostedService
/// из Puzzle): тик = снапшот /clusters/ + /service/ → классификация → клэйм →
/// процесс; ошибка тика не роняет цикл (лог + ErrorDelayMs, следующий тик — ретрай).
/// Кластеры обрабатываются параллельно (SemaphoreSlim MaxClusters), внутри
/// кластера — строго последовательно. Эвакуационные события надзора
/// (SuperviseOutcome.DeadShards — значение тика, не состояние синглтона)
/// передаются BucketEvacuator в этом же тике.
/// </summary>
internal sealed class ReconcileLoop(
    IOptionsMonitor<PgWorkerOptions> options,
    IEtcdGateway etcd,
    ClaimStore claims,
    IClusterProcesses processes,
    WorkJournal journal,
    ILogger<ReconcileLoop> logger,
    HealthState health,
    Shared.Metrics.Worker.WorkerMetricsInstrumentation metrics) : BackgroundService, IHealthCheckService
{
    public bool Inited { get; private set; }

    public bool Working { get; private set; }

    public Result StatusError { get; private set; } = Result.Success();

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        Inited = true;
        try
        {
            Working = true;
            while (!stoppingToken.IsCancellationRequested)
            {
                var started = Stopwatch.GetTimestamp();
                var tick = await TickSafelyAsync(stoppingToken);
                metrics.LoopDuration("reconcile", Stopwatch.GetElapsedTime(started).TotalSeconds);
                if (tick.IsSuccess)
                {
                    // healthz = «последний тик» (живой-Ф7): успешный тик гасит ошибку
                    // прошлого — иначе единственный упавший тик = вечный unhealthy.
                    StatusError = Result.Success();
                    await Task.Delay(
                        TimeSpan.FromSeconds(options.CurrentValue.Loops.ScanIntervalSec), stoppingToken);
                }
                else
                {
                    metrics.LoopTick("reconcile", ok: false);
                    // Тик не прошёл (etcd недоступен и т.п.): лог + короткая задержка.
                    StatusError = tick;
                    logger.LogError(tick.Error, "ReconcileLoop tick failed: {Message}", tick.Error!.Message);
                    await Task.Delay(
                        TimeSpan.FromMilliseconds(options.CurrentValue.Loops.ErrorDelayMs), stoppingToken);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // штатная остановка host'а
        }
        finally
        {
            Working = false;
        }
    }

    /// <summary>
    /// Тик с защитой тела цикла (rework №3): исключение тика (внезапный баг) не
    /// роняет BackgroundService (ExceptionBehavior.StopHost) — превращается в
    /// ошибку тика (лог + ErrorDelayMs), следующий проход продолжит.
    /// </summary>
    internal async Task<Result> TickSafelyAsync(CancellationToken ct)
    {
        try
        {
            return await TickAsync(ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw; // остановка host'а — не «ошибка тика»
        }
        catch (Exception ex)
        {
            return Result.Failed(ex);
        }
    }

    /// <summary>Один тик (публичен для тестов/health): снапшот → параллельная обработка кластеров.</summary>
    internal async Task<Result> TickAsync(CancellationToken ct)
    {
        var endpoints = options.CurrentValue.Etcd.Endpoints.ToArray();
        if (endpoints.Length == 0)
            return Result.Failed(new ApplicationException("PgWorker:Etcd:Endpoints are not set"));

        health.MarkReconcileActivity(); // heartbeat: старт итерации (долгие фазы отмечаются глубже)

        // Снапшот контроль-плейна (P9: без свежего чтения мутаций не делаем).
        var clustersKvs = await RangeWithFailoverAsync(endpoints, "/clusters/", ct);
        if (!clustersKvs.IsSuccess)
            return Result.Failed(clustersKvs.Error!);
        var serviceKvs = await RangeWithFailoverAsync(endpoints, "/service/", ct);
        if (!serviceKvs.IsSuccess)
            return Result.Failed(serviceKvs.Error!);

        // Бэкапы (t02/t03, arch/19 §4): префикс читаем ВСЕГДА — стоп-семантика t03
        // (Enabled=false → финальный STOPPED) обязана видеть живые ключи; пустой
        // префикс дёшев. Потребители фильтруются по Enabled сами (t02-процесс).
        var backupsKvs = await RangeWithFailoverAsync(endpoints, "/pgworker/backups/", ct);
        if (!backupsKvs.IsSuccess)
            return Result.Failed(backupsKvs.Error!);
        var backupsParsed = BackupsParser.Parse(backupsKvs.Value, out var backupsErrors);
        foreach (var error in backupsErrors)
            logger.LogWarning("skipped broken backups key: {Error}", error);
        IReadOnlyList<ClusterBackups> backups = backupsParsed.Value;

        health.MarkEtcdOk();

        var parsed = ClusterSnapshotParser.ParseClusters(clustersKvs.Value, out var parseErrors);
        foreach (var error in parseErrors)
            logger.LogWarning("skipped broken key: {Error}", error);

        // Параллельная обработка кластеров с лимитом; ошибка кластера не роняет тик
        // (journal процесса уже несёт last_error — следующий тик продолжит).
        var gate = new SemaphoreSlim(Math.Max(1, options.CurrentValue.Parallelism.MaxClusters));
        try
        {
            var backupsList = backupsParsed.Value;
            var tasks = parsed.Value
                .Select(snap => ProcessClusterAsync(snap, gate, backups, ct))
                .ToArray();
            await Task.WhenAll(tasks);
        }
        finally
        {
            gate.Dispose();
        }

        health.MarkReconcileTick(ok: true, claimsHeld: parsed.Value.Count(c => claims.IsMine(c.Config.Cluster)));
        metrics.LoopTick("reconcile", ok: true);
        metrics.ClaimsHeld(parsed.Value.Count(c => claims.IsMine(c.Config.Cluster)));
        return Result.Success();
    }

    // Обработка одного кластера под семафором: клэйм → процесс → эвакуация.
    // Исключение ЛЮБОГО кластера не роняет тик и сервис (rework №3): catch-all
    // → лог + journal.last_error, следующий тик продолжит этот кластер.
    private async Task ProcessClusterAsync(
        ClusterSnapshot snap, SemaphoreSlim gate, IReadOnlyList<ClusterBackups> backups, CancellationToken ct)
    {
        await gate.WaitAsync(ct);
        CancellationTokenSource? lease = null; // linked-токен клэйма — виден catch'ам
        try
        {
            var cluster = snap.Config.Cluster;
            var work = ClusterClassifier.Classify(snap.Config);

            // Клэйм ДО процесса (Д2): exclusivity; занят другим — не ошибка.
            var claimed = await claims.TryClaimClusterAsync(cluster, ct);
            if (!claimed.IsSuccess)
            {
                logger.LogError(claimed.Error, "claim {Cluster} failed: {Message}", cluster, claimed.Error!.Message);
                return;
            }

            if (!claimed.Value)
                return; // обрабатывает другой инстанс

            // Cancellation-гвард клэйма (arch/17 S5): потеря lease прерывает
            // исполняющийся тик кластера — linked-токен течёт во все операции.
            lease = CancellationTokenSource.CreateLinkedTokenSource(ct, claims.LeaseToken(cluster));
            var tick = lease.Token;

            switch (work)
            {
                case ClusterWork.Provision:
                    await RunClusterOpAsync(cluster, "provision",
                        () => processes.ProvisionAsync(snap, tick), ct, tick);
                    break;

                case ClusterWork.Deprovision:
                    await RunClusterOpAsync(cluster, "deprovision",
                        () => processes.DeprovisionAsync(snap, tick), ct, tick);
                    break;

                default:
                    var supervised = await RunSuperviseAsync(cluster, snap, backups, ct, tick);
                    if (supervised is null)
                        break;

                    // Усыновление (spec §3.2, arch/14 §5 J): адреса dsn-шард без
                    // portalloc — до scale (add смотрит pinned portalloc) и до
                    // repair/moves (SQL нужен адрес).
                    await RunClusterOpAsync(cluster, "adopt",
                        () => processes.AdoptAsync(snap, tick), ct, tick);

                    // Scale-проход (t06 spec §5.1): remove → add, после надзора, до
                    // эвакуаций/moves — демонтаж освобождает хосты/порты для подъёма (Д13).
                    await RunClusterOpAsync(cluster, "scale-shards",
                        () => processes.ScaleShardsAsync(snap, tick), ct, tick);

                    // Ротация app-пароля (spec §4.3, arch/14 §5 I): короткая плановая
                    // операция — до эвакуаций/переездов, не ждёт длинных moves.
                    await RunClusterOpAsync(cluster, "rotate-app-password",
                        () => processes.RotateAppPasswordAsync(snap, tick), ct, tick);

                    // Полные бэкапы (t02, arch/19 §2): после коротких плановых
                    // операций, до репарации; тик запускает/поллит джобы, не ждёт их.
                    // Выключенная подсистема не зовётся вовсе (поведение без t02).
                    if (options.CurrentValue.Backups.Enabled)
                        await RunClusterOpAsync(cluster, "backups",
                            () => processes.BackupsAsync(snap, backups, tick), ct, tick);

                    // WAL-архивация (t03, arch/19 §3): после ротации (креды агента —
                    // свежий пароль в пересоздании) и до repair/moves (короткая).
                    // Зовётся ВСЕГДА: при Enabled=false это стоп-семантика
                    // (агенты вниз + финальный STOPPED в живых ключах).
                    await RunClusterOpAsync(cluster, "backup-wal",
                        () => processes.WalStreamAsync(snap, backups, tick), ct, tick);

                    // Проверки полных бэкапов (t04, arch/19 §5): после WAL-потока,
                    // до ретенции и repair; тик запускает/поллит verify-джобы, не ждёт
                    // их (non-blocking). Выключенная подсистема не зовётся вовсе.
                    if (options.CurrentValue.Backups.Enabled)
                        await RunClusterOpAsync(cluster, "backup-verify",
                            () => processes.VerifyBackupsAsync(snap, backups, tick), ct, tick);

                    // Ретенция (t06, arch/19 §4): после backup-wal/verify (чистит только
                    // то, что не нужно оставляемым полным; порядок verify→retention канон
                    // не фиксирует — проверяем до удаления), до repair; тик короткий
                    // (один кандидат/проход, batch-чанки). Выключенная подсистема
                    // не зовётся вовсе (AC8).
                    if (options.CurrentValue.Backups.Enabled)
                        await RunClusterOpAsync(cluster, "backups-retention",
                            () => processes.RetentionAsync(snap, backups, tick), ct, tick);

                    // Сверка S3↔etcd (t07, arch/19 §4): мусор full/<id>/ живого шарда
                    // без etcd-ключа; после ретенции (гигиена FAILED может оставить
                    // объекты), до restore (гвард владельца). Выключенная подсистема
                    // не зовётся.
                    if (options.CurrentValue.Backups.Enabled)
                        await RunClusterOpAsync(cluster, "backup-supervisor",
                            () => processes.SuperviseBackupsAsync(snap, backups, tick), ct, tick);

                    // Restore шардов из бэкапов (t05, arch/19 §3.5): после wal-потока
                    // (агент снесён демонтажём — процесс сам стопает), до repair/moves.
                    // Выключенная подсистема не зовётся; одна активная заявка за тик —
                    // машина состояний процесса (старейшая по Id).
                    if (options.CurrentValue.Backups.Enabled)
                        await RunClusterOpAsync(cluster, "backup-restore",
                            () => processes.RestoreAsync(snap, backups, tick), ct, tick);

                    // Дрилл восстановимости (reliability t02, arch/19 §3.6): после backup-restore
                    // (гвард restore-владения отработал в отборе), до repair. Зовётся ВСЕГДА:
                    // Backups:Enabled=false / интервал 0 — стоп-семантика в процессе (доводка
                    // активного дрилла и сноса контура, новых запусков нет).
                    await RunClusterOpAsync(cluster, "backup-drill",
                        () => processes.DrillAsync(snap, backups, tick), ct, tick);

                    // Репарация брошенных переездов (spec §3.5, arch/14 §5 K): синтетические
                    // заявки до moves — этот же тик начнёт их обработку (старейшая заявка).
                    await RunClusterOpAsync(cluster, "repair",
                        () => processes.RepairAsync(snap, tick), ct, tick);

                    // События эвакуации: полностью мёртвые шарды (spec §6.4 D/E).
                    foreach (var deadShard in supervised.DeadShards)
                        await RunClusterOpAsync(cluster, $"evacuate/{deadShard}",
                            () => processes.EvacuateAsync(snap, deadShard, backups, tick), ct, tick);

                    // Заявки переездов бакетов (t01, spec §5.3): после надзора и
                    // эвакуаций; MoveProcess сам держит клэйм-гвард (инвариант §4.3).
                    await RunClusterOpAsync(cluster, "moves",
                        () => processes.ProcessMovesAsync(snap, tick), ct, tick);
                    break;
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // остановка host'а посреди тика — не ошибка цикла
        }
        catch (OperationCanceledException oce) when (lease?.IsCancellationRequested == true)
        {
            // Потеря клэйма прервала операцию кластера вне обёрток (arch/17 S5):
            // внятная фаза в журнале; кластер подхватит следующий тик/инстанс.
            logger.LogWarning("cluster {Cluster}: lease lost — tick aborted ({Message})",
                snap.Config.Cluster, oce.Message);
            await WriteLeaseLostAsync(snap.Config.Cluster, "tick");
        }
        catch (Exception ex)
        {
            // Страховка контура кластера (rework №3): даже исключение вне
            // вызова процесса не должно уходить в Task.WhenAll → StopHost.
            logger.LogError(ex, "cluster {Cluster}: unhandled exception (tick continues)",
                snap.Config.Cluster);
        }
        finally
        {
            lease?.Dispose();
            gate.Release();
        }
    }

    // Вызов процесса под catch-all: исключение процесса → лог + журнал
    // (phase=crashed, last_error), штатный Result — в обычный лог цикла.
    // host — токен остановки хоста (дискриминатор: OCE по нему = остановка,
    // OCE по ct = потеря клэйма — гварда arch/17 S5).
    private async Task RunClusterOpAsync(
        string cluster, string op, Func<Task<Result<ProcessOutcome>>> call, CancellationToken host, CancellationToken ct)
    {
        // heartbeat (канон t19): завершение каждой операции кластера — активность
        // тика; долгие операции отмечают прогресс сами (provisioning/deprovision/
        // supervisor-S3), стыки между ними закрывает эта отметка — на медленном
        // хосте цепочка операций без отметок давала watchdog-тишину > порога.
        health.MarkReconcileActivity();
        try
        {
            await LogOutcomeAsync(cluster, op, await call());
        }
        catch (OperationCanceledException) when (host.IsCancellationRequested)
        {
            throw; // остановка host'а — пробрасываем (обработана уровнем выше)
        }
        catch (OperationCanceledException)
        {
            await WriteLeaseLostAsync(cluster, op); // потеря клэйма — гвард arch/17 S5
        }
        catch (Exception ex)
        {
            await LogCrashAsync(cluster, op, ex);
        }
        health.MarkReconcileActivity(); // heartbeat: операция завершена — тик жив
    }

    // Надзор: результат — SuperviseOutcome (мёртвые шарды); null = не прошёл
    // (ошибка Result залогирована) либо упал исключением (журнал записан).
    private async Task<SuperviseOutcome?> RunSuperviseAsync(
        string cluster, ClusterSnapshot snap, IReadOnlyList<ClusterBackups> backups, CancellationToken host, CancellationToken ct)
    {
        try
        {
            var supervised = await processes.SuperviseAsync(snap, backups, ct);
            if (!supervised.IsSuccess)
            {
                logger.LogError(supervised.Error, "supervise {Cluster} failed: {Message}",
                    cluster, supervised.Error!.Message);
                return null;
            }

            return supervised.Value;
        }
        catch (OperationCanceledException) when (host.IsCancellationRequested)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            await WriteLeaseLostAsync(cluster, "supervise"); // потеря клэйма — гвард arch/17 S5
            return null;
        }
        catch (Exception ex)
        {
            await LogCrashAsync(cluster, "supervise", ex);
            return null;
        }
    }

    // Фаза lease-lost в журнале воркера (arch/17 S5): потери клэйма видно из etcd,
    // без логов воркера. CancellationToken.None — токен тика отменён, запись
    // обязана доехать (как в LogCrashAsync).
    private async Task WriteLeaseLostAsync(string cluster, string op)
    {
        try
        {
            await journal.WritePhaseAsync(
                cluster, op.Split('/')[0], "lease-lost", claims.InstanceId, null, CancellationToken.None);
        }
        catch (Exception journalEx)
        {
            logger.LogWarning(journalEx, "work journal for {Cluster} not written after lease loss", cluster);
        }
    }

    // Крэш процесса (P7-след): journal.last_error — оператору видно из etcd,
    // не только из логов; op для журнала = метка до «/» (evacuate/shard1 → evacuate).
    private async Task LogCrashAsync(string cluster, string op, Exception ex)
    {
        logger.LogError(ex, "process {Op} {Cluster} threw exception: {Message}", op, cluster, ex.Message);
        try
        {
            // CancellationToken.None: запись должна доехать даже при остановке
            // host'а посреди тика.
            await journal.WritePhaseAsync(
                cluster, op.Split('/')[0], "crashed", claims.InstanceId, ex.Message, CancellationToken.None);
        }
        catch (Exception journalEx)
        {
            logger.LogWarning(journalEx, "work journal for {Cluster} not written after exception", cluster);
        }
    }

    private Task LogOutcomeAsync(string cluster, string op, Result<ProcessOutcome> outcome)
    {
        if (outcome.IsSuccess)
            logger.LogInformation("{Op} {Cluster}: {Outcome}", op, cluster, outcome.Value);
        else
            logger.LogError(outcome.Error, "{Op} {Cluster} failed: {Message}", op, cluster, outcome.Error!.Message);

        return Task.CompletedTask;
    }

    // Failover: первый успешный endpoint выигрывает (общий хелпер).
    private Task<Result<IReadOnlyList<Kv>>> RangeWithFailoverAsync(
        string[] endpoints, string prefix, CancellationToken ct)
        => EtcdEndpointFailover.CallAsync(endpoints, (endpoint, token) => etcd.RangeAsync(endpoint, prefix, token), ct);
}
