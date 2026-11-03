using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using Microsoft.Extensions.Options;
using Shared.Core.HealthChecks;
using KafkaWorker.App;
using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using Shared.Etcd.Client;
using KafkaWorker.Etcd.Parsing;
using KafkaWorker.Provisioning.Kafka;

namespace KafkaWorker.App.Loops;

/// <summary>
/// Главный цикл воркера (arch/16 §5; порт ReconcileLoop PgWorker; чтение
/// состояния — arch/17 S10): контроль-плейн читается опросными кэшами
/// (IPrefixPollSources), тик применяется ТОЛЬКО при факте изменения
/// etcd-состояния — гейт по дельте: с последнего обработанного состояния
/// изменений не было → тик БЕЗ изменений (processes не зовутся, клэймов/
/// мутаций нет); heartbeat и healthz loops-alive отмечаются всегда
/// (watchdog жив, arch/16 §6). Сбой опроса кэша дельту не поднимает — тик
/// без изменений; при появлении изменения дельта будет — тик обработает.
/// Кластеры обрабатываются параллельно (SemaphoreSlim MaxClusters), внутри
/// кластера — строго последовательно. Ошибка тика не роняет цикл (лог +
/// ErrorDelayMs, следующий тик — ретрай).
/// </summary>
internal sealed class ReconcileLoop(
    IOptionsMonitor<KafkaWorkerOptions> options,
    IPrefixPollSources sources,
    ClaimStore claims,
    IKafkaClusterProcesses processes,
    WorkJournal journal,
    ILogger<ReconcileLoop> logger,
    HealthState health,
    Shared.Metrics.Worker.WorkerMetricsInstrumentation metrics,
    KafkaClusterBackoff backoff) : BackgroundService, IHealthCheckService
{
    // Baseline гейта тика (arch/17 S10): Version источников на момент
    // последнего ОБРАБОТАННОГО тика; −1 = ещё не обрабатывали (первый тик
    // всегда работает — стартовый слепок кэшей и есть обрабатываемое состояние).
    private long _processedVersion = -1;

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
                    // healthz = «последний тик» (живой-Ф7, порт PgWorker ReconcileLoop): успешный
                    // тик гасит ошибку прошлого — иначе единственный упавший тик = вечный unhealthy.
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
            // Останов опросных кэшей (идемпотентно; остановка лучшего эффекта —
            // процесс останавливается).
            try
            {
                await sources.StopAsync(CancellationToken.None);
            }
            catch
            {
                // host останавливается — повтор не требуется
            }
        }
    }

    /// <summary>
    /// Тик с защитой тела цикла (порт rework №3 PgWorker): исключение тика не
    /// роняет BackgroundService — превращается в ошибку тика (лог + ErrorDelayMs).
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

    /// <summary>Один тик (публичен для тестов/health): гейт по дельте → слепок
    /// кэша → параллельная обработка кластеров.</summary>
    internal async Task<Result> TickAsync(CancellationToken ct)
    {
        var endpoints = options.CurrentValue.Etcd.Endpoints.ToArray();
        if (endpoints.Length == 0)
            return Result.Failed(new ApplicationException("KafkaWorker:Etcd:Endpoints is not set"));

        // Запуск опросных кэшей (идемпотентно; сбой старта — ошибка тика:
        // цикл залогирует и повторит с ErrorDelayMs).
        try
        {
            await sources.StartAsync(ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            return Result.Failed(ex);
        }

        // ГЕЙТ ТИКА (arch/17 S10, реестр E10): применяется только фактическое
        // изменение etcd-состояния. Дельты с последнего обработанного состояния
        // нет (в т.ч. после сбоя опроса — кэш дельту не поднимает) → тик БЕЗ
        // изменений: processes/клэймы/мутации не вызываются. Heartbeat
        // отмечается ВСЕГДА (watchdog жив, arch/16 §6), loops-alive живёт
        // (клэймы не трогаем — их держит keepalive-цикл), метрика тика —
        // успешная (цикл жив, работы не было).
        var version = sources.Version;
        if (version == Interlocked.Read(ref _processedVersion))
        {
            health.MarkReconcileActivity();
            health.MarkReconcileNoChangeTick();
            metrics.LoopTick("reconcile.nochange", ok: true); // «тик без изменений» — отдельная серия
            return Result.Success();
        }

        health.MarkReconcileActivity();

        // Парсинг контроль-плейна — из слепка кэша (не etcd; P9 выполняется
        // опросным циклом кэша, единая точка failover — arch/17 S7).
        var parsed = KafkaSnapshotParser.Parse(ToKvs(sources.ClustersSnapshot));
        foreach (var cluster in parsed.Value)
        foreach (var error in cluster.ParseErrors)
            logger.LogWarning("skipped a broken key: {Error}", error);

        // Кластеры исчезли из слепка — backoff-состояние не копится.
        backoff.ForgetMissing(parsed.Value.Select(c => c.Cluster).ToHashSet());

        // Параллельная обработка кластеров с лимитом; ошибка кластера не роняет тик
        // (journal процесса уже несёт last_error — дельта следующего изменения
        // продолжит доводку: процессы идемпотентны, arch/17 S10).
        var gate = new SemaphoreSlim(Math.Max(1, options.CurrentValue.Parallelism.MaxClusters));
        try
        {
            var tasks = parsed.Value
                .Select(snap => ProcessClusterAsync(snap, gate, ct))
                .ToArray();
            await Task.WhenAll(tasks);
        }
        finally
        {
            gate.Dispose();
        }

        // Тик обработан: baseline = Version на входе тика; изменения, пришедшие
        // ВО ВРЕМЯ обработки, остались за baseline'ом — их донесёт следующий тик.
        Interlocked.Exchange(ref _processedVersion, version);
        health.MarkReconcileTick(ok: true, claimsHeld: parsed.Value.Count(c => claims.IsMine(c.Cluster)));
        metrics.LoopTick("reconcile", ok: true);
        metrics.ClaimsHeld(parsed.Value.Count(c => claims.IsMine(c.Cluster)));
        return Result.Success();
    }

    // Слепок кэша → Kv-список парсера (значения без изменений).
    private static List<Kv> ToKvs(IReadOnlyDictionary<string, (string Value, long ModRevision)> snapshot)
        => snapshot
            .Select(pair => new Kv(pair.Key, pair.Value.Value, (ulong)pair.Value.ModRevision))
            .ToList();

    // Обработка одного кластера под семафором: клэйм → процесс по классификации.
    // Исключение ЛЮБОГО кластера не роняет тик и сервис: catch-all → лог +
    // journal.last_error, следующий тик продолжит этот кластер.
    private async Task ProcessClusterAsync(KafkaClusterSnapshot snap, SemaphoreSlim gate, CancellationToken ct)
    {
        await gate.WaitAsync(ct);
        CancellationTokenSource? lease = null; // linked-токен клэйма — виден catch'ам
        try
        {
            var cluster = snap.Cluster;
            var work = KafkaClusterClassifier.Classify(snap.Config);

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
                case KafkaClusterWork.Provision:
                    await RunClusterOpAsync(cluster, "provision",
                        () => processes.ProvisionAsync(snap, tick), ct, tick);
                    break;

                case KafkaClusterWork.Deprovision:
                    await RunClusterOpAsync(cluster, "deprovision",
                        () => processes.DeprovisionAsync(snap, tick), ct, tick);
                    break;

                default:
                    // Active-ветка: надзор → converge; scale-проход (remove → add),
                    // ротация и TopicSync — расширения волн B/C (arch/16 §5).
                    await RunClusterOpAsync(cluster, "active",
                        () => processes.ActiveAsync(snap, tick), ct, tick);
                    break;
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // остановка host'а посреди тика — не ошибка цикла
        }
        catch (OperationCanceledException oce) when (lease?.IsCancellationRequested == true)
        {
            // Потеря клэйма прервала операцию кластера (гвард arch/17 S5):
            // внятная фаза в журнале; кластер подхватит следующий тик/инстанс.
            logger.LogWarning("cluster {Cluster}: lease lost — tick aborted ({Message})",
                snap.Cluster, oce.Message);
            await WriteLeaseLostAsync(snap.Cluster, "tick");
        }
        catch (Exception ex)
        {
            // Страховка контура кластера: даже исключение вне вызова процесса
            // не должно уходить в Task.WhenAll → StopHost.
            logger.LogError(ex, "cluster {Cluster}: unhandled exception (tick continues)",
                snap.Cluster);
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
        string cluster, string op, Func<Task<Result>> call, CancellationToken host, CancellationToken ct)
    {
        try
        {
            var outcome = await call();
            if (outcome.IsSuccess)
                logger.LogInformation("{Op} {Cluster}: ok", op, cluster);
            else
                logger.LogError(outcome.Error, "{Op} {Cluster} failed: {Message}", op, cluster, outcome.Error!.Message);
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
    }

    // Фаза lease-lost в журнале воркера (arch/17 S5); CancellationToken.None —
    // токен тика отменён, запись обязана доехать (как в LogCrashAsync).
    private async Task WriteLeaseLostAsync(string cluster, string op)
    {
        try
        {
            await journal.WritePhaseAsync(
                cluster, op, "lease-lost", claims.InstanceId, null, CancellationToken.None);
        }
        catch (Exception journalEx)
        {
            logger.LogWarning(journalEx, "work journal for {Cluster} was not written after lease loss", cluster);
        }
    }

    // Крэш процесса (P7-след): journal.last_error — оператору видно из etcd.
    private async Task LogCrashAsync(string cluster, string op, Exception ex)
    {
        logger.LogError(ex, "process {Op} {Cluster} threw an exception: {Message}", op, cluster, ex.Message);
        try
        {
            // CancellationToken.None: запись должна доехать даже при остановке host'а.
            await journal.WritePhaseAsync(
                cluster, op, "crashed", claims.InstanceId, ex.Message, CancellationToken.None);
        }
        catch (Exception journalEx)
        {
            logger.LogWarning(journalEx, "work journal for {Cluster} was not written after the exception", cluster);
        }
    }
}
