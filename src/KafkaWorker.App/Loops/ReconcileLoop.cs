using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using Microsoft.Extensions.Options;
using KafkaWorker.App.HealthChecks;
using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using KafkaWorker.Etcd.Client;
using KafkaWorker.Etcd.Coordination;
using KafkaWorker.Etcd.Parsing;

namespace KafkaWorker.App.Loops;

/// <summary>
/// Главный цикл воркера (arch/16 §5; порт ReconcileLoop PgWorker): тик =
/// снапшот /kafka/clusters/ → классификация → клэйм → процесс. Ошибка тика
/// не роняет цикл (лог + ErrorDelayMs, следующий тик — ретрай). Кластеры
/// обрабатываются параллельно (SemaphoreSlim MaxClusters), внутри кластера —
/// строго последовательно. Scale-проход/ротация/TopicSync — заглушки волн B/C.
/// </summary>
internal sealed class ReconcileLoop(
    IOptionsMonitor<KafkaWorkerOptions> options,
    IEtcdGateway etcd,
    ClaimStore claims,
    IKafkaClusterProcesses processes,
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
                    logger.LogError(tick.Error, "тик ReconcileLoop не прошёл: {Message}", tick.Error!.Message);
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

    /// <summary>Один тик (публичен для тестов/health): снапшот → параллельная обработка кластеров.</summary>
    internal async Task<Result> TickAsync(CancellationToken ct)
    {
        var endpoints = options.CurrentValue.Etcd.Endpoints.ToArray();
        if (endpoints.Length == 0)
            return Result.Failed(new ApplicationException("KafkaWorker:Etcd:Endpoints не заданы"));

        // Снапшот контроль-плейна (P9: без свежего чтения мутаций не делаем).
        var clustersKvs = await RangeWithFailoverAsync(endpoints, "/kafka/clusters/", ct);
        if (!clustersKvs.IsSuccess)
            return Result.Failed(clustersKvs.Error!);

        health.MarkEtcdOk();

        var parsed = KafkaSnapshotParser.Parse(clustersKvs.Value);
        foreach (var cluster in parsed.Value)
        foreach (var error in cluster.ParseErrors)
            logger.LogWarning("пропущен битый ключ: {Error}", error);

        // Параллельная обработка кластеров с лимитом; ошибка кластера не роняет тик
        // (journal процесса уже несёт last_error — следующий тик продолжит).
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

        health.MarkReconcileTick(ok: true, claimsHeld: parsed.Value.Count(c => claims.IsMine(c.Cluster)));
        metrics.LoopTick("reconcile", ok: true);
        metrics.ClaimsHeld(parsed.Value.Count(c => claims.IsMine(c.Cluster)));
        return Result.Success();
    }

    // Обработка одного кластера под семафором: клэйм → процесс по классификации.
    // Исключение ЛЮБОГО кластера не роняет тик и сервис: catch-all → лог +
    // journal.last_error, следующий тик продолжит этот кластер.
    private async Task ProcessClusterAsync(KafkaClusterSnapshot snap, SemaphoreSlim gate, CancellationToken ct)
    {
        await gate.WaitAsync(ct);
        try
        {
            var cluster = snap.Cluster;
            var work = KafkaClusterClassifier.Classify(snap.Config);

            // Клэйм ДО процесса (Д2): exclusivity; занят другим — не ошибка.
            var claimed = await claims.TryClaimClusterAsync(cluster, ct);
            if (!claimed.IsSuccess)
            {
                logger.LogError(claimed.Error, "клэйм {Cluster} не удался: {Message}", cluster, claimed.Error!.Message);
                return;
            }

            if (!claimed.Value)
                return; // обрабатывает другой инстанс

            switch (work)
            {
                case KafkaClusterWork.Provision:
                    await RunClusterOpAsync(cluster, "provision",
                        () => processes.ProvisionAsync(snap, ct), ct);
                    break;

                case KafkaClusterWork.Deprovision:
                    await RunClusterOpAsync(cluster, "deprovision",
                        () => processes.DeprovisionAsync(snap, ct), ct);
                    break;

                default:
                    // Active-ветка: надзор → converge; scale-проход (remove → add),
                    // ротация и TopicSync — расширения волн B/C (arch/16 §5).
                    await RunClusterOpAsync(cluster, "active",
                        () => processes.ActiveAsync(snap, ct), ct);
                    break;
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // остановка host'а посреди тика — не ошибка цикла
        }
        catch (Exception ex)
        {
            // Страховка контура кластера: даже исключение вне вызова процесса
            // не должно уходить в Task.WhenAll → StopHost.
            logger.LogError(ex, "кластер {Cluster}: необработанное исключение (тик продолжается)",
                snap.Cluster);
        }
        finally
        {
            gate.Release();
        }
    }

    // Вызов процесса под catch-all: исключение процесса → лог + журнал
    // (phase=crashed, last_error), штатный Result — в обычный лог цикла.
    private async Task RunClusterOpAsync(
        string cluster, string op, Func<Task<Result>> call, CancellationToken ct)
    {
        try
        {
            var outcome = await call();
            if (outcome.IsSuccess)
                logger.LogInformation("{Op} {Cluster}: ok", op, cluster);
            else
                logger.LogError(outcome.Error, "{Op} {Cluster} не прошёл: {Message}", op, cluster, outcome.Error!.Message);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw; // остановка host'а — пробрасываем (обработана уровнем выше)
        }
        catch (Exception ex)
        {
            await LogCrashAsync(cluster, op, ex);
        }
    }

    // Крэш процесса (P7-след): journal.last_error — оператору видно из etcd.
    private async Task LogCrashAsync(string cluster, string op, Exception ex)
    {
        logger.LogError(ex, "процесс {Op} {Cluster} бросил исключение: {Message}", op, cluster, ex.Message);
        try
        {
            // CancellationToken.None: запись должна доехать даже при остановке host'а.
            await journal.WriteAsync(
                cluster, op, "crashed", claims.InstanceId, ex.Message, CancellationToken.None);
        }
        catch (Exception journalEx)
        {
            logger.LogWarning(journalEx, "журнал работы {Cluster} не записан после исключения", cluster);
        }
    }

    private async Task<Result<IReadOnlyList<Kv>>> RangeWithFailoverAsync(
        string[] endpoints, string prefix, CancellationToken ct)
    {
        Result<IReadOnlyList<Kv>>? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.RangeAsync(endpoint, prefix, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }
}
