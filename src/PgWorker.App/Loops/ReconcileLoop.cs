using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PgWorker.App.HealthChecks;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;
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
    HealthState health) : BackgroundService, IHealthCheckService
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
                var tick = await TickSafelyAsync(stoppingToken);
                if (tick.IsSuccess)
                {
                    await Task.Delay(
                        TimeSpan.FromSeconds(options.CurrentValue.Loops.ScanIntervalSec), stoppingToken);
                }
                else
                {
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
            return Result.Failed(new ApplicationException("PgWorker:Etcd:Endpoints не заданы"));

        // Снапшот контроль-плейна (P9: без свежего чтения мутаций не делаем).
        var clustersKvs = await RangeWithFailoverAsync(endpoints, "/clusters/", ct);
        if (!clustersKvs.IsSuccess)
            return Result.Failed(clustersKvs.Error!);
        var serviceKvs = await RangeWithFailoverAsync(endpoints, "/service/", ct);
        if (!serviceKvs.IsSuccess)
            return Result.Failed(serviceKvs.Error!);

        health.MarkEtcdOk();

        var parsed = ClusterSnapshotParser.ParseClusters(clustersKvs.Value, out var parseErrors);
        foreach (var error in parseErrors)
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

        health.MarkReconcileTick(ok: true, claimsHeld: parsed.Value.Count(c => claims.IsMine(c.Config.Cluster)));
        return Result.Success();
    }

    // Обработка одного кластера под семафором: клэйм → процесс → эвакуация.
    // Исключение ЛЮБОГО кластера не роняет тик и сервис (rework №3): catch-all
    // → лог + journal.last_error, следующий тик продолжит этот кластер.
    private async Task ProcessClusterAsync(ClusterSnapshot snap, SemaphoreSlim gate, CancellationToken ct)
    {
        await gate.WaitAsync(ct);
        try
        {
            var cluster = snap.Config.Cluster;
            var work = ClusterClassifier.Classify(snap.Config);

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
                case ClusterWork.Provision:
                    await RunClusterOpAsync(cluster, "provision",
                        () => processes.ProvisionAsync(snap, ct), ct);
                    break;

                case ClusterWork.Deprovision:
                    await RunClusterOpAsync(cluster, "deprovision",
                        () => processes.DeprovisionAsync(snap, ct), ct);
                    break;

                default:
                    var supervised = await RunSuperviseAsync(cluster, snap, ct);
                    if (supervised is null)
                        break;

                    // Усыновление (spec §3.2, arch/14 §5 J): адреса dsn-шард без
                    // portalloc — до scale (add смотрит pinned portalloc) и до
                    // repair/moves (SQL нужен адрес).
                    await RunClusterOpAsync(cluster, "adopt",
                        () => processes.AdoptAsync(snap, ct), ct);

                    // Scale-проход (t06 spec §5.1): remove → add, после надзора, до
                    // эвакуаций/moves — демонтаж освобождает хосты/порты для подъёма (Д13).
                    await RunClusterOpAsync(cluster, "scale-shards",
                        () => processes.ScaleShardsAsync(snap, ct), ct);

                    // Ротация app-пароля (spec §4.3, arch/14 §5 I): короткая плановая
                    // операция — до эвакуаций/переездов, не ждёт длинных moves.
                    await RunClusterOpAsync(cluster, "rotate-app-password",
                        () => processes.RotateAppPasswordAsync(snap, ct), ct);

                    // Репарация брошенных переездов (spec §3.5, arch/14 §5 K): синтетические
                    // заявки до moves — этот же тик начнёт их обработку (старейшая заявка).
                    await RunClusterOpAsync(cluster, "repair",
                        () => processes.RepairAsync(snap, ct), ct);

                    // События эвакуации: полностью мёртвые шарды (spec §6.4 D/E).
                    foreach (var deadShard in supervised.DeadShards)
                        await RunClusterOpAsync(cluster, $"evacuate/{deadShard}",
                            () => processes.EvacuateAsync(snap, deadShard, ct), ct);

                    // Заявки переездов бакетов (t01, spec §5.3): после надзора и
                    // эвакуаций; MoveProcess сам держит клэйм-гвард (инвариант §4.3).
                    await RunClusterOpAsync(cluster, "moves",
                        () => processes.ProcessMovesAsync(snap, ct), ct);
                    break;
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // остановка host'а посреди тика — не ошибка цикла
        }
        catch (Exception ex)
        {
            // Страховка контура кластера (rework №3): даже исключение вне
            // вызова процесса не должно уходить в Task.WhenAll → StopHost.
            logger.LogError(ex, "кластер {Cluster}: необработанное исключение (тик продолжается)",
                snap.Config.Cluster);
        }
        finally
        {
            gate.Release();
        }
    }

    // Вызов процесса под catch-all: исключение процесса → лог + журнал
    // (phase=crashed, last_error), штатный Result — в обычный лог цикла.
    private async Task RunClusterOpAsync(
        string cluster, string op, Func<Task<Result<ProcessOutcome>>> call, CancellationToken ct)
    {
        try
        {
            await LogOutcomeAsync(cluster, op, await call());
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

    // Надзор: результат — SuperviseOutcome (мёртвые шарды); null = не прошёл
    // (ошибка Result залогирована) либо упал исключением (журнал записан).
    private async Task<SuperviseOutcome?> RunSuperviseAsync(string cluster, ClusterSnapshot snap, CancellationToken ct)
    {
        try
        {
            var supervised = await processes.SuperviseAsync(snap, ct);
            if (!supervised.IsSuccess)
            {
                logger.LogError(supervised.Error, "supervise {Cluster} не прошёл: {Message}",
                    cluster, supervised.Error!.Message);
                return null;
            }

            return supervised.Value;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            await LogCrashAsync(cluster, "supervise", ex);
            return null;
        }
    }

    // Крэш процесса (P7-след): journal.last_error — оператору видно из etcd,
    // не только из логов; op для журнала = метка до «/» (evacuate/shard1 → evacuate).
    private async Task LogCrashAsync(string cluster, string op, Exception ex)
    {
        logger.LogError(ex, "процесс {Op} {Cluster} бросил исключение: {Message}", op, cluster, ex.Message);
        try
        {
            // CancellationToken.None: запись должна доехать даже при остановке
            // host'а посреди тика.
            await journal.WritePhaseAsync(
                cluster, op.Split('/')[0], "crashed", claims.InstanceId, ex.Message, CancellationToken.None);
        }
        catch (Exception journalEx)
        {
            logger.LogWarning(journalEx, "журнал работы {Cluster} не записан после исключения", cluster);
        }
    }

    private Task LogOutcomeAsync(string cluster, string op, Result<ProcessOutcome> outcome)
    {
        if (outcome.IsSuccess)
            logger.LogInformation("{Op} {Cluster}: {Outcome}", op, cluster, outcome.Value);
        else
            logger.LogError(outcome.Error, "{Op} {Cluster} не прошёл: {Message}", op, cluster, outcome.Error!.Message);

        return Task.CompletedTask;
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
