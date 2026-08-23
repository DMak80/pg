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
                var tick = await TickAsync(stoppingToken);
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
                    await LogOutcomeAsync(cluster, "provision", await processes.ProvisionAsync(snap, ct));
                    break;

                case ClusterWork.Deprovision:
                    await LogOutcomeAsync(cluster, "deprovision", await processes.DeprovisionAsync(snap, ct));
                    break;

                default:
                    var supervised = await processes.SuperviseAsync(snap, ct);
                    if (!supervised.IsSuccess)
                    {
                        logger.LogError(supervised.Error, "supervise {Cluster} не прошёл: {Message}",
                            cluster, supervised.Error!.Message);
                        return;
                    }

                    // События эвакуации: полностью мёртвые шарды (spec §6.4 D/E).
                    foreach (var deadShard in supervised.Value.DeadShards)
                        await LogOutcomeAsync(cluster, $"evacuate/{deadShard}",
                            await processes.EvacuateAsync(snap, deadShard, ct));
                    break;
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // остановка host'а посреди тика — не ошибка цикла
        }
        finally
        {
            gate.Release();
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
