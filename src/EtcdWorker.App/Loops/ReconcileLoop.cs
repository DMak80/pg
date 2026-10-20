using System.Diagnostics;
using EtcdWorker.Core;
using EtcdWorker.Etcd;
using EtcdWorker.Provisioning;
using EtcdWorker.Provisioning.Processes;
using Microsoft.Extensions.Options;
using Shared.Core.HealthChecks;
using Shared.Etcd.Client;

namespace EtcdWorker.App.Loops;

// Цикл доводки состава (arch/25 §3, шаг 13 t32): лидер ClaimStore →
// кворум-гейт (StatusAsync → LeaderMemberId; без лидера — наблюдательный сон)
// → надзор (факты проблем) → MembershipProcess (недобор/перебор/замена)
// → repair → orphan-cleanup; короткий сон ScanIntervalSec (пульсирующий).
// Не лидер — только попытка захвата и сон.
internal sealed class ReconcileLoop(
    IOptionsMonitor<EtcdWorkerOptions> options,
    ClaimStore claims,
    IEtcdGateway etcd,
    MembershipProcess membership,
    NodeSupervisor supervisor,
    EtcdPortAllocStore portAlloc,
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
                if (!claims.IsLeader)
                {
                    var became = await claims.TryBecomeLeaderAsync(stoppingToken);
                    if (became.IsSuccess && became.Value)
                        logger.LogInformation("membership leadership acquired: {InstanceId}", claims.InstanceId);
                }

                var started = Stopwatch.GetTimestamp();
                if (claims.IsLeader)
                {
                    var tick = await TickLeaderAsync(stoppingToken);
                    if (!tick.IsSuccess && tick.Error is { } error)
                        logger.LogWarning(error, "membership tick failed: {Message}", error.Message);
                }

                health.MarkReconcileActivity();
                health.MarkReconcileTick(ok: true, claimsHeld: 1);
                metrics.LoopTick("reconcile", ok: true);
                metrics.LoopDuration("reconcile", Stopwatch.GetElapsedTime(started).TotalSeconds);
                await Shared.Core.Hosting.PulsingDelay.SleepAsync(
                    TimeSpan.FromSeconds(options.CurrentValue.Loops.ScanIntervalSec),
                    TimeSpan.FromSeconds(options.CurrentValue.Loops.Watchdog.CheckIntervalSec),
                    health.MarkReconcileActivity, stoppingToken);
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

    private async Task<Result> TickLeaderAsync(CancellationToken ct)
    {
        var endpoints = options.CurrentValue.Etcd.Endpoints;

        // Кворум-гейт: без лидера состав не трогаем.
        EtcdStatusPayload? status = null;
        foreach (var endpoint in endpoints)
        {
            var probe = await etcd.StatusAsync(endpoint, ct);
            if (probe.IsSuccess)
            {
                status = probe.Value;
                break;
            }
        }

        if (status?.LeaderMemberId is null)
            return Result.Success(); // наблюдение: кластер без лидера — следующий тик

        var members = await MemberListFailoverAsync(endpoints, ct);
        if (!members.IsSuccess)
            return Result.Failed(members.Error!);
        var (managed, _) = EtcdContourMembers.Build(members.Value);

        var nodes = await ReadNodesAsync(endpoints, ct);
        var portsResult = await portAlloc.ReadAllAsync(ct);
        var ports = portsResult.IsSuccess
            ? portsResult.Value
            : new Dictionary<int, EtcdPortAllocStore.NodePort>();

        // Надзор: факты проблем (метки ProblemSinceUnix) + state-актуализация.
        var facts = await supervisor.ObserveAsync(managed, nodes, ports, ct);
        if (!facts.IsSuccess)
            return Result.Failed(facts.Error!);

        // Доводка состава (недобор/перебор/авто-замена).
        var converged = await membership.TickAsync(facts.Value, ct);
        if (!converged.IsSuccess)
            return converged;

        // Repair (один шаг) + orphan-cleanup (состав перечитывает сам —
        // снимок начала тика не отражает члена, добавленного этим тиком).
        var repaired = await supervisor.RepairAsync(members.Value, managed, nodes, ports, ct);
        if (!repaired.IsSuccess)
            return Result.Failed(repaired.Error!);
        return await supervisor.OrphanCleanupAsync(ct);
    }

    private async Task<Result<IReadOnlyList<EtcdMember>>> MemberListFailoverAsync(
        string[] endpoints, CancellationToken ct)
    {
        Result<IReadOnlyList<EtcdMember>> last = default;
        foreach (var endpoint in endpoints)
        {
            var list = await etcd.MemberListAsync(endpoint, ct);
            if (list.IsSuccess)
                return list;
            last = list;
        }

        return Result<IReadOnlyList<EtcdMember>>.Failed(
            last.Error ?? new EtcdUnreachableException("etcd endpoints are not set"));
    }

    private async Task<IReadOnlyList<EtcdNodesParser.ManagedNodeState>> ReadNodesAsync(
        string[] endpoints, CancellationToken ct)
    {
        foreach (var endpoint in endpoints)
        {
            var range = await etcd.RangeAsync(endpoint, EtcdDomainKeys.NodesPrefix, ct);
            if (range.IsSuccess)
                return EtcdNodesParser.Parse(range.Value);
        }

        return [];
    }
}
