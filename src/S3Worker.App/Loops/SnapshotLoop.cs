using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using Microsoft.Extensions.Options;
using Shared.Core.HealthChecks;
using S3Worker.App;

namespace S3Worker.App.Loops;

/// <summary>
/// Цикл регулярных снапшотов etcd (arch/24 §6, P12): только глобальный лидер
/// (/s3worker/leader) снимает слепок раз в SnapshotIntervalMin (контроль-плейн
/// /s3/ + /s3worker/ — SnapshotJob снимает etcd целиком); не-лидер периодически
/// пытается захватить лидерство (takeover ≤ TTL 15 с + тик). Внеочередные
/// снапшоты в точках изменений снимают сами процессы.
/// </summary>
internal sealed class SnapshotLoop(
    IOptionsMonitor<S3WorkerOptions> options,
    ClaimStore claims,
    SnapshotJob snapshots,
    ILogger<SnapshotLoop> logger,
    HealthState health,
    TimeProvider clock,
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
                // Лидерство — только здесь (singleton-работа снапшотов).
                if (!claims.IsLeader)
                {
                    var became = await claims.TryBecomeLeaderAsync(stoppingToken);
                    if (became.IsSuccess && became.Value)
                        logger.LogInformation("snapshot leadership acquired: {InstanceId}", claims.InstanceId);
                }

                if (claims.IsLeader)
                {
                    try
                    {
                        // Cancellation-гвард лидерства (arch/17 S5): потеря leader-lease
                        // прерывает тик лидера; цикл продолжит как не-лидер.
                        using var leaderLease = CancellationTokenSource.CreateLinkedTokenSource(
                            stoppingToken, claims.LeaderToken);
                        var tick = leaderLease.Token;

                        var started = Stopwatch.GetTimestamp();
                        var shot = await snapshots.TakeAsync(tick);
                        metrics.LoopDuration("snapshot", Stopwatch.GetElapsedTime(started).TotalSeconds);
                        if (shot.IsSuccess)
                        {
                            // healthz = «последний тик» (живой-Ф7): успешный снимок гасит ошибку
                            // прошлого — иначе единственный фейл = вечный unhealthy.
                            StatusError = Result.Success();
                            health.MarkSnapshotTaken();
                            metrics.SnapshotTaken(clock.GetUtcNow());
                            logger.LogInformation("etcd snapshot taken: {Path}", shot.Value);
                            // Обслуживание etcd: compact + defrag (не чаще раза в интервал).
                            var maintenance = await snapshots.MaintainAsync(tick);
                            if (!maintenance.IsSuccess)
                                logger.LogWarning(maintenance.Error, "etcd maintenance not performed: {Message}", maintenance.Error!.Message);
                        }
                        else
                        {
                            StatusError = shot;
                            logger.LogError(shot.Error, "etcd snapshot not taken: {Message}", shot.Error!.Message);
                        }

                        health.MarkSnapshotTick();
                        metrics.LoopTick("snapshot", ok: true);
                        // Сон длиннее порога сноса — пульсирующий (чанк < окна проверки 15 c):
                        // MarkSnapshotActivity — активность без тика (healthz loops-alive не меняется)
                        await Shared.Core.Hosting.PulsingDelay.SleepAsync(
                            TimeSpan.FromMinutes(options.CurrentValue.Loops.SnapshotIntervalMin),
                            TimeSpan.FromSeconds(options.CurrentValue.Loops.Watchdog.CheckIntervalSec),
                            health.MarkSnapshotActivity, tick);
                    }
                    catch (OperationCanceledException) when (!stoppingToken.IsCancellationRequested)
                    {
                        // Потеря leader-lease прервала тик (гвард arch/17 S5): не ошибка —
                        // цикл продолжит как не-лидер и пере-захватит ≤ TTL + тик скана.
                        logger.LogWarning("snapshot leadership lost mid-tick");
                    }
                }
                else
                {
                    // Не лидер: ждём до следующей попытки захвата (интервал сканирования).
                    health.MarkSnapshotTick();
                    metrics.LoopTick("snapshot", ok: true);
                    await Task.Delay(
                        TimeSpan.FromSeconds(options.CurrentValue.Loops.ScanIntervalSec), stoppingToken);
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
}
