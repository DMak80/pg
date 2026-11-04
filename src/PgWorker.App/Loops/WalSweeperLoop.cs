using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PgWorker.Backups.Supervisor;
using Shared.Etcd.Coordination;
using System.Diagnostics;

namespace PgWorker.App.Loops;

/// <summary>Лидерный фоновый цикл чистки wal/ (arch/19 §4 «wal-sweeper»):
/// глобальный лидер /pgworker/leader выполняет WalSweeperProcess.SweepAsync
/// раз в 60 с (канон: per-shard оценка .partial-гигиены НЕ РЕЖЕ раза в
/// минуту); не-лидер пере-захватывает лидерство и ждёт Loops:ScanIntervalSec.
/// Backups:Enabled=false — серию LoopTick не пишем (живость выключенной
/// подсистемы не алертится — паттерн orphan-sweep, arch/18 §2.2). Потеря
/// leader-lease прерывает проход (гвард arch/17 S5). НЕ входит в
/// LoopsVitality/healthz: живость — только метрики LoopTick/LoopDuration.</summary>
internal sealed class WalSweeperLoop(
    IOptionsMonitor<PgWorkerOptions> options,
    ClaimStore claims,
    WalSweeperProcess sweeper,
    Shared.Metrics.Worker.WorkerMetricsInstrumentation metrics,
    ILogger<WalSweeperLoop> logger) : BackgroundService
{
    // Канон arch/19 §4: оценка per-shard не реже раза в минуту (константа,
    // internal — инъекция тестам).
    internal static readonly TimeSpan SweepInterval = TimeSpan.FromSeconds(60);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                if (!claims.IsLeader)
                {
                    var became = await claims.TryBecomeLeaderAsync(stoppingToken);
                    if (became.IsSuccess && became.Value)
                        logger.LogInformation(
                            "leadership acquired (wal-sweeper): {InstanceId}", claims.InstanceId);
                }

                if (claims.IsLeader && options.CurrentValue.Backups.Enabled)
                {
                    // Cancellation-гвард лидерства (arch/17 S5).
                    using var leaderLease = CancellationTokenSource.CreateLinkedTokenSource(
                        stoppingToken, claims.LeaderToken);
                    var tick = leaderLease.Token;
                    var startedAt = Stopwatch.GetTimestamp();
                    var result = await sweeper.SweepAsync(tick);
                    metrics.LoopDuration("wal-sweeper", Stopwatch.GetElapsedTime(startedAt).TotalSeconds);
                    metrics.LoopTick("wal-sweeper", ok: result.IsSuccess);
                    if (!result.IsSuccess)
                        logger.LogWarning("wal-sweeper: {Error}", result.Error?.Message);
                    await Task.Delay(SweepInterval, tick);
                }
                else
                {
                    await Task.Delay(
                        TimeSpan.FromSeconds(options.CurrentValue.Loops.ScanIntervalSec),
                        stoppingToken);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break; // штатная остановка
            }
            catch (OperationCanceledException)
            {
                // Потеря leader-lease — пауза скана, пере-захват следующим тиком.
                try
                {
                    await Task.Delay(
                        TimeSpan.FromSeconds(options.CurrentValue.Loops.ScanIntervalSec),
                        stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "wal-sweeper loop: {Message}", ex.Message);
                try
                {
                    await Task.Delay(
                        TimeSpan.FromSeconds(options.CurrentValue.Loops.ScanIntervalSec),
                        stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }
    }
}
