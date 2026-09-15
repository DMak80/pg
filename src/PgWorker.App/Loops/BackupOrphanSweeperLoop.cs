using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PgWorker.Backups.Supervisor;

namespace PgWorker.App.Loops;

/// <summary>Лидерный фоновый цикл сирот S3 (t07, arch/19 §4, паттерн
/// SnapshotLoop упрощённый — без health-обёртки): глобальный лидер
/// /pgworker/leader выполняет BackupOrphanSweeper.SweepAsync раз в
/// Supervisor:IntervalSec; не-лидер периодически пытается захватить лидерство
/// (takeover ≤ TTL 15 с + тик) и ждёт Loops:ScanIntervalSec. Backups:Enabled=
/// false — лидерство поддерживается, сверки не выполняются (no-op).</summary>
internal sealed class BackupOrphanSweeperLoop(
    IOptionsMonitor<PgWorkerOptions> options,
    ClaimStore claims,
    BackupOrphanSweeper sweeper,
    ILogger<BackupOrphanSweeperLoop> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // Лидерство — только здесь (singleton-проход по всему bucket'у).
                if (!claims.IsLeader)
                {
                    var became = await claims.TryBecomeLeaderAsync(stoppingToken);
                    if (became.IsSuccess && became.Value)
                        logger.LogInformation(
                            "захвачено лидерство (orphan-sweeper): {InstanceId}", claims.InstanceId);
                }

                if (claims.IsLeader && options.CurrentValue.Backups.Enabled)
                {
                    // Ошибка прохода — лог (SweepAsync сам Result; transient-отказы
                    // S3/etcd повторит следующий проход). Реестр в etcd переживает
                    // смену лидера — продолжение с факта, не с нуля.
                    var result = await sweeper.SweepAsync(stoppingToken);
                    if (!result.IsSuccess)
                        logger.LogWarning("orphan-sweep: {Error}", result.Error?.Message);

                    await Task.Delay(
                        TimeSpan.FromSeconds(options.CurrentValue.Backups.Supervisor.IntervalSec),
                        stoppingToken);
                }
                else
                {
                    // Не-лидер/выключено — редкий тик (как SnapshotLoop).
                    await Task.Delay(
                        TimeSpan.FromSeconds(options.CurrentValue.Loops.ScanIntervalSec),
                        stoppingToken);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break; // штатная остановка
            }
            catch (Exception ex)
            {
                // ошибка цикла не валит сервис (§6.2): лог + пауза скана
                logger.LogError(ex, "orphan-sweeper loop: {Message}", ex.Message);
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
