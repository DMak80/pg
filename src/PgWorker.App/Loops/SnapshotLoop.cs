using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PgWorker.App.HealthChecks;
using PgWorker.Core;
using PgWorker.Etcd.Coordination;
using PgWorker.Provisioning.Snapshots;

namespace PgWorker.App.Loops;

/// <summary>
/// Цикл регулярных снапшотов etcd (задача 23; spec §6.2 цикл №3, P12): только
/// глобальный лидер (Д2) снимает слепок раз в SnapshotIntervalMin; не-лидер
/// периодически пытается захватить лидерство (takeover ≤ TTL 15 с + тик).
/// Внеочередные снапшоты в точках изменений снимают сами процессы.
/// </summary>
internal sealed class SnapshotLoop(
    IOptionsMonitor<PgWorkerOptions> options,
    ClaimStore claims,
    SnapshotJob snapshots,
    ILogger<SnapshotLoop> logger,
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
                // Лидерство — только здесь (singleton-работа снапшотов).
                if (!claims.IsLeader)
                {
                    var became = await claims.TryBecomeLeaderAsync(stoppingToken);
                    if (became.IsSuccess && became.Value)
                        logger.LogInformation("захвачено лидерство снапшотов: {InstanceId}", claims.InstanceId);
                }

                if (claims.IsLeader)
                {
                    var shot = await snapshots.TakeAsync(stoppingToken);
                    if (shot.IsSuccess)
                    {
                        health.MarkSnapshotTaken();
                        logger.LogInformation("снапшот etcd снят: {Path}", shot.Value);
                        // Обслуживание etcd: compact + defrag (не чаще раза в час).
                        var maintenance = await snapshots.MaintainAsync(stoppingToken);
                        if (!maintenance.IsSuccess)
                            logger.LogWarning(maintenance.Error, "обслуживание etcd не выполнено: {Message}", maintenance.Error!.Message);
                    }
                    else
                    {
                        StatusError = shot;
                        logger.LogError(shot.Error, "снапшот etcd не снят: {Message}", shot.Error!.Message);
                    }

                    health.MarkSnapshotTick();
                    await Task.Delay(
                        TimeSpan.FromMinutes(options.CurrentValue.Loops.SnapshotIntervalMin), stoppingToken);
                }
                else
                {
                    // Не лидер: ждём до следующей попытки захвата (интервал сканирования).
                    health.MarkSnapshotTick();
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
