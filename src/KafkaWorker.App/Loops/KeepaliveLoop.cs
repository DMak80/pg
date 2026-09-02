using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using KafkaWorker.App.HealthChecks;
using KafkaWorker.Core;
using KafkaWorker.Etcd.Coordination;

namespace KafkaWorker.App.Loops;

/// <summary>
/// Цикл продления координации (задача 23; spec §6.2 цикл №2): запускает
/// фоновый keepalive-контур ClaimStore (все удерживаемые lease + instance-ключ
/// /kafkaworker/instances/&lt;id&gt;) и живёт heartbeat-тиками для наблюдаемости.
/// Смерть процесса гасит lease'ы ≤15 с (takeover другим инстансом).
/// </summary>
internal sealed class KeepaliveLoop(
    IOptionsMonitor<KafkaWorkerOptions> options,
    ClaimStore claims,
    ILogger<KeepaliveLoop> logger,
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
            // Продление lease'ов + instance-ключ — фоновый контур ClaimStore (задача 12).
            await claims.StartAsync(stoppingToken);
            logger.LogInformation("keepalive-контур запущен: instance {InstanceId}", claims.InstanceId);

            while (!stoppingToken.IsCancellationRequested)
            {
                // healthz = «последний тик» (живой-Ф7, симметрия остальных циклов):
                // проход контура жив — ошибка прошлого тика (если появится) гасится.
                StatusError = Result.Success();
                health.MarkKeepaliveTick();
                await Task.Delay(
                    TimeSpan.FromSeconds(options.CurrentValue.Loops.KeepaliveSec), stoppingToken);
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
