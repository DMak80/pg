using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PgWorker.Etcd.Coordination;

namespace PgWorker.App.Loops;

/// <summary>
/// Цикл продления координации (задача 23; spec §6.2 цикл №2): запускает
/// фоновый keepalive-контур ClaimStore (все удерживаемые lease + instance-ключ
/// /pgworker/instances/&lt;id&gt;) и живёт heartbeat-тиками для наблюдаемости.
/// Смерть процесса гасит lease'ы ≤15 с (takeover другим инстансом).
/// </summary>
internal sealed class KeepaliveLoop(
    IOptionsMonitor<PgWorkerOptions> options,
    ClaimStore claims,
    ILogger<KeepaliveLoop> logger,
    HealthState health) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            // Продление lease'ов + instance-ключ — фоновый контур ClaimStore (задача 12).
            await claims.StartAsync(stoppingToken);
            logger.LogInformation("keepalive-контур запущен: instance {InstanceId}", claims.InstanceId);

            while (!stoppingToken.IsCancellationRequested)
            {
                health.MarkKeepaliveTick();
                await Task.Delay(
                    TimeSpan.FromSeconds(options.CurrentValue.Loops.KeepaliveSec), stoppingToken);
            }
        }
        catch (OperationCanceledException)
        {
            // штатная остановка host'а
        }
    }
}
