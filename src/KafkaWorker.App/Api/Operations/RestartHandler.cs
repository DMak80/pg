using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace KafkaWorker.App.Api.Operations;

// Ответ 202 POST /api/restart (spec §3.2 п.2).
public sealed record RestartDto(bool Restarting);

// Graceful self-stop воркера (spec §3.2 п.2/§4.4): панель НЕ имеет docker-доступа —
// рестарт = самостоятельный стоп; контейнер поднимает docker-политика
// restart: unless-stopped. 202 → пауза ~1 c (ответ успевает уйти) →
// StopApplication. В etcd ничего не пишет: клэймы/джорнал живут в lease,
// операцию продолжит этот же или другой инстанс.
// Копия PgWorker.App/Api/Operations/RestartHandler.cs (arch/16 §1.1).
public sealed class RestartHandler(
    IHostApplicationLifetime lifetime,
    ILogger<RestartHandler> logger,
    TimeSpan? stopDelay = null)
{
    public RestartDto Handle(string? requestedBy)
    {
        logger.LogInformation(
            "POST /api/restart: запрошен перезапуск (оператор {Operator})", requestedBy ?? "unknown");
        _ = Task.Run(async () =>
        {
            await Task.Delay(stopDelay ?? TimeSpan.FromSeconds(1));
            lifetime.StopApplication();
        });
        return new RestartDto(true);
    }
}
