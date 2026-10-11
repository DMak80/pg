using Microsoft.Extensions.Logging;
using OwnS3.Storage.Erasure;

namespace OwnS3.App.Pipeline;

// Периодическая обёртка heal-прохода (канон 06 §9; прецедент VolumeCleanupService):
// старт + каждые 10 минут (чаще чисток 15 мин — возврат к полноте не медленнее
// чисток; чаще — лишний IO).
public sealed class ErasureHealHostedService(ErasureHealService heal,
    ILogger<ErasureHealHostedService> logger) : BackgroundService
{
    private static readonly TimeSpan Period = TimeSpan.FromMinutes(10);

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        // старт-проход детерминирован к моменту завершения StartAsync
        await RunPassSafeAsync(cancellationToken);
        await base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(Period);
        while (!stoppingToken.IsCancellationRequested)
        {
            bool ticked;
            try
            {
                ticked = await timer.WaitForNextTickAsync(stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            if (!ticked)
                break;
            await RunPassSafeAsync(stoppingToken);
        }
    }

    private async Task RunPassSafeAsync(CancellationToken ct)
    {
        try
        {
            await heal.RunPassAsync(ct);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "EC heal-проход упал");
        }
    }
}
