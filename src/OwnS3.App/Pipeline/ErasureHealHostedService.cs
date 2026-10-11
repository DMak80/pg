using Microsoft.Extensions.Logging;
using OwnS3.App;
using OwnS3.Storage.Erasure;

namespace OwnS3.App.Pipeline;

// Периодическая обёртка heal-прохода (канон 06 §9; прецедент VolumeCleanupService):
// старт + каждые 10 минут (чаще чисток 15 мин — возврат к полноте не медленнее
// чисток; чаще — лишний IO). Активна только в EC-режиме: heal существует лишь в
// XL Erasure (канон 05 §7), single — старт/цикл no-op. Режим известен после
// Build — сервис регистрируется один на оба режима и развивается по
// StorageModeInfo; ErasureHealService разрешается из провайдера (в single его
// граф зависимостей не строится).
public sealed class ErasureHealHostedService(
    StorageModeInfo mode,
    IServiceProvider services,
    ILogger<ErasureHealHostedService> logger) : BackgroundService
{
    private static readonly TimeSpan Period = TimeSpan.FromMinutes(10);

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        if (!mode.IsErasure)
            return;
        // старт-проход детерминирован к моменту завершения StartAsync
        await RunPassSafeAsync(services.GetRequiredService<ErasureHealService>(), cancellationToken);
        await base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!mode.IsErasure)
            return;
        var heal = services.GetRequiredService<ErasureHealService>();
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
            await RunPassSafeAsync(heal, stoppingToken);
        }
    }

    private async Task RunPassSafeAsync(ErasureHealService heal, CancellationToken ct)
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
