using Microsoft.Extensions.Hosting;

namespace Metrics.SdGenerator;

// Фоновый цикл генератора: PeriodicTimer с интервалом NormalizeInterval(RefreshIntervalSec);
// исключение тика — warning (хост жив), отмена — штатный выход.
internal sealed class SdGeneratorHostedService(
    SdGeneratorOptions options, SdGeneratorLoop loop, ILogger<SdGeneratorHostedService> logger)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // <=0 → 15 + warning (паттерн valkey §4.2).
        var intervalSec = SdGeneratorOptions.NormalizeInterval(options.RefreshIntervalSec);
        if (options.RefreshIntervalSec <= 0)
            logger.LogWarning(
                "интервал тика SdGenerator:RefreshIntervalSec={Given} невалиден → {Sec} с",
                options.RefreshIntervalSec, intervalSec);

        logger.LogInformation("старт цикла file_sd-генератора: интервал {Sec} с, файл {Path}",
            intervalSec, options.OutputPath);
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(intervalSec));
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                try
                {
                    await loop.TickAsync(stoppingToken);
                }
                // Ошибка тика не валит хост: файл и last_success остаются прежними (консервативная свежесть).
                catch (Exception e) when (e is not OperationCanceledException)
                {
                    logger.LogWarning(e, "тик генератора упал — продолжаю по расписанию");
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Штатная остановка хоста.
        }
    }
}
