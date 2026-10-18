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
                "SdGenerator:RefreshIntervalSec={Given} tick interval invalid → {Sec} s",
                options.RefreshIntervalSec, intervalSec);

        logger.LogInformation("file_sd generator loop started: interval {Sec} s, file {Path}",
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
                    logger.LogWarning(e, "generator tick failed — continuing on schedule");
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Штатная остановка хоста.
        }
    }
}
