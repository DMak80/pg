using System.Diagnostics;
using OwnS3.Storage;

namespace OwnS3.App.Pipeline;

// Фоновый сервис чисток тома (спека §4.7): немедленный проход при старте
// (старт-чистки уже выполнены Initialize — повтор безвреден; главное — метрики
// диска заполнены сразу), далее каждые 15 минут. Исключение тика — лог Error,
// сервис живёт.
public sealed class VolumeCleanupService(XlVolume volume, OwnS3Metrics metrics,
    ILogger<VolumeCleanupService> logger) : BackgroundService
{
    private static readonly TimeSpan Period = TimeSpan.FromMinutes(15);

    // Немедленный проход выполняется В StartAsync (детерминированно к моменту
    // его завершения — gauge диска заполнены сразу, без 15-мин ожидания),
    // затем стартует периодический цикл.
    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        await RunPassAsync(cancellationToken);
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
            await RunPassAsync(stoppingToken);
        }
    }

    private async Task RunPassAsync(CancellationToken ct)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            await volume.RunCleanupAsync(ct);
            UpdateDiskGauges();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Проход чистки тома {Root} упал", volume.Root);
            return;
        }
        logger.LogInformation("[CLEANUP] durationMs={DurationMs}", stopwatch.ElapsedMilliseconds);
    }

    // Каталожные метрики диска (канон 05 §5): used = total − free; DriveInfo
    // недоступен — warning, значения не трогаются (эксплуатационные).
    private void UpdateDiskGauges()
    {
        try
        {
            var drive = new DriveInfo(volume.Root);
            metrics.DiskTotalBytes = drive.TotalSize;
            metrics.DiskUsedBytes = drive.TotalSize - drive.TotalFreeSpace;
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Дисковые метрики недоступны для {Root}", volume.Root);
        }
    }
}
