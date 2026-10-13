using System.Diagnostics;
using OwnS3.App;
using OwnS3.Storage;

namespace OwnS3.App.Pipeline;

// Фоновый сервис чисток тома (спека §4.7): немедленный проход при старте
// (старт-чистки уже выполнены Initialize — повтор безвреден; главное — метрики
// диска заполнены сразу), далее каждые 15 минут. Исключение тика — лог Error,
// сервис живёт.
public sealed class VolumeCleanupService(IXlDrive drive, StorageModeInfo mode, OwnS3Metrics metrics,
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
            var cleanup = await drive.RunCleanupAsync(ct);
            if (!cleanup.IsSuccess)
            {
                logger.LogError(cleanup.Error, "Проход чистки данных упал");
                return;
            }
            UpdateDiskGauges();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Проход чистки данных упал");
            return;
        }
        logger.LogInformation("[CLEANUP] durationMs={DurationMs}", stopwatch.ElapsedMilliseconds);
    }

    // Каталожные метрики дисков (канон 05 §5): used = total − free, сумма по
    // корням режима (single — один том; EC — все диски пула); DriveInfo
    // недоступен — warning, значения не трогаются (эксплуатационные).
    private void UpdateDiskGauges()
    {
        try
        {
            long total = 0, free = 0;
            foreach (var root in mode.DataRoots)
            {
                var driveInfo = new DriveInfo(root);
                total += driveInfo.TotalSize;
                free += driveInfo.TotalFreeSpace;
            }

            metrics.DiskTotalBytes = total;
            metrics.DiskUsedBytes = total - free;
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Дисковые метрики недоступны");
        }
    }
}
