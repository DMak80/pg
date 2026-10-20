using System.Diagnostics;
using EtcdWorker.Core;
using EtcdWorker.Etcd;
using EtcdWorker.Provisioning;
using Microsoft.Extensions.Options;
using Shared.Core.HealthChecks;

namespace EtcdWorker.App.Loops;

// Цикл DR-слепков etcd (arch/25 §4, шаг 9 t32): лидер ClaimStore доводит
// отстающую выгрузку (CatchUp ДО снятия — транзиент цели не растягивает
// RPO-окно), решает снятие чистым SnapshotSchedule (плановый раз в
// IntervalMin + внеочередной при росте revision и выгрузке старше
// MinIntervalSec), снимает SnapshotJob'ом (без sink — цикл сам экспортирует),
// выгружает SnapshotExporter'ом (резолв цели внутри, FAILED+маркер при
// отказе), maintenance после успешного снятия. Сон: отставание/FAILED →
// RetryIntervalSec; иначе короткий пульсирующий ScanIntervalSec (детекция
// изменений смотрит revision каждый тик).
internal sealed class SnapshotLoop(
    IOptionsMonitor<EtcdWorkerOptions> options,
    ClaimStore claims,
    Shared.Etcd.Maintenance.SnapshotJob snapshots,
    SnapshotExporter exporter,
    Shared.Etcd.Client.IEtcdGateway etcd,
    ILogger<SnapshotLoop> logger,
    HealthState health,
    TimeProvider clock,
    Shared.Metrics.Worker.WorkerMetricsInstrumentation metrics) : BackgroundService, IHealthCheckService
{
    // Revision последней успешной выгрузки (память процесса; прецедент
    // SnapshotJob._lastMaintenanceUtc) — вход детекции изменений.
    private static long? _lastExportedRevision;

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
                // Лидерство — только здесь (singleton-работа слепков).
                if (!claims.IsLeader)
                {
                    var became = await claims.TryBecomeLeaderAsync(stoppingToken);
                    if (became.IsSuccess && became.Value)
                        logger.LogInformation("snapshot leadership acquired: {InstanceId}", claims.InstanceId);
                }

                if (claims.IsLeader)
                {
                    var behind = true;
                    var delay = await TickLeaderAsync(stoppingToken);
                    behind = delay.Behind;

                    // Сон тика: отставание/FAILED — RetryIntervalSec (RPO-окно
                    // транзиента закрывается минутами); здоровый контур — короткий
                    // ScanIntervalSec (детекция изменений каждый тик). Сон длиннее
                    // порога сноса — пульсирующий (чанк < окна watchdog).
                    var sleep = behind
                        ? TimeSpan.FromSeconds(options.CurrentValue.Snapshots.RetryIntervalSec)
                        : TimeSpan.FromSeconds(options.CurrentValue.Loops.ScanIntervalSec);
                    await Shared.Core.Hosting.PulsingDelay.SleepAsync(sleep,
                        TimeSpan.FromSeconds(options.CurrentValue.Loops.Watchdog.CheckIntervalSec),
                        health.MarkSnapshotActivity, stoppingToken);
                }
                else
                {
                    // Не лидер: ждём до следующей попытки захвата.
                    health.MarkSnapshotTick();
                    metrics.LoopTick("snapshot", ok: true);
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

    private async Task<(bool Behind, Result Status)> TickLeaderAsync(CancellationToken ct)
    {
        var snapOpts = options.CurrentValue.Snapshots;

        // Доводка отстающей выгрузки ДО снятия (механика t08).
        var catchUp = await exporter.CatchUpAsync(snapOpts.Dir, ct);
        var behind = catchUp.IsSuccess ? catchUp.Value : true;

        // Решение снятия: расписание + детекция изменений (revision etcd).
        var status = await exporter.ReadStatusAsync(ct);
        var latest = SnapshotExporter.LatestLocalFile(snapOpts.Dir);
        var lastTakeUnix = EtcdSnapshotStatus.TakenUnixFromName(
            latest is null ? "" : Path.GetFileName(latest));
        var lastUploadedUnix = status.IsSuccess ? status.Value?.LastUploadedUnix : null;
        var revision = await CurrentRevisionAsync(ct);
        var take = SnapshotSchedule.ShouldTake(
            clock.GetUtcNow().ToUnixTimeSeconds(),
            lastTakeUnix,
            lastUploadedUnix,
            revision ?? 0,
            _lastExportedRevision,
            snapOpts.IntervalMin,
            snapOpts.MinIntervalSec);

        if (take)
        {
            var started = Stopwatch.GetTimestamp();
            var shot = await snapshots.TakeAsync(ct);
            metrics.LoopDuration("snapshot", Stopwatch.GetElapsedTime(started).TotalSeconds);
            if (shot.IsSuccess)
            {
                health.MarkSnapshotTaken();
                metrics.SnapshotTaken(clock.GetUtcNow());
                logger.LogInformation("etcd snapshot taken: {Path}", shot.Value);

                // Выгрузка: цикл сам экспортирует (резолв цели внутри экспортёра).
                var data = await File.ReadAllBytesAsync(shot.Value, ct);
                var exported = await exporter.ExportAsync(
                    Path.GetFileName(shot.Value), data, revision, ct);
                if (exported.IsSuccess)
                {
                    if (revision is { } rev)
                        _lastExportedRevision = rev;
                    StatusError = Result.Success();
                }
                else
                {
                    // Отказ выгрузки — домен статус-ключа, НЕ цикла (arch/25 §5:
                    // FAILED с маркером etcd-backup-missing/транзиент — ретраи
                    // тиком, CatchUp догонит; алерт — панель): снятие прошло,
                    // слепок в локальном томе, цикл здоров.
                    logger.LogError(exported.Error, "etcd snapshot export failed: {Message}", exported.Error!.Message);
                }

                // Обслуживание etcd: compact + defrag (интервал в Snapshots).
                var maintenance = await snapshots.MaintainAsync(ct);
                if (!maintenance.IsSuccess)
                    logger.LogWarning(maintenance.Error, "etcd maintenance failed: {Message}", maintenance.Error!.Message);
            }
            else
            {
                StatusError = shot;
                logger.LogError(shot.Error, "etcd snapshot failed: {Message}", shot.Error!.Message);
            }
        }
        else
        {
            // Снятия нет — успешный тик гасит ошибку прошлого.
            StatusError = Result.Success();
        }

        health.MarkSnapshotTick();
        metrics.LoopTick("snapshot", ok: true);

        // Повторный расчёт отставания ПОСЛЕ снятия (механика t08): первый
        // транзиент перевёл статус в FAILED уже после расчёта «до» — без
        // пере-расчёта цикл ушёл бы в длинный сон с невыгруженным слепком.
        var fresh = await exporter.ReadStatusAsync(ct);
        var freshLatest = SnapshotExporter.LatestLocalFile(snapOpts.Dir);
        behind = !fresh.IsSuccess
                 || EtcdSnapshotStatus.IsBehind(
                     fresh.Value,
                     EtcdSnapshotStatus.TakenUnixFromName(
                         freshLatest is null ? "" : Path.GetFileName(freshLatest)));

        return (behind, StatusError);
    }

    // Текущая revision etcd (best-effort, failover по endpoints): рост
    // относительно последней выгрузки — признак изменений для расписания.
    private async Task<long?> CurrentRevisionAsync(CancellationToken ct)
    {
        foreach (var endpoint in options.CurrentValue.Etcd.Endpoints)
        {
            var status = await etcd.StatusAsync(endpoint, ct);
            if (status.IsSuccess && status.Value?.Revision is { } revision)
                return (long)revision;
        }

        return null;
    }
}
