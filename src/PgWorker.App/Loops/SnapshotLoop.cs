using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using Microsoft.Extensions.Options;
using PgWorker.Backups.EtcdExport;
using Shared.Core.HealthChecks;
using PgWorker.Core;

namespace PgWorker.App.Loops;

/// <summary>
/// Цикл регулярных снапшотов etcd (задача 23; spec §6.2 цикл №3, P12): только
/// глобальный лидер (Д2) снимает слепок раз в SnapshotIntervalMin; не-лидер
/// периодически пытается захватить лидерство (takeover ≤ TTL 15 с + тик).
/// Внеочередные снапшоты в точках изменений снимают сами процессы.
/// t08: перед снятием — доводка отстающей выгрузки в S3 (CatchUpAsync); сон
/// при отставании — RetryIntervalSec (иначе SnapshotIntervalMin).
/// </summary>
internal sealed class SnapshotLoop(
    IOptionsMonitor<PgWorkerOptions> options,
    ClaimStore claims,
    SnapshotJob snapshots,
    ILogger<SnapshotLoop> logger,
    HealthState health,
    TimeProvider clock,
    Shared.Metrics.Worker.WorkerMetricsInstrumentation metrics,
    EtcdSnapshotSink? exportSink = null) : BackgroundService, IHealthCheckService
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
            while (!stoppingToken.IsCancellationRequested)
            {
                // Лидерство — только здесь (singleton-работа снапшотов).
                if (!claims.IsLeader)
                {
                    var became = await claims.TryBecomeLeaderAsync(stoppingToken);
                    if (became.IsSuccess && became.Value)
                        logger.LogInformation("захвачено лидерство снапшотов: {InstanceId}", claims.InstanceId);
                }

                if (claims.IsLeader)
                {
                    // t08 (spec §3.5 п.1/п.3): доводка отстающей выгрузки ДО снятия — транзиент S3
                    // не растягивает RPO-окно до планового тика. CatchUpAsync возвращает отставание
                    // ПО СТАТУС-ключу (IsBehind: FAILED/ключа нет/локальный новее — включая FAILED
                    // при пустом томе, без re-export); ошибка доводки — как ошибка экспорта
                    // (статус-ключ пишется внутри sink) — тоже короткий сон.
                    var behind = false;
                    if (exportSink is not null)
                    {
                        var catchUp = await exportSink.CatchUpAsync(
                            options.CurrentValue.Snapshots.Dir, stoppingToken);
                        behind = catchUp.IsSuccess ? catchUp.Value : true;
                    }

                    var started = Stopwatch.GetTimestamp();
                    var shot = await snapshots.TakeAsync(stoppingToken);
                    metrics.LoopDuration("snapshot", Stopwatch.GetElapsedTime(started).TotalSeconds);
                    if (shot.IsSuccess)
                    {
                        // healthz = «последний тик» (живой-Ф7): успешный снимок гасит
                        // ошибку прошлого — иначе единственный фейл = вечный unhealthy.
                        StatusError = Result.Success();
                        health.MarkSnapshotTaken();
                        metrics.SnapshotTaken(clock.GetUtcNow());
                        logger.LogInformation("снапшот etcd снят: {Path}", shot.Value);
                        // Обслуживание etcd: compact + defrag (не чаще раза в час).
                        var maintenance = await snapshots.MaintainAsync(stoppingToken);
                        if (!maintenance.IsSuccess)
                            logger.LogWarning(maintenance.Error, "обслуживание etcd не выполнено: {Message}", maintenance.Error!.Message);
                    }
                    else
                    {
                        StatusError = shot;
                        logger.LogError(shot.Error, "снапшот etcd не снят: {Message}", shot.Error!.Message);
                    }

                    health.MarkSnapshotTick();
                    metrics.LoopTick("snapshot", ok: true);
                    // t08 (круг 7, spec §3.5 п.3): повторный расчёт ПОСЛЕ снятия — первый
                    // транзиент S3 в тике TakeAsync переводит статус-ключ в FAILED уже ПОСЛЕ
                    // расчёта «до»; без пере-расчёта лидер ушёл бы в полный SnapshotIntervalMin
                    // с невыгруженным слепком (запрещено §3.5 п.3). Считаем по свежему
                    // статус-ключу и метке новейшего локального слепка (тот самый, только что
                    // снятый); отказ чтения ключа — трактуем как отставание (короткий сон).
                    if (exportSink is not null)
                    {
                        var fresh = await exportSink.ReadStatusAsync(stoppingToken);
                        var latest = EtcdSnapshotSink.LatestLocalFile(options.CurrentValue.Snapshots.Dir);
                        behind = !fresh.IsSuccess
                                 || EtcdSnapshotStatus.IsBehind(
                                     fresh.Value, EtcdSnapshotStatus.TakenUnixFromName(Path.GetFileName(latest ?? "")));
                    }

                    // Сон тика лидера (spec §3.5 п.3): выгрузка здорова (state=OK, не отстаёт —
                    // каждый успешный проход sink'а продвигает покрытие к метке нового слепка) —
                    // SnapshotIntervalMin как раньше; отстаёт/FAILED — RetryIntervalSec
                    // (RPO-окно транзиента закрывается минутами; исправный контур выгружает
                    // каждый плановый слепок — каденс остаётся плановым, AC2).
                    var delay = exportSink is not null && behind
                        ? TimeSpan.FromSeconds(options.CurrentValue.Snapshots.Export.RetryIntervalSec)
                        : TimeSpan.FromMinutes(options.CurrentValue.Loops.SnapshotIntervalMin);
                    // Сон длиннее порога сноса — пульсирующий (чанк < окна проверки 15 c):
                    // MarkSnapshotActivity — активность без тика (healthz loops-alive не меняется)
                    await Shared.Core.Hosting.PulsingDelay.SleepAsync(delay,
                        TimeSpan.FromSeconds(options.CurrentValue.Loops.Watchdog.CheckIntervalSec),
                        health.MarkSnapshotActivity, stoppingToken);
                }
                else
                {
                    // Не лидер: ждём до следующей попытки захвата (интервал сканирования).
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
}
