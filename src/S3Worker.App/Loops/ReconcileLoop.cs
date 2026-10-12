using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using Microsoft.Extensions.Options;
using Shared.Core.HealthChecks;
using S3Worker.App;

namespace S3Worker.App.Loops;

/// <summary>
/// Главный цикл воркера (arch/24 §5; порт ReconcileLoop ValkeyWorker): тик =
/// обработка домена /s3/ → классификация → клэйм → процесс (IS3ClusterProcesses).
/// Ошибка тика не роняет цикл (лог + ErrorDelayMs, следующий тик — ретрай).
/// </summary>
internal sealed class ReconcileLoop(
    IOptionsMonitor<S3WorkerOptions> options,
    IS3ClusterProcesses processes,
    ILogger<ReconcileLoop> logger,
    HealthState health,
    Shared.Metrics.Worker.WorkerMetricsInstrumentation metrics) : BackgroundService, IHealthCheckService
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
                var started = Stopwatch.GetTimestamp();
                var tick = await TickSafelyAsync(stoppingToken);
                metrics.LoopDuration("reconcile", Stopwatch.GetElapsedTime(started).TotalSeconds);
                if (tick.IsSuccess)
                {
                    // healthz = «последний тик» (живой-Ф7): успешный тик гасит ошибку
                    // прошлого — иначе единственный упавший тик = вечный unhealthy.
                    StatusError = Result.Success();
                    await Task.Delay(
                        TimeSpan.FromSeconds(options.CurrentValue.Loops.ScanIntervalSec), stoppingToken);
                }
                else
                {
                    metrics.LoopTick("reconcile", ok: false);
                    // Тик не прошёл (etcd недоступен и т.п.): лог + короткая задержка.
                    StatusError = tick;
                    logger.LogError(tick.Error, "тик ReconcileLoop не прошёл: {Message}", tick.Error!.Message);
                    await Task.Delay(
                        TimeSpan.FromMilliseconds(options.CurrentValue.Loops.ErrorDelayMs), stoppingToken);
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

    /// <summary>
    /// Тик с защитой тела цикла: исключение тика не роняет BackgroundService —
    /// превращается в ошибку тика (лог + ErrorDelayMs).
    /// </summary>
    internal async Task<Result> TickSafelyAsync(CancellationToken ct)
    {
        try
        {
            return await TickAsync(ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw; // остановка host'а — не «ошибка тика»
        }
        catch (Exception ex)
        {
            return Result.Failed(ex);
        }
    }

    /// <summary>Один тик (публичен для тестов/health): процессы A–E над снапшотом /s3/.</summary>
    internal async Task<Result> TickAsync(CancellationToken ct)
    {
        var endpoints = options.CurrentValue.Etcd.Endpoints.ToArray();
        if (endpoints.Length == 0)
            return Result.Failed(new ApplicationException("S3Worker:Etcd:Endpoints не заданы"));

        health.MarkReconcileActivity(); // heartbeat: старт итерации (долгие фазы отмечаются глубже)

        var claimsHeld = await processes.TickAsync(ct);

        health.MarkEtcdOk();
        // Секция health «claims»: фактическое число клэймов этого инстанса.
        health.MarkReconcileTick(ok: true, claimsHeld);
        metrics.LoopTick("reconcile", ok: true);
        return Result.Success();
    }
}
