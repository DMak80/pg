using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using Microsoft.Extensions.Options;
using Shared.Core.HealthChecks;
using Shared.Etcd.Client;
using S3Worker.App;

namespace S3Worker.App.Loops;

/// <summary>
/// Главный цикл воркера (arch/24 §5; порт ReconcileLoop ValkeyWorker; чтение
/// состояния — arch/17 S10): контроль-плейн читается из слепка опросного
/// кэша (IPrefixPollSources) — etcd-чтений в тике ноль (E10). Тик работает
/// КАЖДЫЙ раз, без гейтов по дельте: self-healing ждёт не-etcd события
/// (health-грань инстанса, docker-факты) — применения etcd-значений
/// процессами идут по факту изменения значения (S10). Heartbeat и healthz
/// loops-alive отмечаются каждым тиком (arch/24 §6). Ошибка тика не роняет
/// цикл (лог + ErrorDelayMs, следующий тик — ретрай).
/// </summary>
internal sealed class ReconcileLoop(
    IOptionsMonitor<S3WorkerOptions> options,
    IPrefixPollSources sources,
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
                    logger.LogError(tick.Error, "ReconcileLoop tick failed: {Message}", tick.Error!.Message);
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
            // Останов опросных кэшей (идемпотентно; остановка лучшего эффекта —
            // процесс останавливается).
            try
            {
                await sources.StopAsync(CancellationToken.None);
            }
            catch
            {
                // host останавливается — повтор не требуется
            }
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

    /// <summary>Один тик (публичен для тестов/health): слепок кэша →
    /// процессы A–E над снапшотом /s3/.</summary>
    internal async Task<Result> TickAsync(CancellationToken ct)
    {
        var endpoints = options.CurrentValue.Etcd.Endpoints.ToArray();
        if (endpoints.Length == 0)
            return Result.Failed(new ApplicationException("S3Worker:Etcd:Endpoints are not configured"));

        // Запуск опросного кэша (идемпотентно; сбой старта — ошибка тика:
        // цикл залогирует и повторит с ErrorDelayMs).
        try
        {
            await sources.StartAsync(ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            return Result.Failed(ex);
        }

        health.MarkReconcileActivity(); // heartbeat: старт итерации (долгие фазы отмечаются глубже)

        // Парсинг контроль-плейна — из слепка кэша (не etcd; P9 выполняется
        // опросным циклом кэша, единая точка failover — arch/17 S7).
        var claimsHeld = await processes.TickAsync(ToKvs(sources.ClustersSnapshot), ct);

        // Секция health «claims»: фактическое число клэймов этого инстанса.
        health.MarkReconcileTick(ok: true, claimsHeld);
        metrics.LoopTick("reconcile", ok: true);
        return Result.Success();
    }

    // Слепок кэша → Kv-список парсера (значения без изменений).
    private static List<Kv> ToKvs(IReadOnlyDictionary<string, (string Value, long ModRevision)> snapshot)
        => snapshot
            .Select(pair => new Kv(pair.Key, pair.Value.Value, (ulong)pair.Value.ModRevision))
            .ToList();
}
