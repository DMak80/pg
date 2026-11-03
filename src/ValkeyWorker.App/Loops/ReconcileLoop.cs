using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using Microsoft.Extensions.Options;
using Shared.Core.HealthChecks;
using Shared.Etcd.Client;
using ValkeyWorker.App;

namespace ValkeyWorker.App.Loops;

/// <summary>
/// Главный цикл воркера (arch/21 §5; порт ReconcileLoop KafkaWorker; чтение
/// состояния — arch/17 S10): контроль-плейн читается опросными кэшами
/// (IPrefixPollSources), тик применяется ТОЛЬКО при факте изменения
/// etcd-состояния — гейт по дельте: с последнего обработанного состояния
/// изменений не было → тик БЕЗ изменений (processes не зовутся, клэймов/
/// мутаций нет); heartbeat и healthz loops-alive отмечаются всегда
/// (watchdog жив, arch/21 §6). Сбой опроса кэша дельту не поднимает — тик
/// без изменений; при появлении изменения дельта будет — тик обработает.
/// Ошибка тика не роняет цикл (лог + ErrorDelayMs, следующий тик — ретрай).
/// </summary>
internal sealed class ReconcileLoop(
    IOptionsMonitor<ValkeyWorkerOptions> options,
    IPrefixPollSources sources,
    IValkeyClusterProcesses processes,
    ILogger<ReconcileLoop> logger,
    HealthState health,
    Shared.Metrics.Worker.WorkerMetricsInstrumentation metrics) : BackgroundService, IHealthCheckService
{
    // Baseline гейта тика (arch/17 S10): Version источников на момент
    // последнего ОБРАБОТАННОГО тика; −1 = ещё не обрабатывали (первый тик
    // всегда работает — стартовый слепок кэшей и есть обрабатываемое состояние).
    private long _processedVersion = -1;

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
                    // healthz = «последний тик» (живой-Ф7, порт PgWorker ReconcileLoop): успешный
                    // тик гасит ошибку прошлого — иначе единственный упавший тик = вечный unhealthy.
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
    /// Тик с защитой тела цикла (порт rework №3 PgWorker): исключение тика не
    /// роняет BackgroundService — превращается в ошибку тика (лог + ErrorDelayMs).
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

    /// <summary>Один тик (публичен для тестов/health): гейт по дельте → слепок
    /// кэша → процессы A–E над снапшотом /valkey/clusters/.</summary>
    internal async Task<Result> TickAsync(CancellationToken ct)
    {
        var endpoints = options.CurrentValue.Etcd.Endpoints.ToArray();
        if (endpoints.Length == 0)
            return Result.Failed(new ApplicationException("ValkeyWorker:Etcd:Endpoints are not set"));

        // Запуск опросных кэшей (идемпотентно; сбой старта — ошибка тика:
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

        // ГЕЙТ ТИКА (arch/17 S10, реестр E10): применяется только фактическое
        // изменение etcd-состояния. Дельты с последнего обработанного состояния
        // нет (в т.ч. после сбоя опроса — кэш дельту не поднимает) → тик БЕЗ
        // изменений: processes/клэймы/мутации не вызываются. Heartbeat
        // отмечается ВСЕГДА (watchdog жив, arch/21 §6), loops-alive живёт
        // (клэймы не трогаем — их держит keepalive-цикл), метрика тика —
        // успешная (цикл жив, работы не было).
        var version = sources.Version;
        if (version == Interlocked.Read(ref _processedVersion))
        {
            health.MarkReconcileActivity();
            health.MarkReconcileNoChangeTick();
            metrics.LoopTick("reconcile.nochange", ok: true); // «тик без изменений» — отдельная серия
            return Result.Success();
        }

        health.MarkReconcileActivity(); // heartbeat: старт итерации (долгие фазы отмечаются глубже)

        // Парсинг контроль-плейна — из слепка кэша (не etcd; P9 выполняется
        // опросным циклом кэша, единая точка failover — arch/17 S7).
        var claimsHeld = await processes.TickAsync(ToKvs(sources.ClustersSnapshot), ct);

        // Тик обработан: baseline = Version на входе тика; изменения, пришедшие
        // ВО ВРЕМЯ обработки, остались за baseline'ом — их донесёт следующий тик.
        Interlocked.Exchange(ref _processedVersion, version);
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
