using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Shared.Core.Hosting;

/// <summary>Сердцебиение одного цикла: имя, последний тик, порог staleness
/// watchdog (множитель уже применён реализацией ILoopsVitality).</summary>
public sealed record LoopHeartbeat(string Name, DateTimeOffset? LastTickAt, TimeSpan StaleAfter);

/// <summary>Источник живости циклов воркера: реализация per-app поверх своего
/// HealthState + LoopsOptions; чтение lock-free (миллисекунды).</summary>
public interface ILoopsVitality
{
    /// <summary>Снимок сердцебиений всех наблюдаемых циклов.</summary>
    IReadOnlyList<LoopHeartbeat> Snapshot();
}

/// <summary>Секция Loops:Watchdog: Enabled=false — компонент не регистрируется;
/// порог = Multiplier × порог healthz loops-alive.</summary>
public sealed class WatchdogOptions
{
    public bool Enabled { get; set; } = true;

    /// <summary>Множитель порога healthz (окно Degraded→рестарт).</summary>
    public int Multiplier { get; set; } = 2;

    /// <summary>Период проверки возрастов тиков.</summary>
    public int CheckIntervalSec { get; set; } = 15;

    /// <summary>Пауза перед StopApplication (лог/событие доезжают в выхлоп).</summary>
    public int StopDelaySec { get; set; } = 1;
}

/// <summary>
/// Watchdog зависших циклов: staleness тика сверх порога → журнал (critical) +
/// маркер метрики (колбэк) + graceful StopApplication (путь POST /api/restart).
/// Без сетевых вызовов и etcd; за собой не следит (его собственное зависание —
/// деградация всего процесса, зона docker HEALTHCHECK).
/// </summary>
public sealed class LoopWatchdog(
    ILoopsVitality vitality,
    IHostApplicationLifetime lifetime,
    ILogger<LoopWatchdog> logger,
    TimeProvider clock,
    WatchdogOptions options,
    Action<string>? restartMark = null) : BackgroundService
{
    /// <summary>Запущен и следит (секция healthz watchdog).</summary>
    public bool Armed { get; private set; }

    /// <summary>Цикл в staleness по последней проверке (null — все живы).</summary>
    public string? StaleLoop { get; private set; }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var startedAt = clock.GetUtcNow();
        Armed = true;
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                foreach (var heartbeat in vitality.Snapshot())
                {
                    var now = clock.GetUtcNow();
                    if (heartbeat.LastTickAt is { } at)
                    {
                        // Firing: одного наблюдения превышения достаточно — порог
                        // сам по себе защита от ложных срабатываний (гистерезис не вводим).
                        var age = now - at;
                        if (age > heartbeat.StaleAfter)
                        {
                            await FireAndStopAsync(heartbeat, age, stoppingToken);
                            return;
                        }
                    }
                    else if (now - startedAt > TimeSpan.FromTicks(heartbeat.StaleAfter.Ticks * 2))
                    {
                        // Grace старта исчерпан: цикл не тикнул вовсе.
                        await FireAndStopAsync(heartbeat, now - startedAt, stoppingToken);
                        return;
                    }
                }

                await Task.Delay(TimeSpan.FromSeconds(Math.Max(1, options.CheckIntervalSec)), stoppingToken);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // штатная остановка host'а
        }
    }

    // Firing: журнал → метрика → пауза StopDelaySec → StopApplication. Дальше —
    // существующее: shutdown (зависший цикл не реагирует на токен — закроет
    // ShutdownTimeout 30 с) → docker restart: unless-stopped.
    private async Task FireAndStopAsync(LoopHeartbeat heartbeat, TimeSpan age, CancellationToken ct)
    {
        StaleLoop = heartbeat.Name;
        logger.LogCritical(
            "watchdog: цикл {Loop} не тикал {Age:F0} c (порог {Threshold:F0} c) — инициирован self-restart",
            heartbeat.Name, age.TotalSeconds, heartbeat.StaleAfter.TotalSeconds);
        try
        {
            restartMark?.Invoke(heartbeat.Name); // метрика — пассивный наблюдатель
        }
        catch
        {
            // ошибка инструментария не отменяет остановку
        }

        if (options.StopDelaySec > 0)
        {
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(options.StopDelaySec), CancellationToken.None);
            }
            catch (OperationCanceledException)
            {
                // CancellationToken.None — недостижимо, страховка
            }
        }

        lifetime.StopApplication();
    }
}
