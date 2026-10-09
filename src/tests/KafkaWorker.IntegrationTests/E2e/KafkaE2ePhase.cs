using System.Diagnostics;

namespace KafkaWorker.IntegrationTests.E2e;

/// <summary>
/// Фазовый телеметрический контур docker-E2E KafkaWorker — порт паттерна
/// PgWorker E2ePhase (docs/e2e-launch.md §2): замер фазы + строки
/// [PHASE]/[PHASE-TICK] в журнал теста (Console) и в
/// <see cref="KafkaE2eEnvironment.ArtifactsDir"/>/phases.log с UTC-меткой;
/// пересечение 60 с — немедленный сбор docker-диагностики; провал окна
/// (ok=False) — сбор failed-phase-* ДО возврата сценарию. Прогресс-делегаты —
/// только дешёвые etcd-чтения (docker-CLI в тиках запрещён: поллинг демона
/// искажает измеряемую нагрузку; docker-картина — в slow/failed-phase-сбор).
/// </summary>
internal static class KafkaE2ePhase
{
    /// <summary>Порог автосбора диагностики (docs/e2e-launch.md §2).</summary>
    private static readonly TimeSpan SlowPhaseThreshold = TimeSpan.FromSeconds(60);

    /// <summary>Интервал прогресс-тиков: короткие окна дают 2–3 строки динамики.</summary>
    private static readonly TimeSpan TickInterval = TimeSpan.FromSeconds(5);

    /// <summary>Полл условия — 0.5 с (канон репо): семантика ожиданий не
    /// меняется — вокруг неё только телеметрия.</summary>
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(500);

    /// <summary>Ожидание условия с фазовой телеметрией: полл, тики и пороги — в
    /// одном цикле. Прогресс-делегат — только дешёвые etcd-снапшоты; docker-CLI
    /// в тиках запрещён (docs/e2e-launch.md §2) — docker-проверки живут в теле
    /// условия (якорь A3) или вне тиков.</summary>
    public static async Task<bool> WaitAsync(
        KafkaE2eEnvironment fx,
        string phase,
        Func<Task<bool>> condition,
        TimeSpan budget,
        CancellationToken ct,
        Func<Task<string>>? progress = null)
    {
        var sw = Stopwatch.StartNew();
        var lastSnapshot = (string?)null;
        var nextTick = TickInterval;
        var slowCollected = false;
        while (true)
        {
            if (await condition())
            {
                lastSnapshot ??= progress is null ? null : await SafeSnapshotAsync(progress);
                Emit(fx, FinalLine(phase, ok: true, sw.Elapsed, budget, lastSnapshot));
                return true;
            }

            if (sw.Elapsed >= budget)
                break;

            // Немедленный slow-phase-сбор: фаза > 60 с — картина живого окружения
            // ДО исчерпания бюджета (docs/e2e-launch.md §2), однократно.
            if (!slowCollected && sw.Elapsed >= SlowPhaseThreshold)
            {
                slowCollected = true;
                await fx.CollectDiagnosticsAsync($"slow-phase-{phase}");
            }

            // Прогресс-тик: снапшот динамики окна; исключения делегата глотаем
            // («лучшими усилиями», снапшот = <progress error: …>): прогресс из
            // etcd может кидать на рестартующих сервисах и не роняет фазу.
            if (progress is not null && sw.Elapsed >= nextTick)
            {
                lastSnapshot = await SafeSnapshotAsync(progress);
                Emit(fx, $"[PHASE-TICK] {phase} t={sw.Elapsed.TotalSeconds:F1}s: {lastSnapshot}");
                nextTick += TickInterval;
            }

            // Отмена теста всплывает (итоговой строки не пишем: фаза не завершена).
            await Task.Delay(PollInterval, ct);
        }

        // Провал окна (ok=False): сбор failed-phase-* ДО возврата сценарию
        // (контейнеры ещё живые, teardown ещё не мутировал).
        await fx.CollectDiagnosticsAsync($"failed-phase-{phase}");
        lastSnapshot ??= progress is null ? null : await SafeSnapshotAsync(progress);
        Emit(fx, FinalLine(phase, ok: false, sw.Elapsed, budget, lastSnapshot));
        return false;
    }

    // Итоговая строка фазы строго формата docs/e2e-launch.md §2; progress-часть —
    // только при заданном делегате (снапшот снят тиком либо однократно для
    // коротких фаз, когда тиков не было).
    private static string FinalLine(
        string phase, bool ok, TimeSpan elapsed, TimeSpan budget, string? snapshot)
        => $"[PHASE] {phase}: ok={ok}, elapsed={elapsed.TotalSeconds:F1}s, budget={budget.TotalSeconds:0.#}s"
           + (snapshot is null ? "" : $", progress={snapshot}");

    // Снапшот прогресса «лучшими усилиями»: лагающий etcd не должен ронять
    // фазу диагностики ради диагностики.
    private static async Task<string> SafeSnapshotAsync(Func<Task<string>> progress)
    {
        try
        {
            return await progress();
        }
        catch (Exception e)
        {
            return $"<progress error: {e.Message}>";
        }
    }

    // Строка телеметрии: Console (журнал теста) + phases.log с UTC-меткой
    // (виден наблюдателю во время «тихих» фаз, метки сопоставимы с
    // docker logs --timestamps); файловая запись не роняет тест.
    private static void Emit(KafkaE2eEnvironment fx, string line)
    {
        Console.WriteLine(line);
        try
        {
            File.AppendAllText(
                Path.Combine(fx.ArtifactsDir, "phases.log"),
                $"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} {line}\n");
        }
        catch
        {
            // файловая телеметрия — «лучшими усилиями»
        }
    }
}
