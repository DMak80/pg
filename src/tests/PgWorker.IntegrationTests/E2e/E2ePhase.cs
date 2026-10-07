using System.Diagnostics;

namespace PgWorker.IntegrationTests.E2e;

/// <summary>
/// Единый фазовый телеметрический контур t29-классов (spec t29 §4.1): замер
/// фазы + строки [PHASE]/[PHASE-TICK] в журнал теста (Console — прецедент
/// StaticPhase) и в <see cref="E2eEnvironment.ArtifactsDir"/>/phases.log с
/// UTC-меткой; пересечение 60 с — немедленный сбор docker-диагностики;
/// провал окна (ok=False) — сбор failed-phase-* ДО возврата сценарию.
/// Копии WaitPhaseAsync в не-t29-классах не мигрируются (объём); канон для
/// новых сценариев — docs/e2e-launch.md.
/// </summary>
internal static class E2ePhase
{
    /// <summary>Порог автосбора диагностики (docs/e2e-launch.md §2).</summary>
    private static readonly TimeSpan SlowPhaseThreshold = TimeSpan.FromSeconds(60);

    /// <summary>Интервал прогресс-тиков: короткие окна 10–15 с дают 2–3 строки
    /// динамики (spec §4.1); docker-CLI/S3 в тиках — не чаще этого интервала.</summary>
    private static readonly TimeSpan TickInterval = TimeSpan.FromSeconds(5);

    /// <summary>Полл условия — 0.5 с, как E2eFixture.WaitForAsync (семантика
    /// ожиданий не меняется: только телеметрия вокруг).</summary>
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(500);

    /// <summary>Ожидание условия с фазовой телеметрией: полл, тики и пороги — в
    /// одном цикле (НЕ обёртка над WaitForAsync, которой не видно внутреннего
    /// времени). Прогресс-делегат — только дешёвые снапшоты (etcd-чтения,
    /// лёгкие SQL-скаляры, S3-листинг); docker-CLI в тиках запрещён (spec §4.1).</summary>
    public static async Task<bool> WaitAsync(
        E2eEnvironment fx,
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
            // SQL/etcd может кидать на рестартующих нодах и не роняет фазу.
            if (progress is not null && sw.Elapsed >= nextTick)
            {
                lastSnapshot = await SafeSnapshotAsync(progress);
                Emit(fx, $"[PHASE-TICK] {phase} t={sw.Elapsed.TotalSeconds:F1}s: {lastSnapshot}");
                nextTick += TickInterval;
            }

            // Отмена теста всплывает как из WaitForAsync (итоговой строки не
            // пишем: фаза не завершена).
            await Task.Delay(PollInterval, ct);
        }

        // Провал окна (ok=False): сбор failed-phase-* ДО возврата сценарию
        // (spec §4.4 — контейнеры ещё живые, teardown ещё не мутировал);
        // CollectDiagnosticsAsync глотает свои ошибки сам.
        await fx.CollectDiagnosticsAsync($"failed-phase-{phase}");
        lastSnapshot ??= progress is null ? null : await SafeSnapshotAsync(progress);
        Emit(fx, FinalLine(phase, ok: false, sw.Elapsed, budget, lastSnapshot));
        return false;
    }

    // Итоговая строка фазы строго формата spec §4.1; progress-часть — только
    // при заданном делегате (снапшот снят тиком либо однократно для коротких
    // фаз, когда тиков не было).
    private static string FinalLine(
        string phase, bool ok, TimeSpan elapsed, TimeSpan budget, string? snapshot)
        => $"[PHASE] {phase}: ok={ok}, elapsed={elapsed.TotalSeconds:F1}s, budget={budget.TotalSeconds:0.#}s"
           + (snapshot is null ? "" : $", progress={snapshot}");

    // Снапшот прогресса «лучшими усилиями»: рестартующие ноды/лагающий etcd
    // не должны ронять фазу диагностики ради диагностики.
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

    // Строка телеметрии: Console (журнал теста; xUnit буферизует до конца
    // факта — прецедент StaticPhase) + phases.log с UTC-меткой (виден
    // наблюдателю во время «тихих» фаз, метки сопоставимы с
    // docker logs --timestamps); файловая запись не роняет тест.
    private static void Emit(E2eEnvironment fx, string line)
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
