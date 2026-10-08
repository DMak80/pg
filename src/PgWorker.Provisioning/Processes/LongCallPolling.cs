using System.Diagnostics;
using Microsoft.Extensions.Logging;
using PgWorker.Core;
using Shared.Core.Hosting;

namespace PgWorker.Provisioning.Processes;

/// <summary>Поллинг-инвариант долгих одиночных вызовов (arch/14 §6): одиночный
/// вызов драйвера не молчит дольше окна проверки watchdog — попытка с
/// таймаутом короче окна (практично — половина); незавершение → идемпотентный
/// повтор (ensure-семантика: повторный create → уже-есть → идентифицирующий
/// инспект подтверждает состояние). Каждая итерация — отметка прогресса ПО
/// ФАКТУ + лог elapsed («сколько фаза уже занимает»). Ошибка драйвера
/// (Result.Failed) — наверх без повторов (следующий тик продолжит): поллинг
/// ловит молчание, не сбои. Общий бюджет фазы — существующие пороги
/// (PatroniBootSec-семантика). Отметка «в обмен на ничто» запрещена: Mark
/// только у исполненной итерации.</summary>
public static class LongCallPolling
{
    public static async Task<Result> EnsureAsync(
        string phase, Func<CancellationToken, Task<Result>> call,
        ILoopProgress? progress, ILogger logger,
        TimeSpan singleCallTimeout, TimeSpan budget, CancellationToken ct)
    {
        var started = Stopwatch.GetTimestamp();
        for (var attempt = 1; ; attempt++)
        {
            using var attemptCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            attemptCts.CancelAfter(singleCallTimeout);
            try
            {
                var result = await call(attemptCts.Token);
                progress?.Mark();
                // elapsed-лог на КАЖДОЙ итерации, включая завершающую успешную
                // (буква приказа п.4: «каждая итерация поллинга даёт отметку
                // активности И пишет в лог elapsed»)
                logger.LogInformation(
                    "provisioning: фаза {Phase}: уже {Elapsed:F0} c (итерация {Attempt}: {Outcome})",
                    phase, Stopwatch.GetElapsedTime(started).TotalSeconds, attempt,
                    result.IsSuccess ? "успех" : $"ошибка: {result.Error?.Message}");
                return result;
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested && attemptCts.IsCancellationRequested)
            {
                // итерация завершилась таймаутом — факт итерации: отметка + elapsed
                progress?.Mark();
                logger.LogInformation(
                    "provisioning: фаза {Phase}: уже {Elapsed:F0} c (итерация {Attempt}: вызов не завершился за {Timeout:F0} c — идемпотентный повтор)",
                    phase, Stopwatch.GetElapsedTime(started).TotalSeconds, attempt, singleCallTimeout.TotalSeconds);
            }

            if (Stopwatch.GetElapsedTime(started) >= budget)
                return Result.Failed(new ApplicationException(
                    $"фаза {phase}: бюджет {budget.TotalSeconds:F0} c исчерпан ({attempt} итераций)"));
        }
    }
}
