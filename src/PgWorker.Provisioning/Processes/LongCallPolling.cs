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
/// ФАКТУ + лог elapsed («сколько фаза уже занимает»). Семантика отмен
/// (spec §1.2 п.4): отмена по итерационному таймауту — «итерация не уложилась»
/// → следующая итерация до исчерпания бюджета (брошенная вызовом OCE ИЛИ
/// проглоченная им в Result.Failed — драйверы конвертируют отмену в Failed);
/// ошибка вызова неотменного характера (валидация/SQL-логика) — наверх без
/// повторов (следующий тик продолжит: поллинг ловит молчание, не сбои);
/// внешний ct.IsCancellationRequested — проброс OCE наверх. Исчерпание
/// бюджета — ошибка бюджета наверх как есть (обработка процесса прежняя).
/// Отметка «в обмен на ничто» запрещена: Mark только у исполненной
/// итерации.</summary>
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
            Result result;
            try
            {
                result = await call(attemptCts.Token);
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested && attemptCts.IsCancellationRequested)
            {
                // итерация не уложилась в таймаут (отмена проброшена вызовом) —
                // факт итерации: отметка + elapsed, повтор
                TimeoutIteration(phase, progress, logger, started, attempt, singleCallTimeout);
                if (Stopwatch.GetElapsedTime(started) >= budget)
                    return BudgetExhausted(phase, budget, attempt);
                continue;
            }

            progress?.Mark();
            if (result.IsSuccess)
            {
                // elapsed-лог на КАЖДОЙ итерации, включая завершающую успешную
                // (буква приказа п.4: «каждая итерация поллинга даёт отметку
                // активности И пишет в лог elapsed»)
                logger.LogInformation(
                    "provisioning: фаза {Phase}: уже {Elapsed:F0} c (итерация {Attempt}: успех)",
                    phase, Stopwatch.GetElapsedTime(started).TotalSeconds, attempt);
                return result;
            }

            // Отмена, проглоченная вызовом (драйверы/SQL конвертируют OCE в
            // Result.Failed — разбор E2E-маркера «SQL-скаляр не выполнен …
            // The operation was canceled»): внешний ct — проброс наверх;
            // итерационный таймаут — «не уложилась», повтор до бюджета.
            if (result.Error is { } error)
            {
                if (ct.IsCancellationRequested)
                    throw new OperationCanceledException(
                        $"provisioning: фаза {phase}: внешняя отмена", error, ct);
                if (attemptCts.IsCancellationRequested && IsCancellation(error))
                {
                    TimeoutIteration(phase, progress, logger, started, attempt, singleCallTimeout);
                    if (Stopwatch.GetElapsedTime(started) >= budget)
                        return BudgetExhausted(phase, budget, attempt);
                    continue;
                }
            }

            // Ошибка вызова неотменного характера — наверх без повторов
            // (следующий тик продолжит; поллинг ловит молчание, не сбои).
            logger.LogInformation(
                "provisioning: фаза {Phase}: уже {Elapsed:F0} c (итерация {Attempt}: ошибка: {Error})",
                phase, Stopwatch.GetElapsedTime(started).TotalSeconds, attempt, result.Error?.Message);
            return result;
        }
    }

    // Факт итерации, не уложившейся в таймаут: отметка ПО ФАКТУ + elapsed-лог.
    private static void TimeoutIteration(
        string phase, ILoopProgress? progress, ILogger logger,
        long started, int attempt, TimeSpan singleCallTimeout)
    {
        progress?.Mark();
        logger.LogInformation(
            "provisioning: фаза {Phase}: уже {Elapsed:F0} c — итерация {Attempt} отменена по таймауту ({Timeout:F0} c), повтор",
            phase, Stopwatch.GetElapsedTime(started).TotalSeconds, attempt, singleCallTimeout.TotalSeconds);
    }

    private static Result BudgetExhausted(string phase, TimeSpan budget, int attempt)
        => Result.Failed(new ApplicationException(
            $"фаза {phase}: бюджет {budget.TotalSeconds:F0} c исчерпан ({attempt} итераций)"));

    // Отмена в цепочке ошибки (само исключение или любое InnerException):
    // вызов-обёртка конвертирует OCE в Result.Failed («The operation was
    // canceled») — отделяем её от ошибок валидации/SQL-логики.
    private static bool IsCancellation(Exception error)
    {
        for (var e = (Exception?)error; e is not null; e = e.InnerException)
            if (e is OperationCanceledException)
                return true;
        return false;
    }
}
