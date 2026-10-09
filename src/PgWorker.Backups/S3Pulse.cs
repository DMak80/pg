using PgWorker.Core;
using Shared.Core.Hosting;

namespace PgWorker.Backups;

/// <summary>S3-вызов с пульсом прогресса (канон t19 — «итерация жива»): прямой
/// вызов на недоступном endpoint висит до HttpClient-таймаута (100 c) без
/// отметок — watchdog (порт сноса 2×CheckIntervalSec = 30 c) гасит цикл
/// посреди операции. Пульс Mark каждые 10 с (полокна проверки) держит
/// активность; transient-отказ возвращает Result.Failed — следующий тик
/// повторит (семантика вызова не меняется). Вызов обязан оставаться
/// идемпотентным к отмене: отмена итерации не вводится — пульс не влияет на
/// таймауты вызова (их несут процессы/CTS поверх).</summary>
public static class S3Pulse
{
    /// <summary>Период пульса: активность не реже полокна проверки watchdog.</summary>
    public static readonly TimeSpan Period = TimeSpan.FromSeconds(10);

    public static async Task<Result<T>> CallAsync<T>(
        ILoopProgress? progress, Func<CancellationToken, Task<Result<T>>> call, CancellationToken ct)
    {
        using var pulse = new Timer(_ => progress?.Mark(), null, TimeSpan.Zero, Period);
        return await call(ct);
    }

    // Негенерик-перегрузка: вызовы без значения (DeleteKeysAsync → Result).
    public static async Task<Result> CallAsync(
        ILoopProgress? progress, Func<CancellationToken, Task<Result>> call, CancellationToken ct)
    {
        using var pulse = new Timer(_ => progress?.Mark(), null, TimeSpan.Zero, Period);
        return await call(ct);
    }
}
