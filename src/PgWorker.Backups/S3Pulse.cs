using PgWorker.Core;
using Shared.Core.Hosting;

namespace PgWorker.Backups;

/// <summary>«Пульс конечного внешнего вызова» (arch/14 §6, arch/19 §5):
/// одиночный S3-вызов не поллингуется (идемпотентный повтор внутри фазы
/// бессмысленен) — его конечность гарантирует пер-попыточный таймаут клиента
/// (BackupS3: AmazonS3Config.Timeout, полокна watchdog), а на время вызова
/// цикл держит пульс активности по расписанию (период — полокна проверки,
/// инъектируется вызывающими/тестами; дефолт 10 с при дефолтных опциях).
/// Отметка-по-расписанию НЕ маскирует зависание: вызов умирает таймаутом →
/// transient-фail тика → повтор следующим тиком/проходом/retry; пульс живёт
/// ровно вокруг одного вызова (слепое окно = одна логическая S3-операция).</summary>
public static class S3Pulse
{
    /// <summary>Дефолт периода: полокна проверки watchdog при дефолтных
    /// опциях (CheckIntervalSec=15); вызывающие с известным окном передают
    /// производный период явно.</summary>
    public static readonly TimeSpan DefaultPeriod = TimeSpan.FromSeconds(10);

    public static async Task<Result<T>> CallAsync<T>(
        ILoopProgress? progress, Func<CancellationToken, Task<Result<T>>> call, CancellationToken ct,
        TimeSpan? period = null)
    {
        using var pulse = new Timer(_ => progress?.Mark(), null, TimeSpan.Zero, period ?? DefaultPeriod);
        return await call(ct);
    }

    // Негенерик-перегрузка: вызовы без значения (DeleteKeysAsync → Result).
    public static async Task<Result> CallAsync(
        ILoopProgress? progress, Func<CancellationToken, Task<Result>> call, CancellationToken ct,
        TimeSpan? period = null)
    {
        using var pulse = new Timer(_ => progress?.Mark(), null, TimeSpan.Zero, period ?? DefaultPeriod);
        return await call(ct);
    }
}
