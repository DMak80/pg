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
        var pulse = new Pulse(progress, period ?? DefaultPeriod);
        try
        {
            return await call(ct);
        }
        finally
        {
            await pulse.StopAsync();
        }
    }

    // Негенерик-перегрузка: вызовы без значения (DeleteKeysAsync → Result).
    public static async Task<Result> CallAsync(
        ILoopProgress? progress, Func<CancellationToken, Task<Result>> call, CancellationToken ct,
        TimeSpan? period = null)
    {
        var pulse = new Pulse(progress, period ?? DefaultPeriod);
        try
        {
            return await call(ct);
        }
        finally
        {
            await pulse.StopAsync();
        }
    }

    // Пульс одного вызова: Timer + остановка с ожиданием уже начатого
    // колбэка (Timer.Dispose его не ждёт — иначе отметка могла бы прийти
    // ПОСЛЕ возврата вызова: «вечного» пульса быть не должно).
    private sealed class Pulse
    {
        private readonly Timer _timer;
        private readonly ILoopProgress? _progress;
        private volatile bool _stopped;
        private int _inCallback;

        public Pulse(ILoopProgress? progress, TimeSpan period)
        {
            _progress = progress;
            _timer = new Timer(_ => Tick(), null, TimeSpan.Zero, period);
        }

        private void Tick()
        {
            if (_stopped)
                return;
            Interlocked.Increment(ref _inCallback);
            try
            {
                if (!_stopped)
                    _progress?.Mark();
            }
            finally
            {
                Interlocked.Decrement(ref _inCallback);
            }
        }

        public async Task StopAsync()
        {
            _stopped = true;
            await _timer.DisposeAsync();
            // Короткое ожидание уже начатого колбэка: он не должен Mark'нуть
            // после возврата из CallAsync.
            var deadline = Environment.TickCount64 + 1_000;
            while (Volatile.Read(ref _inCallback) > 0 && Environment.TickCount64 < deadline)
                await Task.Delay(1);
        }
    }
}
