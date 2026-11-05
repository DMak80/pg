namespace PgWorker.IntegrationTests.E2e;

/// <summary>Приоритетный асинхронный семафор-слот (arch/testing/01-e2e-launch.md §5,
/// LPT): освобождающийся слот уходит ждущему с МАКСИМАЛЬНЫМ весом; равные
/// веса — FIFO (наименьший seq выходит первым). Быстрый путь (свободный
/// слот) — синхронный, без очереди. Отмена ждущего атомарна с передачей
/// слота: TrySetCanceled/TrySetResult выигрывает ровно один — отменённый
/// waiter слота не получает, Release его пропускает (слот не теряется и
/// не дублируется). Continuations — асинхронные: Release пробуждает ждущего
/// НЕ под локом (иначе continuation выполнялся бы в потоке Release,
/// удерживающем лок, — дедлок).</summary>
internal sealed class E2ePrioritySlotGate(int capacity)
{
    private readonly object _sync = new();

    private readonly PriorityQueue<TaskCompletionSource, (int NegWeight, long Seq)> _waiters = new();

    private int _available = capacity;

    private long _seq;

    /// <summary>Число ждущих слот (диагностика/тесты инвариантов очереди).</summary>
    public int WaitersCount
    {
        get
        {
            lock (_sync)
                return _waiters.Count;
        }
    }

    /// <summary>Захват слота с весом: свободный слот — сразу; иначе ожидание в
    /// приоритетной очереди (больший вес — раньше). Отмена по токену уводит
    /// из очереди без слота (OperationCanceledException наружу).</summary>
    public async ValueTask AcquireAsync(int weight, CancellationToken ct)
    {
        TaskCompletionSource? pending = null;
        lock (_sync)
        {
            if (_available > 0)
            {
                _available--;
                return; // быстрый путь: синхронное завершение ValueTask
            }
            pending = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _waiters.Enqueue(pending, (-weight, _seq++));
        }

        // Отмена и передача слота гоняются атомарно: ровно один TrySet*
        // выигрывает. Выиграла отмена → слот не наш (Release передал его
        // следующему по TrySetResult == false у нашего waiter'а); наружу —
        // OCE с ИСХОДНЫМ токеном (WaitAsync-семантика).
        using (ct.Register(static s =>
        {
            var (tcs, token) = ((TaskCompletionSource, CancellationToken))s!;
            tcs.TrySetCanceled(token);
        }, (pending, ct)))
            await pending.Task.ConfigureAwait(false);
    }

    /// <summary>Освобождение слота: при пустой очереди возвращается в пул
    /// (_available), иначе передаётся напрямую живому ждущему с максимальным
    /// весом (отменённые пропускаются — их TrySetResult == false).</summary>
    public void Release()
    {
        lock (_sync)
        {
            while (_waiters.TryDequeue(out var w, out _))
                if (w.TrySetResult())
                    return; // слот передан напрямую
            _available++;
        }
    }
}
