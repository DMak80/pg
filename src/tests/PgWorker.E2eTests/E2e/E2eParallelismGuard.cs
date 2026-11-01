namespace PgWorker.IntegrationTests.E2e;

/// <summary>Дроссель живых E2E-контуров (docs/e2e-launch.md §5): ЖЁСТКИЙ
/// SemaphoreSlim-потолок (не только предупреждение) — факты-оркестраторы ждут
/// слот до освобождения контура. Дефолт 3 — СТАБИЛЬНЫЙ старт параллельности
/// (5 контуров на 7.7GB docker-VM перегружали демон — каскад таймаутов);
/// настраивается без правок кода: env PGW_E2E_MAX_CONTOURS (5/8 — подъём
/// после наращивания ресурсов). xunit.runner.json maxParallelThreads —
/// потолок ПОТОКОВ фактов, не контуров (5 — выше дросселя намеренно).
/// При превышении дефолта — громкая пометка [E2E-PARALLELISM] (результаты
/// вне стабильного канона требуют пометки).</summary>
internal static class E2eParallelismGuard
{
    private static readonly int MaxLiveEnvironments = ParseMax();
    private static readonly SemaphoreSlim Gate = new(MaxLiveEnvironments, MaxLiveEnvironments);

    private static int _live;

    private static int ParseMax()
    {
        var raw = Environment.GetEnvironmentVariable("PGW_E2E_MAX_CONTOURS");
        return int.TryParse(raw, out var n) && n >= 1 ? n : 3;
    }

    /// <summary>Захват слота контура (ждёт освобождения; отмена — по токену).
    /// Парный вызов — <see cref="Release"/> (в DisposeAsync окружения либо в
    /// catch провалившегося подъёма). Захват = «живой контур»: счётчик
    /// инкрементируется здесь, а не по факту подъёма — иначе Release при
    /// провале подъёма уводит баланс в минус (slot imbalance в каждой серии).</summary>
    public static void Acquire(string slug, CancellationToken ct)
    {
        Gate.Wait(ct);
        Interlocked.Increment(ref _live);
    }

    /// <summary>Освобождение слота контура (DisposeAsync / провал подъёма).</summary>
    public static void Release(string slug)
    {
        var live = Interlocked.Decrement(ref _live);
        Gate.Release();
        if (live < 0)
            Console.Error.WriteLine($"e2e[{slug}]: [E2E-PARALLELISM] Release without Acquire — slot imbalance");
    }
}
