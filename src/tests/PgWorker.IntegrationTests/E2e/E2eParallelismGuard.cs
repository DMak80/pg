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
    private static int _warned;

    private static int ParseMax()
    {
        var raw = Environment.GetEnvironmentVariable("PGW_E2E_MAX_CONTOURS");
        return int.TryParse(raw, out var n) && n >= 1 ? n : 3;
    }

    /// <summary>Захват слота контура (ждёт освобождения; отмена — по токену).
    /// Парный вызов — <see cref="Release"/> (в DisposeAsync окружения либо в
    /// catch провалившегося подъёма).</summary>
    public static void Acquire(string slug, CancellationToken ct)
        => Gate.Wait(ct);

    /// <summary>Освобождение слота контура (DisposeAsync / провал подъёма).</summary>
    public static void Release(string slug)
    {
        var live = Interlocked.Decrement(ref _live);
        Gate.Release();
        if (live < 0)
            Console.Error.WriteLine($"e2e[{slug}]: [E2E-PARALLELISM] Release without Acquire — slot imbalance");
    }

    /// <summary>Регистрация живого контура (после успешного подъёма):
    /// при превышении дефолта — громкая пометка вне стабильного канона.</summary>
    public static void OnEnvironmentStarted(string slug)
    {
        var live = Interlocked.Increment(ref _live);
        if (live > MaxLiveEnvironments && Interlocked.Exchange(ref _warned, 1) == 0)
        {
            var line = $"{DateTime.UtcNow:HH:mm:ss} [E2E-PARALLELISM] e2e[{slug}]: live E2E contours {live} > canon"
                + $" {MaxLiveEnvironments} (PGW_E2E_MAX_CONTOURS / docs/e2e-launch.md §5)"
                + " — the run is outside the stable canon: the results require this marking.";
            Console.Error.WriteLine(line);
            try
            {
                File.AppendAllText("/tmp/pgw-e2e-static-phase.log", line + Environment.NewLine);
            }
            catch
            {
                // файловая телеметрия — «лучшими усилиями», не роняет серию
            }
        }
    }

    /// <summary>Освобождение контура (DisposeAsync окружения, host-ветка).</summary>
    public static void OnEnvironmentDisposed() => Release(string.Empty);
}
