namespace PgWorker.IntegrationTests.E2e;

using Shared.TestInfra.Parallelism;

/// <summary>Дроссель живых E2E-контуров (arch/testing/01-e2e-launch.md §5): ЖЁСТКИЙ
/// потолок PrioritySlotGate (Shared.TestInfra) — факты-оркестраторы ждут слот
/// до освобождения контура. Дефолт 3 — СТАБИЛЬНЫЙ старт параллельности
/// (5 контуров на 7.7GB docker-VM перегружали демон — каскад таймаутов);
/// настраивается без правок кода: env PGW_E2E_MAX_CONTOURS (5/8 — подъём
/// после наращивания ресурсов). xunit.runner.json maxParallelThreads —
/// потолок ПОТОКОВ фактов, не контуров.
/// Захват ПРИОРИТЕТНЫЙ (LPT): вес класса = число фактов ([Fact]+[Theory])
/// его тестовых классов; освобождающийся слот уходит ждущему с максимальным
/// весом (тяжёлые классы отрабатывают раньше — не вытягивают хвост прогона),
/// равные веса — FIFO. Приоритет — только среди ждущих слот. При превышении
/// дефолта — громкая пометка [E2E-PARALLELISM] (результаты вне стабильного
/// канона требуют пометки).</summary>
internal static class E2eParallelismGuard
{
    private static readonly PrioritySlotGate Gate = new(ParseMax());

    private static int _live;

    private static int ParseMax()
    {
        var raw = Environment.GetEnvironmentVariable("PGW_E2E_MAX_CONTOURS");
        return int.TryParse(raw, out var n) && n >= 1 ? n : 3;
    }

    /// <summary>Вес класса-фикстуры (делегат Shared.TestInfra.Parallelism).</summary>
    public static int WeightOf(Type fixtureType) => TestClassWeight.OfFixture(fixtureType);

    /// <summary>Захват слота контура с приоритетом по весу класса-фикстуры
    /// (ждёт освобождения; отмена — по токену). Парный вызов —
    /// <see cref="Release"/> (в DisposeAsync окружения либо в catch
    /// провалившегося подъёма). Захват = «живой контур»: счётчик
    /// инкрементируется здесь, а не по факту подъёма — иначе Release при
    /// провале подъёма уводит баланс в минус (slot imbalance в каждой серии).</summary>
    public static async ValueTask AcquireAsync(Type fixtureType, string slug, CancellationToken ct)
    {
        await Gate.AcquireAsync(WeightOf(fixtureType), ct).ConfigureAwait(false);
        Interlocked.Increment(ref _live);
    }

    /// <summary>Освобождение слота контура (DisposeAsync / провал подъёма):
    /// слот уходит ждущему с максимальным весом либо возвращается в пул.</summary>
    public static void Release(string slug)
    {
        var live = Interlocked.Decrement(ref _live);
        Gate.Release();
        if (live < 0)
            Console.Error.WriteLine($"e2e[{slug}]: [E2E-PARALLELISM] Release without Acquire — slot imbalance");
    }
}
