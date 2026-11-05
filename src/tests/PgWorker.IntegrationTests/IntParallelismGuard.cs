using Shared.TestInfra.Parallelism;

namespace PgWorker.IntegrationTests;

/// <summary>Дроссель параллелизма интеграционных классов (arch/testing/01-e2e-launch.md
/// §5): ЖЁСТКИЙ потолок PrioritySlotGate (Shared.TestInfra) — классы ждут слот
/// до освобождения контура. Дефолт 4 контура (env PGW_INT_MAX_CONTOURS):
/// контур интеграционных легче E2E (etcd/WAF без PG-нод), но часть классов
/// поднимает pg-образы. Захват ПРИОРИТЕТНЫЙ (LPT): вес = число фактов
/// (класс — сам, фикстура — суммарно по её владельцам: тип фикстуры один на
/// группу классов, приоритет между группами честный, внутри — FIFO);
/// освобождающийся слот уходит ждущему с максимальным весом, равные веса —
/// FIFO. Слот держит КЛАСС (InitializeAsync → DisposeAsync): per-fact контуры
/// класса живут под его слотом.</summary>
internal static class IntParallelismGuard
{
    private static readonly PrioritySlotGate Gate = new(ParseMax());

    private static int _live;

    private static int ParseMax()
    {
        var raw = Environment.GetEnvironmentVariable("PGW_INT_MAX_CONTOURS");
        return int.TryParse(raw, out var n) && n >= 1 ? n : 4;
    }

    /// <summary>Захват слота с приоритетом по весу (ждёт освобождения; отмена —
    /// по токену). Парный вызов — <see cref="Release"/> (DisposeAsync класса
    /// либо catch провалившегося подъёма). Захват = «живой контур» класса.</summary>
    public static async ValueTask AcquireAsync(int weight, string slug, CancellationToken ct)
    {
        await Gate.AcquireAsync(weight, ct).ConfigureAwait(false);
        Interlocked.Increment(ref _live);
    }

    /// <summary>Освобождение слота контура: слот уходит ждущему с максимальным
    /// весом либо возвращается в пул.</summary>
    public static void Release(string slug)
    {
        var live = Interlocked.Decrement(ref _live);
        Gate.Release();
        if (live < 0)
            Console.Error.WriteLine($"int[{slug}]: [INT-PARALLELISM] Release without Acquire — slot imbalance");
    }
}

/// <summary>База тест-классов с per-fact docker-контекстом (без контурной
/// класс-фикстуры): слот Guard'а на всю жизнь класса — его контейнеры (пусть
/// и per-fact) живут под потолком контуров. Вес — точный вес класса.</summary>
public abstract class IntSlotLifetime : IAsyncLifetime
{
    private string Slug => GetType().Name;

    public async ValueTask InitializeAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        await IntParallelismGuard.AcquireAsync(
            Shared.TestInfra.Parallelism.TestClassWeight.OfTestClass(GetType()), Slug, ct);
    }

    public ValueTask DisposeAsync()
    {
        IntParallelismGuard.Release(Slug);
        return ValueTask.CompletedTask;
    }
}
