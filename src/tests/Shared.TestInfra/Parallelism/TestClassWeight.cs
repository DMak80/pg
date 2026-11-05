using System.Collections.Concurrent;
using System.Reflection;
using Xunit;

namespace Shared.TestInfra.Parallelism;

/// <summary>Вес тест-класса для приоритетного захвата слота параллелизма
/// (arch/testing/01-e2e-launch.md §5, LPT): вес = суммарное число фактов
/// ([Fact]+[Theory]; Theory считается одним фактом) тестовых классов
/// фикстуры (для класс-фикстуры) либо самого класса. Кэш по типу.</summary>
public static class TestClassWeight
{
    private static readonly ConcurrentDictionary<Type, int> Cache = new();

    /// <summary>Вес класс-фикстуры: суммарное число фактов всех тестовых
    /// классов, привязанных к ней через IClassFixture&lt;&gt; (унаследованные
    /// факты абстрактных сценариев считаются — GetMethods их видит).
    /// Фикстура без тестовых классов — вес 0.</summary>
    public static int OfFixture(Type fixtureType)
        => Cache.GetOrAdd(fixtureType, static t =>
        {
            var marker = typeof(IClassFixture<>).MakeGenericType(t);
            return t.Assembly.GetTypes()
                .Where(test => test != t && marker.IsAssignableFrom(test))
                .Sum(CountFacts);
        });

    /// <summary>Вес самого тест-класса (окружение поднимает сам класс,
    /// без отдельной фикстуры): число его фактов, включая унаследованные.</summary>
    public static int OfTestClass(Type testType)
        => Cache.GetOrAdd(testType, static t => CountFacts(t));

    private static int CountFacts(Type testType)
        => testType.GetMethods()
            .Count(m => m.IsDefined(typeof(FactAttribute), inherit: false)); // Theory : Fact
}
