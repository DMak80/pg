using FluentAssertions;
using ValkeyWorker.Core.Writing;
using Xunit;

namespace ValkeyWorker.UnitTests.Writing;

// Каноническая запись домена (arch/20 §2.1; pg §9.3): единое место сборки
// значений ключей — тесты фиксируют дословные строки.
public class ValkeyWritingTests
{
    [Fact]
    public void ConfigJson_ДословноКанонActiveБезState()
    {
        // Arrange: канонический Active-вид (arch/20 §2.1).
        // Act
        var json = ValkeyWriting.ConfigJson(1, 536870912, "allkeys-lru", 1756500000);

        // Assert: дословно каноническая строка (snake_case, без пробелов, без state).
        json.Should().Be(
            """{"nodes":1,"maxmemory_bytes":536870912,"maxmemory_policy":"allkeys-lru","created_unix":1756500000}""");
    }

    [Fact]
    public void ConfigJson_КредыНеПопадают()
    {
        // Arrange: пароли с кавычкоопасными символами исключены генератором,
        // но инвариант проверяем: сигнатура ConfigJson креды не принимает.
        // Act
        var json = ValkeyWriting.ConfigJson(1, 1, "noeviction", 1);

        // Assert: в JSON конфига нет ни app_, ни admin_, ни password.
        json.Should().NotContain("password");
        json.Should().NotContain("app_user");
        json.Should().NotContain("admin_user");
    }

    [Fact]
    public void EndpointsValue_ФорматHostPort()
    {
        // Arrange: advertised-хост + клиентский порт из portalloc.
        // Act
        var value = ValkeyWriting.EndpointsValue("host.docker.internal", 17001);

        // Assert: "<host>:<port>".
        value.Should().Be("host.docker.internal:17001");
    }

    [Fact]
    public void ResourcesJson_КанонPg93()
    {
        // Arrange: заявка ресурсов (cpu decimal-строкой, mem/disk только Gi).
        // Act
        var json = ValkeyWriting.ResourcesJson(2m, 4, 40);

        // Assert: канон pg §9.3.
        json.Should().Be("""{"cpu":"2","mem":"4Gi","disk":"40Gi"}""");
    }

    [Fact]
    public void ResourcesJson_ДробныйCpu_ИнвариантнаяКультура()
    {
        // Arrange: cpu 0.5 (точка-разделитель в любой культуре).
        // Act
        var json = ValkeyWriting.ResourcesJson(0.5m, 1, 10);

        // Assert: без локализованной запятой.
        json.Should().Be("""{"cpu":"0.5","mem":"1Gi","disk":"10Gi"}""");
    }

    [Fact]
    public void KnownPolicies_ВосемьЗначенийКанона()
    {
        // Arrange/Act: набор политик канона.
        var policies = ValkeyWriting.KnownPolicies;

        // Assert: ровно 8 значений arch/20 §2.
        policies.Should().HaveCount(8);
        policies.Should().Contain(["allkeys-lru", "allkeys-lfu", "volatile-lru", "volatile-lfu",
            "allkeys-random", "volatile-random", "volatile-ttl", "noeviction"]);
    }

    [Fact]
    public void CanonicalCpu_ФорматБезЛишнихНулей()
    {
        // Arrange: целое и дробное значения.
        // Act/Assert: "2" и "0.5" (строки-каноны etcd).
        ValkeyWriting.CanonicalCpu(2m).Should().Be("2");
        ValkeyWriting.CanonicalCpu(0.5m).Should().Be("0.5");
    }
}
