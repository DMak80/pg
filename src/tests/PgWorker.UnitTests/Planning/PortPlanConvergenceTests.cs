using PgWorker.Core.Model;
using PgWorker.Core.Planning;

namespace PgWorker.UnitTests.Planning;

// PortPlanConvergence (spec §3.7 Д1, arch/14 §5 A P1): закрепление, не
// подтверждённое фактом своего живого контейнера и занятое чужим
// (docker-биндинг соседа минус свои ∪ portalloc соседей), снимается —
// PortAllocator выделит ноде свободные порты, EnsureNode создаст контейнер
// в том же тике. object-записи (усыновлённые) не трогаются (R9).

public class PortPlanConvergenceTests
{
    private static NodeAddress Addr(string host, int pg) => new(host, new NodePorts(pg, pg + 3000, pg + 1500));

    [Fact]
    public void DetachColliding_ForeignPgPort_RemovesRecord()
    {
        // Arrange: запись без контейнера; её pg-порт занят чужим docker-фактом.
        var existing = new Dictionary<string, NodeAddress> { ["s1/n1"] = Addr("h1", 15000) };
        var foreign = new HashSet<(string, int)> { ("h1", 15000) };

        // Act
        var changed = PortPlanConvergence.DetachColliding(existing, new HashSet<(string, int)>(), foreign);

        // Assert: коллизионное закрепление снято (недобор → аллокация заново).
        changed.Should().BeTrue();
        existing.Should().NotContainKey("s1/n1");
    }

    [Fact]
    public void DetachColliding_SelfFactRecord_Survives()
    {
        // Arrange: запись подтверждена фактом своего живого контейнера — её порты
        // есть в docker-busy (живая публикация), но это НЕ чужая занятость
        // (spec §8.10: без вычитания selfFact перепланирование сносило бы
        // здоровые закрепления).
        var existing = new Dictionary<string, NodeAddress> { ["s1/n1"] = Addr("h1", 15000) };
        var selfFact = new HashSet<(string, int)> { ("h1", 15000), ("h1", 18000), ("h1", 16500) };
        var foreign = new HashSet<(string, int)> { ("h1", 15000) };

        // Act
        var changed = PortPlanConvergence.DetachColliding(existing, selfFact, foreign);

        // Assert: своя живая нода не перепланируется.
        changed.Should().BeFalse();
        existing.Should().ContainKey("s1/n1");
    }

    [Fact]
    public void DetachColliding_ObjectRecord_Untouched()
    {
        // Arrange: object-запись (усыновлённая) с портом в foreign.
        var existing = new Dictionary<string, NodeAddress>
        {
            ["s1/n1"] = new("h1", new NodePorts(15000, 18000, 16500), Object: "external-1"),
        };

        // Act
        var changed = PortPlanConvergence.DetachColliding(existing, new HashSet<(string, int)>(), new HashSet<(string, int)> { ("h1", 15000) });

        // Assert: чужие контейнеры не трогаем (R9).
        changed.Should().BeFalse();
        existing.Should().ContainKey("s1/n1");
    }

    [Fact]
    public void DetachColliding_PatroniPortCollision_RemovesRecord()
    {
        // Arrange: занят PATRONI-порт (18000) — коллизия по любому из трёх портов.
        var existing = new Dictionary<string, NodeAddress> { ["s1/n1"] = Addr("h1", 15000) };

        // Act
        var changed = PortPlanConvergence.DetachColliding(existing, new HashSet<(string, int)>(), new HashSet<(string, int)> { ("h1", 18000) });

        // Assert
        changed.Should().BeTrue();
        existing.Should().BeEmpty();
    }

    [Fact]
    public void DetachColliding_NoCollisions_NoChanges()
    {
        // Arrange: все записи чисты; занятость host-специфична (чужой хост не мешает).
        var existing = new Dictionary<string, NodeAddress> { ["s1/n1"] = Addr("h1", 15000) };

        // Act
        var changed = PortPlanConvergence.DetachColliding(existing, new HashSet<(string, int)>(), new HashSet<(string, int)> { ("h2", 15000) });

        // Assert
        changed.Should().BeFalse();
        existing.Should().ContainKey("s1/n1");
    }
}
