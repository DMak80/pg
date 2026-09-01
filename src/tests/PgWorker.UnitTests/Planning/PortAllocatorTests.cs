using PgWorker.Core.Model;
using PgWorker.Core.Planning;

namespace PgWorker.UnitTests.Planning;

// PortAllocator: выделение и закрепление троек портов нод (spec §6.3, Д5):
// pg=base, doorman=base+1500, patroni=base+3000.

public class PortAllocatorTests
{
    private static readonly IReadOnlyList<NodePlacement> TwoNodes =
    [
        new("shard1", "shard1a", "h1"),
        new("shard1", "shard1b", "h1"),
    ];

    [Fact]
    public void Allocate_PinnedAddress_IsReused()
    {
        // Arrange: за нодой уже закреплён адрес в portalloc.
        var plan = new PlacementPlan([new("shard1", "shard1a", "h1")]);
        var existing = new Dictionary<string, NodeAddress>
        {
            ["shard1/shard1a"] = new("h1", new NodePorts(15123, 18123, 16623)),
        };

        // Act: аллокация с пустой занятостью.
        var result = PortAllocator.Allocate(plan, existing,
            new HashSet<(string, int)>(), 15000, 16000);

        // Assert: закреплённый адрес переиспользован без изменений.
        result.IsSuccess.Should().BeTrue();
        result.Value["shard1/shard1a"].Should().Be(existing["shard1/shard1a"]);
    }

    [Fact]
    public void Allocate_NewNode_GetsFirstFreeBase()
    {
        // Arrange: нод без закрепления, порты свободны.
        var plan = new PlacementPlan([new("shard1", "shard1a", "h1")]);

        // Act: аллокация в диапазоне от 15000.
        var result = PortAllocator.Allocate(plan,
            new Dictionary<string, NodeAddress>(),
            new HashSet<(string, int)>(), 15000, 16000);

        // Assert: базовый порт — первый свободный (15000).
        result.Value["shard1/shard1a"].Ports.Pg.Should().Be(15000);
    }

    [Fact]
    public void Allocate_BusyConflict_ShiftsToNextBase()
    {
        // Arrange: чужой контейнер занял (h1, 15000) — база 15000 недоступна.
        var plan = new PlacementPlan(TwoNodes);
        var busy = new HashSet<(string, int)> { ("h1", 15000) };

        // Act: аллокация с занятым портом.
        var result = PortAllocator.Allocate(plan,
            new Dictionary<string, NodeAddress>(), busy, 15000, 16000);

        // Assert: первая нода сдвинулась на base 15001, вторая — на 15002.
        result.Value["shard1/shard1a"].Ports.Pg.Should().Be(15001);
        result.Value["shard1/shard1b"].Ports.Pg.Should().Be(15002);
    }

    [Fact]
    public void Allocate_RangeExhausted_ReturnsFailed()
    {
        // Arrange: диапазон из одного base, и тот занят.
        var plan = new PlacementPlan([new("shard1", "shard1a", "h1")]);
        var busy = new HashSet<(string, int)> { ("h1", 15000) };

        // Act: аллокация в исчерпанном диапазоне [15000, 15001).
        var result = PortAllocator.Allocate(plan,
            new Dictionary<string, NodeAddress>(), busy, 15000, 15001);

        // Assert: свободной тройки нет — Result.Failed.
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().NotBeNull();
    }

    [Fact]
    public void Allocate_Offsets_AreBaseDoormanPatroni()
    {
        // Arrange: чистый хост, base 15000.
        var plan = new PlacementPlan([new("shard1", "shard1a", "h1")]);

        // Act: аллокация тройки портов.
        var result = PortAllocator.Allocate(plan,
            new Dictionary<string, NodeAddress>(),
            new HashSet<(string, int)>(), 15000, 16000);

        // Assert: смещения по spec §6.3 — pg=base, doorman=base+1500, patroni=base+3000.
        var addr = result.Value["shard1/shard1a"];
        addr.Host.Should().Be("h1");
        addr.Ports.Pg.Should().Be(15000);
        addr.Ports.Doorman.Should().Be(16500);
        addr.Ports.Patroni.Should().Be(18000);
    }

    // AAA: дубль-страховка контракта C (spec §3.3/§6): busy-union, переданный
    // вызывателем, содержит закрепления соседей — аллокатор обязан их обходить
    [Fact]
    public void Allocate_PinnedPortInBusyWithoutExisting_AllocatesNext()
    {
        // Arrange: busy-union (docker ∪ portalloc соседей) занял 15000-тройку; existing пуст.
        var plan = new PlacementPlan([new("shard1", "shard1a", "h1")]);
        var busy = new HashSet<(string, int)> { ("h1", 15000), ("h1", 18000), ("h1", 16500) };

        // Act
        var result = PortAllocator.Allocate(plan, new Dictionary<string, NodeAddress>(), busy, 15000, 16000);

        // Assert: база сдвинута — соседская тройка не переиспользуется.
        result.Value["shard1/shard1a"].Ports.Pg.Should().Be(15001);
    }
}
