namespace Shared.Core.UnitTests.Planning;

// PortAllocator: выделение и закрепление адресов нод (t09, обобщение Pg/Kfw;
// Pg-кейсы перенесены из PgWorker.UnitTests — generic-инстанс с локальной
// моделью тройки портов pg/patroni/doorman, ключ результата "shard/node").

public class PortAllocatorTests
{
    // Локальная Pg-модель адреса вместо доменной (Shared домена не знает).
    private sealed record PgAddr(string Host, int Pg, int Patroni, int Doorman);

    private static IReadOnlyList<int> PortsOf(PgAddr a) => [a.Pg, a.Patroni, a.Doorman];
    private static string HostOf(PgAddr a) => a.Host;
    private static PgAddr MakeAddress(string host, int basePort) => new(host, basePort, basePort + 3000, basePort + 1500);
    private static string KeyOf(NodePlacement p) => $"{p.Group}/{p.Node}";

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
        var existing = new Dictionary<string, PgAddr>
        {
            ["shard1/shard1a"] = new("h1", 15123, 18123, 16623),
        };

        // Act: аллокация с пустой занятостью.
        var result = PortAllocator.Allocate(plan, existing,
            new HashSet<(string, int)>(), 15000, 16000,
            PortsOf, HostOf, MakeAddress, KeyOf);

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
            new Dictionary<string, PgAddr>(),
            new HashSet<(string, int)>(), 15000, 16000,
            PortsOf, HostOf, MakeAddress, KeyOf);

        // Assert: базовый порт — первый свободный (15000).
        result.Value["shard1/shard1a"].Pg.Should().Be(15000);
    }

    [Fact]
    public void Allocate_BusyConflict_ShiftsToNextBase()
    {
        // Arrange: чужой контейнер занял (h1, 15000) — база 15000 недоступна.
        var plan = new PlacementPlan(TwoNodes);
        var busy = new HashSet<(string, int)> { ("h1", 15000) };

        // Act: аллокация с занятым портом.
        var result = PortAllocator.Allocate(plan,
            new Dictionary<string, PgAddr>(), busy, 15000, 16000,
            PortsOf, HostOf, MakeAddress, KeyOf);

        // Assert: первая нода сдвинулась на base 15001, вторая — на 15002.
        result.Value["shard1/shard1a"].Pg.Should().Be(15001);
        result.Value["shard1/shard1b"].Pg.Should().Be(15002);
    }

    [Fact]
    public void Allocate_RangeExhausted_ReturnsFailed()
    {
        // Arrange: диапазон из одного base, и тот занят.
        var plan = new PlacementPlan([new("shard1", "shard1a", "h1")]);
        var busy = new HashSet<(string, int)> { ("h1", 15000) };

        // Act: аллокация в исчерпанном диапазоне [15000, 15001).
        var result = PortAllocator.Allocate(plan,
            new Dictionary<string, PgAddr>(), busy, 15000, 15001,
            PortsOf, HostOf, MakeAddress, KeyOf);

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
            new Dictionary<string, PgAddr>(),
            new HashSet<(string, int)>(), 15000, 16000,
            PortsOf, HostOf, MakeAddress, KeyOf);

        // Assert: смещения по spec §6.3 — pg=base, doorman=base+1500, patroni=base+3000.
        var addr = result.Value["shard1/shard1a"];
        addr.Host.Should().Be("h1");
        addr.Pg.Should().Be(15000);
        addr.Doorman.Should().Be(16500);
        addr.Patroni.Should().Be(18000);
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
        var result = PortAllocator.Allocate(plan, new Dictionary<string, PgAddr>(), busy, 15000, 16000,
            PortsOf, HostOf, MakeAddress, KeyOf);

        // Assert: база сдвинута — соседская тройка не переиспользуется.
        result.Value["shard1/shard1a"].Pg.Should().Be(15001);
    }

    // --- Зеркальные Kfw-кейсы (generic-инстанс с одним портом) ---

    private sealed record KfwAddr(string Host, int Port); // локальная модель вместо доменной

    [Fact]
    public void Allocate_SinglePort_NewNode_GetsFirstFree()
    {
        // Arrange: одна нода, один client-порт (Kfw-инстанс)
        var plan = new PlacementPlan([new NodePlacement("c1", "b1", "h1")]);

        // Act
        var result = PortAllocator.Allocate(plan, new Dictionary<string, KfwAddr>(),
            new HashSet<(string, int)>(), 16000, 17000,
            a => [a.Port], a => a.Host, (h, p) => new KfwAddr(h, p), p => p.Node);

        // Assert: первый свободный порт диапазона; ключ — имя ноды
        result.Value["b1"].Port.Should().Be(16000);
    }

    [Fact]
    public void Allocate_SinglePort_PinnedReused_AndRangeExhausted_Fails()
    {
        // Arrange: закрепление b1→(h1,16000) переиспользуется; диапазон [16000,16001)
        var plan = new PlacementPlan([
            new NodePlacement("c1", "b1", "h1"),
            new NodePlacement("c1", "b2", "h1")]);
        var existing = new Dictionary<string, KfwAddr> { ["b1"] = new("h1", 16000) };

        // Act
        var result = PortAllocator.Allocate(plan, existing,
            new HashSet<(string, int)>(), 16000, 16001,
            a => [a.Port], a => a.Host, (h, p) => new KfwAddr(h, p), p => p.Node);

        // Assert: pinned без изменений; b2 — свободного порта нет
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().NotBeNull();
    }
}
