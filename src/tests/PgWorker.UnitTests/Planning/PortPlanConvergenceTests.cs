using PgWorker.Core.Model;
using PgWorker.Core.Planning;

namespace PgWorker.UnitTests.Planning;

// PortPlanConvergence (spec §3.7 Д1, arch/14 §5 A P1): закрепление, не
// подтверждённое фактом СВОЕГО контейнера и занятое любой фактической
// публикацией (docker-биндинг соседа — в т.ч. СВОЕГО кластера — минус свой
// контейнер ∪ portalloc соседей), снимается — PortAllocator выделит ноде
// свободные порты, EnsureNode создаст контейнер в том же тике. object-записи
// (усыновлённые) не трогаются (R9).

public class PortPlanConvergenceTests
{
    private static NodeAddress Addr(string host, int pg) => new(host, new NodePorts(pg, pg + 3000, pg + 1500));

    private static IReadOnlyDictionary<string, IReadOnlySet<(string Host, int Port)>> Facts(
        params (string Key, int Pg, string Host)[] nodes)
    {
        var map = new Dictionary<string, IReadOnlySet<(string, int)>>();
        foreach (var (key, pg, host) in nodes)
            map[key] = new HashSet<(string, int)> { (host, pg), (host, pg + 3000), (host, pg + 1500) };
        return map;
    }

    [Fact]
    public void DetachColliding_ForeignPgPort_RemovesRecord()
    {
        // Arrange: запись без контейнера; её pg-порт занят чужим docker-фактом.
        var existing = new Dictionary<string, NodeAddress> { ["s1/n1"] = Addr("h1", 15000) };
        var busy = new HashSet<(string, int)> { ("h1", 15000) };

        // Act
        var changed = PortPlanConvergence.DetachColliding(existing, Facts(), busy);

        // Assert: коллизионное закрепление снято (недобор → аллокация заново).
        changed.Should().BeTrue();
        existing.Should().NotContainKey("s1/n1");
    }

    [Fact]
    public void DetachColliding_SelfFactRecord_Survives()
    {
        // Arrange: запись подтверждена фактом своего живого контейнера — её порты
        // есть в docker-busy (живая публикация), но это НЕ занятость для неё самой
        // (spec §8.10: без per-node-подтверждения перепланирование сносило бы
        // здоровые закрепления).
        var existing = new Dictionary<string, NodeAddress> { ["s1/n1"] = Addr("h1", 15000) };
        var selfFactByNode = Facts(("s1/n1", 15000, "h1"));
        var busy = new HashSet<(string, int)> { ("h1", 15000), ("h1", 18000), ("h1", 16500) };

        // Act
        var changed = PortPlanConvergence.DetachColliding(existing, selfFactByNode, busy);

        // Assert: своя живая нода не перепланируется.
        changed.Should().BeFalse();
        existing.Should().ContainKey("s1/n1");
    }

    // AAA: Д1/живой-Ф7 — дубликат порта ВНУТРИ своего кластера: контейнер
    // соседней ноды кластера занимает порт так же, как чужой (агрегатный SelfFact
    // вычитал бы его и оставлял битую запись вечной — «already allocated» цикл)
    [Fact]
    public void DetachColliding_DuplicateWithinCluster_RemovesUnconfirmed()
    {
        // Arrange: n1 жива на h1:15000 (факт); n2 — та же тройка портов, контейнера
        // нет (Created-черепок / запись переставлена) — порт занят СВОЕЙ соседкой.
        var existing = new Dictionary<string, NodeAddress>
        {
            ["s1/n1"] = Addr("h1", 15000),
            ["s1/n2"] = Addr("h1", 15000),
        };
        var selfFactByNode = Facts(("s1/n1", 15000, "h1"));
        var busy = new HashSet<(string, int)> { ("h1", 15000), ("h1", 18000), ("h1", 16500) };

        // Act
        var changed = PortPlanConvergence.DetachColliding(existing, selfFactByNode, busy);

        // Assert: подтверждённая n1 жива; неподтверждённый дубликат n2 снят.
        changed.Should().BeTrue();
        existing.Should().ContainKey("s1/n1");
        existing.Should().NotContainKey("s1/n2");
    }

    // AAA: ревью-Ф7 (R1) — EnableDoorman=false: запись и факт без пулера
    // (Doorman=0; нулевой порт в факт не попадает) — запись ПОДТВЕРЖДЕНА,
    // recreate нет (требование всех трёх портов давало вечный detach →
    // бесконечный пересоздание контейнеров в режиме без пулера)
    [Fact]
    public void DetachColliding_DoormanDisabledZeroPort_RecordConfirmed()
    {
        // Arrange: режим R1 — без пулера: запись pg=15000/patroni=18000/doorman=0;
        // факт контейнера — те же два порта (0 в факт не собирается).
        var existing = new Dictionary<string, NodeAddress>
        {
            ["s1/n1"] = new("h1", new NodePorts(15000, 18000, 0)),
        };
        var selfFactByNode = new Dictionary<string, IReadOnlySet<(string, int)>>
        {
            ["s1/n1"] = new HashSet<(string, int)> { ("h1", 15000), ("h1", 18000) },
        };
        var busy = new HashSet<(string, int)> { ("h1", 15000), ("h1", 18000) };

        // Act
        var changed = PortPlanConvergence.DetachColliding(existing, selfFactByNode, busy);

        // Assert: подтверждена фактом двух живых портов — detach нет.
        changed.Should().BeFalse();
        existing.Should().ContainKey("s1/n1");
    }

    [Fact]
    public void DetachColliding_ObjectRecord_Untouched()
    {
        // Arrange: object-запись (усыновлённая) с портом в занятости.
        var existing = new Dictionary<string, NodeAddress>
        {
            ["s1/n1"] = new("h1", new NodePorts(15000, 18000, 16500), Object: "external-1"),
        };

        // Act
        var changed = PortPlanConvergence.DetachColliding(existing, Facts(),
            new HashSet<(string, int)> { ("h1", 15000) });

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
        var changed = PortPlanConvergence.DetachColliding(existing, Facts(),
            new HashSet<(string, int)> { ("h1", 18000) });

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
        var changed = PortPlanConvergence.DetachColliding(existing, Facts(),
            new HashSet<(string, int)> { ("h2", 15000) });

        // Assert
        changed.Should().BeFalse();
        existing.Should().ContainKey("s1/n1");
    }

    // AAA: ConfirmedFact — порты подтверждённых записей: вычитаются из занятости
    // для PortAllocator (иначе переиспользование валидных записей ломалось бы и
    // EnsureNode пересоздавал живые контейнеры, spec §8.10)
    [Fact]
    public void ConfirmedFact_OnlyConfirmedRecordsPorts()
    {
        // Arrange: n1 подтверждена фактом (h1:15000), n2 — нет.
        var existing = new Dictionary<string, NodeAddress>
        {
            ["s1/n1"] = Addr("h1", 15000),
            ["s1/n2"] = Addr("h2", 15001),
        };
        var selfFactByNode = Facts(("s1/n1", 15000, "h1"));

        // Act
        var confirmed = PortPlanConvergence.ConfirmedFact(existing, selfFactByNode);

        // Assert: только тройка портов живой n1 — не вся docker-занятость.
        confirmed.Should().BeEquivalentTo(new[] { ("h1", 15000), ("h1", 18000), ("h1", 16500) });
    }
}
