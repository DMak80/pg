namespace Shared.Core.UnitTests.Planning;

// PlacementPlanner: анти-аффинити нод группы по docker-хостам (t09, обобщение
// Pg/Kfw; Pg-кейсы перенесены из PgWorker.UnitTests, вход — NodeGroup).

public class PlacementPlannerTests
{
    // Pg-инстанс входа: группа = шард, имена нод shard1a/shard1b/...
    private static NodeGroup Group(string name, int replicas) =>
        new(name, Enumerable.Range(0, replicas).Select(i => $"{name}{(char)('a' + i)}").ToList());

    [Fact]
    public void Plan_HostsEqualsReplicas_NodesOnDistinctHosts()
    {
        // Arrange: 3 хоста, группа из 3 нод — топология позволяет полный разброс.
        var hosts = new List<HostInfo> { new("h1", 0), new("h2", 0), new("h3", 0) };
        var groups = new List<NodeGroup> { Group("shard1", 3) };

        // Act: строим план размещения.
        var plan = PlacementPlanner.Plan(groups, hosts);

        // Assert: все ноды группы — на разных хостах (анти-аффинити).
        plan.Nodes.Select(n => n.Host).Should().OnlyHaveUniqueItems();
        plan.Nodes.Should().HaveCount(3);
    }

    [Fact]
    public void Plan_SingleHost_AllNodesOnIt()
    {
        // Arrange: 1 хост, группа из 2 нод — равномерность невозможна.
        var hosts = new List<HostInfo> { new("h1", 0) };
        var groups = new List<NodeGroup> { Group("shard1", 2) };

        // Act: строим план размещения.
        var plan = PlacementPlanner.Plan(groups, hosts);

        // Assert: обе ноды на единственном хосте.
        plan.Nodes.Should().OnlyContain(n => n.Host == "h1");
    }

    [Fact]
    public void Plan_HostsFewerThanReplicas_MinimalRepeats()
    {
        // Arrange: 2 хоста, группа из 3 нод — разброс 2+1 (минимум повторов).
        var hosts = new List<HostInfo> { new("h1", 0), new("h2", 0) };
        var groups = new List<NodeGroup> { Group("shard1", 3) };

        // Act: строим план размещения.
        var plan = PlacementPlanner.Plan(groups, hosts);

        // Assert: распределение 2+1, никакой хост не держит больше 2 нод.
        plan.Nodes.GroupBy(n => n.Host).Should().HaveCount(2);
        plan.Nodes.GroupBy(n => n.Host).Should().OnlyContain(g => g.Count() <= 2);
    }

    [Fact]
    public void Plan_UsedSlots_PreferredHostIsLeastLoaded()
    {
        // Arrange: h1 перегружен (5 занятых слотов), h2 свободен.
        var hosts = new List<HostInfo> { new("h1", 5), new("h2", 0) };
        var groups = new List<NodeGroup> { Group("shard1", 2) };

        // Act: строим план размещения.
        var plan = PlacementPlanner.Plan(groups, hosts);

        // Assert: первая нода уходит на свободный h2, вторая — на h1.
        plan.Nodes.Should().Contain(n => n.Node == "shard1a" && n.Host == "h2");
        plan.Nodes.Should().Contain(n => n.Node == "shard1b" && n.Host == "h1");
    }

    [Fact]
    public void Plan_SameInput_SameOutput()
    {
        // Arrange: одинаковый вход для двух вызовов.
        var hosts = new List<HostInfo> { new("h1", 1), new("h2", 0), new("h3", 2) };
        var groups = new List<NodeGroup> { Group("shard1", 3), Group("shard2", 2) };

        // Act: два независимых прогона планировщика.
        var first = PlacementPlanner.Plan(groups, hosts);
        var second = PlacementPlanner.Plan(groups, hosts);

        // Assert: детерминизм — планы эквивалентны (тот же порядок, те же хосты).
        second.Nodes.Should().BeEquivalentTo(first.Nodes, o => o.WithStrictOrdering());
    }

    [Fact]
    public void Plan_SingleGroup_ClusterNodes_SpreadAcrossHosts()
    {
        // Arrange: одна группа (Kfw-инстанс: кластер) из 3 нод, 3 хоста
        var hosts = new List<HostInfo> { new("h1", 0), new("h2", 0), new("h3", 0) };
        var groups = new List<NodeGroup> { new("c1", ["b1", "b2", "b3"]) };

        // Act
        var plan = PlacementPlanner.Plan(groups, hosts);

        // Assert: анти-аффинити внутри группы; Group отражён в каждой записи
        plan.Nodes.Select(n => n.Host).Should().OnlyHaveUniqueItems();
        plan.Nodes.Should().OnlyContain(n => n.Group == "c1");
    }
}
