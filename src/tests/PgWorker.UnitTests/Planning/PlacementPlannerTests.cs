using PgWorker.Core.Model;
using PgWorker.Core.Planning;

namespace PgWorker.UnitTests.Planning;

// PlacementPlanner: анти-аффинити нод шарда по docker-хостам (spec §6.3, Д5).

public class PlacementPlannerTests
{
    private static ShardSpec Shard(string name, int replicas) => new(
        name, replicas, null, null,
        Enumerable.Range(0, replicas)
           .Select(i => new NodeSpec(name, $"{name}{(char)('a' + i)}", NodeState.NotInitialized))
           .ToList());

    [Fact]
    public void Plan_HostsEqualsReplicas_NodesOnDistinctHosts()
    {
        // Arrange: 3 хоста, шард из 3 нод — топология позволяет полный разброс.
        var hosts = new List<HostInfo> { new("h1", 0), new("h2", 0), new("h3", 0) };
        var shards = new List<ShardSpec> { Shard("shard1", 3) };

        // Act: строим план размещения.
        var plan = PlacementPlanner.Plan(shards, hosts);

        // Assert: все ноды шарда — на разных хостах (анти-аффинити).
        plan.Nodes.Select(n => n.Host).Should().OnlyHaveUniqueItems();
        plan.Nodes.Should().HaveCount(3);
    }

    [Fact]
    public void Plan_SingleHost_AllNodesOnIt()
    {
        // Arrange: 1 хост, шард из 2 нод — равномерность невозможна.
        var hosts = new List<HostInfo> { new("h1", 0) };
        var shards = new List<ShardSpec> { Shard("shard1", 2) };

        // Act: строим план размещения.
        var plan = PlacementPlanner.Plan(shards, hosts);

        // Assert: обе ноды на единственном хосте.
        plan.Nodes.Should().OnlyContain(n => n.Host == "h1");
    }

    [Fact]
    public void Plan_HostsFewerThanReplicas_MinimalRepeats()
    {
        // Arrange: 2 хоста, шард из 3 нод — разброс 2+1 (минимум повторов).
        var hosts = new List<HostInfo> { new("h1", 0), new("h2", 0) };
        var shards = new List<ShardSpec> { Shard("shard1", 3) };

        // Act: строим план размещения.
        var plan = PlacementPlanner.Plan(shards, hosts);

        // Assert: распределение 2+1, никакой хост не держит больше 2 нод.
        plan.Nodes.GroupBy(n => n.Host).Should().HaveCount(2);
        plan.Nodes.GroupBy(n => n.Host).Should().OnlyContain(g => g.Count() <= 2);
    }

    [Fact]
    public void Plan_UsedSlots_PreferredHostIsLeastLoaded()
    {
        // Arrange: h1 перегружен (5 занятых слотов), h2 свободен.
        var hosts = new List<HostInfo> { new("h1", 5), new("h2", 0) };
        var shards = new List<ShardSpec> { Shard("shard1", 2) };

        // Act: строим план размещения.
        var plan = PlacementPlanner.Plan(shards, hosts);

        // Assert: первая нода уходит на свободный h2, вторая — на h1.
        plan.Nodes.Should().Contain(n => n.Node == "shard1a" && n.Host == "h2");
        plan.Nodes.Should().Contain(n => n.Node == "shard1b" && n.Host == "h1");
    }

    [Fact]
    public void Plan_SameInput_SameOutput()
    {
        // Arrange: одинаковый вход для двух вызовов.
        var hosts = new List<HostInfo> { new("h1", 1), new("h2", 0), new("h3", 2) };
        var shards = new List<ShardSpec> { Shard("shard1", 3), Shard("shard2", 2) };

        // Act: два независимых прогона планировщика.
        var first = PlacementPlanner.Plan(shards, hosts);
        var second = PlacementPlanner.Plan(shards, hosts);

        // Assert: детерминизм — планы эквивалентны (тот же порядок, те же хосты).
        second.Nodes.Should().BeEquivalentTo(first.Nodes, o => o.WithStrictOrdering());
    }
}
