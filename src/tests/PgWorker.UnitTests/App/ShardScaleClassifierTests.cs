using PgWorker.App.Loops;
using PgWorker.Core.Model;
using Xunit;

namespace PgWorker.UnitTests.App;

// Детекция кандидатов scale-прохода Active-ветки (t06 spec §5.1).
public class ShardScaleClassifierTests
{
    private static ShardSpec Shard(string name, bool toRemove = false, string? dsn = null, int nodes = 2)
        => new(name, nodes, dsn, null,
            Enumerable.Range(0, nodes)
                .Select(i => new NodeSpec(name, $"{name}{(char)('a' + i)}", NodeState.Running))
                .ToList(), toRemove);

    private static ClusterSnapshot Snap(params ShardSpec[] shards)
        => new(new ClusterConfig("shop", 6, "shop", null, ClusterState.Active), shards, []);

    [Fact]
    public void Detect_DeclaredWithoutDsn_IsAddCandidate()
    {
        // Arrange — панель заявила shard3 (ноды есть, dsn нет)
        var snap = Snap(Shard("shard1", dsn: "host=h1"), Shard("shard3", dsn: null));

        // Act
        var candidates = ShardScaleClassifier.Detect(snap);

        // Assert
        candidates.Add.Should().Equal("shard3");
        candidates.Remove.Should().BeEmpty();
    }

    [Fact]
    public void Detect_ToRemoveMarker_IsRemoveCandidate()
    {
        // Arrange — шард поднят и помечен к удалению
        var snap = Snap(Shard("shard1", toRemove: true, dsn: "host=h1"));

        // Act
        var candidates = ShardScaleClassifier.Detect(snap);

        // Assert
        candidates.Remove.Should().Equal("shard1");
        candidates.Add.Should().BeEmpty();
    }

    [Fact]
    public void Detect_RegisteredWithoutMarker_IsNeither()
    {
        // Arrange — обычный живой шард
        var snap = Snap(Shard("shard1", dsn: "host=h1"));

        // Act / Assert
        ShardScaleClassifier.Detect(snap).Should().BeEquivalentTo(
            new ShardScaleCandidates([], []));
    }

    [Fact]
    public void Detect_MarkedUndeclaredShard_IsInBothLists()
    {
        // Arrange — помечен к удалению и не поднят (declared без dsn): оба списка
        // (spec §8: «оба одновременно — оба списка»; порядок прохода remove→add
        // и guard ToRemove в A1 разбирают конфликт — Task 4/6)
        var snap = Snap(Shard("shard1", toRemove: true, dsn: null));

        // Act
        var candidates = ShardScaleClassifier.Detect(snap);

        // Assert
        candidates.Remove.Should().Equal("shard1");
        candidates.Add.Should().Equal("shard1");
    }

    [Fact]
    public void Detect_NoNodesNoDsnNotCandidate()
    {
        // Arrange — ключи шарда без declared-нод (внешние кластеры без nodes) — не add
        var snap = Snap(Shard("s1", dsn: null, nodes: 0));

        // Act / Assert
        ShardScaleClassifier.Detect(snap).Add.Should().BeEmpty();
    }
}
