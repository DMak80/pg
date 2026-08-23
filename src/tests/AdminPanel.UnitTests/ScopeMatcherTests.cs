using AdminPanel.Core;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Связь scope "<C>-<X>" → (cluster, shard) по известным кластерам (spec §5, arch/02 §2.2).
public class ScopeMatcherTests
{
    private static readonly IReadOnlyList<ClusterInfo> Clusters =
    [
        new("demo", "demo", 16, null, ClusterState.Active,
            [new ShardInfo("s1", "", [], null, null, null, null, null, [], null),
             new ShardInfo("s2", "", [], null, null, null, null, null, [], null)],
            [], []),
        new("shop", "shop", 4, null, ClusterState.Active,
            [new ShardInfo("shard1", "", [], null, null, null, null, null, [], null)],
            [], []),
    ];

    [Fact]
    public void Match_KnownClusterAndShard_ReturnsMatched()
    {
        // Arrange — scope demo-s1
        // Act
        var (cluster, shard, matched) = ScopeMatcher.Match("demo-s1", Clusters);

        // Assert
        matched.Should().BeTrue();
        cluster.Should().Be("demo");
        shard.Should().Be("s1");
    }

    [Fact]
    public void Match_SuffixNotShard_ClusterWithoutShard()
    {
        // Arrange — s9 не является шардом demo
        // Act
        var (cluster, shard, matched) = ScopeMatcher.Match("demo-s9", Clusters);

        // Assert
        matched.Should().BeFalse();
        cluster.Should().Be("demo");
        shard.Should().BeNull();
    }

    [Fact]
    public void Match_UnknownPrefix_AllNull()
    {
        // Arrange — чужой service в общем etcd — норма (arch/02 §7)
        // Act
        var (cluster, shard, matched) = ScopeMatcher.Match("other-scope", Clusters);

        // Assert
        matched.Should().BeFalse();
        cluster.Should().BeNull();
        shard.Should().BeNull();
    }

    [Fact]
    public void Match_LongerClusterName_NotConfused()
    {
        // Arrange — "shop2" не путается с "shop" из-за дефиса в префиксе
        // Act
        var (cluster, _, matched) = ScopeMatcher.Match("shop2-x", Clusters);

        // Assert
        matched.Should().BeFalse();
        cluster.Should().BeNull();
    }

    [Fact]
    public void Match_NoClusters_AllNull()
    {
        // Arrange — /clusters/ пуст
        // Act
        var (cluster, shard, matched) = ScopeMatcher.Match("demo-s1", []);

        // Assert
        matched.Should().BeFalse();
        cluster.Should().BeNull();
        shard.Should().BeNull();
    }
}
