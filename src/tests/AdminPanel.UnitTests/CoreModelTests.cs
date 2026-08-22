using AdminPanel.Core;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Вычислимые пометки модели снапшота (spec §3.6).
public class CoreModelTests
{
    [Fact]
    public void ClusterInfo_WithoutConfig_IsIncomplete()
    {
        // Arrange
        var cluster = new ClusterInfo("demo", null, 0, null, [], [], []);

        // Act
        var incomplete = cluster.Incomplete;

        // Assert
        incomplete.Should().BeTrue();
    }

    [Fact]
    public void ClusterInfo_WithConfig_IsComplete()
    {
        // Arrange
        var cluster = new ClusterInfo("demo", "demo", 16, 1755800000, [], [], []);

        // Act
        var incomplete = cluster.Incomplete;

        // Assert
        incomplete.Should().BeFalse();
    }

    [Fact]
    public void ShardInfo_MasterAddressNull_LeaseNotAlive()
    {
        // Arrange
        var shard = new ShardInfo("s1", "dsn", ["s1a"], 5432, "demo", "u", 1, null, null);

        // Act
        var alive = shard.MasterLeaseAlive;

        // Assert
        alive.Should().BeFalse();
    }

    [Fact]
    public void ShardInfo_MasterAddressSet_LeaseAlive()
    {
        // Arrange
        var shard = new ShardInfo("s1", "dsn", ["s1a"], 5432, "demo", "u", 1, "s1a:5432", null);

        // Act
        var alive = shard.MasterLeaseAlive;

        // Assert
        alive.Should().BeTrue();
    }
}
