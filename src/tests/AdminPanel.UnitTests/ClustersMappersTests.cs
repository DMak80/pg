using AdminPanel.Api.Inspection;
using AdminPanel.Core;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Мапперы кластерных DTO: чистые функции (spec §10.2).
public class ClustersMappersTests
{
    private static readonly DateTimeOffset Now = new(2026, 8, 22, 12, 0, 0, TimeSpan.Zero);
    private static readonly long NowUnix = Now.ToUnixTimeSeconds();

    [Fact]
    public void ClustersMapper_CountsShardsMastersMoves()
    {
        // Arrange: MovingCluster — 2 шарда (s2 без master), 3 не-ACTIVE бакета.

        // Act
        var summaries = ClustersMapper.Map([TestSnapshots.MovingCluster(Now)]);

        // Assert: счётчики UI-таблицы Clusters (arch/03 §3; spec §3.2).
        var summary = summaries.Should().ContainSingle().Subject;
        summary.Name.Should().Be("demo");
        summary.DbName.Should().Be("demo");
        summary.BucketsCount.Should().Be(16);
        summary.ShardsTotal.Should().Be(2);
        summary.ShardsWithMaster.Should().Be(1);
        summary.ActiveMoves.Should().Be(3);
        summary.Incomplete.Should().BeFalse();
    }

    [Fact]
    public void ClustersMapper_IncompleteFlagAndNullDbName()
    {
        // Arrange / Act
        var summaries = ClustersMapper.Map([TestSnapshots.GhostCluster()]);

        // Assert: incomplete-кластер в сводке — dbname null, флаг поднят (spec §3.2).
        var summary = summaries.Should().ContainSingle().Subject;
        summary.Incomplete.Should().BeTrue();
        summary.DbName.Should().BeNull();
    }
}
