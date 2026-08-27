using AdminPanel.Core;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Parsing;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Сборка EtcdSnapshot из частей тика (spec §10.7).
public class SnapshotBuilderTests
{
    // FixedTimeProvider — существующий из t02 (src/tests/AdminPanel.UnitTests/FixedTimeProvider.cs).
    [Fact]
    public void Build_FullParts_AssemblesSnapshot()
    {
        // Arrange
        var time = new FixedTimeProvider { Utc = new(2026, 8, 22, 12, 0, 0, TimeSpan.Zero) };
        var clusters = ClustersParser.Parse(EtcdFixtures.LoadKv("clusters-full.json"));
        var service = ServiceParser.Parse(EtcdFixtures.LoadKv("service-full.json"), clusters.Clusters);
        var nodes = StandNodesParser.Parse(EtcdFixtures.LoadKv("stand-nodes.json"));
        var members = new List<EtcdMember> { new(42, "test", ["http://p"], ["http://c"]) };
        var alarms = new List<EtcdAlarm> { new(42, EtcdAlarmType.NoSpace) };
        var etcd = new EtcdStatus(true, [], members, alarms, "http://e1", false, time.GetUtcNow(), 0);

        // Act
        var snapshot = SnapshotBuilder.Build(
            time, clusters, service, nodes, MovesQueueParser.Parse([]), members, alarms, etcd);

        // Assert
        snapshot.BuiltAtUtc.Should().Be(time.Utc);
        snapshot.Clusters.Should().ContainSingle(c => c.Name == "demo");
        snapshot.HaScopes.Should().Contain(s => s.Scope == "demo-s1");
        snapshot.StandNodes.Should().HaveCount(4);
        snapshot.MoveTickets.Should().BeEmpty(); // очередь заявок — образец portalloc
        snapshot.Alerts.Should().BeEmpty();   // AlertEngine — t04
        snapshot.Probes.Should().BeEmpty();   // пробы — t06
        snapshot.UnknownKeyCount.Should().Be(0);
        snapshot.ParseErrors.Should().BeEmpty();
    }

    [Fact]
    public void Build_SumsDiagnostics()
    {
        // Arrange — вырожденные фикстуры дают ошибки и unknown-ключи обоих префиксов
        var time = new FixedTimeProvider();
        var clusters = ClustersParser.Parse(EtcdFixtures.LoadKv("clusters-degenerate.json"));
        var service = ServiceParser.Parse(EtcdFixtures.LoadKv("service-unmatched.json"), clusters.Clusters);
        var etcd = new EtcdStatus(true, [], [], [], null, false, time.GetUtcNow(), 0);

        // Act
        var snapshot = SnapshotBuilder.Build(time, clusters, service, [], MovesQueueParser.Parse([]), [], [], etcd);

        // Assert
        snapshot.UnknownKeyCount.Should().Be(2); // surprise (/clusters/) + stray (/service/)
        snapshot.ParseErrors.Should().HaveCount(4); // status-битый, replicas, master-пустой, bucket_abc
    }
}
