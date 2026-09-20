using System.Diagnostics.Metrics;
using FluentAssertions;
using ValkeyWorker.App;
using Xunit;

namespace ValkeyWorker.UnitTests.App;

// Юнит-тесты ValkeyMetricsState (t05, arch/18 §2.6): затирание ушедших нод
// UpdateCluster, перезапись значений, консервативный LastSuccess, DebugSnapshot.
public sealed class ValkeyMetricsStateTests
{
    private static ValkeyNodeSample Sample(string node, long used = 1048576,
        string? role = "master", long? max = 536870912)
        => new(node, used, max, 1, 0, 0, 0, 5, 2, 7, 10, 0, 42, role, 0);

    // AAA: UpdateCluster затирает предыдущие записи кластера — ушедшие ноды не копятся.
    [Fact]
    public void UpdateCluster_ЗатираетУшедшиеНоды()
    {
        // Arrange: кластер c1 с node1; новый тик приносит только node2.
        var state = new ValkeyMetricsState(new Meter("TestValkeyState"));
        state.UpdateCluster("c1", [Sample("node1")]);

        // Act
        state.UpdateCluster("c1", [Sample("node2")]);

        // Assert: node1 исчез, node2 на месте; другой кластер не тронут.
        var nodes = state.DebugSnapshot().Nodes;
        nodes.Keys.Should().NotContain(("c1", "node1"));
        nodes.Keys.Should().Contain(("c1", "node2"));
    }

    // AAA: повторный сбор той же ноды перезаписывает значения (не дублирует).
    [Fact]
    public void UpdateCluster_ПерезаписываетЗначения()
    {
        // Arrange
        var state = new ValkeyMetricsState(new Meter("TestValkeyState"));
        state.UpdateCluster("c1", [Sample("node1", used: 100)]);

        // Act
        state.UpdateCluster("c1", [Sample("node1", used: 200)]);

        // Assert
        state.DebugSnapshot().Nodes[("c1", "node1")].UsedMemoryBytes.Should().Be(200);
        state.DebugSnapshot().Nodes.Should().HaveCount(1);
    }

    // AAA: MarkSuccess фиксирует время только явно (консервативный LastSuccess §4.2).
    [Fact]
    public void MarkSuccess_ФиксируетВремя()
    {
        // Arrange
        var state = new ValkeyMetricsState(new Meter("TestValkeyState"));
        var at = DateTimeOffset.UnixEpoch.AddHours(3);

        // Act
        state.MarkSuccess(at);

        // Assert
        state.DebugSnapshot().LastSuccess.Should().Be(at);
    }

    // AAA: поля сэмпла (вкл. role enum-паттерна и null-поля) видны в снапшоте.
    [Fact]
    public void DebugSnapshot_ПоляСэмплаВключаяRoleИNull()
    {
        // Arrange: сэмпл с отсутствующим maxmemory (null) и ролью slave.
        var state = new ValkeyMetricsState(new Meter("TestValkeyState"));

        // Act
        state.UpdateCluster("c1", [new ValkeyNodeSample(
            "node1", 1048576, null, 1, 0, 0, 0, 5, 2, 7, 10, 0, 42, "slave", null)]);

        // Assert: null-поля сохранены как null (серия не эмитится).
        var node = state.DebugSnapshot().Nodes[("c1", "node1")];
        node.MaxMemoryBytes.Should().BeNull();
        node.ConnectedSlaves.Should().BeNull();
        node.Role.Should().Be("slave");
    }
}
