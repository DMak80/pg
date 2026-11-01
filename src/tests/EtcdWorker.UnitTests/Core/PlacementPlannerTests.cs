using EtcdWorker.Core;
using Shared.Core;
// Глобальный using Shared.Core.Planning даёт одноимённый класс — точный алиас.
using PlacementPlanner = EtcdWorker.Core.PlacementPlanner;

namespace EtcdWorker.UnitTests.Core;

// Планировщик размещения узла (PlacementPlanner.PickHost): анти-аффинити —
// первая свободная нода по Ordinal; свободных нет — least-loaded, tie — Ordinal.
public class PlacementPlannerTests
{
    [Fact]
    public void FreeNodeAvailable_ReturnsFirstFreeByOrdinal()
    {
        // Arrange — нода "b" занята, "a" и "c" свободны
        string[] swarmNodes = ["a", "b", "c"];
        string[] placedOn = ["b"];

        // Act
        var pick = PlacementPlanner.PickHost(swarmNodes, placedOn);

        // Assert — из свободных берётся первая по Ordinal
        pick.IsSuccess.Should().BeTrue();
        pick.Value.Should().Be("a");
    }

    [Fact]
    public void AllNodesOccupied_ReturnsLeastLoaded()
    {
        // Arrange — все ноды заняты, загрузки: a=2, b=1, c=3
        string[] swarmNodes = ["a", "b", "c"];
        string[] placedOn = ["a", "a", "b", "c", "c", "c"];

        // Act
        var pick = PlacementPlanner.PickHost(swarmNodes, placedOn);

        // Assert — выбирается наименее загруженная нода
        pick.IsSuccess.Should().BeTrue();
        pick.Value.Should().Be("b");
    }

    [Fact]
    public void EqualLoad_TieBreaksByOrdinal()
    {
        // Arrange — все ноды загружены одинаково (по одному размещению)
        string[] swarmNodes = ["a", "b", "c"];
        string[] placedOn = ["a", "b", "c"];

        // Act
        var pick = PlacementPlanner.PickHost(swarmNodes, placedOn);

        // Assert — при равной загрузке берётся первая по Ordinal
        pick.IsSuccess.Should().BeTrue();
        pick.Value.Should().Be("a");
    }

    [Fact]
    public void EmptySwarmNodes_ReturnsFailed()
    {
        // Arrange — список swarm-нод пуст
        string[] swarmNodes = [];
        string[] placedOn = [];

        // Act
        var pick = PlacementPlanner.PickHost(swarmNodes, placedOn);

        // Assert — размещение невозможно: Failed c ApplicationException
        pick.IsSuccess.Should().BeFalse();
        pick.Error.Should().BeOfType<ApplicationException>();
    }
}
