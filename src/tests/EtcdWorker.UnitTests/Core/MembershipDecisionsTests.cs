using EtcdWorker.Core;

namespace EtcdWorker.UnitTests.Core;

// Сквозная нумерация узлов контура (MembershipDecisions.NextNumber): следующий
// номер = max(existing)+1, пропуски в нумерации не заполняются.
public class MembershipDecisionsTests
{
    [Fact]
    public void EmptyExistingNumbers_ReturnsOne()
    {
        // Arrange — контур без существующих узлов
        var existing = Array.Empty<int>();

        // Act
        var next = MembershipDecisions.NextNumber(existing);

        // Assert — первый номер сквозной нумерации — 1
        next.Should().Be(1);
    }

    [Fact]
    public void ExistingNumbers_ReturnsMaxPlusOne()
    {
        // Arrange — плотная нумерация без пропусков
        int[] existing = [1, 2];

        // Act
        var next = MembershipDecisions.NextNumber(existing);

        // Assert — следующий за максимумом
        next.Should().Be(3);
    }

    [Fact]
    public void GapInExistingNumbers_IsNotReused()
    {
        // Arrange — пропуск в нумерации (нет узла 2)
        int[] existing = [1, 3];

        // Act
        var next = MembershipDecisions.NextNumber(existing);

        // Assert — пропуски не заполняются, берём max+1
        next.Should().Be(4);
    }
}
