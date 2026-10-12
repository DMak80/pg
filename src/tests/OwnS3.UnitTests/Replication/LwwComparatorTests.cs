using OwnS3.Replication.Lww;

namespace OwnS3.UnitTests.Replication;

// LWW-компаратор (t35, глава 07 §7): ПОЛНАЯ таблица исходов — больше/меньше modTime,
// равные modTime при равных/разных sha (тай-брейк лексикографически больший sha,
// конфликт журналируется).
public class LwwComparatorTests
{
    [Fact]
    public void IncomingModGreater_IncomingWins()
    {
        // Arrange — входящее новее локального
        // Act
        var (outcome, conflict) = LwwComparator.Compare(2000, "aa", 1000, "zz");

        // Assert
        outcome.Should().Be(LwwOutcome.IncomingWins);
        conflict.Should().BeFalse();
    }

    [Fact]
    public void IncomingModLess_LocalWins()
    {
        // Arrange — входящее старее локального
        // Act
        var (outcome, conflict) = LwwComparator.Compare(1000, "zz", 2000, "aa");

        // Assert
        outcome.Should().Be(LwwOutcome.LocalWins);
        conflict.Should().BeFalse();
    }

    [Fact]
    public void EqualModEqualSha_Identical()
    {
        // Arrange — одинаковые версии
        // Act
        var (outcome, conflict) = LwwComparator.Compare(1000, "abcd", 1000, "abcd");

        // Assert
        outcome.Should().Be(LwwOutcome.Identical);
        conflict.Should().BeFalse();
    }

    [Fact]
    public void EqualModShaLexicographicallyGreater_IncomingWinsWithConflict()
    {
        // Arrange — равные modTime, входящий sha больше ("aa" > "a9")
        // Act
        var (outcome, conflict) = LwwComparator.Compare(1000, "aa", 1000, "a9");

        // Assert — тай-брейк по sha + конфликт (журналируется)
        outcome.Should().Be(LwwOutcome.IncomingWins);
        conflict.Should().BeTrue();
    }

    [Fact]
    public void EqualModShaLexicographicallyLess_LocalWinsWithConflict()
    {
        // Arrange — равные modTime, входящий sha меньше ("a9" < "aa")
        // Act
        var (outcome, conflict) = LwwComparator.Compare(1000, "a9", 1000, "aa");

        // Assert — локальный побеждает по sha + конфликт
        outcome.Should().Be(LwwOutcome.LocalWins);
        conflict.Should().BeTrue();
    }
}
