using FluentAssertions;
using PgWorker.Backups;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Ключ .partial-версии живого сегмента (arch/19 §3): <segment>.<endLsn:x16>.partial;
// фиксированная ширина суффикса = числовому порядку (лексикографический max).
public class WalPartialNameTests
{
    [Fact]
    public void Key_CanonicalLowercaseHex()
    {
        // Arrange
        const string segment = "000000010000000000000002";

        // Act
        var key = WalPartialName.Key(segment, 0x1000000UL);

        // Assert
        key.Should().Be("000000010000000000000002.0000000001000000.partial");
    }

    [Fact]
    public void TryParse_Valid_ReturnsSegmentAndLsn()
    {
        // Arrange
        var key = WalPartialName.Key("000000010000000000000002", 0x1000000UL);

        // Act
        var parsed = WalPartialName.TryParse(key);

        // Assert
        parsed.Should().NotBeNull();
        parsed!.Value.Segment.Name.Should().Be("000000010000000000000002");
        parsed.Value.EndLsn.Should().Be(0x1000000UL);
    }

    [Fact]
    public void TryParse_Rejects()
    {
        // Arrange — финал, .history, короткий суффикс, не-hex, мусор
        string[] notPartial =
        [
            "000000010000000000000002",
            "00000002.history",
            "000000010000000000000002.0100.partial",
            "000000010000000000000002.zzzzzzzzzzzzzzzz.partial",
            "abc",
        ];

        // Act
        var parsed = notPartial.Select(WalPartialName.TryParse).ToList();

        // Assert
        parsed.Should().AllSatisfy(p => p.Should().BeNull());
    }

    [Fact]
    public void SegmentPrefixOf_EndsWithDot()
    {
        // Arrange
        const string segment = "000000010000000000000002";

        // Act
        var prefix = WalPartialName.SegmentPrefixOf(segment);

        // Assert
        prefix.Should().Be("000000010000000000000002.");
    }

    [Fact]
    public void Lexicographic_MaxSuffix_IsMaxLsn()
    {
        // Arrange
        const string segment = "000000010000000000000002";
        var names = new[]
        {
            WalPartialName.Key(segment, 0x10), WalPartialName.Key(segment, 0x9),
            WalPartialName.Key(segment, 0xFF),
        };

        // Act
        var max = names.OrderBy(n => n, StringComparer.Ordinal).Last();

        // Assert
        max.Should().Be(WalPartialName.Key(segment, 0xFF));
    }
}
