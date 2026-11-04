using FluentAssertions;
using PgWorker.Backups;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Имя дельта-куска (arch/pgnode/03 §2, spec §4.1): фиксированные ширины,
// hex строчные, лексикографическая сортировка = порядок по start;
// вырожденный диапазон (start >= end) запрещён конструированием (spec №29).
public class WalDeltaNameTests
{
    private const string Seg = "000000010000000000000001";

    [Fact]
    public void Key_CanonicalFormat()
    {
        // Arrange / Act
        var key = WalDeltaName.Key(
            WalFileName.TryParse(Seg)!.Value, 0x0, 0x1234ABCDEF567890UL, 2, true, "0123456789abcdef0123456789abcdef");

        // Assert
        key.Should().Be(
            $"{Seg}.partial_0000000000000000_1234abcdef567890_00000002_m_0123456789abcdef0123456789abcdef");
    }

    [Fact]
    public void Key_ReplicaRole_IsR()
    {
        // Arrange / Act
        var key = WalDeltaName.Key(
            WalFileName.TryParse(Seg)!.Value, 1, 2, 0, false, "a".PadRight(32, '0'));

        // Assert
        key.Should().EndWith("_00000000_r_" + "a".PadRight(32, '0'));
    }

    [Fact]
    public void Key_StartNotLessThanEnd_Throws()
    {
        // Arrange
        var seg = WalFileName.TryParse(Seg)!.Value;

        // Act
        var act = () => WalDeltaName.Key(seg, 5, 5, 1, true, "0".PadRight(32, '0'));

        // Assert — вырожденный/перевёрнутый диапазон запрещён (spec №29)
        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void TryParse_ValidName_AllFields()
    {
        // Arrange
        var name = $"{Seg}.partial_0000000000000000_1234abcdef567890_0000000a_r_0123456789abcdef0123456789abcdef";

        // Act
        var piece = WalDeltaName.TryParse(name);

        // Assert
        piece.Should().NotBeNull();
        piece!.Segment.Name.Should().Be(Seg);
        piece.Start.Should().Be(0);
        piece.End.Should().Be(0x1234ABCDEF567890UL);
        piece.ChainLife.Should().Be(0xA);
        piece.IsMaster.Should().BeFalse();
        piece.Md5Hex.Should().Be("0123456789abcdef0123456789abcdef");
    }

    [Fact]
    public void TryParse_StartNotLessThanEnd_ReturnsNull()
    {
        // Arrange — start == end: вырожденный кусок
        var name = $"{Seg}.partial_0000000000000005_0000000000000005_00000002_m_0123456789abcdef0123456789abcdef";

        // Act / Assert
        WalDeltaName.TryParse(name).Should().BeNull();
    }

    [Theory]
    [InlineData("")]                                   // пусто
    [InlineData("000000010000000000000001")]           // финал
    [InlineData("000000010000000000000001.0000000000000005.partial")] // t14-версия — НЕ кусок
    [InlineData("000000010000000000000001.partial_0_1_2_m_md5")]      // нефиксированные ширины
    [InlineData("000000010000000000000001.partial_0000000000000000_1234abcdef567890_00000002_x_0123456789abcdef0123456789abcdef")] // роль не m/r
    [InlineData("000000010000000000000001.partial_0000000000000000_1234abcdef567890_00000002_m_0123456789abcdef0123456789abcdeg")] // не hex md5
    public void TryParse_Invalid_ReturnsNull(string name)
    {
        // Arrange / Act / Assert
        WalDeltaName.TryParse(name).Should().BeNull();
    }

    [Fact]
    public void SegmentPrefixOf_IsNarrowListingPrefix()
    {
        // Arrange / Act / Assert
        WalDeltaName.SegmentPrefixOf(Seg).Should().Be(Seg + ".");
    }

    [Fact]
    public void Key_SortsByStart_Lexicographically()
    {
        // Arrange
        var seg = WalFileName.TryParse(Seg)!.Value;
        var md5 = "0".PadRight(32, '0');
        var a = WalDeltaName.Key(seg, 0, 1, 1, true, md5);
        var b = WalDeltaName.Key(seg, 1, 2, 1, true, md5);
        var legacy = $"{Seg}.ffffffffffffffff.partial"; // t14-версия

        // Act / Assert — суффикс куска ПОСЛЕ hex-суффиксов legacy ('p' > любой hex)
        string.CompareOrdinal(a, b).Should().BeNegative();
        string.CompareOrdinal(legacy, a).Should().BeNegative();
    }

    // ---: CoverageOf — покрытие по именам кусков текущей жизни (t15) ----

    // AAA: пустой список — покрытия нет.
    [Fact]
    public void CoverageOf_Empty_Zero()
    {
        // Arrange / Act / Assert
        WalDeltaName.CoverageOf([], chainLife: 1).Should().Be(0UL);
    }

    // AAA: дельта без базы (start > 0) покрытие не начинает.
    [Fact]
    public void CoverageOf_NoBase_Zero()
    {
        // Arrange — только дельта [0x100..0x200)
        var seg = WalFileName.TryParse(Seg)!.Value;
        var delta = WalDeltaName.Key(seg, 0x100, 0x200, 1, true, new string('a', 32));

        // Act / Assert
        WalDeltaName.CoverageOf([delta], chainLife: 1).Should().Be(0UL);
    }

    // AAA: база + примыкающие дельты — покрытие до конца последней.
    [Fact]
    public void CoverageOf_BaseAndAdjacent_EndOfLast()
    {
        // Arrange
        var seg = WalFileName.TryParse(Seg)!.Value;
        string[] names =
        [
            WalDeltaName.Key(seg, 0, 0x100, 1, true, new string('a', 32)),
            WalDeltaName.Key(seg, 0x100, 0x200, 1, false, new string('b', 32)),
            WalDeltaName.Key(seg, 0x200, 0x300, 1, false, new string('c', 32)),
        ];

        // Act / Assert
        WalDeltaName.CoverageOf(names, chainLife: 1).Should().Be(0x300UL);
    }

    // AAA: дыра в покрытии — покрытие останавливается на её границе.
    [Fact]
    public void CoverageOf_Gap_StopsAtGap()
    {
        // Arrange — [0..0x100) и [0x200..0x300), дыра [0x100..0x200)
        var seg = WalFileName.TryParse(Seg)!.Value;
        string[] names =
        [
            WalDeltaName.Key(seg, 0, 0x100, 1, true, new string('a', 32)),
            WalDeltaName.Key(seg, 0x200, 0x300, 1, false, new string('b', 32)),
        ];

        // Act / Assert
        WalDeltaName.CoverageOf(names, chainLife: 1).Should().Be(0x100UL);
    }

    // AAA: куски чужого номера жизни не участвуют.
    [Fact]
    public void CoverageOf_ForeignLife_Ignored()
    {
        // Arrange — база жизни 2 при запросе жизни 1
        var seg = WalFileName.TryParse(Seg)!.Value;
        var foreign = WalDeltaName.Key(seg, 0, 0x100, 2, true, new string('a', 32));

        // Act / Assert
        WalDeltaName.CoverageOf([foreign], chainLife: 1).Should().Be(0UL);
        WalDeltaName.CoverageOf([foreign], chainLife: 2).Should().Be(0x100UL);
    }

    // AAA: финал и t14-версия в списке имён — не куски, игнорируются.
    [Fact]
    public void CoverageOf_FinalAndLegacy_Ignored()
    {
        // Arrange
        string[] names = [Seg, WalPartialName.Key(Seg, 0x500)];

        // Act / Assert
        WalDeltaName.CoverageOf(names, chainLife: 0).Should().Be(0UL);
    }
}
