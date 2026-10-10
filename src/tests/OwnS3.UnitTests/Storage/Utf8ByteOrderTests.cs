using System.Text;
using OwnS3.Storage;

namespace OwnS3.UnitTests.Storage;

// Байтовый порядок UTF-8 (P9): единый компаратор листингов/маркеров/бакетов.
public class Utf8ByteOrderTests
{
    [Fact]
    public void Compare_ByteOrderNotUtf16()
    {
        // Arrange: U+FFFF — байты EF BF BF; U+1F600 — F0 9F 98 80 (байтово больше).
        // string Ordinal сравнивает UTF-16 кодовые единицы и ставит суррогаты
        // (D800-DFFF) НИЖЕ U+E000-U+FFFF — расходится с байтовым порядком.
        var left = "\uFFFF";
        var right = "\U0001F600";

        // Act
        var byteOrder = Utf8ByteOrder.Compare(left, right);
        var utf16Order = string.Compare(left, right, StringComparison.Ordinal);

        // Assert: байтово «😀» БОЛЬШЕ U+FFFF; Ordinal дал бы обратное
        byteOrder.Should().BeNegative();
        utf16Order.Should().BePositive("фиксация P9: Ordinal расходится с байтовым порядком");
    }

    [Fact]
    public void Compare_AsciiAndDiacritics()
    {
        // Arrange: байты 'Z'=0x5A < 'a'=0x61 < '~'=0x7E < 'Á'=C3 81
        // Act / Assert
        Utf8ByteOrder.Compare("Z", "a").Should().BeNegative();
        Utf8ByteOrder.Compare("a", "~").Should().BeNegative();
        Utf8ByteOrder.Compare("~", "Á").Should().BeNegative();
    }

    [Fact]
    public void Compare_PrefixIsLessThanLonger()
    {
        // Arrange / Act / Assert: общий префикс — более короткая строка меньше
        Utf8ByteOrder.Compare("a", "ab").Should().BeNegative();
        Utf8ByteOrder.Compare("ab", "a").Should().BePositive();
        Utf8ByteOrder.Compare("ab", "ab").Should().Be(0);
    }

    [Fact]
    public void CompareBytes_MatchesStringComparison()
    {
        // Arrange / Act / Assert: байтовая и строковые формы согласованы
        Utf8ByteOrder.CompareBytes("a"u8, "b"u8).Should().BeNegative();
        Utf8ByteOrder.CompareBytes("ab"u8, "b"u8).Should().BeNegative();
        Utf8ByteOrder.CompareBytes("b"u8, "b"u8).Should().Be(0);
    }

    [Fact]
    public void StartsWith_BytePrefix()
    {
        // Arrange / Act / Assert: байтовый префикс — как у листинговых фильтров
        Utf8ByteOrder.StartsWith("photos/2020/x", "photos/2").Should().BeTrue();
        Utf8ByteOrder.StartsWith("photos/2020/x", "photos/2x").Should().BeFalse();
        Utf8ByteOrder.StartsWith("abc", "abc").Should().BeTrue();
        Utf8ByteOrder.StartsWith("ab", "abc").Should().BeFalse();
    }

    [Fact]
    public void StartsWith_MultibytePrefix_ByteSemantics()
    {
        // Arrange: префикс из многобайтовых символов — сравнение по байтам
        var value = Encoding.UTF8.GetBytes("café-au-lait");

        // Act / Assert
        Utf8ByteOrder.StartsWith("café-au-lait", "café").Should().BeTrue();
        Utf8ByteOrder.StartsWith("cafe", "café").Should().BeFalse();
    }
}
