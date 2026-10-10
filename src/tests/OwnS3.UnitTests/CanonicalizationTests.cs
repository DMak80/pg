using OwnS3.Protocol.Requests;
using OwnS3.Protocol.Uris;

namespace OwnS3.UnitTests;

// Канонизация SigV4 (arch/owns3/03 §1, вкл. arch-правки 7 и 9): URI по образцу
// s3utils.EncodePath референса (сохранение отправленного %XX), query — сортировка
// до кодирования и RFC 3986-кодирование (пробел -> %20, '+' -> %2B).
public sealed class CanonicalizationTests
{
    [Theory]
    [InlineData("/b/k", "/b/k")]                                 // без спецсимволов — как есть
    [InlineData("/b/key with space", "/b/key%20with%20space")]   // пробел → %20
    [InlineData("/b/key+with space", "/b/key%2Bwith%20space")]   // литеральный '+' в ПУТИ → %2B (RFC 3986: '+' не unreserved; референс s3utils.EncodePath)
    [InlineData("/b/caf%C3%A9", "/b/caf%C3%A9")]                 // существующее кодирование сохраняется как отправлено
    [InlineData("/b/100%", "/b/100%25")]                         // незакодированный % кодируется
    public void EncodePath_Canonicalizes(string raw, string expected)
    {
        // Arrange / Act
        var actual = UriEncoding.EncodePath(raw);
        // Assert: сегментное кодирование RFC 3986 ('/' — разделитель, '+' — кодируется как %2B)
        actual.Should().Be(expected);
    }

    [Fact]
    public void EncodeQuery_SortsBeforeEncoding_AndEncodesSpacesAsPercent20()
    {
        // Arrange: НЕотсортированные пары со спецсимволами и '+'
        var pairs = new[] { ("prefix", "a b"), ("marker", "z"), ("max-keys", "2") };
        // Act
        var actual = UriEncoding.EncodeQuery(pairs);
        // Assert: сортировка по ключу до кодирования, пробел → %20 (не '+');
        // '+' в ЗНАЧЕНИИ query кодируется как %2B
        actual.Should().Be("marker=z&max-keys=2&prefix=a%20b");
    }

    [Fact]
    public void EncodeQuery_EncodesPlusInValueAsPercent2B()
    {
        // Arrange: значение со знаком '+'
        var pairs = new[] { ("prefix", "a+b") };
        // Act
        var actual = UriEncoding.EncodeQuery(pairs);
        // Assert: RFC 3986-кодирование значения — '+' → %2B (не остаётся литералом)
        actual.Should().Be("prefix=a%2Bb");
    }
}
