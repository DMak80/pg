using OwnS3.Storage.Erasure;

namespace OwnS3.UnitTests.Erasure;

// Тест-векторы SipHash-2-4 референса Aumasson/DJB (ключ 000102..0f,
// вход = первые i байт последовательности 00 01 02 ..., i = 0..63).
public class SipHash24Tests
{
    [Theory]
    [InlineData(0, "726fdb47dd0e0e31")]
    [InlineData(1, "74f839c593dc67fd")]
    [InlineData(2, "0d6c8009d9a94f5a")]
    [InlineData(3, "85676696d7fb7e2d")]
    [InlineData(4, "cf2794e0277187b7")]
    [InlineData(5, "18765564cd99a68d")]
    [InlineData(6, "cbc9466e58fee3ce")]
    [InlineData(7, "ab0200f58b01d137")]
    [InlineData(61, "9f626da15c9625f3")]
    [InlineData(62, "e51b38608ef25f57")]
    [InlineData(63, "958a324ceb064572")]
    public void Hash_MatchesReferenceVectors(int inputLength, string expectedHex)
    {
        // Arrange
        var key = Enumerable.Range(0, 16).Select(i => (byte)i).ToArray();
        var data = Enumerable.Range(0, inputLength).Select(i => (byte)i).ToArray();

        // Act
        var hash = SipHash24.Hash(key, data);

        // Assert
        hash.ToString("x16").Should().Be(expectedHex);
    }

    [Fact]
    public void Hash_LongInput_StreamedCorrectly()
    {
        // Arrange — много полных 8-байтовых слов + хвост (блок 1 КиБ)
        var key = Enumerable.Range(0, 16).Select(i => (byte)i).ToArray();
        var data = new byte[1024];
        new Random(5).NextBytes(data);

        // Act/Assert — детерминированность и чувствительность к последнему байту
        var first = SipHash24.Hash(key, data);
        SipHash24.Hash(key, data).Should().Be(first);
        data[^1] ^= 1;
        SipHash24.Hash(key, data).Should().NotBe(first);
    }
}
