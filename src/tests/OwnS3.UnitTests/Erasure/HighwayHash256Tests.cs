using OwnS3.Storage.Erasure;

namespace OwnS3.UnitTests.Erasure;

// Тесты порта HighwayHash-256 против векторов Go-реализации (t40, план Task 3).
public class HighwayHash256Tests
{
    [Fact]
    public void Vectors_All_MatchGoReference()
    {
        // Arrange/Act/Assert — каждый вектор: Hash(key, data) == ожидаемый hex
        foreach (var vector in HighwayHashVectors.All)
        {
            var actual = HighwayHash256.Hash(vector.Key, vector.Data);
            Convert.ToHexString(actual).ToLowerInvariant().Should().Be(
                vector.ExpectedHex256, "вектор {0} обязан совпадать с Go-реализацией", vector.Name);
        }
    }

    [Fact]
    public void HashStream_EqualsHashForMemoryStream()
    {
        // Arrange
        var vector = HighwayHashVectors.All.Single(v => v.Name == "magic_rand1000");

        // Act
        using var stream = new MemoryStream(vector.Data);
        var streamed = HighwayHash256.HashStream(vector.Key, stream);

        // Assert
        Convert.ToHexString(streamed).ToLowerInvariant().Should().Be(vector.ExpectedHex256);
    }

    [Fact]
    public void Hash_EmptyInput_MatchesVector()
    {
        // Arrange — пустой ввод входит в набор векторов (magic_empty)
        var empty = HighwayHashVectors.All.Single(v => v.Name == "magic_empty");

        // Act
        var actual = HighwayHash256.Hash(empty.Key, Array.Empty<byte>());

        // Assert
        Convert.ToHexString(actual).ToLowerInvariant().Should().Be(empty.ExpectedHex256);
    }

    [Fact]
    public void MagicKey_MatchesReferenceBytes()
    {
        // Arrange — магический ключ bitrot-контроля референса cmd/bitrot.go (32 байта)
        var expected = new byte[]
        {
            0x4b, 0xe7, 0x34, 0xfa, 0x8e, 0x23, 0x8a, 0xcd, 0x26, 0x3e, 0x83, 0xe6, 0xbb, 0x96, 0x85, 0x52,
            0x04, 0x0f, 0x93, 0x5d, 0xa3, 0x9f, 0x44, 0x14, 0x97, 0xe0, 0x9d, 0x13, 0x22, 0xde, 0x36, 0xa0,
        };

        // Act
        var magic = HighwayHash256.MagicKey.ToArray();

        // Assert
        magic.Should().Equal(expected);
    }

    [Fact]
    public void Hash_WrongKeySize_Throws()
    {
        // Arrange
        var shortKey = new byte[16];

        // Act
        var act = () => HighwayHash256.Hash(shortKey, Array.Empty<byte>());

        // Assert
        act.Should().Throw<ArgumentException>();
    }
}
