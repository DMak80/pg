using OwnS3.Storage;

namespace OwnS3.UnitTests.Storage;

// Кодирование ключа в xl-путь: эталонные векторы канона 04 §1 (спецслучаи,
// лимит 255 байт, отсутствие коллизий) и обратное преобразование.
public class XlPathEncoderTests
{
    [Theory]
    [InlineData("a/b/c", "a/b/c")]
    [InlineData("dir/", "dir__XLDIR__")]
    [InlineData("a//b", "a/%/b")]
    [InlineData("a//", "a/%__XLDIR__")]
    [InlineData(".", "%2E")]
    [InlineData("..", "%2E%2E")]
    [InlineData("100%", "100%25")]
    [InlineData("lit%2E", "lit%252E")]
    [InlineData("x__XLDIR__", "x__XLDIR_%5F")]
    [InlineData("x__XLDIR__/", "x__XLDIR_%5F__XLDIR__")]
    [InlineData("café", "caf%C3%A9")]
    public void Encode_KeyToPath_CanonicalVectors(string key, string expectedPath)
    {
        // Arrange / Act
        var path = XlPathEncoder.EncodePath(key);
        // Assert: вектор канона 04 §1 (спецслучаи кодирования)
        path.Should().Be(expectedPath);
        XlPathEncoder.DecodeSegments(path.Split('/')).Should().Be(key); // roundtrip
    }

    [Fact]
    public void Decode_RawSinglePercent_IsEmptySegment()
    {
        // Arrange: сырой одиночный «%» возникает только из пустого сегмента
        var segments = new[] { "a", "%", "b" };

        // Act
        var key = XlPathEncoder.DecodeSegments(segments);

        // Assert: спецслучай применяется ДО percent-декода — «a//b», не «a/%/b»
        key.Should().Be("a//b");
    }

    [Fact]
    public void Encode_SegmentLongerThan255Bytes_ThrowsInvalidArgument()
    {
        // Arrange: 300 ASCII-символов — escape не удлиняет, сегмент > 255 байт
        var key = new string('a', 300);

        // Act
        var act = () => XlPathEncoder.EncodePath(key);

        // Assert: лимит компонента пути ФС (канон 04 §1)
        act.Should().Throw<XlInvalidArgumentException>()
            .WithMessage("Key too long after encoding");
    }

    [Fact]
    public void Encode_DirMarkerLengthCounts_TowardSegmentLimit()
    {
        // Arrange: 250 байт сегмента + 8 байт маркера «__XLDIR__» = 258 > 255
        var key = new string('a', 250) + "/";

        // Act
        var act = () => XlPathEncoder.EncodePath(key);

        // Assert: проверка длины выполняется ПОСЛЕ приклейки маркера
        act.Should().Throw<XlInvalidArgumentException>();
    }

    [Theory]
    [InlineData("a//b", "a/%/b")]     // путь пустого сегмента ≠ путь ключа с литеральным «%»
    [InlineData("lit.", "lit%2E")]     // ключ с литеральной точкой ≠ ключ с литеральным «%2E»
    [InlineData("x__XLDIR__", "x__XLDIR__/")]
    public void Encode_DistinctKeys_DistinctPaths(string key, string otherKey)
    {
        // Arrange / Act
        var path = XlPathEncoder.EncodePath(key);
        var otherPath = XlPathEncoder.EncodePath(otherKey);

        // Assert: разные ключи — разные кодированные пути (коллизий нет)
        path.Should().NotBe(otherPath);
    }
}
