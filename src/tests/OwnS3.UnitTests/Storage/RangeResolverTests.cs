using OwnS3.Storage;

namespace OwnS3.UnitTests.Storage;

// Резолв одиночного Range канона 02 §1: a-b / a- / -N, обрезка end, суффикс,
// пустой объект, If-Range (сильный ETag; дата-форма — несовпадение).
public class RangeResolverTests
{
    private const string Etag = "5d41402abc4b2a76b9719d911017c592";

    [Fact]
    public void ClosedRange_Applied()
    {
        // Arrange / Act
        var applied = RangeResolver.Resolve(new ByteRange(2, 3), null, Etag, 6);

        // Assert
        applied.Should().Be(new AppliedByteRange(2, 3, 6));
    }

    [Fact]
    public void OpenEndRange_ClampedToSizeMinusOne()
    {
        // Arrange: bytes=2- (End=null) / end за размером — обрезан
        // Act / Assert
        RangeResolver.Resolve(new ByteRange(2, null), null, Etag, 6)
            .Should().Be(new AppliedByteRange(2, 5, 6));
        RangeResolver.Resolve(new ByteRange(2, 100), null, Etag, 6)
            .Should().Be(new AppliedByteRange(2, 5, 6));
    }

    [Fact]
    public void SuffixRange_LastNBytes()
    {
        // Arrange: ParseRange кодирует bytes=-N как End=-N
        // Act / Assert
        RangeResolver.Resolve(new ByteRange(null, -2), null, Etag, 6)
            .Should().Be(new AppliedByteRange(4, 5, 6));
    }

    [Fact]
    public void SuffixRange_LongerThanObject_ReturnsWholeObject()
    {
        // Arrange / Act / Assert: N >= size → весь объект
        RangeResolver.Resolve(new ByteRange(null, -10), null, Etag, 6)
            .Should().Be(new AppliedByteRange(0, 5, 6));
        RangeResolver.Resolve(new ByteRange(null, -6), null, Etag, 6)
            .Should().Be(new AppliedByteRange(0, 5, 6));
    }

    [Fact]
    public void StartOutOfBounds_InvalidRange()
    {
        // Arrange / Act
        var act = () => RangeResolver.Resolve(new ByteRange(6, null), null, Etag, 6);

        // Assert: start >= size → 416
        act.Should().Throw<ObjectStoreException>()
            .Where(ex => ex.Code == ObjectStoreErrorCode.InvalidRange);
    }

    [Fact]
    public void EmptyObject_WithRange_InvalidRange()
    {
        // Arrange / Act
        var act = () => RangeResolver.Resolve(new ByteRange(0, null), null, Etag, 0);

        // Assert: пустой объект не имеет валидных диапазонов
        act.Should().Throw<ObjectStoreException>()
            .Where(ex => ex.Code == ObjectStoreErrorCode.InvalidRange);
    }

    [Fact]
    public void NoRange_FullObject()
    {
        // Arrange / Act / Assert: Range отсутствует → null (200 полным)
        RangeResolver.Resolve(null, null, Etag, 6).Should().BeNull();
    }

    [Fact]
    public void IfRange_Match_AppliesRange()
    {
        // Arrange / Act
        var applied = RangeResolver.Resolve(new ByteRange(2, 3), $"\"{Etag}\"", Etag, 6);

        // Assert: сильный ETag совпал — Range применяется
        applied.Should().Be(new AppliedByteRange(2, 3, 6));
    }

    [Fact]
    public void IfRange_Mismatch_FullObject()
    {
        // Arrange / Act / Assert: If-Range не совпал → 200 полным
        RangeResolver.Resolve(new ByteRange(2, 3), "\"other\"", Etag, 6).Should().BeNull();
    }

    [Fact]
    public void IfRange_DateForm_TreatedAsMismatch()
    {
        // Arrange: дата-форма If-Range — трактуется как несовпадение (206 не делаем)
        var ifRangeDate = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero).ToString("R");

        // Act / Assert
        RangeResolver.Resolve(new ByteRange(2, 3), ifRangeDate, Etag, 6).Should().BeNull();
    }

    [Fact]
    public void IfRange_Mismatch_MakesEvenInvalidRange_Harmless()
    {
        // Arrange: If-Range не совпал — Range не применяется вовсе, невалидность
        // не проверяется (полный объект 200)
        // Act / Assert
        RangeResolver.Resolve(new ByteRange(100, null), "\"other\"", Etag, 6).Should().BeNull();
    }
}
