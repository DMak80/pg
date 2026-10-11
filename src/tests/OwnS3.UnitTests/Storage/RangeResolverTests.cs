using OwnS3.Storage;

namespace OwnS3.UnitTests.Storage;

// Резолв одиночного Range канона 02 §1: a-b / a- / -N, обрезка end, суффикс,
// пустой объект.
public class RangeResolverTests
{
    [Fact]
    public void ClosedRange_Applied()
    {
        // Arrange / Act
        var applied = RangeResolver.Resolve(new ByteRange(2, 3), 6);

        // Assert
        applied.Should().Be(new AppliedByteRange(2, 3, 6));
    }

    [Fact]
    public void OpenEndRange_ClampedToSizeMinusOne()
    {
        // Arrange: bytes=2- (End=null) / end за размером — обрезан
        // Act / Assert
        RangeResolver.Resolve(new ByteRange(2, null), 6)
            .Should().Be(new AppliedByteRange(2, 5, 6));
        RangeResolver.Resolve(new ByteRange(2, 100), 6)
            .Should().Be(new AppliedByteRange(2, 5, 6));
    }

    [Fact]
    public void SuffixRange_LastNBytes()
    {
        // Arrange: ParseRange кодирует bytes=-N как End=-N
        // Act / Assert
        RangeResolver.Resolve(new ByteRange(null, -2), 6)
            .Should().Be(new AppliedByteRange(4, 5, 6));
    }

    [Fact]
    public void SuffixRange_LongerThanObject_ReturnsWholeObject()
    {
        // Arrange / Act / Assert: N >= size → весь объект
        RangeResolver.Resolve(new ByteRange(null, -10), 6)
            .Should().Be(new AppliedByteRange(0, 5, 6));
        RangeResolver.Resolve(new ByteRange(null, -6), 6)
            .Should().Be(new AppliedByteRange(0, 5, 6));
    }

    [Fact]
    public void StartOutOfBounds_InvalidRange()
    {
        // Arrange / Act
        var act = () => RangeResolver.Resolve(new ByteRange(6, null), 6);

        // Assert: start >= size → 416
        act.Should().Throw<ObjectStoreException>()
            .Where(ex => ex.Code == ObjectStoreErrorCode.InvalidRange);
    }

    [Fact]
    public void EmptyObject_WithRange_InvalidRange()
    {
        // Arrange / Act
        var act = () => RangeResolver.Resolve(new ByteRange(0, null), 0);

        // Assert: пустой объект не имеет валидных диапазонов
        act.Should().Throw<ObjectStoreException>()
            .Where(ex => ex.Code == ObjectStoreErrorCode.InvalidRange);
    }

    [Fact]
    public void NoRange_FullObject()
    {
        // Arrange / Act / Assert: Range отсутствует → null (200 полным)
        RangeResolver.Resolve(null, 6).Should().BeNull();
    }
}
