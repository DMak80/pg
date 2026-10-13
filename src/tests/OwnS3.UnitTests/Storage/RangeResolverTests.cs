using OwnS3.Storage;

namespace OwnS3.UnitTests.Storage;

// Резолв одиночного Range канона 02 §1: a-b / a- / -N, обрезка end, суффикс,
// пустой объект. Исходы — Result-конвенция: валидный диапазон/«не задан» —
// Success, некорректный — Failed(InvalidRange).
public class RangeResolverTests
{
    [Fact]
    public void ClosedRange_Applied()
    {
        // Act
        var applied = RangeResolver.Resolve(new ByteRange(2, 3), 6).Throw().Value;

        // Assert
        applied.Should().Be(new AppliedByteRange(2, 3, 6));
    }

    [Fact]
    public void OpenEndRange_ClampedToSizeMinusOne()
    {
        // Arrange: bytes=2- (End=null) / end за размером — обрезан
        // Act / Assert
        RangeResolver.Resolve(new ByteRange(2, null), 6).Throw().Value
            .Should().Be(new AppliedByteRange(2, 5, 6));
        RangeResolver.Resolve(new ByteRange(2, 100), 6).Throw().Value
            .Should().Be(new AppliedByteRange(2, 5, 6));
    }

    [Fact]
    public void SuffixRange_LastNBytes()
    {
        // Arrange: ParseRange кодирует bytes=-N как End=-N
        // Act / Assert
        RangeResolver.Resolve(new ByteRange(null, -2), 6).Throw().Value
            .Should().Be(new AppliedByteRange(4, 5, 6));
    }

    [Fact]
    public void SuffixRange_LongerThanObject_ReturnsWholeObject()
    {
        // Act / Assert: N >= size → весь объект
        RangeResolver.Resolve(new ByteRange(null, -10), 6).Throw().Value
            .Should().Be(new AppliedByteRange(0, 5, 6));
        RangeResolver.Resolve(new ByteRange(null, -6), 6).Throw().Value
            .Should().Be(new AppliedByteRange(0, 5, 6));
    }

    [Fact]
    public void StartOutOfBounds_FailedInvalidRange()
    {
        // Act
        var result = RangeResolver.Resolve(new ByteRange(6, null), 6);

        // Assert: start >= size → 416 (Failed с кодом)
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<ObjectStoreException>()
            .Which.Code.Should().Be(ObjectStoreErrorCode.InvalidRange);
    }

    [Fact]
    public void Resolve_ПустойОбъект_FailedInvalidRange()
    {
        // Act
        var result = RangeResolver.Resolve(new ByteRange(0, null), size: 0);

        // Assert — пустой объект не имеет валидных диапазонов
        (!result.IsSuccess).Should().BeTrue();
        result.Error.Should().BeOfType<ObjectStoreException>()
            .Which.Code.Should().Be(ObjectStoreErrorCode.InvalidRange);
    }

    [Fact]
    public void Resolve_RangeНеЗадан_SuccessNull_ДиапазонНеПрименяется()
    {
        // Act
        var result = RangeResolver.Resolve(null, size: 10);

        // Assert — «Range не задан» — терминальное знание, не отказ
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().BeNull();
    }
}
