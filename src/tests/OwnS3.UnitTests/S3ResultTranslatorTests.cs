using OwnS3.App.Pipeline;
using OwnS3.Protocol.Errors;
using OwnS3.Storage;

namespace OwnS3.UnitTests;

// Трансляция Result.Failed в S3-ошибки (конвенция главы 01 §4, каталог 03 §5).
public sealed class S3ResultTranslatorTests
{
    [Theory]
    [InlineData(ObjectStoreErrorCode.NoSuchBucket, S3ErrorCode.NoSuchBucket)]
    [InlineData(ObjectStoreErrorCode.NoSuchKey, S3ErrorCode.NoSuchKey)]
    [InlineData(ObjectStoreErrorCode.BucketNotEmpty, S3ErrorCode.BucketNotEmpty)]
    [InlineData(ObjectStoreErrorCode.InvalidRange, S3ErrorCode.InvalidRange)]
    [InlineData(ObjectStoreErrorCode.InsufficientReadQuorum, S3ErrorCode.InsufficientReadQuorum)]
    [InlineData(ObjectStoreErrorCode.PreconditionFailed, S3ErrorCode.PreconditionFailed)]
    public void Translate_ОшибкиКаталога_S3КодПоИмени(ObjectStoreErrorCode code, S3ErrorCode expected)
    {
        // Act
        var error = S3ResultTranslator.Translate(new ObjectStoreException(code), "/b/k", "req", "host");
        // Assert
        error.Code.Should().Be(expected);
        error.Info.HttpStatus.Should().BePositive();
    }

    [Fact]
    public void Translate_XlInvalidArgument_400InvalidArgument()
    {
        // Act
        var error = S3ResultTranslator.Translate(new XlInvalidArgumentException("лимит сегмента"), "/b/k", "req", "host");
        // Assert
        error.Code.Should().Be(S3ErrorCode.InvalidArgument);
        error.Info.HttpStatus.Should().Be(400);
    }

    [Fact]
    public void Translate_ПрочаяОшибка_500InternalError()
    {
        // Act
        var error = S3ResultTranslator.Translate(new InvalidOperationException("x"), "/b/k", "req", "host");
        // Assert
        error.Code.Should().Be(S3ErrorCode.InternalError);
        error.Info.HttpStatus.Should().Be(500);
    }
}
