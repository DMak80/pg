using OwnS3.Protocol.Requests;

namespace OwnS3.UnitTests;

// Парсер path-style пути (arch/owns3/03 §6): /, /{bucket}, /{bucket}/{key+};
// сегменты ключа URL-декодируются, слэши — часть ключа.
public sealed class S3PathParserTests
{
    [Fact]
    public void Parse_Root_NoBucketNoKey()
    {
        // Arrange / Act
        var path = S3PathParser.Parse("/");
        // Assert
        path.Bucket.Should().BeNull();
        path.Key.Should().BeNull();
    }

    [Fact]
    public void Parse_BucketOnly()
    {
        // Arrange / Act
        var path = S3PathParser.Parse("/b");
        // Assert
        path.Bucket.Should().Be("b");
        path.Key.Should().BeNull();
    }

    [Fact]
    public void Parse_MultiSegmentKey_JoinedWithSlashes()
    {
        // Arrange / Act
        var path = S3PathParser.Parse("/b/k1/k2/x");
        // Assert: слэши внутри ключа сохраняются
        path.Bucket.Should().Be("b");
        path.Key.Should().Be("k1/k2/x");
    }

    [Fact]
    public void Parse_EncodedSpace_DecodedInKey()
    {
        // Arrange / Act
        var path = S3PathParser.Parse("/b/ключ%20с%20пробелом");
        // Assert
        path.Bucket.Should().Be("b");
        path.Key.Should().Be("ключ с пробелом");
    }

    [Fact]
    public void Parse_EncodedSlash_DecodesToSlashInsideKey()
    {
        // Arrange / Act: %2F в сегменте — декодируется в '/' внутри ключа
        var path = S3PathParser.Parse("/b/a%2Fb");
        // Assert
        path.Bucket.Should().Be("b");
        path.Key.Should().Be("a/b");
    }

    [Fact]
    public void Parse_TrailingSlash_KeptInKey()
    {
        // Arrange / Act: хвостовой слэш — часть ключа (пустой последний сегмент)
        var path = S3PathParser.Parse("/b/k/");
        // Assert
        path.Bucket.Should().Be("b");
        path.Key.Should().Be("k/");
    }
}
