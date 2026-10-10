using OwnS3.Protocol.Errors;
using OwnS3.Protocol.Xml;

namespace OwnS3.UnitTests;

// Каталог ошибок (arch/owns3/03 §5, вкл. InvalidAccessKeyId): код ↔ статус ↔
// канонический Message для всех 25 значений; S3ErrorXmlWriter; MessageOverride.
public sealed class S3ErrorTests
{
    [Theory]
    [InlineData(S3ErrorCode.NoSuchBucket, 404)]
    [InlineData(S3ErrorCode.NoSuchKey, 404)]
    [InlineData(S3ErrorCode.BucketAlreadyExists, 409)]
    [InlineData(S3ErrorCode.BucketAlreadyOwnedByYou, 409)]
    [InlineData(S3ErrorCode.BucketNotEmpty, 409)]
    [InlineData(S3ErrorCode.InvalidRange, 416)]
    [InlineData(S3ErrorCode.PreconditionFailed, 412)]
    [InlineData(S3ErrorCode.NotModified, 304)]
    [InlineData(S3ErrorCode.EntityTooLarge, 400)]
    [InlineData(S3ErrorCode.InvalidPart, 400)]
    [InlineData(S3ErrorCode.InvalidPartOrder, 400)]
    [InlineData(S3ErrorCode.MalformedXML, 400)]
    [InlineData(S3ErrorCode.AuthorizationHeaderMalformed, 400)]
    [InlineData(S3ErrorCode.AuthorizationQueryParametersError, 400)]
    [InlineData(S3ErrorCode.SignatureDoesNotMatch, 403)]
    [InlineData(S3ErrorCode.InvalidAccessKeyId, 403)]
    [InlineData(S3ErrorCode.AccessDenied, 403)]
    [InlineData(S3ErrorCode.RequestTimeTooSkewed, 403)]
    [InlineData(S3ErrorCode.BadDigest, 400)]
    [InlineData(S3ErrorCode.NoSuchUpload, 404)]
    [InlineData(S3ErrorCode.InvalidArgument, 400)]
    [InlineData(S3ErrorCode.InvalidBucketName, 400)]
    [InlineData(S3ErrorCode.InvalidRequest, 400)]
    [InlineData(S3ErrorCode.NotImplemented, 501)]
    [InlineData(S3ErrorCode.InternalError, 500)]
    public void Catalog_MapsCodeToStatus(S3ErrorCode code, int expectedStatus)
    {
        // Arrange / Act
        var info = S3ErrorCatalog.Get(code);

        // Assert: статус точно по таблице §5; канонический Message непуст
        info.HttpStatus.Should().Be(expectedStatus);
        info.Code.Should().Be(code.ToString());
        info.Message.Should().NotBeNullOrWhiteSpace();
    }

    [Fact]
    public void Catalog_NotImplementedMessage_IsCanonical()
    {
        // Arrange / Act
        var info = S3ErrorCatalog.Get(S3ErrorCode.NotImplemented);

        // Assert: Message фиксирован каноном главы 02 §1
        info.Message.Should().Be("A header you provided implies functionality that is not implemented");
    }

    [Fact]
    public void Catalog_InvalidAccessKeyIdMessage_IsCanonical()
    {
        // Arrange / Act
        var info = S3ErrorCatalog.Get(S3ErrorCode.InvalidAccessKeyId);

        // Assert: Message из арх-правки 1 (глава 03 §1)
        info.Message.Should().Be("The AWS access key Id you provided does not exist in our records.");
    }

    [Fact]
    public void ErrorXmlWriter_NoSuchKey_FullLiteral()
    {
        // Arrange: образец §5 главы 03
        var error = new S3Error(S3ErrorCode.NoSuchKey,
            Resource: "/bucket/key",
            RequestId: "3fa85f64-5717-4562-b3fc-2c963f66afa6",
            HostId: "owns3-1");

        // Act
        var xml = S3ErrorXmlWriter.Write(error);

        // Assert: канонический формат всех пяти элементов
        xml.Should().Be(
            "<?xml version=\"1.0\" encoding=\"utf-8\"?><Error><Code>NoSuchKey</Code>" +
            "<Message>The specified key does not exist.</Message>" +
            "<Resource>/bucket/key</Resource>" +
            "<RequestId>3fa85f64-5717-4562-b3fc-2c963f66afa6</RequestId>" +
            "<HostId>owns3-1</HostId></Error>");
    }

    [Fact]
    public void ErrorXmlWriter_MessageOverride_ReplacesCanonicalMessage()
    {
        // Arrange: override подменяет канонический Message (исходы с деталями)
        var error = new S3Error(S3ErrorCode.InvalidArgument, MessageOverride: "Unsupported request");

        // Act
        var xml = S3ErrorXmlWriter.Write(error);

        // Assert
        xml.Should().Contain("<Message>Unsupported request</Message>")
            .And.Contain("<Code>InvalidArgument</Code>");
    }
}
