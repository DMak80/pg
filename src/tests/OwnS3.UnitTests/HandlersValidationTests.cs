using OwnS3.App.Handlers;
using OwnS3.Protocol.Errors;

namespace OwnS3.UnitTests;

// Чистые валидации аргументов хендлеров (глава 02): partNumber, лимиты
// Delete/Complete, encoding-type, пагинация, metadata-directive,
// object-attributes, EntityTooLarge.
public sealed class HandlersValidationTests
{
    // — partNumber (UploadPart/UploadPartCopy) —

    [Theory]
    [InlineData(null)]
    [InlineData("abc")]
    [InlineData("0")]
    [InlineData("10001")]
    [InlineData("-5")]
    public void ParsePartNumber_Invalid_ThrowsInvalidArgument(string? value)
    {
        // Arrange / Act
        var act = () => OperationValidation.ParsePartNumber(value);

        // Assert
        act.Should().Throw<S3ProtocolException>().Which.Code.Should().Be(S3ErrorCode.InvalidArgument);
    }

    [Theory]
    [InlineData("1")]
    [InlineData("10000")]
    public void ParsePartNumber_Boundaries_Ok(string value)
    {
        // Arrange / Act / Assert
        OperationValidation.ParsePartNumber(value).Should().Be(int.Parse(value));
    }

    // — DeleteObjects: пусто / больше 1000 → MalformedXML; 1000 — ок —

    [Theory]
    [InlineData(0)]
    [InlineData(1001)]
    public void ValidateDeleteKeys_OutOfLimits_MalformedXML(int count)
    {
        // Arrange / Act
        var act = () => OperationValidation.ValidateDeleteKeys(count);

        // Assert
        act.Should().Throw<S3ProtocolException>().Which.Code.Should().Be(S3ErrorCode.MalformedXML);
    }

    [Fact]
    public void ValidateDeleteKeys_Exactly1000_Ok()
    {
        // Arrange / Act / Assert
        OperationValidation.ValidateDeleteKeys(1000);
    }

    // — Complete-манифест: пустой → MalformedXML; нестрогий рост → InvalidPartOrder —

    [Fact]
    public void ValidateCompleteManifest_Empty_MalformedXML()
    {
        // Arrange / Act
        var act = () => OperationValidation.ValidateCompleteManifest([]);

        // Assert
        act.Should().Throw<S3ProtocolException>().Which.Code.Should().Be(S3ErrorCode.MalformedXML);
    }

    [Fact]
    public void ValidateCompleteManifest_NonAscending_InvalidPartOrder()
    {
        // Arrange: порядок 1, 3, 2 — нарушение
        var parts = new[] { (1, "\"e1\""), (3, "\"e3\""), (2, "\"e2\"") };

        // Act
        var act = () => OperationValidation.ValidateCompleteManifest(parts);

        // Assert
        act.Should().Throw<S3ProtocolException>().Which.Code.Should().Be(S3ErrorCode.InvalidPartOrder);
    }

    [Fact]
    public void ValidateCompleteManifest_StrictlyAscending_Ok()
    {
        // Arrange / Act / Assert
        OperationValidation.ValidateCompleteManifest([(1, "\"e1\""), (2, "\"e2\""), (10, "\"e10\"")]);
    }

    // — encoding-type: только url —

    [Theory]
    [InlineData(null)]
    [InlineData("url")]
    public void ParseEncodingType_Valid_Ok(string? value)
    {
        // Arrange / Act / Assert
        OperationValidation.ParseEncodingType(value).Should().Be(value);
    }

    [Fact]
    public void ParseEncodingType_Xml_InvalidArgument()
    {
        // Arrange / Act
        var act = () => OperationValidation.ParseEncodingType("xml");

        // Assert
        act.Should().Throw<S3ProtocolException>().Which.Code.Should().Be(S3ErrorCode.InvalidArgument);
    }

    // — max-keys/max-parts/max-uploads: дефолт 1000; > 1000 обрезается; < 0 — ошибка —

    [Fact]
    public void ParsePagingLimit_DefaultsAndClamps()
    {
        // Arrange / Act / Assert
        OperationValidation.ParsePagingLimit(null, "max-keys").Should().Be(1000);
        OperationValidation.ParsePagingLimit("0", "max-keys").Should().Be(0);
        OperationValidation.ParsePagingLimit("5000", "max-keys").Should().Be(1000);
    }

    [Theory]
    [InlineData("-1")]
    [InlineData("abc")]
    public void ParsePagingLimit_Invalid_Throws(string value)
    {
        // Arrange / Act
        var act = () => OperationValidation.ParsePagingLimit(value, "max-keys");

        // Assert
        act.Should().Throw<S3ProtocolException>().Which.Code.Should().Be(S3ErrorCode.InvalidArgument);
    }

    // — x-amz-metadata-directive: строго COPY/REPLACE (точное сравнение) —

    [Theory]
    [InlineData("copy")]      // нижний регистр — невалиден
    [InlineData("MERGE")]
    [InlineData("replace")]
    public void ParseMetadataDirective_StrictComparison_Throws(string value)
    {
        // Arrange / Act
        var act = () => OperationValidation.ParseMetadataDirective(value);

        // Assert
        act.Should().Throw<S3ProtocolException>().Which.Code.Should().Be(S3ErrorCode.InvalidArgument);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("COPY")]
    [InlineData("REPLACE")]
    public void ParseMetadataDirective_Valid_Ok(string? value)
    {
        // Arrange / Act / Assert
        OperationValidation.ParseMetadataDirective(value).Should().Be(value ?? "COPY");
    }

    // — x-amz-object-attributes: пустой/отсутствующий → InvalidArgument;
    // Checksum и прочие вне набора → 501 NotImplemented —

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("  ")]
    public void ParseObjectAttributes_MissingOrEmpty_InvalidArgument(string? value)
    {
        // Arrange / Act
        var act = () => ObjectHandlers.ParseObjectAttributes(value);

        // Assert
        act.Should().Throw<S3ProtocolException>().Which.Code.Should().Be(S3ErrorCode.InvalidArgument);
    }

    [Fact]
    public void ParseObjectAttributes_Checksum_NotImplemented()
    {
        // Arrange / Act
        var act = () => ObjectHandlers.ParseObjectAttributes("ETag,Checksum");

        // Assert: вне-наборная грань (глава 02 §1)
        act.Should().Throw<S3ProtocolException>().Which.Code.Should().Be(S3ErrorCode.NotImplemented);
    }

    [Fact]
    public void ParseObjectAttributes_SupportedSet_Ok()
    {
        // Arrange / Act
        var attributes = ObjectHandlers.ParseObjectAttributes("ETag,ObjectSize");

        // Assert
        attributes.Should().Equal(OwnS3.Storage.ObjectAttributeName.ETag,
            OwnS3.Storage.ObjectAttributeName.ObjectSize);
    }

    // — EntityTooLarge: 5 ГБ + 1 → ошибка; ровно 5 ГБ — ок —

    [Fact]
    public void ValidateObjectSize_Boundary()
    {
        // Arrange
        const long fiveGb = OperationValidation.MaxObjectSize;

        // Act / Assert
        OperationValidation.ValidateObjectSize(fiveGb);
        var act = () => OperationValidation.ValidateObjectSize(fiveGb + 1);
        act.Should().Throw<S3ProtocolException>().Which.Code.Should().Be(S3ErrorCode.EntityTooLarge);
    }

    // — response-* → канонические имена заголовков ответа —

    [Theory]
    [InlineData("response-cache-control", "Cache-Control")]
    [InlineData("response-content-disposition", "Content-Disposition")]
    [InlineData("response-content-encoding", "Content-Encoding")]
    [InlineData("response-content-language", "Content-Language")]
    [InlineData("response-content-type", "Content-Type")]
    [InlineData("response-expires", "Expires")]
    public void ResponseOverrideHeaderName_MapsToCanonicalHeaderNames(string queryName, string expected)
    {
        // Arrange / Act
        var header = ObjectHandlers.ResponseOverrideHeaderName(queryName);

        // Assert: в ответе ставятся канонические имена, не «response-*»
        header.Should().Be(expected);
    }

    [Theory]
    [InlineData("response-x-custom")]
    [InlineData("response-foo")]
    public void ResponseOverrideHeaderName_UnknownResponseParameters_Ignored(string queryName)
    {
        // Arrange / Act
        var header = ObjectHandlers.ResponseOverrideHeaderName(queryName);

        // Assert: прочие response-* не отображаются в заголовки — игнорируются
        header.Should().BeNull();
    }

    // — copy-source / copy-source-range —

    [Fact]
    public void ParseCopySource_Valid_Decoded()
    {
        // Arrange / Act
        var (bucket, key) = OperationValidation.ParseCopySource("/src-b/k%20ey");

        // Assert
        bucket.Should().Be("src-b");
        key.Should().Be("k ey");
    }

    [Theory]
    [InlineData("no-slash")]
    [InlineData("/onlybucket")]
    [InlineData(null)]
    public void ParseCopySource_Invalid_Throws(string? value)
    {
        // Arrange / Act
        var act = () => OperationValidation.ParseCopySource(value);

        // Assert
        act.Should().Throw<S3ProtocolException>().Which.Code.Should().Be(S3ErrorCode.InvalidArgument);
    }

    [Fact]
    public void ParseCopySourceRange_Valid_AndInvalid()
    {
        // Arrange / Act / Assert: валидный диапазон
        OperationValidation.ParseCopySourceRange("bytes=0-99").Should().Be(
            new OwnS3.Storage.ByteRange(0, 99));
        // невалидный синтаксис и a > b — один код InvalidArgument (глава 02)
        var act1 = () => OperationValidation.ParseCopySourceRange("bytes=99-0");
        var act2 = () => OperationValidation.ParseCopySourceRange("chunk=0-99");
        act1.Should().Throw<S3ProtocolException>().Which.Code.Should().Be(S3ErrorCode.InvalidArgument);
        act2.Should().Throw<S3ProtocolException>().Which.Code.Should().Be(S3ErrorCode.InvalidArgument);
        // отсутствие — нет диапазона (весь источник)
        OperationValidation.ParseCopySourceRange(null).Should().BeNull();
    }
}
