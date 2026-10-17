using Microsoft.AspNetCore.Http;
using OwnS3.App.Handlers;
using OwnS3.Protocol.Errors;

namespace OwnS3.UnitTests;

// Чистые валидации аргументов хендлеров (глава 02): partNumber, лимиты
// Delete/Complete, encoding-type, пагинация, metadata-directive,
// object-attributes, EntityTooLarge.
public sealed class HandlersValidationTests
{
    // — partNumber (UploadPart) —

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

    // — x-amz-checksum-* PutObject: парсер ровно одного заголовка семейства —

    private static IHeaderDictionary Headers(params (string Name, string Value)[] pairs)
    {
        var headers = new HeaderDictionary();
        foreach (var (name, value) in pairs)
            headers[name] = value;
        return headers;
    }

    // Валидное значение каждого алгоритма → ожидание с тем же алгоритмом и значением.
    [Theory]
    [InlineData("x-amz-checksum-crc32",  "crc32",  "Ox7nCg==")]                  // вектор 6: crc32(TestVectors.PutBody)
    [InlineData("x-amz-checksum-crc32c", "crc32c", "4waSgw==")]                  // эталон RFC 3720 (big-endian)
    [InlineData("x-amz-checksum-sha1",   "sha1",   "2jmj7l5rSw0yVb/vlWAYkK/YBwk=")] // sha1("")
    [InlineData("x-amz-checksum-sha256", "sha256", "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=")] // sha256("")
    public void ParseChecksumHeader_SingleKnown_ReturnsExpectation(string header, string algorithm, string value)
    {
        // Arrange / Act
        var expectation = OperationValidation.ParseChecksumHeader(Headers((header, value)));

        // Assert: алгоритм и base64-значение распознаны (CRC-конвенция — big-endian)
        expectation.Should().NotBeNull();
        expectation!.Algorithm.ToString().ToLowerInvariant().Should().Be(algorithm);
        expectation.ExpectedBase64.Should().Be(value);
    }

    // Не-base64 → InvalidArgument (каждый алгоритм); base64 неверной длины
    // (4/4/20/32 нарушены) → InvalidArgument (каждый алгоритм).
    [Theory]
    [InlineData("x-amz-checksum-crc32",  "!!!not-base64!!!")]
    [InlineData("x-amz-checksum-crc32c", "!!!not-base64!!!")]
    [InlineData("x-amz-checksum-sha1",   "!!!not-base64!!!")]
    [InlineData("x-amz-checksum-sha256", "!!!not-base64!!!")]
    [InlineData("x-amz-checksum-crc32",  "AAAA")]        // 2 байта вместо 4
    [InlineData("x-amz-checksum-crc32c", "AAAAAAAA")]    // 6 байт вместо 4
    [InlineData("x-amz-checksum-sha1",   "Ox7nCg==")]    // 4 байта вместо 20
    [InlineData("x-amz-checksum-sha256", "AAAA")]        // 2 байта вместо 32
    public void ParseChecksumHeader_InvalidFormat_ThrowsInvalidArgument(string header, string value)
    {
        // Arrange / Act
        var act = () => OperationValidation.ParseChecksumHeader(Headers((header, value)));

        // Assert: формат-отказ с фактическим именем алгоритма в сообщении
        act.Should().Throw<S3ProtocolException>()
            .Which.Code.Should().Be(S3ErrorCode.InvalidArgument);
        act.Should().Throw<S3ProtocolException>()
            .Which.Message.Should().Be($"Value for {header} header is invalid.");
    }

    // Два заголовка семейства → InvalidArgument «Invalid checksum provided.» —
    // включая пару известный+неизвестный (семейство, а не известность).
    [Theory]
    [InlineData("x-amz-checksum-sha256", "x-amz-checksum-crc32")]
    [InlineData("x-amz-checksum-sha256", "x-amz-checksum-crc64nvme")]
    public void ParseChecksumHeader_MultipleFamilyHeaders_ThrowsInvalidArgument(string first, string second)
    {
        // Arrange / Act
        var act = () => OperationValidation.ParseChecksumHeader(
            Headers((first, "Ox7nCg=="), (second, "Ox7nCg==")));

        // Assert: текст ErrInvalidChecksum референса
        act.Should().Throw<S3ProtocolException>()
            .Which.Code.Should().Be(S3ErrorCode.InvalidArgument);
        act.Should().Throw<S3ProtocolException>()
            .Which.Message.Should().Be("Invalid checksum provided.");
    }

    // Единственный НЕизвестный алгоритм → null (игнор); пустой набор → null;
    // x-amz-sdk-checksum-algorithm на исход не влияет (не семейство).
    [Fact]
    public void ParseChecksumHeader_UnknownSingleOrNullSet_ReturnsNull()
    {
        // Arrange / Act / Assert: единственный crc64nvme — молчаливый игнор
        OperationValidation.ParseChecksumHeader(Headers(("x-amz-checksum-crc64nvme", "Ox7nCg==")))
            .Should().BeNull();
        // пустой набор
        OperationValidation.ParseChecksumHeader(Headers()).Should().BeNull();
        // sdk-подсказка не считается заголовком семейства и не мешает известному
        var expectation = OperationValidation.ParseChecksumHeader(
            Headers(("x-amz-sdk-checksum-algorithm", "SHA256"), ("x-amz-checksum-crc32", "Ox7nCg==")));
        expectation!.Algorithm.Should().Be(ChecksumAlgorithm.Crc32);
        expectation.ExpectedBase64.Should().Be("Ox7nCg==");
    }
}
