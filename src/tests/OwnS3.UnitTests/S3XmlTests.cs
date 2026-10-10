using OwnS3.Protocol.Errors;
using OwnS3.Protocol.Xml;

namespace OwnS3.UnitTests;

// XML-схемы (arch/owns3/03 §4): побайтовые эталоны всех 12 ответных схем,
// спецсимволы — сущности, недопустимые кодпоинты — числовые ссылки,
// десериализация запросных XML и MalformedXML.
public sealed class S3XmlTests
{
    private const string Ns = "http://s3.amazonaws.com/doc/2006-03-01/";
    private const string Date = "2026-01-01T00:00:00.000Z";

    [Fact]
    public void Serialize_ListBucketResult_MatchesSample()
    {
        // Arrange
        var value = new ListBucketResult
        {
            Name = "b", Prefix = "p", Marker = "m", MaxKeys = 1000, IsTruncated = false,
            Contents =
            [
                new() { Key = "k1", LastModified = Date, ETag = "\"e1\"", Size = 1 },
                new() { Key = "k2", LastModified = Date, ETag = "\"e2\"", Size = 22 },
            ],
            CommonPrefixes = [new() { Prefix = "pre/" }],
        };
        const string Expected =
            """<?xml version="1.0" encoding="utf-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>b</Name><Prefix>p</Prefix><Marker>m</Marker><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated><Contents><Key>k1</Key><LastModified>2026-01-01T00:00:00.000Z</LastModified><ETag>"e1"</ETag><Size>1</Size><StorageClass>STANDARD</StorageClass></Contents><Contents><Key>k2</Key><LastModified>2026-01-01T00:00:00.000Z</LastModified><ETag>"e2"</ETag><Size>22</Size><StorageClass>STANDARD</StorageClass></Contents><CommonPrefixes><Prefix>pre/</Prefix></CommonPrefixes></ListBucketResult>""";

        // Act
        var actual = S3Xml.Serialize(value);

        // Assert
        actual.Should().Be(Expected);
    }

    [Fact]
    public void Serialize_ListVersionsResult_MatchesSample()
    {
        // Arrange: unversioned-вид — KeyMarker/VersionIdMarker пустые, без VersionId/IsLatest
        var value = new ListVersionsResult
        {
            Name = "b", Prefix = "p", MaxKeys = 1000, IsTruncated = false,
            Versions = [new() { Key = "v1", LastModified = Date, ETag = "\"e1\"", Size = 3 }],
        };
        const string Expected =
            """<?xml version="1.0" encoding="utf-8"?><ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>b</Name><Prefix>p</Prefix><KeyMarker /><VersionIdMarker /><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated><Version><Key>v1</Key><LastModified>2026-01-01T00:00:00.000Z</LastModified><ETag>"e1"</ETag><Size>3</Size><StorageClass>STANDARD</StorageClass></Version></ListVersionsResult>""";

        // Act
        var actual = S3Xml.Serialize(value);

        // Assert
        actual.Should().Be(Expected);
    }

    [Fact]
    public void Serialize_DeleteResult_MatchesSample()
    {
        // Arrange
        var value = new DeleteResult
        {
            Deleted = [new() { Key = "d1" }],
            Errors = [new() { Key = "e1", Code = "InternalError", Message = "oops" }],
        };
        const string Expected =
            """<?xml version="1.0" encoding="utf-8"?><DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Deleted><Key>d1</Key></Deleted><Error><Key>e1</Key><Code>InternalError</Code><Message>oops</Message></Error></DeleteResult>""";

        // Act
        var actual = S3Xml.Serialize(value);

        // Assert
        actual.Should().Be(Expected);
    }

    [Fact]
    public void Serialize_InitiateMultipartUpload_MatchesCanonicalSample()
    {
        // Arrange: экземпляр с фиксированными значениями + эталон по образцу arch/owns3/03 §4
        var value = new InitiateMultipartUploadResult { Bucket = "b", Key = "k", UploadId = "u" };
        const string Expected =
            """<?xml version="1.0" encoding="utf-8"?><InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>b</Bucket><Key>k</Key><UploadId>u</UploadId></InitiateMultipartUploadResult>""";

        // Act
        var actual = S3Xml.Serialize(value);

        // Assert: побайтовое совпадение с эталоном канона (без пробелов между элементами)
        actual.Should().Be(Expected);
    }

    [Fact]
    public void Serialize_CompleteMultipartUploadResult_MatchesSample()
    {
        // Arrange
        var value = new CompleteMultipartUploadResult
            { Location = "http://host/bucket/key", Bucket = "bucket", Key = "key", ETag = "\"2-md5\"" };
        const string Expected =
            """<?xml version="1.0" encoding="utf-8"?><CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Location>http://host/bucket/key</Location><Bucket>bucket</Bucket><Key>key</Key><ETag>"2-md5"</ETag></CompleteMultipartUploadResult>""";

        // Act
        var actual = S3Xml.Serialize(value);

        // Assert
        actual.Should().Be(Expected);
    }

    [Fact]
    public void Serialize_ListPartsResult_MatchesSample()
    {
        // Arrange
        var value = new ListPartsResult
        {
            Bucket = "b", Key = "k", UploadId = "u", PartNumberMarker = 0, MaxParts = 1000, IsTruncated = false,
            Parts = [new() { PartNumber = 1, LastModified = Date, ETag = "\"e1\"", Size = 5 }],
            Initiator = new() { ID = "i", DisplayName = "n" },
            Owner = new() { ID = "o", DisplayName = "n2" },
        };
        const string Expected =
            """<?xml version="1.0" encoding="utf-8"?><ListPartsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>b</Bucket><Key>k</Key><UploadId>u</UploadId><PartNumberMarker>0</PartNumberMarker><MaxParts>1000</MaxParts><IsTruncated>false</IsTruncated><Part><PartNumber>1</PartNumber><LastModified>2026-01-01T00:00:00.000Z</LastModified><ETag>"e1"</ETag><Size>5</Size></Part><Initiator><ID>i</ID><DisplayName>n</DisplayName></Initiator><Owner><ID>o</ID><DisplayName>n2</DisplayName></Owner><StorageClass>STANDARD</StorageClass></ListPartsResult>""";

        // Act
        var actual = S3Xml.Serialize(value);

        // Assert
        actual.Should().Be(Expected);
    }

    [Fact]
    public void Serialize_ListMultipartUploadsResult_MatchesSample()
    {
        // Arrange
        var value = new ListMultipartUploadsResult
        {
            Bucket = "b", KeyMarker = "", UploadIdMarker = "", MaxUploads = 1000, IsTruncated = false,
            Uploads = [new() { Key = "u1", UploadId = "id", Initiated = Date, Owner = new() { ID = "o", DisplayName = "n" } }],
            CommonPrefixes = [new() { Prefix = "pre/" }],
        };
        const string Expected =
            """<?xml version="1.0" encoding="utf-8"?><ListMultipartUploadsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>b</Bucket><KeyMarker /><UploadIdMarker /><MaxUploads>1000</MaxUploads><IsTruncated>false</IsTruncated><Upload><Key>u1</Key><UploadId>id</UploadId><Initiated>2026-01-01T00:00:00.000Z</Initiated><StorageClass>STANDARD</StorageClass><Owner><ID>o</ID><DisplayName>n</DisplayName></Owner></Upload><CommonPrefixes><Prefix>pre/</Prefix></CommonPrefixes></ListMultipartUploadsResult>""";

        // Act
        var actual = S3Xml.Serialize(value);

        // Assert
        actual.Should().Be(Expected);
    }

    [Fact]
    public void Serialize_ListAllMyBucketsResult_MatchesSample()
    {
        // Arrange: Owner-заполнитель ownS3, бакеты по алфавиту
        var value = new ListAllMyBucketsResult
        {
            Owner = new() { ID = "owns3", DisplayName = "owns3" },
            Buckets =
            [
                new() { Name = "alpha", CreationDate = Date },
                new() { Name = "beta", CreationDate = Date },
            ],
        };
        const string Expected =
            """<?xml version="1.0" encoding="utf-8"?><ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Owner><ID>owns3</ID><DisplayName>owns3</DisplayName></Owner><Buckets><Bucket><Name>alpha</Name><CreationDate>2026-01-01T00:00:00.000Z</CreationDate></Bucket><Bucket><Name>beta</Name><CreationDate>2026-01-01T00:00:00.000Z</CreationDate></Bucket></Buckets></ListAllMyBucketsResult>""";

        // Act
        var actual = S3Xml.Serialize(value);

        // Assert
        actual.Should().Be(Expected);
    }

    [Fact]
    public void Serialize_LocationConstraint_EmptyElement()
    {
        // Arrange / Act
        var actual = S3Xml.Serialize(new LocationConstraint());

        // Assert: пустой элемент (= us-east-1)
        actual.Should().Be(
            """<?xml version="1.0" encoding="utf-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/" />""");
    }

    [Fact]
    public void Serialize_CopyObjectResult_MatchesSample()
    {
        // Arrange
        var value = new CopyObjectResult { LastModified = Date, ETag = "\"e1\"" };
        const string Expected =
            """<?xml version="1.0" encoding="utf-8"?><CopyObjectResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><LastModified>2026-01-01T00:00:00.000Z</LastModified><ETag>"e1"</ETag></CopyObjectResult>""";

        // Act
        var actual = S3Xml.Serialize(value);

        // Assert
        actual.Should().Be(Expected);
    }

    [Fact]
    public void Serialize_CopyPartResult_MatchesSample()
    {
        // Arrange
        var value = new CopyPartResult { LastModified = Date, ETag = "\"e1\"" };
        const string Expected =
            """<?xml version="1.0" encoding="utf-8"?><CopyPartResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><LastModified>2026-01-01T00:00:00.000Z</LastModified><ETag>"e1"</ETag></CopyPartResult>""";

        // Act
        var actual = S3Xml.Serialize(value);

        // Assert
        actual.Should().Be(Expected);
    }

    [Fact]
    public void Serialize_GetObjectAttributesOutput_MatchesSample()
    {
        // Arrange
        var value = new GetObjectAttributesOutput
        {
            ETag = "\"e1\"", ObjectSize = 5, StorageClass = "STANDARD",
            ObjectParts = new ObjectParts
            {
                PartsCount = 1, PartNumberMarker = 0, MaxParts = 1000, IsTruncated = false,
                Parts = [new() { PartNumber = 1, Size = 5 }],
            },
        };
        const string Expected =
            """<?xml version="1.0" encoding="utf-8"?><GetObjectAttributesOutput xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><ETag>"e1"</ETag><ObjectSize>5</ObjectSize><StorageClass>STANDARD</StorageClass><ObjectParts><PartsCount>1</PartsCount><PartNumberMarker>0</PartNumberMarker><MaxParts>1000</MaxParts><IsTruncated>false</IsTruncated><Part><PartNumber>1</PartNumber><Size>5</Size></Part></ObjectParts></GetObjectAttributesOutput>""";

        // Act
        var actual = S3Xml.Serialize(value);

        // Assert
        actual.Should().Be(Expected);
    }

    [Fact]
    public void Serialize_SpecialCharacters_AreEntities()
    {
        // Arrange: ключ со спецсимволами — сущности, не числовые ссылки
        var value = new ListBucketResult
        {
            Name = "b", Prefix = "", MaxKeys = 0,
            Contents = [new() { Key = "a<b&c", LastModified = Date, ETag = "\"e\"", Size = 0 }],
        };

        // Act
        var actual = S3Xml.Serialize(value);

        // Assert
        actual.Should().Contain("<Key>a&lt;b&amp;c</Key>");
    }

    [Fact]
    public void Serialize_InvalidCodePoints_AreNumericReferences()
    {
        // Arrange: control-символы недопустимы в XML 1.0 — числовые ссылки
        var listBucket = new ListBucketResult
        {
            Name = "b", Prefix = "", MaxKeys = 0,
            Contents = [new() { Key = "key\u0001x", LastModified = Date, ETag = "\"e\"", Size = 0 }],
        };
        var versions = new ListVersionsResult
        {
            Name = "b", Prefix = "", MaxKeys = 0,
            Versions = [new() { Key = "k\u000Cy", LastModified = Date, ETag = "\"e\"", Size = 0 }],
        };

        // Act
        var listBucketXml = S3Xml.Serialize(listBucket);
        var versionsXml = S3Xml.Serialize(versions);

        // Assert: сериализация не бросает, кодпоинты — числовыми ссылками
        listBucketXml.Should().Contain("<Key>key&#x1;x</Key>");
        versionsXml.Should().Contain("<Key>k&#xC;y</Key>");
    }

    [Fact]
    public void Deserialize_DeleteRequest_ParsesKeysAndQuiet()
    {
        // Arrange
        const string Xml =
            """<?xml version="1.0" encoding="utf-8"?><Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Object><Key>k1</Key></Object><Object><Key>k2</Key></Object><Object><Key>k3</Key></Object><Quiet>true</Quiet></Delete>""";

        // Act
        var parsed = S3Xml.Deserialize<DeleteRequest>(Xml);

        // Assert
        parsed.Objects.Select(o => o.Key).Should().Equal("k1", "k2", "k3");
        parsed.Quiet.Should().BeTrue();
    }

    [Fact]
    public void Deserialize_CompleteMultipartUploadRequest_ParsesParts()
    {
        // Arrange
        const string Xml =
            """<?xml version="1.0" encoding="utf-8"?><CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Part><PartNumber>1</PartNumber><ETag>"e1"</ETag></Part><Part><PartNumber>2</PartNumber><ETag>"e2"</ETag></Part></CompleteMultipartUpload>""";

        // Act
        var parsed = S3Xml.Deserialize<CompleteMultipartUploadRequest>(Xml);

        // Assert
        parsed.Parts.Should().HaveCount(2);
        parsed.Parts[0].PartNumber.Should().Be(1);
        parsed.Parts[1].ETag.Should().Be("\"e2\"");
    }

    [Fact]
    public void Deserialize_BrokenXml_MalformedXML()
    {
        // Arrange / Act
        var act = () => S3Xml.Deserialize<DeleteRequest>("<not-xml");

        // Assert
        act.Should().Throw<S3ProtocolException>()
            .Which.Code.Should().Be(S3ErrorCode.MalformedXML);
    }
}
