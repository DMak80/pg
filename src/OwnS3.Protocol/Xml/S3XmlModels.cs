using System.Xml.Serialization;

namespace OwnS3.Protocol.Xml;

// XML-схемы ownS3 (arch/owns3/03 §4): namespace единый, дат-поля — строки
// (форматирование — уровень хендлера), XmlSerializer-атрибуты; порядок
// свойств = порядок элементов ответа.
public static class S3XmlNamespace
{
    public const string Value = "http://s3.amazonaws.com/doc/2006-03-01/";
}

// — запросные —

[XmlRoot("Delete", Namespace = S3XmlNamespace.Value)]
public sealed class DeleteRequest
{
    [XmlElement("Object")]
    public List<DeleteObjectEntry> Objects { get; set; } = [];

    public bool Quiet { get; set; }
}

public sealed class DeleteObjectEntry
{
    public string Key { get; set; } = "";
    public string? VersionId { get; set; }   // игнорируется (versioning нет)
}

[XmlRoot("CompleteMultipartUpload", Namespace = S3XmlNamespace.Value)]
public sealed class CompleteMultipartUploadRequest
{
    [XmlElement("Part")]
    public List<ManifestPart> Parts { get; set; } = [];
}

public sealed class ManifestPart
{
    public int PartNumber { get; set; }
    public string ETag { get; set; } = "";
}

[XmlRoot("CreateBucketConfiguration", Namespace = S3XmlNamespace.Value)]
public sealed class CreateBucketConfigurationRequest
{
    public string? LocationConstraint { get; set; }   // принимается и игнорируется
}

// — ответные: листинги —

[XmlRoot("ListBucketResult", Namespace = S3XmlNamespace.Value)]
public sealed class ListBucketResult
{
    public string Name { get; set; } = "";
    public string Prefix { get; set; } = "";
    public string? Marker { get; set; }                    // v1
    public string? NextMarker { get; set; }                // v1 (при delimiter)
    public string? StartAfter { get; set; }                // V2
    public string? ContinuationToken { get; set; }         // V2
    public string? NextContinuationToken { get; set; }     // V2
    public int? KeyCount { get; set; }                     // V2
    public bool ShouldSerializeKeyCount() => KeyCount.HasValue;
    public int MaxKeys { get; set; }
    public bool IsTruncated { get; set; }
    public string? Delimiter { get; set; }
    public string? EncodingType { get; set; }

    [XmlElement("Contents")]
    public List<ContentsEntry> Contents { get; set; } = [];

    [XmlElement("CommonPrefixes")]
    public List<CommonPrefixEntry> CommonPrefixes { get; set; } = [];
}

public sealed class ContentsEntry
{
    public string Key { get; set; } = "";
    public string LastModified { get; set; } = "";
    public string ETag { get; set; } = "";
    public long Size { get; set; }
    public string StorageClass { get; set; } = "STANDARD";
    public OwnerEntry? Owner { get; set; }                 // только по fetch-owner
}

[XmlRoot("ListVersionsResult", Namespace = S3XmlNamespace.Value)]
public sealed class ListVersionsResult
{
    public string Name { get; set; } = "";
    public string Prefix { get; set; } = "";
    public string KeyMarker { get; set; } = "";            // возвращается пустым
    public string VersionIdMarker { get; set; } = "";      // возвращается пустым
    public string? NextKeyMarker { get; set; }
    public int MaxKeys { get; set; }
    public bool IsTruncated { get; set; }
    public string? Delimiter { get; set; }
    public string? EncodingType { get; set; }

    [XmlElement("Version")]
    public List<VersionEntry> Versions { get; set; } = []; // без VersionId/IsLatest/delete markers

    [XmlElement("CommonPrefixes")]
    public List<CommonPrefixEntry> CommonPrefixes { get; set; } = [];
}

public sealed class VersionEntry
{
    public string Key { get; set; } = "";
    public string LastModified { get; set; } = "";
    public string ETag { get; set; } = "";
    public long Size { get; set; }
    public string StorageClass { get; set; } = "STANDARD";
    public OwnerEntry? Owner { get; set; }
}

public sealed class CommonPrefixEntry
{
    public string Prefix { get; set; } = "";
}

// — ответные: delete / multipart —

[XmlRoot("DeleteResult", Namespace = S3XmlNamespace.Value)]
public sealed class DeleteResult
{
    [XmlElement("Deleted")]
    public List<DeletedEntry> Deleted { get; set; } = [];

    [XmlElement("Error")]
    public List<DeleteErrorEntry> Errors { get; set; } = [];
}

public sealed class DeletedEntry
{
    public string Key { get; set; } = "";
}

public sealed class DeleteErrorEntry
{
    public string Key { get; set; } = "";
    public string Code { get; set; } = "";
    public string Message { get; set; } = "";
}

[XmlRoot("InitiateMultipartUploadResult", Namespace = S3XmlNamespace.Value)]
public sealed class InitiateMultipartUploadResult
{
    public string Bucket { get; set; } = "";
    public string Key { get; set; } = "";
    public string UploadId { get; set; } = "";
}

[XmlRoot("CompleteMultipartUploadResult", Namespace = S3XmlNamespace.Value)]
public sealed class CompleteMultipartUploadResult
{
    public string Location { get; set; } = "";
    public string Bucket { get; set; } = "";
    public string Key { get; set; } = "";
    public string ETag { get; set; } = "";
}

[XmlRoot("ListPartsResult", Namespace = S3XmlNamespace.Value)]
public sealed class ListPartsResult
{
    public string Bucket { get; set; } = "";
    public string Key { get; set; } = "";
    public string UploadId { get; set; } = "";
    public int PartNumberMarker { get; set; }
    public int? NextPartNumberMarker { get; set; }
    public bool ShouldSerializeNextPartNumberMarker() => NextPartNumberMarker.HasValue;
    public int MaxParts { get; set; }
    public bool IsTruncated { get; set; }

    [XmlElement("Part")]
    public List<PartEntry> Parts { get; set; } = [];

    public OwnerEntry Initiator { get; set; } = new();
    public OwnerEntry Owner { get; set; } = new();
    public string StorageClass { get; set; } = "STANDARD";
}

public sealed class PartEntry
{
    public int PartNumber { get; set; }
    public string LastModified { get; set; } = "";
    public string ETag { get; set; } = "";
    public long Size { get; set; }
}

[XmlRoot("ListMultipartUploadsResult", Namespace = S3XmlNamespace.Value)]
public sealed class ListMultipartUploadsResult
{
    public string Bucket { get; set; } = "";
    public string KeyMarker { get; set; } = "";
    public string UploadIdMarker { get; set; } = "";
    public string? NextKeyMarker { get; set; }
    public string? NextUploadIdMarker { get; set; }
    public string? Prefix { get; set; }
    public string? Delimiter { get; set; }
    public int MaxUploads { get; set; }
    public bool IsTruncated { get; set; }

    [XmlElement("Upload")]
    public List<UploadEntry> Uploads { get; set; } = [];

    [XmlElement("CommonPrefixes")]
    public List<CommonPrefixEntry> CommonPrefixes { get; set; } = [];
}

public sealed class UploadEntry
{
    public string Key { get; set; } = "";
    public string UploadId { get; set; } = "";
    public string Initiated { get; set; } = "";
    public string StorageClass { get; set; } = "STANDARD";
    public OwnerEntry? Owner { get; set; }
    public OwnerEntry? Initiator { get; set; }
}

// — ответные: бакеты / копирование / атрибуты —

[XmlRoot("ListAllMyBucketsResult", Namespace = S3XmlNamespace.Value)]
public sealed class ListAllMyBucketsResult
{
    public OwnerEntry Owner { get; set; } = new();

    [XmlArray("Buckets"), XmlArrayItem("Bucket")]
    public List<BucketEntry> Buckets { get; set; } = [];
}

public sealed class BucketEntry
{
    public string Name { get; set; } = "";
    public string CreationDate { get; set; } = "";
}

public sealed class OwnerEntry
{
    public string ID { get; set; } = "";
    public string DisplayName { get; set; } = "";
}

[XmlRoot("LocationConstraint", Namespace = S3XmlNamespace.Value)]
public sealed class LocationConstraint;

[XmlRoot("CopyObjectResult", Namespace = S3XmlNamespace.Value)]
public sealed class CopyObjectResult
{
    public string LastModified { get; set; } = "";
    public string ETag { get; set; } = "";
}

[XmlRoot("CopyPartResult", Namespace = S3XmlNamespace.Value)]
public sealed class CopyPartResult
{
    public string LastModified { get; set; } = "";
    public string ETag { get; set; } = "";
}

[XmlRoot("GetObjectAttributesOutput", Namespace = S3XmlNamespace.Value)]
public sealed class GetObjectAttributesOutput
{
    public string? ETag { get; set; }
    public long? ObjectSize { get; set; }
    public bool ShouldSerializeObjectSize() => ObjectSize.HasValue;
    public string? StorageClass { get; set; }
    public ObjectParts? ObjectParts { get; set; }
}

public sealed class ObjectParts
{
    public int PartsCount { get; set; }
    public int PartNumberMarker { get; set; }
    public int? NextPartNumberMarker { get; set; }
    public bool ShouldSerializeNextPartNumberMarker() => NextPartNumberMarker.HasValue;
    public int MaxParts { get; set; }
    public bool IsTruncated { get; set; }

    [XmlElement("Part")]
    public List<AttributesPartEntry> Parts { get; set; } = [];
}

public sealed class AttributesPartEntry
{
    public int PartNumber { get; set; }
    public long Size { get; set; }
}
