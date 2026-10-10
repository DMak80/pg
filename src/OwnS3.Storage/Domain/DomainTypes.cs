// Доменные типы объектного слоя (глава 02): собственные имена Storage,
// без ссылки на Protocol. Метаданные, записи листингов, условия, запросы.
namespace OwnS3.Storage;

public sealed record BucketEntry(string Name, DateTimeOffset CreationDate);

public sealed record ObjectUploadMetadata(string ContentType, IReadOnlyDictionary<string, string> UserMetadata);

// LastModified — modTime закоммиченной записи: CopyObjectResult несёт время
// объекта, а не UtcNow обработки.
public sealed record PutResult(string ETag, DateTimeOffset LastModified);

public sealed record ObjectMetadata(string Key, string ETag, long Size, DateTimeOffset LastModified,
    string ContentType, IReadOnlyDictionary<string, string> UserMetadata);

public sealed record ObjectConditions(string? IfMatch, string? IfNoneMatch,
    DateTimeOffset? IfModifiedSince, DateTimeOffset? IfUnmodifiedSince);

public sealed record ByteRange(long? Start, long? End);

public sealed record ObjectReadOptions(ObjectConditions? Conditions, ByteRange? Range, string? IfRange);

// Применённый диапазон: start/end включительно + полный размер объекта.
public sealed record AppliedByteRange(long Start, long End, long Total);

// NotModified=true: Body=Stream.Null, Metadata заполнен (ETag/LastModified для 304).
// Range != null: применённый диапазон; Body — Stream части объекта (у Head — Stream.Null).
public sealed record ObjectContent(ObjectMetadata Metadata, Stream Body, bool NotModified, AppliedByteRange? Range);

// Исход GetObjectAttributes (P10): NotModifiedMetadata != null → 304 (Metadata несёт
// ETag/LastModified для заголовков); иначе Attributes заполнены.
public sealed record ObjectAttributesResult(ObjectAttributes? Attributes, ObjectMetadata? NotModifiedMetadata);

public sealed record CopyRequest(string SourceBucket, string SourceKey, string DestBucket, string DestKey,
    bool ReplaceMetadata, ObjectUploadMetadata? NewMetadata, ObjectConditions? SourceConditions);

public sealed record PartCopyRequest(string SourceBucket, string SourceKey,
    string DestBucket, string DestKey, string UploadId, int PartNumber,
    ByteRange? SourceRange, ObjectConditions? SourceConditions);

public sealed record DeletedKeyResult(string Key, bool Deleted, string? ErrorCode, string? ErrorMessage);

public sealed record ListEntry(string Key, string ETag, long Size, DateTimeOffset LastModified);

public sealed record CommonPrefixEntry(string Prefix);

public enum ListVariant { V1, V2, Versions }

public sealed record ListQuery(string? Prefix, string? Delimiter, string? Marker, string? StartAfter,
    string? ContinuationToken, int? MaxKeys, string? EncodingType, bool FetchOwner, ListVariant Variant);

public sealed record ListPage(IReadOnlyList<ListEntry> Contents, IReadOnlyList<CommonPrefixEntry> CommonPrefixes,
    bool IsTruncated, string? NextMarker, string? NextContinuationToken, int KeyCount);

public sealed record PartEtag(int PartNumber, string ETag);

/// <summary>Составной ETag «N-md5» — вычисляет реализация хранения.</summary>
public sealed record CompleteResult(string ETag);

public sealed record PartEntry(int PartNumber, string ETag, long Size, DateTimeOffset LastModified);

public sealed record PartsPage(IReadOnlyList<PartEntry> Parts, bool IsTruncated, int? NextPartNumberMarker);

public sealed record UploadEntry(string Key, string UploadId, DateTimeOffset Initiated);

public sealed record UploadsQuery(string? Prefix, string? Delimiter, string? KeyMarker,
    string? UploadIdMarker, int? MaxUploads, string? EncodingType, UploadVisibility Visibility);

/// <summary>Видимость загрузок для ListParts/ListMultipartUploads (канон 05 §3):
/// все — read-write/admin; только свои — read-only (чужая = несуществующая).</summary>
public abstract record UploadVisibility
{
    public static readonly UploadVisibility AllUploads = new All();
    public static UploadVisibility OwnedBy(string accessKey) => new Owned(accessKey);
    public sealed record All : UploadVisibility;
    public sealed record Owned(string AccessKey) : UploadVisibility;
}

public sealed record UploadsPage(IReadOnlyList<UploadEntry> Uploads, IReadOnlyList<CommonPrefixEntry> CommonPrefixes,
    bool IsTruncated, string? NextKeyMarker, string? NextUploadIdMarker);

public enum ObjectAttributeName { ETag, ObjectSize, StorageClass, ObjectParts }

public sealed record ObjectPartsAttributes(int PartsCount, int PartNumberMarker, int? NextPartNumberMarker,
    int MaxParts, bool IsTruncated, IReadOnlyList<(int PartNumber, long Size)> Parts);

public sealed record ObjectAttributes(string ETag, long ObjectSize, string StorageClass,
    ObjectPartsAttributes? Parts, DateTimeOffset LastModified);
