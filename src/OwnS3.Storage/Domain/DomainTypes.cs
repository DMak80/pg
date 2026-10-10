// Доменные типы объектного слоя (глава 02): собственные имена Storage,
// без ссылки на Protocol. Метаданные, записи листингов, условия, запросы.
namespace OwnS3.Storage;

public sealed record BucketEntry(string Name, DateTimeOffset CreationDate);

public sealed record ObjectUploadMetadata(string ContentType, IReadOnlyDictionary<string, string> UserMetadata);

public sealed record PutResult(string ETag);

public sealed record ObjectMetadata(string Key, string ETag, long Size, DateTimeOffset LastModified,
    string ContentType, IReadOnlyDictionary<string, string> UserMetadata);

/// <summary>Содержимое объекта; Body освобождает вызывающий.</summary>
public sealed record ObjectContent(ObjectMetadata Metadata, Stream Body);

public sealed record ObjectConditions(string? IfMatch, string? IfNoneMatch,
    DateTimeOffset? IfModifiedSince, DateTimeOffset? IfUnmodifiedSince);     // оценка — t37

public sealed record ByteRange(long? Start, long? End);                       // оценка — t37

public sealed record ObjectReadOptions(ObjectConditions? Conditions, ByteRange? Range);

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
    string? UploadIdMarker, int? MaxUploads, string? EncodingType);

public sealed record UploadsPage(IReadOnlyList<UploadEntry> Uploads, IReadOnlyList<CommonPrefixEntry> CommonPrefixes,
    bool IsTruncated, string? NextKeyMarker, string? NextUploadIdMarker);

public enum ObjectAttributeName { ETag, ObjectSize, StorageClass, ObjectParts }

public sealed record ObjectPartsAttributes(int PartsCount, int PartNumberMarker, int? NextPartNumberMarker,
    int MaxParts, bool IsTruncated, IReadOnlyList<(int PartNumber, long Size)> Parts);

public sealed record ObjectAttributes(string ETag, long ObjectSize, string StorageClass, ObjectPartsAttributes? Parts);
