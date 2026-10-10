namespace OwnS3.Storage;

// Контракт 22 операций в доменных терминах главы 02 (реализация xl — t37/t38).
public interface IObjectStore
{
    // бакеты
    Task CreateBucketAsync(string bucket, CancellationToken ct);                       // исходы: BucketAlreadyOwnedByYou
    Task DeleteBucketAsync(string bucket, CancellationToken ct);                       // исходы: NoSuchBucket, BucketNotEmpty
    Task<bool> BucketExistsAsync(string bucket, CancellationToken ct);
    Task<IReadOnlyList<BucketEntry>> ListBucketsAsync(CancellationToken ct);

    // объекты
    Task<PutResult> PutObjectAsync(string bucket, string key, Stream body, long contentLength,
        ObjectUploadMetadata metadata, CancellationToken ct);
    Task<ObjectContent> GetObjectAsync(string bucket, string key, ObjectReadOptions options, CancellationToken ct);
    Task<ObjectMetadata> HeadObjectAsync(string bucket, string key, CancellationToken ct);
    Task DeleteObjectAsync(string bucket, string key, CancellationToken ct);           // идемпотентен (204-семантика)
    Task<IReadOnlyList<DeletedKeyResult>> DeleteObjectsAsync(string bucket,
        IReadOnlyList<string> keys, bool quiet, CancellationToken ct);
    Task<PutResult> CopyObjectAsync(CopyRequest request, CancellationToken ct);
    Task<ObjectAttributes> GetObjectAttributesAsync(string bucket, string key,
        IReadOnlyList<ObjectAttributeName> attributes, int? maxParts, int? partNumberMarker, CancellationToken ct);

    // листинги
    Task<ListPage> ListObjectsAsync(string bucket, ListQuery query, CancellationToken ct);

    // multipart
    Task<string> CreateMultipartUploadAsync(string bucket, string key,
        ObjectUploadMetadata metadata, CancellationToken ct);                          // → uploadId (UUID v4)
    Task<PutResult> UploadPartAsync(string bucket, string key, string uploadId, int partNumber,
        Stream body, long contentLength, CancellationToken ct);
    Task<PutResult> UploadPartCopyAsync(PartCopyRequest request, CancellationToken ct);
    Task<CompleteResult> CompleteMultipartUploadAsync(string bucket, string key, string uploadId,
        IReadOnlyList<PartEtag> parts, CancellationToken ct);
    Task AbortMultipartUploadAsync(string bucket, string key, string uploadId, CancellationToken ct);
    Task<PartsPage> ListPartsAsync(string bucket, string key, string uploadId,
        int? maxParts, int? partNumberMarker, CancellationToken ct);
    Task<UploadsPage> ListMultipartUploadsAsync(string bucket, UploadsQuery query, CancellationToken ct);
}
