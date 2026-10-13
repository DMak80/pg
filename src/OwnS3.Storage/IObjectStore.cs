namespace OwnS3.Storage;

// Контракт 21 операции в доменных терминах главы 02 (реализация xl — t37/t38).
// Доставка исходов — Result-конвенция главы 01 §4: ошибки каталога операций
// (NoSuchKey, BucketNotEmpty, PreconditionFailed, …) — Failed(ObjectStoreException)
// с кодом для каталога S3-ошибок; значения успеха (PutResult/CompleteResult/
// DeletedKeyResult/страницы) — в Result.Value.
public interface IObjectStore
{
    // бакеты
    Task<Result> CreateBucketAsync(string bucket, CancellationToken ct);                       // BucketAlreadyOwnedByYou → Failed
    Task<Result> DeleteBucketAsync(string bucket, CancellationToken ct);                       // NoSuchBucket/BucketNotEmpty → Failed
    Task<Result<bool>> BucketExistsAsync(string bucket, CancellationToken ct);
    Task<Result<IReadOnlyList<BucketEntry>>> ListBucketsAsync(CancellationToken ct);

    // объекты
    Task<Result<PutResult>> PutObjectAsync(string bucket, string key, Stream body, long contentLength,
        ObjectUploadMetadata metadata, CancellationToken ct);
    Task<Result<ObjectContent>> GetObjectAsync(string bucket, string key, ObjectReadOptions options, CancellationToken ct); // NotModified/Range — в ObjectContent (форма не меняется)
    Task<Result<ObjectContent>> HeadObjectAsync(string bucket, string key, ObjectReadOptions? options, CancellationToken ct);
    Task<Result> DeleteObjectAsync(string bucket, string key, CancellationToken ct);           // идемпотентен (204-семантика)
    Task<Result<IReadOnlyList<DeletedKeyResult>>> DeleteObjectsAsync(string bucket,
        IReadOnlyList<string> keys, bool quiet, CancellationToken ct);                          // per-key состав — значение; отказ всей операции → Failed
    Task<Result<PutResult>> CopyObjectAsync(CopyRequest request, CancellationToken ct);
    Task<Result<GetAttributesOutcome>> GetObjectAttributesAsync(string bucket, string key,
        IReadOnlyList<ObjectAttributeName> attributes, int? maxParts, int? partNumberMarker,
        ObjectConditions? conditions, CancellationToken ct);                                    // NotModified|Full — терминальные Success

    // листинги
    Task<Result<ListPage>> ListObjectsAsync(string bucket, ListQuery query, CancellationToken ct); // битый continuation-token → Failed(XlInvalidArgumentException) — 400

    // multipart
    Task<Result<string>> CreateMultipartUploadAsync(string bucket, string key,
        ObjectUploadMetadata metadata, string initiatorAccessKey, CancellationToken ct); // → uploadId (UUID v4); владелец — в uploads.json (канон 05)
    Task<Result<PutResult>> UploadPartAsync(string bucket, string key, string uploadId, int partNumber,
        Stream body, long contentLength, CancellationToken ct);
    Task<Result<CompleteResult>> CompleteMultipartUploadAsync(string bucket, string key, string uploadId,
        IReadOnlyList<PartEtag> parts, CancellationToken ct);
    Task<Result> AbortMultipartUploadAsync(string bucket, string key, string uploadId, CancellationToken ct);
    Task<Result<PartsPage>> ListPartsAsync(string bucket, string key, string uploadId,
        int? maxParts, int? partNumberMarker, UploadVisibility visibility, CancellationToken ct);
    Task<Result<UploadsPage>> ListMultipartUploadsAsync(string bucket, UploadsQuery query, CancellationToken ct);
}
