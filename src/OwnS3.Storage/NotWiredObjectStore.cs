namespace OwnS3.Storage;

// Заглушка t36 (решение пользователя 1, spec §3.3): сервис без хранилища.
// Методы с телом (PutObject/UploadPart) сначала дочитывают поток до конца —
// drain-семантика: конвейер App успевает сверить подпись и хэши тела
// (sha256-режим, Content-MD5, чанковая подпись, trailer-checksum) до отказа;
// затем бросают ObjectStoreUnavailableException (App маппит в 500 InternalError).
// Методы без тела бросают её сразу.
public sealed class NotWiredObjectStore : IObjectStore
{
    public Task CreateBucketAsync(string bucket, CancellationToken ct) => Throw();
    public Task DeleteBucketAsync(string bucket, CancellationToken ct) => Throw();
    public Task<bool> BucketExistsAsync(string bucket, CancellationToken ct) => Throw<bool>();
    public Task<IReadOnlyList<BucketEntry>> ListBucketsAsync(CancellationToken ct) => Throw<IReadOnlyList<BucketEntry>>();

    public async Task<PutResult> PutObjectAsync(string bucket, string key, Stream body, long contentLength,
        ObjectUploadMetadata metadata, CancellationToken ct)
    {
        await DrainAsync(body, ct);
        throw new ObjectStoreUnavailableException();
    }

    public Task<ObjectContent> GetObjectAsync(string bucket, string key, ObjectReadOptions options, CancellationToken ct) =>
        Throw<ObjectContent>();
    public Task<ObjectContent> HeadObjectAsync(string bucket, string key, ObjectReadOptions? options, CancellationToken ct) =>
        Throw<ObjectContent>();
    public Task DeleteObjectAsync(string bucket, string key, CancellationToken ct) => Throw();
    public Task<IReadOnlyList<DeletedKeyResult>> DeleteObjectsAsync(string bucket,
        IReadOnlyList<string> keys, bool quiet, CancellationToken ct) => Throw<IReadOnlyList<DeletedKeyResult>>();
    public Task<PutResult> CopyObjectAsync(CopyRequest request, CancellationToken ct) => Throw<PutResult>();
    public Task<ObjectAttributesResult> GetObjectAttributesAsync(string bucket, string key,
        IReadOnlyList<ObjectAttributeName> attributes, int? maxParts, int? partNumberMarker,
        ObjectConditions? conditions, CancellationToken ct) =>
        Throw<ObjectAttributesResult>();

    public Task<ListPage> ListObjectsAsync(string bucket, ListQuery query, CancellationToken ct) => Throw<ListPage>();

    public Task<string> CreateMultipartUploadAsync(string bucket, string key,
        ObjectUploadMetadata metadata, CancellationToken ct) => Throw<string>();

    public async Task<PutResult> UploadPartAsync(string bucket, string key, string uploadId, int partNumber,
        Stream body, long contentLength, CancellationToken ct)
    {
        await DrainAsync(body, ct);
        throw new ObjectStoreUnavailableException();
    }

    public Task<PutResult> UploadPartCopyAsync(PartCopyRequest request, CancellationToken ct) => Throw<PutResult>();
    public Task<CompleteResult> CompleteMultipartUploadAsync(string bucket, string key, string uploadId,
        IReadOnlyList<PartEtag> parts, CancellationToken ct) => Throw<CompleteResult>();
    public Task AbortMultipartUploadAsync(string bucket, string key, string uploadId, CancellationToken ct) => Throw();
    public Task<PartsPage> ListPartsAsync(string bucket, string key, string uploadId,
        int? maxParts, int? partNumberMarker, CancellationToken ct) => Throw<PartsPage>();
    public Task<UploadsPage> ListMultipartUploadsAsync(string bucket, UploadsQuery query, CancellationToken ct) =>
        Throw<UploadsPage>();

    // Дочитать тело до конца: сверка подписи/хэшей конвейером наблюдаема в тестах.
    private static async Task DrainAsync(Stream body, CancellationToken ct)
    {
        var buffer = new byte[64 * 1024];
        while (await body.ReadAsync(buffer, ct) > 0)
        {
            // только чтение — сверку делает обёртка тела конвейера
        }
    }

    private static Task Throw() => throw new ObjectStoreUnavailableException();

    private static Task<T> Throw<T>() => throw new ObjectStoreUnavailableException();
}
