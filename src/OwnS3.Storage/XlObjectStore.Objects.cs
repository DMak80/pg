namespace OwnS3.Storage;

// Объектные операции XlObjectStore — транзитные заглушки задач 6–9 (P6):
// каждая следующая задача заменяет свою группу методов на реализацию.
public sealed partial class XlObjectStore
{
    public async Task<PutResult> PutObjectAsync(string bucket, string key, Stream body, long contentLength,
        ObjectUploadMetadata metadata, CancellationToken ct)
    {
        await DrainAsync(body, ct); // t37: реализация в Task 6 плана
        throw new ObjectStoreUnavailableException();
    }

    public Task<ObjectContent> GetObjectAsync(string bucket, string key, ObjectReadOptions options,
        CancellationToken ct) =>
        ThrowUnavailable<ObjectContent>(); // t37: реализация в Task 7 плана

    public Task<ObjectMetadata> HeadObjectAsync(string bucket, string key, CancellationToken ct) =>
        ThrowUnavailable<ObjectMetadata>(); // t37: реализация в Task 7 плана

    public Task DeleteObjectAsync(string bucket, string key, CancellationToken ct) =>
        throw new ObjectStoreUnavailableException(); // t37: реализация в Task 6 плана

    public Task<IReadOnlyList<DeletedKeyResult>> DeleteObjectsAsync(string bucket,
        IReadOnlyList<string> keys, bool quiet, CancellationToken ct) =>
        ThrowUnavailable<IReadOnlyList<DeletedKeyResult>>(); // t37: реализация в Task 6 плана

    public Task<PutResult> CopyObjectAsync(CopyRequest request, CancellationToken ct) =>
        ThrowUnavailable<PutResult>(); // t37: реализация в Task 8 плана

    public Task<ObjectAttributes> GetObjectAttributesAsync(string bucket, string key,
        IReadOnlyList<ObjectAttributeName> attributes, int? maxParts, int? partNumberMarker,
        CancellationToken ct) =>
        ThrowUnavailable<ObjectAttributes>(); // t37: реализация в Task 8 плана

    public Task<ListPage> ListObjectsAsync(string bucket, ListQuery query, CancellationToken ct) =>
        ThrowUnavailable<ListPage>(); // t37: реализация в Task 9 плана
}
