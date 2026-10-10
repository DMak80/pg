namespace OwnS3.Storage;

// Multipart-методы XlObjectStore — заглушки до t38 (спека §5.4: заглушечное
// поведение переезжает из NotWiredObjectStore; App маппит в 500 InternalError).
public sealed partial class XlObjectStore
{
    public Task<string> CreateMultipartUploadAsync(string bucket, string key,
        ObjectUploadMetadata metadata, CancellationToken ct) =>
        ThrowUnavailable<string>(); // t38

    public async Task<PutResult> UploadPartAsync(string bucket, string key, string uploadId, int partNumber,
        Stream body, long contentLength, CancellationToken ct)
    {
        await DrainAsync(body, ct); // drain до отказа — сверка конвейера наблюдаема
        throw new ObjectStoreUnavailableException();
    }

    public Task<PutResult> UploadPartCopyAsync(PartCopyRequest request, CancellationToken ct) =>
        ThrowUnavailable<PutResult>(); // t38

    public Task<CompleteResult> CompleteMultipartUploadAsync(string bucket, string key, string uploadId,
        IReadOnlyList<PartEtag> parts, CancellationToken ct) =>
        ThrowUnavailable<CompleteResult>(); // t38

    public Task AbortMultipartUploadAsync(string bucket, string key, string uploadId, CancellationToken ct) =>
        throw new ObjectStoreUnavailableException(); // t38

    public Task<PartsPage> ListPartsAsync(string bucket, string key, string uploadId,
        int? maxParts, int? partNumberMarker, CancellationToken ct) =>
        ThrowUnavailable<PartsPage>(); // t38

    public Task<UploadsPage> ListMultipartUploadsAsync(string bucket, UploadsQuery query, CancellationToken ct) =>
        ThrowUnavailable<UploadsPage>(); // t38

    // Дочитать тело до конца: сверка подписи/хэшей конвейером наблюдаема в тестах.
    private static async Task DrainAsync(Stream body, CancellationToken ct)
    {
        var buffer = new byte[64 * 1024];
        while (await body.ReadAsync(buffer, ct) > 0)
        {
            // только чтение — сверку делает обёртка тела конвейера
        }
    }

    private static Task<T> ThrowUnavailable<T>() => throw new ObjectStoreUnavailableException();
}
