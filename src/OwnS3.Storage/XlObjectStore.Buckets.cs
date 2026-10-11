namespace OwnS3.Storage;

// Бакетные операции XlObjectStore поверх IXlDrive (спека §4.6).
public sealed partial class XlObjectStore
{
    private static readonly IReadOnlyDictionary<string, string> EmptyHeaders =
        new Dictionary<string, string>();

    // Существует → BucketAlreadyOwnedByYou; иначе каталоги + bucket.json (drive).
    public Task CreateBucketAsync(string bucket, CancellationToken ct)
    {
        if (Drive.BucketExists(bucket))
            throw new ObjectStoreException(ObjectStoreErrorCode.BucketAlreadyOwnedByYou);
        Drive.CreateBucket(bucket);
        return Task.CompletedTask;
    }

    // Нет → NoSuchBucket; emptiness-проверка (объекты/живые загрузки) →
    // BucketNotEmpty; иначе trash — всё в drive.
    public Task DeleteBucketAsync(string bucket, CancellationToken ct)
    {
        EnsureBucket(bucket);
        Drive.DeleteBucket(bucket);
        return Task.CompletedTask;
    }

    // Каталог существует и не служебный.
    public Task<bool> BucketExistsAsync(string bucket, CancellationToken ct) =>
        Task.FromResult(Drive.BucketExists(bucket));

    // Сортировка Utf8ByteOrder + CreationDate из bucket.json — drive.
    public Task<IReadOnlyList<BucketEntry>> ListBucketsAsync(CancellationToken ct) =>
        Task.FromResult(Drive.ListBuckets());
}
