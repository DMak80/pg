namespace OwnS3.Storage;

// Бакетные операции XlObjectStore поверх IXlDrive (спека §4.6).
public sealed partial class XlObjectStore
{
    private static readonly IReadOnlyDictionary<string, string> EmptyHeaders =
        new Dictionary<string, string>();

    // Существует → BucketAlreadyOwnedByYou; иначе каталоги + bucket.json (drive).
    public Task CreateBucketAsync(string bucket, CancellationToken ct)
    {
        // мост t38: демонтаж в задаче перевода IObjectStore/IReplicaStore
        if (Drive.BucketExists(bucket).Throw().Value)
            throw new ObjectStoreException(ObjectStoreErrorCode.BucketAlreadyOwnedByYou);
        Drive.CreateBucket(bucket).Throw(); // мост t38
        return Task.CompletedTask;
    }

    // Нет → NoSuchBucket; emptiness-проверка (объекты/живые загрузки) →
    // BucketNotEmpty; иначе trash — всё в drive.
    public Task DeleteBucketAsync(string bucket, CancellationToken ct)
    {
        EnsureBucket(bucket);
        Drive.DeleteBucket(bucket).Throw(); // мост t38: демонтаж в задаче перевода IObjectStore/IReplicaStore
        return Task.CompletedTask;
    }

    // Каталог существует и не служебный.
    public Task<bool> BucketExistsAsync(string bucket, CancellationToken ct) =>
        // мост t38: демонтаж в задаче перевода IObjectStore/IReplicaStore
        Task.FromResult(Drive.BucketExists(bucket).Throw().Value);

    // Сортировка Utf8ByteOrder + CreationDate из bucket.json — drive.
    public Task<IReadOnlyList<BucketEntry>> ListBucketsAsync(CancellationToken ct) =>
        // мост t38: демонтаж в задаче перевода IObjectStore/IReplicaStore
        Task.FromResult(Drive.ListBuckets().Throw().Value);
}
