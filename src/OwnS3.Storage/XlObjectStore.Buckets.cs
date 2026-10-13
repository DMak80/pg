namespace OwnS3.Storage;

// Бакетные операции XlObjectStore поверх IXlDrive (спека §4.6).
public sealed partial class XlObjectStore
{
    private static readonly IReadOnlyDictionary<string, string> EmptyHeaders =
        new Dictionary<string, string>();

    // Существует → BucketAlreadyOwnedByYou; иначе каталоги + bucket.json (drive).
    public Task<Result> CreateBucketAsync(string bucket, CancellationToken ct)
    {
        var exists = Drive.BucketExists(bucket);
        if (!exists.IsSuccess)
            return Task.FromResult(Result.Failed(exists.Error!)); // кворумный отказ — Failed («503», канон 06 §7)
        if (exists.Value)
            return Task.FromResult(Result.Failed(
                new ObjectStoreException(ObjectStoreErrorCode.BucketAlreadyOwnedByYou)));
        return Task.FromResult(Drive.CreateBucket(bucket));
    }

    // Нет → NoSuchBucket; emptiness-проверка (объекты/живые загрузки) →
    // BucketNotEmpty; иначе trash — всё в drive.
    public Task<Result> DeleteBucketAsync(string bucket, CancellationToken ct)
    {
        var ensured = EnsureBucket(bucket);
        if (!ensured.IsSuccess)
            return Task.FromResult(ensured);
        return Task.FromResult(Drive.DeleteBucket(bucket));
    }

    // Каталог существует и не служебный.
    public Task<Result<bool>> BucketExistsAsync(string bucket, CancellationToken ct) =>
        Task.FromResult(Drive.BucketExists(bucket));

    // Сортировка Utf8ByteOrder + CreationDate из bucket.json — drive.
    public Task<Result<IReadOnlyList<BucketEntry>>> ListBucketsAsync(CancellationToken ct) =>
        Task.FromResult(Drive.ListBuckets());
}
