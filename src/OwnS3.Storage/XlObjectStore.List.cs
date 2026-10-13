namespace OwnS3.Storage;

// Листинги XlObjectStore: обход ListWalker (порядок/фильтры/свёртка — там)
// поверх курсора драйва.
public sealed partial class XlObjectStore
{
    public Task<Result<ListPage>> ListObjectsAsync(string bucket, ListQuery query, CancellationToken ct)
    {
        var ensured = EnsureBucket(bucket);
        if (!ensured.IsSuccess)
            return Task.FromResult(Result<ListPage>.Failed(ensured.Error!));
        var walker = new ListWalker(Drive, _logger);
        return Task.FromResult(walker.Walk(bucket, query));
    }
}
