namespace OwnS3.Storage;

// Листинги XlObjectStore: обход ListWalker (порядок/фильтры/свёртка — там).
public sealed partial class XlObjectStore
{
    public Task<ListPage> ListObjectsAsync(string bucket, ListQuery query, CancellationToken ct)
    {
        EnsureBucket(bucket);
        var walker = new ListWalker(volume, _logger);
        return Task.FromResult(walker.Walk(bucket, query));
    }
}
