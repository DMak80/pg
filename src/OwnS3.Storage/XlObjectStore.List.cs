namespace OwnS3.Storage;

// Листинги XlObjectStore: обход ListWalker (порядок/фильтры/свёртка — там)
// поверх курсора драйва.
public sealed partial class XlObjectStore
{
    public Task<ListPage> ListObjectsAsync(string bucket, ListQuery query, CancellationToken ct)
    {
        EnsureBucket(bucket);
        var walker = new ListWalker(Drive, _logger);
        return Task.FromResult(walker.Walk(bucket, query));
    }
}
