using System.Collections.Concurrent;
using PgWorker.Backups;
using PgWorker.Backups.Sql;
using PgWorker.Core;

namespace PgWorker.IntegrationTests.Backups;

// Фейковый SQL-слой: слоты в памяти, LSN управляется тестом (AAA-Act).
public sealed class FakeWalSqlExecutor : IWalSqlExecutor
{
    public ConcurrentDictionary<string, bool> Slots { get; } = new();

    public (string Lsn, int Tli) Current { get; set; } = ("0/1000000", 1);

    public Task<Result<bool>> SlotExistsAsync(string adminDsn, string slot, CancellationToken ct)
        => Task.FromResult(Result<bool>.Success(Slots.TryGetValue(slot, out var alive) && alive));

    public Task<Result> EnsureSlotAsync(string adminDsn, string slot, CancellationToken ct)
    {
        Slots[slot] = true;
        return Task.FromResult(Result.Success());
    }

    public Task<Result<(string Lsn, int Tli)>> CurrentWalAsync(string adminDsn, CancellationToken ct)
        => Task.FromResult(Result<(string, int)>.Success(Current));
}

// Фейковый S3 (поверхность IBackupS3): объекты в памяти, стартовое наполнение —
// тестом. t06: PrefixObjects — объекты произвольных префиксов (полные ключи),
// DeletedKeys — журнал удалений, FailNextDelete — сбой для transient-сценариев.
public sealed class FakeBackupS3 : IBackupS3
{
    public List<(string Cluster, string Shard, string Name)> Objects { get; } = [];

    // Объекты произвольных ключей (list/delete через ListPrefixAsync/DeleteKeysAsync).
    public List<(string Key, long SizeBytes)> PrefixObjects { get; } = [];

    public List<string> DeletedKeys { get; } = [];

    // Один сбой batch-delete (transient-сценарий AC3): следующий вызов падает.
    public bool FailNextDelete { get; set; }

    public DateTimeOffset LastModified { get; set; } = DateTimeOffset.UtcNow;

    public Task<Result<bool>> BucketExistsAsync(CancellationToken ct)
        => Task.FromResult(Result<bool>.Success(true));

    public Task<Result<IReadOnlyList<WalObject>>> ListWalAsync(
        string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default)
        => Task.FromResult(Result<IReadOnlyList<WalObject>>.Success(
            (IReadOnlyList<WalObject>)Objects
                .Where(o => o.Cluster == cluster && o.Shard == shard)
                .Select(o => new WalObject(o.Name, LastModified))
                .ToList()));

    public Task<Result<IReadOnlyList<S3ObjectInfo>>> ListPrefixAsync(
        string prefix, int? maxKeysPerTest = null, CancellationToken ct = default)
    {
        var walKeys = Objects.Select(o =>
            new S3ObjectInfo($"{o.Cluster}/{o.Shard}/wal/{o.Name}", 16, LastModified));
        var prefixKeys = PrefixObjects.Select(o => new S3ObjectInfo(o.Key, o.SizeBytes, LastModified));
        var all = walKeys.Concat(prefixKeys)
            .Where(o => o.Key.StartsWith(prefix, StringComparison.Ordinal))
            .ToList();
        return Task.FromResult(Result<IReadOnlyList<S3ObjectInfo>>.Success(
            (IReadOnlyList<S3ObjectInfo>)all));
    }

    public Task<Result> DeleteKeysAsync(IReadOnlyList<string> keys, CancellationToken ct = default)
    {
        // Transient-сбой по требованию теста (статусы etcd не меняем — AC3).
        if (FailNextDelete)
        {
            FailNextDelete = false;
            return Task.FromResult(Result.Failed(
                new ApplicationException("fake S3 delete failure (FailNextDelete)")));
        }

        foreach (var key in keys)
        {
            DeletedKeys.Add(key);
            Objects.RemoveAll(o => $"{o.Cluster}/{o.Shard}/wal/{o.Name}" == key);
            PrefixObjects.RemoveAll(o => o.Key == key);
        }

        return Task.FromResult(Result.Success());
    }
}
