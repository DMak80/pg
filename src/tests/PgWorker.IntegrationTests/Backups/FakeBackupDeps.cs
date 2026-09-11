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

// Фейковый S3 (поверхность IBackupS3 по spec — только list/exists): объекты в памяти,
// стартовое наполнение — тестом.
public sealed class FakeBackupS3 : IBackupS3
{
    public List<(string Cluster, string Shard, string Name)> Objects { get; } = [];

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
}
