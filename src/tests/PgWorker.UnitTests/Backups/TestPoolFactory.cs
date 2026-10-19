using PgWorker.Backups;
using PgWorker.Etcd.Parsing;
using PgWorker.Etcd.S3;
using Shared.Core;

namespace PgWorker.UnitTests.Backups;

// t36: тестовая фабрика пула для юнит-процессов — резолвер всегда возвращает
// контекст с одной fake-нодой (BackupProcess использует фабрику ТОЛЬКО для
// резолва; target-фабрика сценариям не нужна — бросок как страховка).
public static class TestPoolFactory
{
    public static BackupS3Factory Single()
    {
        var pool = new S3Pool("backups", false,
            [new S3PoolNode("node1", "RUNNING", "h1", "h1", 9000)], ["h1:9000"],
            Secrets: new("ak", "sk"));
        var store = new FixedPoolStore(pool);
        return new BackupS3Factory(
            new BackupPoolResolver(store,
                policyReader: (_, _) => Task.FromResult<BackupPolicy?>(new BackupPolicy(
                    1, 1, 1, 86400, true,
                    S3: new BackupPolicyS3("backups", ["node1"], ["node1"])))),
            store,
            () => "pgworker-backups", () => null, TimeSpan.FromSeconds(5),
            targetFactory: _ => throw new NotSupportedException(
                "the unit scenario of the S3 operations planner does not execute"));
    }

    private sealed class FixedPoolStore(S3Pool? pool) : IS3PoolStore
    {
        public Task<Result<S3Pool?>> ReadAsync(string name, CancellationToken ct)
            => Task.FromResult(Result<S3Pool?>.Success(pool));

        public Task<Result<IReadOnlyList<string>>> ListPoolNamesAsync(CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<string>>.Success(pool is null ? [] : [pool.Name]));
    }
}
