using PgWorker.Backups;
using PgWorker.Etcd.S3;
using Shared.Core;

namespace PgWorker.UnitTests.App;

// t36: фабрика/пулы для юнит-процессов сирот (BackupOrphanSweeperLoop) —
// union по одному фиктивному пулу; цели — переданный IBackupS3 теста.
public static class UnitPoolFactory
{
    public static BackupS3Factory Wrap(IBackupS3 target) => new(
        Resolver(), Store(), Secrets(),
        () => "pgworker-backups", () => null, TimeSpan.FromSeconds(5),
        targetFactory: _ => target);

    public static BackupS3Factory Disabled() => new(
        Resolver(), Store(), Secrets(),
        () => "pgworker-backups", () => null, TimeSpan.FromSeconds(5),
        targetFactory: _ => throw new NotSupportedException("Disabled"));

    public static IS3PoolStore Store() => new FixedPoolStore();

    private static BackupPoolResolver Resolver() => new(
        Store(), Secrets(),
        policyReader: (_, _) => Task.FromResult<PgWorker.Etcd.Parsing.BackupPolicy?>(null));

    private static BackupSecretProfileStore Secrets() => new(
        getenv: _ => "x",
        envNames: ["PGW_BACKUP_S3_PROFILES__backups-main__ACCESS_KEY", "PGW_BACKUP_S3_PROFILES__backups-main__SECRET_KEY"]);

    private sealed class FixedPoolStore : IS3PoolStore
    {
        public Task<Result<S3Pool?>> ReadAsync(string name, CancellationToken ct)
            => Task.FromResult(Result<S3Pool?>.Success(new S3Pool(
                "backups", "backups-main", false,
                [new S3PoolNode("node1", "RUNNING", "h1", "h1", 9000)], ["h1:9000"])));

        public Task<Result<IReadOnlyList<string>>> ListPoolNamesAsync(CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<string>>.Success((IReadOnlyList<string>)["backups"]));
    }
}
