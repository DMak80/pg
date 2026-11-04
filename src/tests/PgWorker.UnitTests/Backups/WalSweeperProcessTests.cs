using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using PgWorker.Backups;
using PgWorker.Backups.Job;
using PgWorker.Backups.Supervisor;
using PgWorker.Core.Model;
using PgWorker.Etcd.Parsing;
using PgWorker.Etcd.S3;
using Shared.Core;
using Shared.Etcd.Client;
using Shared.Etcd.Coordination;
using PgWorker.UnitTests.Provisioning;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Фоновая чистка мусора wal/ (arch/19 §4): только глобальный лидер, per-shard
// cutoff по GFS-отбору, .partial-гигиена, ОДИН batch-delete на шард,
// независимость шардов; Enabled=false — no-op (arch/18 §2.2).
public class WalSweeperProcessTests
{
    private const string Ep = "http://etcd:2379";
    private static readonly WalFileName Seg1 = new(1, 0, 1);
    private static readonly WalFileName Seg2 = new(1, 0, 2);
    private static readonly WalFileName Seg3 = new(1, 0, 3);
    private static readonly WalFileName Seg5 = new(1, 0, 5);
    private static readonly WalFileName Seg6 = new(1, 0, 6);

    private static ClusterSnapshot ClusterSnap(string cluster, params string[] shards)
        => new(
            new ClusterConfig(cluster, 2, cluster, null, ClusterState.Active),
            [.. shards.Select(s => new ShardSpec(s, 1, null, null, []))],
            []);

    private static ShardBackups BackupsWithVerifiedFull(string walStart, long startedUnix)
        => new(
            [
                new FullBackupState("20260101000000Z", FullBackupStatus.Completed, "n1",
                    BackupSourceRole.Replica, startedUnix, startedUnix + 60, walStart, 1024, null,
                    new BackupVerify(BackupVerifyStatus.Ok, startedUnix + 60)),
            ],
            null);

    /// <summary>Мини-фейк S3: листинг wal/ по (cluster, shard), batch-delete
    /// журналируется; программируемый отказ листинга шарда.</summary>
    private sealed class FakeWalS3 : IBackupS3
    {
        public readonly Dictionary<(string Cluster, string Shard), List<string>> Wal = [];
        public readonly List<List<string>> Deletes = [];
        public readonly HashSet<(string Cluster, string Shard)> FailList = [];
        public int ListCalls;

        public Task<Result<IReadOnlyList<WalObject>>> ListWalAsync(
            string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default)
        {
            ListCalls++;
            if (FailList.Contains((cluster, shard)))
                return Task.FromResult(Result<IReadOnlyList<WalObject>>.Failed(
                    new ApplicationException("test: list wal down")));
            var names = Wal.TryGetValue((cluster, shard), out var list) ? list : [];
            return Task.FromResult(Result<IReadOnlyList<WalObject>>.Success(
                [.. names.Select(n => new WalObject(n, DateTimeOffset.UnixEpoch))]));
        }

        public Task<Result> DeleteKeysAsync(IReadOnlyList<string> keys, CancellationToken ct = default)
        {
            Deletes.Add([.. keys]);
            foreach (var key in keys)
            {
                var parts = key.Split('/');
                if (parts.Length >= 4 && Wal.TryGetValue((parts[0], parts[1]), out var names))
                    names.Remove(parts[^1]);
            }
            return Task.FromResult(Result.Success());
        }

        public Task<Result> PutFileAsync(string key, string filePath, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result> PutObjectAsync(string key, byte[] data, string? sha256, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<bool>> BucketExistsAsync(CancellationToken ct) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<S3ObjectInfo>>> ListPrefixAsync(string prefix, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<WalObject>>> ListAsync(string cluster, string shard, string prefix, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<string>> GetObjectAsync(string cluster, string shard, string key, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<string>>> ListFullsAsync(string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result> GetObjectToFileAsync(string cluster, string shard, string key, string destFilePath, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<string>> DownloadTextAsync(string cluster, string shard, string objectKey, CancellationToken ct = default) => throw new NotSupportedException();
    }

    private sealed class FixedPoolStore(S3Pool? pool) : IS3PoolStore
    {
        public Task<Result<S3Pool?>> ReadAsync(string name, CancellationToken ct)
            => Task.FromResult(Result<S3Pool?>.Success(pool));

        public Task<Result<IReadOnlyList<string>>> ListPoolNamesAsync(CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<string>>.Success(pool is null ? [] : [pool.Name]));
    }

    // Фабрика пула с given fake-клиентом (образец TestPoolFactory.Single с
    // targetFactory: _ => s3 — операции прохода исполняет фейк).
    private static BackupS3Factory Factory(IBackupS3 s3)
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
            targetFactory: _ => s3,
            // breaker-окно 0: отказ листинга одного шарда не исключает ноду для
            // следующего шарда (иначе тест «независимости» ловил бы breaker, а не
            // изоляцию ошибки шарда).
            breakerWindow: TimeSpan.Zero);
    }

    private static WalSweeperProcess Create(
        FakeWalS3 s3, Fakes.FakeEtcd etcd, ClaimStore claims, Func<BackupsRuntimeOptions?> runtime)
        => new(etcd, [Ep], Factory(s3), claims, new WorkJournal("/pgworker", etcd, [Ep]),
            runtime, TimeProvider.System, NullLogger<WalSweeperProcess>.Instance);

    private static ClaimStore Claims(Fakes.FakeEtcd etcd)
        => new("/pgworker", [Ep], etcd, TimeProvider.System);

    // AAA: не-лидер — no-op без единого S3-вызова (единственный писатель — лидер).
    [Fact]
    public async Task NotLeader_NoOp()
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd();
        var s3 = new FakeWalS3();
        var claims = Claims(etcd);
        claims.IsLeader.Should().BeFalse("a fresh ClaimStore holds no leader lease");
        var process = Create(s3, etcd, claims, () => new BackupsRuntimeOptions(Enabled: true));

        // Act
        var result = await process.SweepAsync(CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        s3.ListCalls.Should().Be(0);
        s3.Deletes.Should().BeEmpty();
    }

    // AAA: подсистема выключена (runtime null) — проход no-op.
    [Fact]
    public async Task Disabled_NoOp()
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd();
        var s3 = new FakeWalS3();
        var process = Create(s3, etcd, Claims(etcd), () => null);

        // Act
        var result = await process.SweepShardsAsync(
            [ClusterSnap("c1", "s1")], new Dictionary<string, ClusterBackups?>(), 1000,
            CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        s3.ListCalls.Should().Be(0);
        s3.Deletes.Should().BeEmpty();
    }

    // AAA: absorbed (финал лежит) + устаревшие версии живого сегмента — один
    // batch-delete на шард, финалы не тронуты (cutoff=null — нет verify-OK).
    [Fact]
    public async Task AbsorbedAndStalePartials_DeletedOneBatch()
    {
        // Arrange — финал seg5 + 2 его версии; seg6 без финала + 3 версии; финалы seg1/seg2
        var etcd = new Fakes.FakeEtcd();
        var s3 = new FakeWalS3();
        s3.Wal[("c1", "s1")] =
        [
            Seg1.Name, Seg2.Name, Seg5.Name,
            WalPartialName.Key(Seg5.Name, 0x100),
            WalPartialName.Key(Seg5.Name, 0x200),
            WalPartialName.Key(Seg6.Name, 0x100),
            WalPartialName.Key(Seg6.Name, 0x200),
            WalPartialName.Key(Seg6.Name, 0x300),
        ];
        var process = Create(s3, etcd, Claims(etcd), () => new BackupsRuntimeOptions(Enabled: true));

        // Act — backups пуст: cutoff отсутствует
        var result = await process.SweepShardsAsync(
            [ClusterSnap("c1", "s1")], new Dictionary<string, ClusterBackups?>(), 1000,
            CancellationToken.None);

        // Assert — ровно один batch из 4 ключей (2 поглощённых + 2 устаревших)
        result.IsSuccess.Should().BeTrue();
        s3.Deletes.Should().HaveCount(1);
        s3.Deletes[0].Should().BeEquivalentTo(
        [
            $"c1/s1/wal/{WalPartialName.Key(Seg5.Name, 0x100)}",
            $"c1/s1/wal/{WalPartialName.Key(Seg5.Name, 0x200)}",
            $"c1/s1/wal/{WalPartialName.Key(Seg6.Name, 0x100)}",
            $"c1/s1/wal/{WalPartialName.Key(Seg6.Name, 0x200)}",
        ]);
        s3.Deletes[0].Should().NotContain(k => k.EndsWith($"/{Seg1.Name}") || k.EndsWith($"/{Seg2.Name}"),
            "without a verify-OK full finals are never pruned");
    }

    // AAA: при verify-OK-полном финалы ниже cutoff + .partial ниже cutoff — в
    // тот же batch; cutoff-сегмент жив.
    [Fact]
    public async Task BelowCutoffFinalsDeleted()
    {
        // Arrange — full с wal_start seg3; финалы 1,2,3 + .partial seg2
        var etcd = new Fakes.FakeEtcd();
        var s3 = new FakeWalS3();
        s3.Wal[("c1", "s1")] =
        [
            Seg1.Name, Seg2.Name, Seg3.Name,
            WalPartialName.Key(Seg2.Name, 0x100),
        ];
        var backups = new ClusterBackups("c1", null,
            new Dictionary<string, ShardBackups> { ["s1"] = BackupsWithVerifiedFull(Seg3.Name, 1000) });
        var process = Create(s3, etcd, Claims(etcd), () => new BackupsRuntimeOptions(Enabled: true));

        // Act
        var result = await process.SweepShardsAsync(
            [ClusterSnap("c1", "s1")],
            new Dictionary<string, ClusterBackups?> { ["c1"] = backups }, 2000, CancellationToken.None);

        // Assert — seg1/seg2 + поглощённая версия seg2; seg3 не тронут
        result.IsSuccess.Should().BeTrue();
        s3.Deletes.Should().HaveCount(1);
        s3.Deletes[0].Should().BeEquivalentTo(
        [
            $"c1/s1/wal/{Seg1.Name}",
            $"c1/s1/wal/{Seg2.Name}",
            $"c1/s1/wal/{WalPartialName.Key(Seg2.Name, 0x100)}",
        ]);
        s3.Deletes[0].Should().NotContain($"c1/s1/wal/{Seg3.Name}");
    }

    // AAA: отказ шарда не валит остальные — проход Success, живой шард вычищен,
    // факт ошибки — journal-фаза shard-error.
    [Fact]
    public async Task ShardError_DoesNotBreakOthers()
    {
        // Arrange — два шарда одного кластера; s1 отвечает отказом листинга
        var etcd = new Fakes.FakeEtcd();
        var s3 = new FakeWalS3();
        s3.FailList.Add(("c1", "s1"));
        s3.Wal[("c1", "s2")] =
        [
            WalPartialName.Key(Seg2.Name, 0x100),
            WalPartialName.Key(Seg2.Name, 0x200),
        ];
        var process = Create(s3, etcd, Claims(etcd), () => new BackupsRuntimeOptions(Enabled: true));

        // Act
        var result = await process.SweepShardsAsync(
            [ClusterSnap("c1", "s1", "s2")], new Dictionary<string, ClusterBackups?>(), 1000,
            CancellationToken.None);

        // Assert — оба шарда обойдены, s2 вычищен одним batch
        result.IsSuccess.Should().BeTrue();
        s3.Deletes.Should().HaveCount(1);
        s3.Deletes[0].Should().Equal($"c1/s2/wal/{WalPartialName.Key(Seg2.Name, 0x100)}");
        // Journal агрегирует фазу кластера — последняя запись побеждает: факт
        // ошибки s1 перекрыт успехом s2, поэтому ассерт — исход здорового шарда.
        etcd.Store.Values.Should().Contain(e => e.Value.Contains("wal-swept/s2/1"),
            "the healthy shard completed its pass despite the failed one");
    }
}
