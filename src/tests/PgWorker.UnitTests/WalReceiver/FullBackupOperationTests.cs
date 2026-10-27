using System.Diagnostics;
using FluentAssertions;
using PgWorker.Backups;
using PgWorker.Etcd.S3;
using PgWorker.WalReceiver;
using PgWorker.WalReceiver.Operations;
using Shared.Core;
using Shared.Etcd.Client;
using Xunit;

namespace PgWorker.UnitTests.WalReceiver;

// Операция полного бэкапа приёмника (arch/19 §2, spec t26 §3.4) на фейках
// IProcessRunner/IBackupS3: порядок фаз running/basebackup → uploading (+
// wal_start_segment из backup_label) → succeeded; put-ключи 1:1 full/<id>/…
// + wal/<seg> без *.partial/*.history; пароль — ТОЛЬКО env PGPASSWORD (не
// argv); отказ pg_basebackup/upload → failed с однострочной причиной;
// cancel → failed `cancelled`; staging чистится после любого исхода.
// AAA-комментарии.
public class FullBackupOperationTests
{
    private const string Cluster = "c1";
    private const string Shard = "s1";
    private const string Node = "n1";
    private const string Id = "20260101000000Z";
    // Фейковый пароль юнит-теста (не секрет): литерала-креденциала в коде нет —
    // значение берётся из env, фолбэк заведомо тестовый.
    private static string Password =>
        Environment.GetEnvironmentVariable("PW_TEST_BACKUP_PASSWORD") ?? "unit-test-only-not-a-secret";

    private static string PasswordKey => $"/clusters/{Cluster}/backup_password";
    private static string PolicyKey => $"/pgworker/backups/{Cluster}/policy";

    private static WalReceiverIdentity Identity(string? bucket = "backups")
        => WalReceiverIdentity.Parse(
            key => key switch
            {
                "PGW_BACKUP_CLUSTER" => Cluster,
                "PGW_BACKUP_SHARD" => Shard,
                "PGW_BACKUP_NODE" => Node,
                "PGW_ETCD" => "http://etcd-1:2379",
                "PGW_BACKUP_S3_BUCKET" => bucket,
                _ => null,
            },
            out _)!;

    /// <summary>Фейк etcd: пароль + policy (тот же резолв, что у стрима).</summary>
    private static FakeEtcd Etcd()
        => new()
        {
            Kvs =
            {
                [PasswordKey] = Password,
                [PolicyKey] = """{"s3":{"pool":"p1","write_master":["s3a"],"write_replica":["s3a"]}}""",
            },
        };

    private static FakePools Pools()
        => new(new S3Pool("p1", false,
            [new S3PoolNode("s3a", "RUNNING", "host1", "s3a", 9000)],
            ["s3a:9000"],
            new S3PoolSecrets("ak", "sk")));

    /// <summary>Фейк etcd-gateway (образец WalReceiverLoopTests): KV-словарь
    /// (Unknown-ветку гейтов покрывают API-тесты BackupApiOperationTests).</summary>
    internal sealed class FakeEtcd : IEtcdGateway
    {
        public Dictionary<string, string> Kvs { get; } = [];

        public Task<Result<Kv?>> GetAsync(string endpoint, string key, CancellationToken ct)
            => Task.FromResult(Result<Kv?>.Success(
                Kvs.TryGetValue(key, out var v) ? new Kv(key, v, 1) : null));

        public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<Kv>>.Success([.. Kvs
                .Where(kv => kv.Key.StartsWith(prefix, StringComparison.Ordinal))
                .Select(kv => new Kv(kv.Key, kv.Value, 1))]));

        public Task<Result> PutAsync(string endpoint, string key, string value, long? lease, CancellationToken ct)
        {
            Kvs[key] = value;
            return Task.FromResult(Result.Success());
        }

        public Task<Result<long>> LeaseGrantAsync(string endpoint, int ttlSec, CancellationToken ct)
            => Task.FromResult(Result<long>.Success(1));
        public Task<Result> LeaseKeepaliveAsync(string endpoint, long lease, CancellationToken ct)
            => Task.FromResult(Result.Success());
        public Task<Result> LeaseRevokeAsync(string endpoint, long lease, CancellationToken ct)
            => Task.FromResult(Result.Success());
        public Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
        {
            Kvs.Remove(keyOrPrefix);
            return Task.FromResult(Result.Success());
        }

        public Task<Result<TxnResult>> TxnAsync(string endpoint, TxnRequest req, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<Result<byte[]>> SnapshotSaveAsync(string endpoint, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<Result<EtcdStatusPayload>> StatusAsync(string endpoint, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<EtcdMember>>> MemberListAsync(string endpoint, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<EtcdAlarm>>> AlarmAsync(string endpoint, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<Result> CompactAsync(string endpoint, long revision, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<Result> DefragmentAsync(string endpoint, CancellationToken ct)
            => throw new NotSupportedException();
    }

    /// <summary>Фейк пул-стора: фиксированный пул.</summary>
    internal sealed class FakePools(S3Pool? pool) : IS3PoolStore
    {
        public Task<Result<S3Pool?>> ReadAsync(string name, CancellationToken ct)
            => Task.FromResult(Result<S3Pool?>.Success(pool));

        public Task<Result<IReadOnlyList<string>>> ListPoolNamesAsync(CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<string>>.Success(pool is null ? [] : [pool.Name]));
    }

    /// <summary>Фейк runner: пишет staging-набор при pg_basebackup (backup_label
    /// + файл данных + закрытый сегмент + .partial + .history), вызывает
    /// OnInvoke — программирование исходов/записи stdout по FileName.</summary>
    private sealed class FakeRunner : IProcessRunner
    {
        public readonly List<ProcessStartInfo> Invocations = [];
        public Func<ProcessStartInfo, (int, string, string)>? Respond;
        public Func<ProcessStartInfo, CancellationToken, Task<(int, string, string)>>? RespondAsync;
        public string? BasebackupStaging;

        public async Task<(int ExitCode, string StdOut, string StdErr)> RunAsync(
            ProcessStartInfo psi, CancellationToken ct)
        {
            Invocations.Add(psi);
            if (RespondAsync is { } async)
                return await async(psi, ct);
            if (Respond is { } sync)
                return sync(psi);
            if (psi.FileName == "pg_basebackup")
            {
                // Фейковый набор: каталог -D из ArgumentList.
                var dest = psi.ArgumentList[psi.ArgumentList.IndexOf("-D") + 1];
                BasebackupStaging = dest;
                Directory.CreateDirectory(Path.Combine(dest, "pg_wal"));
                await File.WriteAllTextAsync(
                    Path.Combine(dest, "backup_label"),
                    "START WAL LOCATION: 0/2000028 (file 000000010000000000000002)\n");
                await File.WriteAllTextAsync(Path.Combine(dest, "postgresql.conf"), "x");
                await File.WriteAllTextAsync(
                    Path.Combine(dest, "pg_wal", "000000010000000000000002"), "wal");
                // Реальный pg_basebackup кладёт сегменты набора ВЕРХНИМ hex —
                // буквенный сегмент проверяет нормализацию дубля wal/.
                await File.WriteAllTextAsync(
                    Path.Combine(dest, "pg_wal", "0000000100000000000000AB"), "wal");
                await File.WriteAllTextAsync(
                    Path.Combine(dest, "pg_wal", "000000010000000000000003.partial"), "p");
                await File.WriteAllTextAsync(
                    Path.Combine(dest, "pg_wal", "00000002.history"), "h");
                return (0, "", "");
            }

            return (0, "", "");
        }
    }

    /// <summary>Фейк IBackupS3: put-файлы в словарь ключей; программирование
    /// отказа put; list/get — для verify/restore (здесь не нужны).</summary>
    private sealed class FakeS3 : IBackupS3
    {
        public readonly List<string> PutKeys = [];
        public bool FailPut;

        public Task<Result> PutFileAsync(string key, string filePath, CancellationToken ct = default)
        {
            PutKeys.Add(key);
            return Task.FromResult(FailPut
                ? Result.Failed(new ApplicationException(
                    $"S3 put-file {key}: all list nodes are unavailable ({S3PoolMarkers.Exhausted})"))
                : Result.Success());
        }

        public Task<Result> PutObjectAsync(string key, byte[] data, string? sha256, CancellationToken ct = default)
            => Task.FromResult(Result.Success());
        public Task<Result> GetObjectToFileAsync(string cluster, string shard, string key, string destFilePath, CancellationToken ct = default)
            => Task.FromResult(Result.Success());
        public Task<Result<bool>> BucketExistsAsync(CancellationToken ct) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<WalObject>>> ListWalAsync(string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<S3ObjectInfo>>> ListPrefixAsync(string prefix, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result> DeleteKeysAsync(IReadOnlyList<string> keys, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<WalObject>>> ListAsync(string cluster, string shard, string prefix, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<string>> GetObjectAsync(string cluster, string shard, string key, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<string>>> ListFullsAsync(string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<string>> DownloadTextAsync(string cluster, string shard, string objectKey, CancellationToken ct = default) => throw new NotSupportedException();
    }

    private static FullBackupOperation Create(
        FakeEtcd etcd, FakeRunner runner, FakeS3? s3, string staging,
        OperationRegistry registry)
        => new(Id, staging, Identity(), etcd, ["http://etcd-1:2379"], Pools(),
            _ => Task.FromResult(WalReceiverRole.Sync), runner, registry,
            _ => (IBackupS3)s3!);

    [Fact]
    public async Task HappyPath_PhasesAndUploadKeys_ThenStagingCleaned()
    {
        // Arrange: фейки etcd/runner/S3 + временный staging.
        var staging = Path.Combine(Path.GetTempPath(), $"t26-full-{Guid.NewGuid():N}");
        Directory.CreateDirectory(staging);
        var etcd = Etcd();
        var runner = new FakeRunner();
        var s3 = new FakeS3();
        var registry = new OperationRegistry();
        var op = Create(etcd, runner, s3, staging, registry);

        // Act: полный пайплайн.
        await op.RunAsync(CancellationToken.None);

        // Assert: фазы running/basebackup → uploading(+wal_start) → succeeded.
        var snap = registry.Current!;
        snap.State.Should().Be("succeeded");
        snap.Phase.Should().Be("uploading");
        snap.WalStartSegment.Should().Be("000000010000000000000002");
        snap.SizeBytes.Should().BeGreaterThan(0);
        // Assert: put 1:1 full/<id>/… + wal-дубль закрытого сегмента.
        var prefix = $"{Cluster}/{Shard}";
        s3.PutKeys.Should().Contain($"{prefix}/full/{Id}/backup_label");
        s3.PutKeys.Should().Contain($"{prefix}/full/{Id}/postgresql.conf");
        s3.PutKeys.Should().Contain($"{prefix}/full/{Id}/pg_wal/000000010000000000000002");
        s3.PutKeys.Should().Contain($"{prefix}/wal/000000010000000000000002");
        // Assert: .partial/.history в wal/ НЕ дублируются.
        s3.PutKeys.Should().NotContain(k => k.StartsWith($"{prefix}/wal/") && k.EndsWith(".partial"));
        s3.PutKeys.Should().NotContain(k => k.StartsWith($"{prefix}/wal/") && k.EndsWith(".history"));
        // Assert: пароль — ТОЛЬКО в env PGPASSWORD (не argv/DSN).
        var psi = runner.Invocations.Single(i => i.FileName == "pg_basebackup");
        psi.EnvironmentVariables["PGPASSWORD"].Should().Be(Password);
        string.Join(" ", psi.ArgumentList).Should().NotContain(Password);
        // Assert: HOME=/tmp — /root недоступен под uid 101 (Spilo: postgresql.crt
        // в /root/.postgresql; libpq без этого падает Permission denied).
        psi.EnvironmentVariables["HOME"].Should().Be("/tmp");
        // Assert: staging очищен после исхода.
        Directory.EnumerateFileSystemEntries(staging).Should().BeEmpty();

        // Cleanup: временный каталог.
        Directory.Delete(staging, recursive: true);
    }

    // AAA: wal-дубли набора заливаются в общий префикс wal/ ПОД НОРМАЛИЗОВАННЫМ
    // (СТРОЧНЫМ) именем — pg_basebackup кладёт сегменты ВЕРХНИМ hex, канон
    // ключей wal/ — строчные (симметрия стриму приёмников и fetch-wal): дубль
    // сливается со стримом, мёртвого груза в верхнем регистре нет (arch/19 §2,
    // spec §3.4). Файл 1:1 в full/<id>/pg_wal/ остаётся ВЕРХНИМ (набор 1:1).
    [Fact]
    public async Task WalDuplicates_UploadedLowercased_MergeWithStream()
    {
        // Arrange: фейки etcd/runner/S3 + временный staging (сегмент 0AB —
        // ВЕРХНИМ в наборе, как кладёт pg_basebackup).
        var staging = Path.Combine(Path.GetTempPath(), $"t26-full-{Guid.NewGuid():N}");
        Directory.CreateDirectory(staging);
        var etcd = Etcd();
        var runner = new FakeRunner();
        var s3 = new FakeS3();
        var registry = new OperationRegistry();
        var op = Create(etcd, runner, s3, staging, registry);

        // Act: полный пайплайн.
        await op.RunAsync(CancellationToken.None);

        // Assert: дубль — СТРОЧНЫМ именем (сливается со стримом); верхнего
        // дубля в wal/ нет.
        var prefix = $"{Cluster}/{Shard}";
        registry.Current!.State.Should().Be("succeeded");
        s3.PutKeys.Should().Contain($"{prefix}/wal/0000000100000000000000ab");
        s3.PutKeys.Should().NotContain(k => k.StartsWith($"{prefix}/wal/")
            && k.EndsWith("0000000100000000000000AB", StringComparison.Ordinal),
            "the wal/ key canon is lowercase — an uppercase duplicate is dead weight");
        // Assert: файл НАБОРА в full/<id>/ — 1:1, ВЕРХНИМ (не тронут).
        s3.PutKeys.Should().Contain($"{prefix}/full/{Id}/pg_wal/0000000100000000000000AB");

        // Cleanup: временный каталог.
        Directory.Delete(staging, recursive: true);
    }

    [Fact]
    public async Task BasebackupFails_FailedWithOneLineError()
    {
        // Arrange: runner отвечает pg_basebackup exit 1 + многострочный stderr.
        var staging = Path.Combine(Path.GetTempPath(), $"t26-full-{Guid.NewGuid():N}");
        Directory.CreateDirectory(staging);
        var runner = new FakeRunner
        {
            Respond = psi => psi.FileName == "pg_basebackup"
                ? (1, "", "pg_basebackup: error: could not connect to server\nsecond line\n")
                : (0, "", ""),
        };
        var registry = new OperationRegistry();
        var op = Create(Etcd(), runner, new FakeS3(), staging, registry);

        // Act.
        await op.RunAsync(CancellationToken.None);

        // Assert: failed + однострочная причина; staging очищен.
        var snap = registry.Current!;
        snap.State.Should().Be("failed");
        snap.Phase.Should().Be("basebackup");
        snap.Error.Should().Be("pg_basebackup: error: could not connect to server");
        Directory.EnumerateFileSystemEntries(staging).Should().BeEmpty();
        Directory.Delete(staging, recursive: true);
    }

    [Fact]
    public async Task UploadFails_FailedWithExhaustedMarker()
    {
        // Arrange: S3-фейк отказывает на put.
        var staging = Path.Combine(Path.GetTempPath(), $"t26-full-{Guid.NewGuid():N}");
        Directory.CreateDirectory(staging);
        var s3 = new FakeS3 { FailPut = true };
        var registry = new OperationRegistry();
        var op = Create(Etcd(), new FakeRunner(), s3, staging, registry);

        // Act.
        await op.RunAsync(CancellationToken.None);

        // Assert: failed с маркером исчерпания пула.
        var snap = registry.Current!;
        snap.State.Should().Be("failed");
        snap.Error.Should().Contain(S3PoolMarkers.Exhausted);
        Directory.Delete(staging, recursive: true);
    }

    [Fact]
    public async Task Cancel_MidOperation_FailedCancelledAndCleaned()
    {
        // Arrange: runner «висит» на pg_basebackup до отмены.
        var staging = Path.Combine(Path.GetTempPath(), $"t26-full-{Guid.NewGuid():N}");
        Directory.CreateDirectory(staging);
        var started = new TaskCompletionSource();
        var runner = new FakeRunner
        {
            RespondAsync = async (_, ct) =>
            {
                started.TrySetResult();
                try
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, ct);
                    return (0, "", "");
                }
                catch (OperationCanceledException)
                {
                    // Прод-ProcessRunner при отмене бросает OCE (kill дерева).
                    throw;
                }
            },
        };
        var registry = new OperationRegistry();
        var op = Create(Etcd(), runner, new FakeS3(), staging, registry);

        // Act: запуск фоном → отмена после старта → ожидание исхода.
        var testCt = TestContext.Current.CancellationToken;
        var run = Task.Run(() => op.RunAsync(CancellationToken.None), testCt);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(10), testCt);
        await op.CancelAsync();
        await run.WaitAsync(TimeSpan.FromSeconds(10), testCt);

        // Assert: failed `cancelled`; staging очищен.
        registry.Current!.State.Should().Be("failed");
        registry.Current.Error.Should().Be("cancelled");
        Directory.EnumerateFileSystemEntries(staging).Should().BeEmpty();
        Directory.Delete(staging, recursive: true);
    }
}
