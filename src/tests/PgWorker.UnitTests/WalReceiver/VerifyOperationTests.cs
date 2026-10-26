using System.Diagnostics;
using FluentAssertions;
using PgWorker.Backups;
using PgWorker.WalReceiver;
using PgWorker.WalReceiver.Operations;
using Shared.Core;
using Xunit;

namespace PgWorker.UnitTests.WalReceiver;

// Операция verify tool-контейнера (arch/19 §5, spec t26 §3.4) на фейках:
// счастливый путь — скачивание всех объектов листинга в staging →
// pg_verifybackup → succeeded; отказ скачивания → failed + phase=downloading
// (МАРКЕР ТРАНЗИЕНТА — ретраи воркера); pg_verifybackup ≠ 0 → failed +
// phase=verifying (PERMANENT). Staging чистится при любом исходе.
// AAA-комментарии.
public class VerifyOperationTests
{
    private const string Id = "20260101000000Z";

    private static PgWorker.WalReceiver.Operations.StartVerifyRequest Request() => new(
        Id,
        new PgWorker.WalReceiver.Operations.S3Context(
            Endpoints: ["http://s3a:9000"],
            AccessKey: "ak",
            SecretKey: "sk",
            Bucket: "backups",
            Prefix: "c1/s1"));

    /// <summary>Фейк runner: pg_verifybackup — программируемый exit/stderr.</summary>
    private sealed class FakeRunner : IProcessRunner
    {
        public readonly List<ProcessStartInfo> Invocations = [];
        public int VerifyExitCode;
        public string VerifyStderr = "";

        public async Task<(int ExitCode, string StdOut, string StdErr)> RunAsync(
            ProcessStartInfo psi, CancellationToken ct)
        {
            Invocations.Add(psi);
            if (psi.FileName == "pg_verifybackup")
                return (VerifyExitCode, "", VerifyStderr);
            return (0, "", "");
        }
    }

    /// <summary>Фейк IBackupS3: листинг/файловый GET (пишет dest); программный
    /// отказ листинга.</summary>
    private sealed class FakeS3 : IBackupS3
    {
        public bool FailList;
        public readonly List<string> Downloaded = [];
        // Композированные ключи запросов (cluster/shard/key) — ассерт состава
        // ключей ловит задвоение префикса (полный obj.Key вместо относительного).
        public readonly List<string> DownloadedKeys = [];

        public Task<Result<IReadOnlyList<S3ObjectInfo>>> ListPrefixAsync(
            string prefix, int? maxKeysPerTest = null, CancellationToken ct = default)
            => Task.FromResult(FailList
                ? Result<IReadOnlyList<S3ObjectInfo>>.Failed(
                    new ApplicationException("S3 list: pool down"))
                : Result<IReadOnlyList<S3ObjectInfo>>.Success(new List<S3ObjectInfo>
                {
                    new($"{prefix}backup_label", 60, DateTimeOffset.UnixEpoch),
                    new($"{prefix}pg_wal/000000010000000000000002", 5, DateTimeOffset.UnixEpoch),
                }));

        public Task<Result> GetObjectToFileAsync(
            string cluster, string shard, string key, string destFilePath, CancellationToken ct = default)
        {
            Downloaded.Add(destFilePath);
            DownloadedKeys.Add($"{cluster}/{shard}/{key}");
            Directory.CreateDirectory(Path.GetDirectoryName(destFilePath)!);
            File.WriteAllText(destFilePath, "data");
            return Task.FromResult(Result.Success());
        }

        public Task<Result> PutFileAsync(string key, string filePath, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result> PutObjectAsync(string key, byte[] data, string? sha256, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<bool>> BucketExistsAsync(CancellationToken ct) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<WalObject>>> ListWalAsync(string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result> DeleteKeysAsync(IReadOnlyList<string> keys, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<WalObject>>> ListAsync(string cluster, string shard, string prefix, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<string>> GetObjectAsync(string cluster, string shard, string key, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<string>>> ListFullsAsync(string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<string>> DownloadTextAsync(string cluster, string shard, string objectKey, CancellationToken ct = default) => throw new NotSupportedException();
    }

    [Fact]
    public async Task HappyPath_DownloadsAll_VerifySucceeds_StagingCleaned()
    {
        // Arrange: staging во временном каталоге; pg_verifybackup exit 0.
        var staging = Path.Combine(Path.GetTempPath(), $"t26-verify-{Guid.NewGuid():N}");
        Directory.CreateDirectory(staging);
        var runner = new FakeRunner { VerifyExitCode = 0 };
        var s3 = new FakeS3();
        var registry = new OperationRegistry();
        registry.TryBegin(new OperationSnapshot(Id, "running", "downloading"));
        var op = new VerifyOperation(Request(), runner, registry, staging, _ => s3);

        // Act.
        await op.RunAsync(CancellationToken.None);

        // Assert: succeeded (фаза verifying); оба объекта скачаны в staging.
        registry.Current!.State.Should().Be("succeeded");
        registry.Current.Phase.Should().Be("verifying");
        s3.Downloaded.Should().HaveCount(2);
        s3.Downloaded.Should().Contain(Path.Combine(staging, "full", Id, "backup_label"));
        s3.Downloaded.Should().Contain(Path.Combine(
            staging, "full", Id, "pg_wal", "000000010000000000000002"));
        // Assert: ключи скачивания — композиция cluster/shard + ОТНОСИТЕЛЬНОГО
        // ключа листинга (передача полного obj.Key давала бы задвоение префикса
        // c1/s1/c1/s1/... — ключа не существует, downloading-transient навсегда).
        s3.DownloadedKeys.Should().BeEquivalentTo([
            $"c1/s1/full/{Id}/backup_label",
            $"c1/s1/full/{Id}/pg_wal/000000010000000000000002"]);
        // Assert: pg_verifybackup запущен на staging-каталоге набора.
        var psi = runner.Invocations.Single(i => i.FileName == "pg_verifybackup");
        psi.ArgumentList.Should().Contain(Path.Combine(staging, "full", Id));
        // Assert: staging очищен после исхода.
        Directory.Exists(Path.Combine(staging, "full", Id)).Should().BeFalse();
        Directory.Delete(staging, recursive: true);
    }

    [Fact]
    public async Task DownloadFails_FailedWithDownloadingPhase_TransientMarker()
    {
        // Arrange: list от всех нод недоступен.
        var staging = Path.Combine(Path.GetTempPath(), $"t26-verify-{Guid.NewGuid():N}");
        Directory.CreateDirectory(staging);
        var registry = new OperationRegistry();
        registry.TryBegin(new OperationSnapshot(Id, "running", "downloading"));
        var op = new VerifyOperation(Request(), new FakeRunner(), registry, staging,
            _ => new FakeS3 { FailList = true });

        // Act.
        await op.RunAsync(CancellationToken.None);

        // Assert: failed + phase=downloading — маркер транзиента для воркера.
        registry.Current!.State.Should().Be("failed");
        registry.Current.Phase.Should().Be("downloading");
        registry.Current.Error.Should().Contain("pool down");
        Directory.Delete(staging, recursive: true);
    }

    [Fact]
    public async Task VerifybackupFails_FailedWithVerifyingPhase_Permanent()
    {
        // Arrange: скачивание ок, pg_verifybackup exit 1.
        var staging = Path.Combine(Path.GetTempPath(), $"t26-verify-{Guid.NewGuid():N}");
        Directory.CreateDirectory(staging);
        var runner = new FakeRunner
        {
            VerifyExitCode = 1,
            VerifyStderr = "backup_manifest checksum mismatch for file \"pg_wal/000000010000000000000002\"\nmore\n",
        };
        var registry = new OperationRegistry();
        registry.TryBegin(new OperationSnapshot(Id, "running", "downloading"));
        var op = new VerifyOperation(Request(), runner, registry, staging, _ => new FakeS3());

        // Act.
        await op.RunAsync(CancellationToken.None);

        // Assert: failed + phase=verifying — permanent (данные плохие);
        // причина — однострочная.
        registry.Current!.State.Should().Be("failed");
        registry.Current.Phase.Should().Be("verifying");
        // OpText.FirstLine Trim('"') снимает хвостовую кавычку одной строки.
        registry.Current.Error.Should().Be(
            "backup_manifest checksum mismatch for file \"pg_wal/000000010000000000000002");
        Directory.Delete(staging, recursive: true);
    }
}
