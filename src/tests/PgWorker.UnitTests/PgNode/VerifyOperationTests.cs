using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using PgWorker.Backups;
using PgWorker.Backups.Tar;
using PgWorker.PgNode.Operations;
using Shared.Core;
// Одноимённые контракты двух проектов (приёмник/воркер): берём контракт ноды.
using StartVerifyRequest = PgWorker.PgNode.Operations.StartVerifyRequest;
using S3Context = PgWorker.PgNode.Operations.S3Context;

namespace PgWorker.UnitTests.PgNode;

// Потоковый verify (spec t01 §3.5, §6.7): манифест из хвоста tar-объекта
// (Range, размер из HEAD) → один проход потока со сверкой sha256 записей;
// OK/битый файл/лишний/отсутствующий/транспортный сбой/сквозной sha256.
public class VerifyOperationTests
{
    private const string Id = "20260101000000Z";

    private static readonly (string Name, byte[] Data) Version = ("PG_VERSION", "18\n"u8.ToArray());
    private static readonly (string Name, byte[] Data) Control = ("global/pg_control", new byte[8192]);
    private static readonly (string Name, byte[] Data) Label = (
        "backup_label",
        "START WAL LOCATION: 0/2000028 (file 000000010000000000000002)\n"u8.ToArray());
    private static readonly (string Name, byte[] Data) Segment = (
        "pg_wal/000000010000000000000001", new byte[16]);

    private static StartVerifyRequest Request(string? sha256 = null) => new(
        Id,
        new S3Context(
            Endpoints: ["http://s3a:9000"],
            AccessKey: "ak",
            SecretKey: "sk",
            Bucket: "backups",
            Prefix: "c1/s1"),
        Sha256: sha256);

    // AAA: валидный набор → succeeded/verifying; Range — от 0 (объект < 8 МиБ).
    [Fact]
    public async Task ValidSet_Succeeded_RangeFromZero()
    {
        // Arrange — файлы + WAL-сегмент (не в манифесте) + манифест последним.
        var tar = BuildTar([Version, Control, Label, Segment], [Version, Control, Label]);
        var s3 = new FakeTarS3(tar);
        var op = Create(s3);

        // Act
        await op.RunAsync(TestContext.Current.CancellationToken);

        // Assert
        var snap = op.Snapshot!;
        snap.State.Should().Be("succeeded");
        snap.Phase.Should().Be("verifying");
        s3.Ranges.Should().ContainSingle();
        s3.Ranges[0].Start.Should().Be(0, "an object smaller than the tail is read from byte 0");
        s3.Ranges[0].End.Should().Be(tar.Length - 1);
    }

    // AAA: байт файла не сходится с манифестом → failed/verifying PERMANENT.
    [Fact]
    public async Task CorruptFile_ChecksumMismatch()
    {
        // Arrange — в tar «19\n», манифест посчитан по «18\n».
        var tar = BuildTar(
            [("PG_VERSION", "19\n"u8.ToArray()), Control, Label],
            [Version, Control, Label]);
        var op = Create(new FakeTarS3(tar));

        // Act
        await op.RunAsync(TestContext.Current.CancellationToken);

        // Assert
        var snap = op.Snapshot!;
        snap.State.Should().Be("failed");
        snap.Phase.Should().Be("verifying");
        snap.Error.Should().Contain("checksum mismatch PG_VERSION");
    }

    // AAA: файл вне манифеста → failed "unexpected file".
    [Fact]
    public async Task ExtraFile_Unexpected()
    {
        // Arrange — лишняя запись, манифест её не содержит.
        var extra = ("extra.txt", "boom"u8.ToArray());
        var tar = BuildTar([extra, Version, Control, Label], [Version, Control, Label]);
        var op = Create(new FakeTarS3(tar));

        // Act
        await op.RunAsync(TestContext.Current.CancellationToken);

        // Assert
        var snap = op.Snapshot!;
        snap.State.Should().Be("failed");
        snap.Error.Should().Contain("unexpected file extra.txt");
    }

    // AAA: файл манифеста отсутствует в tar → failed "missing file".
    [Fact]
    public async Task MissingFile_Missing()
    {
        // Arrange — манифест перечисляет файл, которого в tar нет.
        var missing = ("global/missing.dat", new byte[4]);
        var tar = BuildTar([Version, Control, Label], [Version, Control, Label, missing]);
        var op = Create(new FakeTarS3(tar));

        // Act
        await op.RunAsync(TestContext.Current.CancellationToken);

        // Assert
        var snap = op.Snapshot!;
        snap.State.Should().Be("failed");
        snap.Error.Should().Contain("missing file global/missing.dat");
    }

    // AAA: транспортный отказ Range → failed/downloading (transient).
    [Fact]
    public async Task RangeTransportFailure_FailedDownloading()
    {
        // Arrange
        var tar = BuildTar([Version, Control, Label], [Version, Control, Label]);
        var op = Create(new FakeTarS3(tar) { FailRange = true });

        // Act
        await op.RunAsync(TestContext.Current.CancellationToken);

        // Assert
        var snap = op.Snapshot!;
        snap.State.Should().Be("failed");
        snap.Phase.Should().Be("downloading");
        snap.Error.Should().NotBeNull();
    }

    // AAA: объекта нет ни на одной цели → failed/downloading.
    [Fact]
    public async Task ObjectNotFound_FailedDownloading()
    {
        // Arrange
        var tar = BuildTar([Version, Control, Label], [Version, Control, Label]);
        var op = Create(new FakeTarS3(tar) { ObjectExists = false });

        // Act
        await op.RunAsync(TestContext.Current.CancellationToken);

        // Assert
        var snap = op.Snapshot!;
        snap.State.Should().Be("failed");
        snap.Phase.Should().Be("downloading");
        snap.Error.Should().Contain("not found");
    }

    // AAA: манифеста нет в объекте → failed/downloading (хвост не нашёл запись).
    [Fact]
    public async Task NoManifestInObject_FailedDownloading()
    {
        // Arrange — tar без записи backup_manifest.
        var tar = BuildTar([Version, Control, Label], [Version, Control, Label], includeManifest: false);
        var op = Create(new FakeTarS3(tar));

        // Act
        await op.RunAsync(TestContext.Current.CancellationToken);

        // Assert
        var snap = op.Snapshot!;
        snap.State.Should().Be("failed");
        snap.Phase.Should().Be("downloading");
        snap.Error.Should().Contain("backup_manifest not found in tail");
    }

    // AAA: сквозной sha256 объекта не сошёлся с etcd-полем → failed.
    [Fact]
    public async Task ObjectShaMismatch_Failed()
    {
        // Arrange — ожидаемый хэш заведомо другой.
        var tar = BuildTar([Version, Control, Label], [Version, Control, Label]);
        var op = Create(new FakeTarS3(tar), Request(sha256: new string('0', 64)));

        // Act
        await op.RunAsync(TestContext.Current.CancellationToken);

        // Assert
        var snap = op.Snapshot!;
        snap.State.Should().Be("failed");
        snap.Error.Should().Contain("object sha256 mismatch");
    }

    // AAA: совпадающий сквозной sha256 (эталон — sha256 объекта) — succeeded.
    [Fact]
    public async Task ObjectShaMatches_Succeeded()
    {
        // Arrange
        var tar = BuildTar([Version, Control, Label], [Version, Control, Label]);
        var op = Create(new FakeTarS3(tar),
            Request(sha256: Convert.ToHexString(SHA256.HashData(tar)).ToLowerInvariant()));

        // Act
        await op.RunAsync(TestContext.Current.CancellationToken);

        // Assert
        op.Snapshot!.State.Should().Be("succeeded");
    }

    private static VerifyOperation Create(FakeTarS3 s3, StartVerifyRequest? request = null)
    {
        var registry = new OperationRegistry();
        registry.TryBegin(new OperationSnapshot(Id, "running", "downloading"));
        return new VerifyOperation(request ?? Request(), registry, _ => s3);
    }

    // Tar-объект: записи + backup_manifest ПОСЛЕДНЕЙ (verify читает хвост);
    // манифест перечисляет manifestFiles (по умолчанию — все, кроме WAL).
    private static byte[] BuildTar(
        IReadOnlyList<(string Name, byte[] Data)> entries,
        IReadOnlyList<(string Name, byte[] Data)>? manifestFiles = null,
        bool includeManifest = true)
    {
        var buffer = new MemoryStream();
        using var writer = new TarStreamWriter(buffer);
        foreach (var (name, data) in entries)
        {
            writer.WriteEntryAsync(
                new TarEntry(name, TarTypeFlags.File, data.Length, 0x1A4, ""), data, default)
                .GetAwaiter().GetResult();
        }

        if (includeManifest)
        {
            var files = (manifestFiles ?? entries)
                .Select(f => new Dictionary<string, object?>
                {
                    ["Path"] = f.Name,
                    ["Size"] = f.Data.Length,
                    ["Checksum-Algorithm"] = "SHA256",
                    ["Checksum"] = Convert.ToHexString(SHA256.HashData(f.Data)),
                })
                .ToList();
            var json = JsonSerializer.Serialize(new Dictionary<string, object?>
            {
                ["PostgreSQL-Backup-Manifest-Version"] = 2,
                ["Files"] = files,
            });
            var manifest = Encoding.ASCII.GetBytes(json);
            writer.WriteEntryAsync(
                new TarEntry("backup_manifest", TarTypeFlags.File, manifest.Length, 0x1A0, ""),
                manifest, default).GetAwaiter().GetResult();
        }

        writer.FinishAsync(default).GetAwaiter().GetResult();
        return buffer.ToArray();
    }

    /// <summary>Фейк S3-цели verify: tar-объект, HEAD (существование/размер),
    /// Range-срезы и стрим; программируемые отказы.</summary>
    private sealed class FakeTarS3(byte[] tar) : IBackupS3, IStreamObjectS3
    {
        public bool ObjectExists { get; init; } = true;

        public bool FailRange { get; init; }

        public List<(long Start, long End)> Ranges { get; } = [];

        public Task<Result<(bool Exists, long SizeBytes)>> HeadObjectAsync(
            string cluster, string shard, string key, CancellationToken ct)
            => Task.FromResult(Result<(bool Exists, long SizeBytes)>.Success(
                ObjectExists ? (true, (long)tar.Length) : (false, 0L)));

        public Task<Result<byte[]>> GetObjectRangeAsync(
            string cluster, string shard, string key, long start, long end, CancellationToken ct)
        {
            if (FailRange)
                return Task.FromResult(Result<byte[]>.Failed(
                    new ApplicationException("S3 get-range: connection reset")));
            Ranges.Add((start, end));
            return Task.FromResult(Result<byte[]>.Success(tar[(int)start..(int)(end + 1)]));
        }

        public Task<Result<(Stream Body, long Size)>> GetObjectStreamAsync(
            string cluster, string shard, string key, CancellationToken ct)
            => Task.FromResult(Result<(Stream Body, long Size)>.Success(
                ((Stream)new MemoryStream(tar), (long)tar.Length)));

        // IBackupS3 — verify не использует.
        public Task<Result<bool>> BucketExistsAsync(CancellationToken ct) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<WalObject>>> ListWalAsync(string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<S3ObjectInfo>>> ListPrefixAsync(string prefix, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result> DeleteKeysAsync(IReadOnlyList<string> keys, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result> PutObjectAsync(string key, byte[] data, string? sha256, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<WalObject>>> ListAsync(string cluster, string shard, string prefix, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<string>> GetObjectAsync(string cluster, string shard, string key, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result> PutObjectMd5Async(string key, byte[] data, string md5Hex, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<(byte[] Data, string? ETag)>> GetObjectBytesAsync(string cluster, string shard, string key, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<string>>> ListFullsAsync(string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<string>> DownloadTextAsync(string cluster, string shard, string objectKey, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result> PutFileAsync(string key, string filePath, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result> GetObjectToFileAsync(string cluster, string shard, string key, string destFilePath, CancellationToken ct = default) => throw new NotSupportedException();
    }
}
