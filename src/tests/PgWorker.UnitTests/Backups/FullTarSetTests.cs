using System.Text;
using PgWorker.Backups;
using PgWorker.Backups.Tar;
using Shared.Core;

namespace PgWorker.UnitTests.Backups;

// backup_label из tar-объекта полного (spec t01 §3.7): первая запись архива,
// Range [0, 64 КиБ−1]; иная первая запись — null; транспортный отказ — Failed.
public class FullTarSetTests
{
    private const string LabelText =
        "START WAL LOCATION: 0/2000028 (file 000000010000000000000002)\n" +
        "CHECKPOINT LOCATION: 0/2000028\nBACKUP METHOD: streamed\n";

    // AAA: первая запись backup_label → текст метки; Range ровно [0,65535].
    [Fact]
    public async Task ReadsBackupLabel_FromFirstEntry()
    {
        // Arrange
        var s3 = new FakeStreamS3(BuildTar(("backup_label", Encoding.ASCII.GetBytes(LabelText))));

        // Act
        var result = await FullTarSet.ReadBackupLabelAsync(
            s3, "c1", "s1", "20260101000000Z", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Contain("START WAL LOCATION: 0/2000028");
        s3.Ranges.Should().ContainSingle();
        s3.Ranges[0].Should().Be((0L, 64 * 1024 - 1));
    }

    // AAA: первая запись не backup_label → Success(null) (метки нет).
    [Fact]
    public async Task FirstEntryNotBackupLabel_ReturnsNull()
    {
        // Arrange
        var s3 = new FakeStreamS3(BuildTar(("PG_VERSION", "18\n"u8.ToArray())));

        // Act
        var result = await FullTarSet.ReadBackupLabelAsync(
            s3, "c1", "s1", "20260101000000Z", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().BeNull();
    }

    // AAA: транспортный отказ Range → Failed (причина наружу).
    [Fact]
    public async Task RangeFailure_Failed()
    {
        // Arrange
        var s3 = new FakeStreamS3([1, 2, 3]) { FailRange = true };

        // Act
        var result = await FullTarSet.ReadBackupLabelAsync(
            s3, "c1", "s1", "20260101000000Z", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error!.Message.Should().Contain("get-range");
    }

    private static byte[] BuildTar((string Name, byte[] Data) first)
    {
        var buffer = new MemoryStream();
        using var writer = new TarStreamWriter(buffer);
        writer.WriteEntryAsync(
            new TarEntry(first.Name, TarTypeFlags.File, first.Data.Length, 0x1A4, ""), first.Data, default)
            .GetAwaiter().GetResult();
        writer.FinishAsync(default).GetAwaiter().GetResult();
        return buffer.ToArray();
    }

    /// <summary>Фейк S3-цели: Range-срез начала объекта; Range фиксируется.</summary>
    private sealed class FakeStreamS3(byte[] objectBytes) : IStreamObjectS3
    {
        public bool FailRange { get; init; }

        public List<(long Start, long End)> Ranges { get; } = [];

        public Task<Result<(Stream Body, long Size)>> GetObjectStreamAsync(
            string cluster, string shard, string key, CancellationToken ct)
            => Task.FromResult(Result<(Stream Body, long Size)>.Success(
                ((Stream)new MemoryStream(objectBytes), (long)objectBytes.Length)));

        public Task<Result<byte[]>> GetObjectRangeAsync(
            string cluster, string shard, string key, long start, long end, CancellationToken ct)
        {
            if (FailRange)
                return Task.FromResult(Result<byte[]>.Failed(
                    new ApplicationException("S3 get-range c1/s1/full/x.tar: connection reset")));
            Ranges.Add((start, end));
            var clampedEnd = (int)Math.Min(end, objectBytes.Length - 1);
            return Task.FromResult(Result<byte[]>.Success(objectBytes[(int)start..(clampedEnd + 1)]));
        }

        public Task<Result<(bool Exists, long SizeBytes)>> HeadObjectAsync(
            string cluster, string shard, string key, CancellationToken ct)
            => Task.FromResult(Result<(bool Exists, long SizeBytes)>.Success(
                (true, (long)objectBytes.Length)));
    }
}
