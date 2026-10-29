using FluentAssertions;
using PgWorker.Backups;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Скачивание объекта в файл (GetObjectToFileAsync — файловая запись): TEMP+RENAME
// семантика WriteDownloadToFileAsync — обрыв тела ответа (фейк-поток бросает
// посреди копирования) НЕ оставляет ни dest, ни .part; по успеху dest содержит
// полный контент. Ретраи идемпотентны (dest перезаписывается).
public class BackupS3FileDownloadTests
{
    // Поток, бросающий исключение посреди отдачи данных (обрыв тела ответа).
    private sealed class BrokenStream : MemoryStream
    {
        private readonly byte[] _prefix;

        public BrokenStream(byte[] prefix) : base(prefix) => _prefix = prefix;

        public override async Task CopyToAsync(
            Stream destination, int bufferSize, CancellationToken ct)
        {
            await destination.WriteAsync(_prefix.AsMemory(0, _prefix.Length), ct);
            await Task.Yield();
            throw new IOException("connection reset mid-body");
        }
    }

    private static string TempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"s3dl-{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);
        return dir;
    }

    // AAA: обрыв посреди копирования → dest отсутствует и .part не остался
    [Fact]
    public async Task MidBodyFailure_LeavesNoDestAndNoPart()
    {
        // Arrange — temp-каталог; поток отдаёт префикс и бросает
        var ct = TestContext.Current.CancellationToken;
        var dir = TempDir();
        try
        {
            var dest = Path.Combine(dir, "segment");
            using var source = new BrokenStream("partial-data-"u8.ToArray());

            // Act
            var act = () => BackupS3.WriteDownloadToFileAsync(source, dest, ct);

            // Assert — исключение пробрасывается (Result.Failed наверху), файловых
            // следов нет: dest не возник (не «валидный» частичный), .part погашен
            await act.Should().ThrowAsync<IOException>();
            File.Exists(dest).Should().BeFalse("a torn body must not surface as a valid file");
            File.Exists(dest + ".part").Should().BeFalse("the .part temp is cleaned up in catch");
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    // AAA: успех → dest с полным контентом, .part переименован (не остался)
    [Fact]
    public async Task Success_WritesFullContentAndConsumesPart()
    {
        // Arrange
        var ct = TestContext.Current.CancellationToken;
        var dir = TempDir();
        try
        {
            var dest = Path.Combine(dir, "backup_label");
            var payload = "START WAL LOCATION: 0/1000000"u8.ToArray();
            using var source = new MemoryStream(payload);

            // Act
            await BackupS3.WriteDownloadToFileAsync(source, dest, ct);

            // Assert
            File.Exists(dest).Should().BeTrue();
            (await File.ReadAllBytesAsync(dest, ct)).Should().Equal(payload);
            File.Exists(dest + ".part").Should().BeFalse();
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    // AAA: ретрай после обрыва — stale .part от чужого сбоя поглощается:
    // успешная попытка перезаписывает, по dest полный контент
    [Fact]
    public async Task RetryAfterFailure_OverwritesStalePart()
    {
        // Arrange — «мусор» чужого оборванного .part + полный поток повторной попытки
        var ct = TestContext.Current.CancellationToken;
        var dir = TempDir();
        try
        {
            var dest = Path.Combine(dir, "file");
            var stale = Path.Combine(dir, "file.part");
            await File.WriteAllTextAsync(stale, "torn", ct);
            var payload = "complete"u8.ToArray();
            using var source = new MemoryStream(payload);

            // Act
            await BackupS3.WriteDownloadToFileAsync(source, dest, ct);

            // Assert — dest полный, .part поглощён переименованием
            (await File.ReadAllTextAsync(dest, ct)).Should().Be("complete");
            File.Exists(stale).Should().BeFalse();
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }
}
