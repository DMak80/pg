using System.Text.Json;
using OwnS3.Storage;

namespace OwnS3.UnitTests.Storage;

// Чистка брошенных загрузок (канон 04 §5–6): 24 ч от initiation-записи, mtime-прокси
// при битом журнале, опустевшие sha-каталоги удаляются; живые загрузки не трогаются.
// Время — TimeProvider.System, возраст — File.SetLastWriteTimeUtc / записью
// InitiatedMs в прошлое (детерминированно, без sleep).
public class MultipartCleanupTests : IDisposable
{
    private static readonly long NowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    private readonly XlVolume _volume = new(
        Path.Combine(Path.GetTempPath(), "owns3-mc-" + Guid.NewGuid().ToString("N")), TimeProvider.System);

    private string Root => _volume.Root;
    private XlVolume Volume => _volume;

    public void Dispose()
    {
        // Teardown: temp-том при любом исходе
        if (Directory.Exists(Root))
            Directory.Delete(Root, recursive: true);
    }

    // Макет живой загрузки: каталог + часть + запись в журнале (внутренний API).
    private string SeedUpload(string bucket, string key, string uploadId, long initiatedMs)
    {
        var keyDir = MultipartJournals.KeyDir(Volume.MultipartDir, bucket, key);
        var uploadDir = MultipartJournals.UploadDirPath(keyDir, uploadId);
        Directory.CreateDirectory(uploadDir);
        File.WriteAllText(Path.Combine(uploadDir, "part.1"), "data");
        var uploads = MultipartJournals.ReadUploads(MultipartJournals.UploadsJsonPath(keyDir));
        uploads.Add(new MultipartJournals.UploadJournalEntry(uploadId, bucket, key, initiatedMs, "writer"));
        MultipartJournals.WriteUploads(MultipartJournals.UploadsJsonPath(keyDir), uploads);
        return keyDir;
    }

    [Fact]
    public async Task RunCleanup_AbandonedByInitiation_TrashedAndRecordRemoved()
    {
        // Arrange: запись инициирована 25 ч назад; mtime каталога — свежий
        _volume.Initialize();
        var keyDir = SeedUpload("b", "k", "u1", NowMs - (long)TimeSpan.FromHours(25).TotalMilliseconds);
        await _volume.RunCleanupAsync(TestContext.Current.CancellationToken);

        // Act
        var uploads = MultipartJournals.ReadUploads(MultipartJournals.UploadsJsonPath(keyDir));

        // Assert: запись удалена; каталог загрузки ушёл в .trash; sha-каталог удалён
        uploads.Should().BeEmpty();
        Directory.Exists(MultipartJournals.UploadDirPath(keyDir, "u1")).Should().BeFalse();
        Directory.Exists(keyDir).Should().BeFalse();
    }

    [Fact]
    public async Task RunCleanup_FreshUpload_Kept()
    {
        // Arrange: живая загрузка моложе порога
        _volume.Initialize();
        var keyDir = SeedUpload("b", "k", "u2", NowMs);
        await _volume.RunCleanupAsync(TestContext.Current.CancellationToken);

        // Act / Assert: запись и каталог на месте
        MultipartJournals.ReadUploads(MultipartJournals.UploadsJsonPath(keyDir))
            .Should().ContainSingle().Which.UploadId.Should().Be("u2");
        Directory.Exists(MultipartJournals.UploadDirPath(keyDir, "u2")).Should().BeTrue();
    }

    [Fact]
    public async Task RunCleanup_BrokenUploadsJson_MtimeProxyTrashes()
    {
        // Arrange: журнал перезаписан мусором; mtime каталога загрузки — в прошлом
        _volume.Initialize();
        var keyDir = SeedUpload("b", "k", "u3", NowMs);
        File.WriteAllText(MultipartJournals.UploadsJsonPath(keyDir), "{broken");
        var uploadDir = MultipartJournals.UploadDirPath(keyDir, "u3");
        File.SetLastWriteTimeUtc(uploadDir, DateTime.UtcNow - TimeSpan.FromHours(25));
        await _volume.RunCleanupAsync(TestContext.Current.CancellationToken);

        // Act / Assert: каталог в .trash по mtime-прокси; битый файл удалён вместе с sha-каталогом
        Directory.Exists(uploadDir).Should().BeFalse();
        Directory.Exists(keyDir).Should().BeFalse();
    }

    [Fact]
    public async Task RunCleanup_EmptyJournalNoDirs_RemovesShaDir()
    {
        // Arrange: пустой журнал и ни одного каталога загрузки
        _volume.Initialize();
        var keyDir = MultipartJournals.KeyDir(Volume.MultipartDir, "b", "k");
        Directory.CreateDirectory(keyDir);
        MultipartJournals.WriteUploads(MultipartJournals.UploadsJsonPath(keyDir), []);
        await _volume.RunCleanupAsync(TestContext.Current.CancellationToken);

        // Act / Assert: опустевший sha-каталог удалён (в нём только uploads.json)
        Directory.Exists(keyDir).Should().BeFalse();
    }

    [Fact]
    public async Task RunCleanup_GhostRecordWithoutDir_RemovedFromJournal()
    {
        // Arrange: запись есть, каталога загрузки нет (утрачен) — «призрачная» запись
        _volume.Initialize();
        var keyDir = SeedUpload("b", "k", "u4", NowMs - (long)TimeSpan.FromHours(30).TotalMilliseconds);
        Directory.Delete(MultipartJournals.UploadDirPath(keyDir, "u4"), recursive: true);
        await _volume.RunCleanupAsync(TestContext.Current.CancellationToken);

        // Act / Assert: запись чистится по возрасту; sha-каталог удаляется
        MultipartJournals.ReadUploads(MultipartJournals.UploadsJsonPath(keyDir)).Should().BeEmpty();
        Directory.Exists(keyDir).Should().BeFalse();
    }

    [Fact]
    public async Task RunCleanup_GhostFreshRecord_JournalKept()
    {
        // Arrange: призрачная запись моложе порога — журнал не пустеет, но и
        // мёртвого каталога нет: чистке нечего удалять, sha-каталог жив
        _volume.Initialize();
        var keyDir = SeedUpload("b", "k", "u5", NowMs);
        Directory.Delete(MultipartJournals.UploadDirPath(keyDir, "u5"), recursive: true);
        await _volume.RunCleanupAsync(TestContext.Current.CancellationToken);

        // Act / Assert: запись ещё в журнале (возраст < 24 ч)
        MultipartJournals.ReadUploads(MultipartJournals.UploadsJsonPath(keyDir))
            .Should().ContainSingle().Which.UploadId.Should().Be("u5");
    }

    [Fact]
    public void Initialize_RunsAbandonedCleanup()
    {
        // Arrange: брошенная загрузка на инициализированном томе (рестарт процесса)
        _volume.Initialize();
        var keyDir = SeedUpload("b", "k", "u6", NowMs - (long)TimeSpan.FromHours(26).TotalMilliseconds);

        // Act: повторный старт тома выполняет чистку брошенных загрузок
        _volume.Initialize();

        // Assert: как фоновый проход — запись удалена, sha-каталог удалён
        MultipartJournals.ReadUploads(MultipartJournals.UploadsJsonPath(keyDir)).Should().BeEmpty();
        Directory.Exists(keyDir).Should().BeFalse();
    }
}
