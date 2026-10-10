using System.Security.Cryptography;
using System.Text.Json;
using OwnS3.Storage;

namespace OwnS3.UnitTests.Storage;

// Журналы multipart-контура (канон 04 §5): sha-пути, roundtrip
// uploads/parts/attempt, атомарность (tmp не остаётся), отсутствие
// файла/пустой файл = пустой список, битый JSON — наружу.
public class MultipartJournalsTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "owns3-mj-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        // Teardown: temp-каталог при любом исходе
        if (Directory.Exists(_root))
            Directory.Delete(_root, recursive: true);
    }

    [Fact]
    public void KeyDir_IsSha256HexOfSlashJoinedBucketKey()
    {
        // Arrange
        var expected = Convert.ToHexString(SHA256.HashData(
            System.Text.Encoding.UTF8.GetBytes("bucket/key with space"))).ToLowerInvariant();

        // Act
        var dir = MultipartJournals.KeyDir(_root, "bucket", "key with space");

        // Assert: hex lowercase sha256, каталог внутри multipart-корня
        dir.Should().Be(Path.Combine(_root, expected));
    }

    [Fact]
    public void UploadsRoundtrip_WritesCamelCaseJsonAtomically()
    {
        // Arrange
        var keyDir = Path.Combine(_root, "abc");
        Directory.CreateDirectory(keyDir);
        var entries = new List<MultipartJournals.UploadJournalEntry>
        {
            new("id1", "b", "k", 1_728_000_000_000, "writer"),
        };

        // Act
        MultipartJournals.WriteUploads(MultipartJournals.UploadsJsonPath(keyDir), entries);
        var read = MultipartJournals.ReadUploads(MultipartJournals.UploadsJsonPath(keyDir));

        // Assert: roundtrip; camelCase-поля; tmp-файл не остался
        read.Should().ContainSingle().Which.Should().Match<MultipartJournals.UploadJournalEntry>(e =>
            e.UploadId == "id1" && e.Bucket == "b" && e.Key == "k"
            && e.InitiatedMs == 1_728_000_000_000 && e.AccessKey == "writer");
        File.Exists(Path.Combine(keyDir, "uploads.json.tmp")).Should().BeFalse();
        JsonDocument.Parse(File.ReadAllText(MultipartJournals.UploadsJsonPath(keyDir)))
            .RootElement[0].EnumerateObject().Select(p => p.Name)
            .Should().BeEquivalentTo(["uploadId", "bucket", "key", "initiatedMs", "accessKey"]);
    }

    [Fact]
    public void PartsRoundtrip_CamelCaseSortedByCaller()
    {
        // Arrange: порядок в файле — как передал вызывающий (сортирует UploadPart)
        var keyDir = Path.Combine(_root, "parts");
        Directory.CreateDirectory(keyDir);
        var entries = new List<MultipartJournals.PartJournalEntry>
        {
            new(2, "cafebabe", 20, 500),
            new(1, "deadbeef", 10, 100),
        };

        // Act
        MultipartJournals.WriteParts(MultipartJournals.PartsJsonPath(keyDir), entries);
        var read = MultipartJournals.ReadParts(MultipartJournals.PartsJsonPath(keyDir));

        // Assert: roundtrip без переупорядочивания; camelCase-поля; tmp не остался
        read.Should().HaveCount(2);
        read[0].Should().Match<MultipartJournals.PartJournalEntry>(p =>
            p.PartNumber == 2 && p.ETag == "cafebabe" && p.Size == 20 && p.ModTimeMs == 500);
        read[1].PartNumber.Should().Be(1);
        File.Exists(Path.Combine(keyDir, "parts.json.tmp")).Should().BeFalse();
        JsonDocument.Parse(File.ReadAllText(MultipartJournals.PartsJsonPath(keyDir)))
            .RootElement[0].EnumerateObject().Select(p => p.Name)
            .Should().BeEquivalentTo(["partNumber", "etag", "size", "modTimeMs"]);
    }

    [Fact]
    public void AttemptRoundtrip_DataDirPreserved()
    {
        // Arrange
        var uploadDir = Path.Combine(_root, "u1");
        Directory.CreateDirectory(uploadDir);

        // Act
        MultipartJournals.WriteAttempt(uploadDir, new MultipartJournals.AttemptMarker("data-dir-name"));
        var read = MultipartJournals.ReadAttempt(uploadDir);

        // Assert: {dataDir}; tmp не остался; camelCase-поле
        read.Should().NotBeNull().And.Be(new MultipartJournals.AttemptMarker("data-dir-name"));
        File.Exists(Path.Combine(uploadDir, "attempt.json.tmp")).Should().BeFalse();
        JsonDocument.Parse(File.ReadAllText(MultipartJournals.AttemptJsonPath(uploadDir)))
            .RootElement.EnumerateObject().Select(p => p.Name).Should().Equal(["dataDir"]);
    }

    [Fact]
    public void Read_MissingFile_EmptyList()
    {
        // Arrange: каталог есть, файлов нет
        var keyDir = Path.Combine(_root, "missing");
        Directory.CreateDirectory(keyDir);

        // Act / Assert: отсутствующие журналы — пустые списки, не исключение
        MultipartJournals.ReadUploads(MultipartJournals.UploadsJsonPath(keyDir)).Should().BeEmpty();
        MultipartJournals.ReadParts(MultipartJournals.PartsJsonPath(keyDir)).Should().BeEmpty();
    }

    [Fact]
    public void ReadAttempt_Missing_Null()
    {
        // Arrange
        var uploadDir = Path.Combine(_root, "no-attempt");
        Directory.CreateDirectory(uploadDir);

        // Act
        var marker = MultipartJournals.ReadAttempt(uploadDir);

        // Assert: маркера нет — null (новой попытке не мешает, М7)
        marker.Should().BeNull();
    }

    [Fact]
    public void Read_EmptyFile_EmptyList()
    {
        // Arrange: пустой файл (краш после создания) — пустой список
        var keyDir = Path.Combine(_root, "empty");
        Directory.CreateDirectory(keyDir);
        File.WriteAllText(MultipartJournals.UploadsJsonPath(keyDir), string.Empty);
        File.WriteAllText(MultipartJournals.PartsJsonPath(keyDir), string.Empty);

        // Act / Assert
        MultipartJournals.ReadUploads(MultipartJournals.UploadsJsonPath(keyDir)).Should().BeEmpty();
        MultipartJournals.ReadParts(MultipartJournals.PartsJsonPath(keyDir)).Should().BeEmpty();
    }

    [Fact]
    public void Read_BrokenJson_Throws()
    {
        // Arrange: мусор вместо JSON — JsonException наружу (политику решает вызывающий)
        var keyDir = Path.Combine(_root, "broken");
        Directory.CreateDirectory(keyDir);
        File.WriteAllText(MultipartJournals.UploadsJsonPath(keyDir), "{not-json");
        File.WriteAllText(MultipartJournals.PartsJsonPath(keyDir), "{not-json");

        // Act / Assert
        var uploads = () => MultipartJournals.ReadUploads(MultipartJournals.UploadsJsonPath(keyDir));
        uploads.Should().Throw<JsonException>();
        var parts = () => MultipartJournals.ReadParts(MultipartJournals.PartsJsonPath(keyDir));
        parts.Should().Throw<JsonException>();
    }

    [Fact]
    public void PartFileNames_Conventions()
    {
        // Arrange / Act / Assert: имена файлов частей по канону 04 §5
        MultipartJournals.PartFileName(1).Should().Be("part.1");
        MultipartJournals.PartFileName(10000).Should().Be("part.10000");
        MultipartJournals.PartTmpFileName(1).Should().Be("part.1.tmp");
    }

    [Fact]
    public void Paths_AreInsideExpectedDirs()
    {
        // Arrange
        var keyDir = Path.Combine(_root, "kd");
        var uploadDir = MultipartJournals.UploadDirPath(keyDir, "u1");

        // Act / Assert: раскладка канона 04 §5
        MultipartJournals.UploadsJsonPath(keyDir).Should().Be(Path.Combine(keyDir, "uploads.json"));
        uploadDir.Should().Be(Path.Combine(keyDir, "u1"));
        MultipartJournals.PartsJsonPath(uploadDir).Should().Be(Path.Combine(uploadDir, "parts.json"));
        MultipartJournals.AttemptJsonPath(uploadDir).Should().Be(Path.Combine(uploadDir, "attempt.json"));
    }
}
