using OwnS3.Storage;

namespace OwnS3.UnitTests.Storage;

// Побайтовый формат xl.meta (спека §4.2 / канон 04 §2): roundtrip через файл,
// порядок записи (bkp → tmp → rename), фолбэк чтения bkp, строгая десериализация.
public class XlMetaFileTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "owns3-test-" + Guid.NewGuid().ToString("N"));

    private static XlMetaRecord SampleRecord(Guid? versionId = null) => new(
        VersionId: versionId ?? Guid.NewGuid(),
        Size: 42,
        ModTime: DateTimeOffset.FromUnixTimeMilliseconds(1_728_000_000_123),
        ETag: "5d41402abc4b2a76b9719d911017c592",   // hex без кавычек (P8)
        ContentType: "text/plain",
        UserMetadata: new Dictionary<string, string> { ["k1"] = "v1", ["k2"] = "v2" },
        Headers: new Dictionary<string, string> { ["Cache-Control"] = "no-cache" },
        ContentSha256: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824");

    public void Dispose()
    {
        // Teardown: temp-каталог удаляется при любом исходе прогона
        if (Directory.Exists(_dir))
            Directory.Delete(_dir, recursive: true);
    }

    [Fact]
    public void SerializeDeserialize_Roundtrip_AllFields()
    {
        // Arrange
        var record = SampleRecord();
        Directory.CreateDirectory(_dir);
        XlMetaFile.Write(_dir, record);

        // Act
        var read = XlMetaFile.ReadFile(Path.Combine(_dir, "xl.meta"));

        // Assert: все поля записи совпадают (userMetadata 2 пары, headers 1 пара,
        // etag hex без кавычек, sha256 64 hex)
        read.Should().BeEquivalentTo(record);
        read.VersionId.Should().Be(record.VersionId);
        read.DataDirName.Should().Be(record.VersionId.ToString("N"));
    }

    [Fact]
    public void WriteRead_ThroughFile_Roundtrip()
    {
        // Arrange
        var record = SampleRecord();
        Directory.CreateDirectory(_dir);
        XlMetaFile.Write(_dir, record);

        // Act
        var read = XlMetaFile.Read(_dir, out var fromBackup);

        // Assert: основной файл валиден — фолбэк не срабатывает
        read.Should().BeEquivalentTo(record);
        fromBackup.Should().BeFalse();
    }

    [Fact]
    public void Write_Overwrite_CreatesBkpAndReplaces()
    {
        // Arrange
        Directory.CreateDirectory(_dir);
        var v1 = SampleRecord(versionId: Guid.Parse("01020304-0506-4708-890a-0b0c0d0e0f10"));
        var v2 = SampleRecord(versionId: Guid.Parse("11121314-1516-4718-891a-1b1c1d1e1f20"));
        XlMetaFile.Write(_dir, v1);

        // Act
        XlMetaFile.Write(_dir, v2);

        // Assert: xl.meta = v2, xl.meta.bkp = v1
        XlMetaFile.ReadFile(Path.Combine(_dir, "xl.meta")).Should().BeEquivalentTo(v2);
        XlMetaFile.ReadFile(Path.Combine(_dir, "xl.meta.bkp")).Should().BeEquivalentTo(v1);
    }

    [Fact]
    public void Read_CorruptedMain_FallsBackToBkp()
    {
        // Arrange: v1, затем v2; основной затирается мусором
        Directory.CreateDirectory(_dir);
        var v1 = SampleRecord(versionId: Guid.Parse("01020304-0506-4708-890a-0b0c0d0e0f10"));
        XlMetaFile.Write(_dir, v1);
        XlMetaFile.Write(_dir, SampleRecord());
        File.WriteAllText(Path.Combine(_dir, "xl.meta"), "мусор");

        // Act
        var read = XlMetaFile.Read(_dir, out var fromBackup);

        // Assert: bkp = самая старая целая копия (v1)
        read.Should().BeEquivalentTo(v1);
        fromBackup.Should().BeTrue();
    }

    [Fact]
    public void Read_CorruptedMain_NoBkp_ThrowsIntegrity()
    {
        // Arrange: первая запись (bkp не создаётся — старого xl.meta не было),
        // основной затирается мусором
        Directory.CreateDirectory(_dir);
        XlMetaFile.Write(_dir, SampleRecord());
        File.WriteAllText(Path.Combine(_dir, "xl.meta"), "мусор");

        // Act
        var act = () => XlMetaFile.Read(_dir, out _);

        // Assert: битый основной + отсутствие bkp — невосстановимая порча
        // (500 InternalError), НЕ «объекта нет» (404) — спека §4.2
        act.Should().Throw<XlIntegrityException>();
    }

    [Fact]
    public void Read_BothCorrupted_ThrowsIntegrity()
    {
        // Arrange
        Directory.CreateDirectory(_dir);
        XlMetaFile.Write(_dir, SampleRecord());
        XlMetaFile.Write(_dir, SampleRecord());
        File.WriteAllText(Path.Combine(_dir, "xl.meta"), "мусор");
        File.WriteAllText(Path.Combine(_dir, "xl.meta.bkp"), "мусор");

        // Act
        var act = () => XlMetaFile.Read(_dir, out _);

        // Assert: оба битые — невосстановимая порча
        act.Should().Throw<XlIntegrityException>();
    }

    [Fact]
    public void ReadFile_BadMagic_ThrowsIntegrity()
    {
        // Arrange
        Directory.CreateDirectory(_dir);
        XlMetaFile.Write(_dir, SampleRecord());
        var bytes = File.ReadAllBytes(Path.Combine(_dir, "xl.meta"));
        bytes[0] = (byte)'X';
        File.WriteAllBytes(Path.Combine(_dir, "xl.meta"), bytes);

        // Act / Assert
        var act = () => XlMetaFile.ReadFile(Path.Combine(_dir, "xl.meta"));
        act.Should().Throw<XlIntegrityException>();
    }

    [Fact]
    public void ReadFile_UnknownFormatVersion_ThrowsIntegrity()
    {
        // Arrange
        Directory.CreateDirectory(_dir);
        XlMetaFile.Write(_dir, SampleRecord());
        var bytes = File.ReadAllBytes(Path.Combine(_dir, "xl.meta"));
        bytes[4] = 2; // uint8 formatVersion
        File.WriteAllBytes(Path.Combine(_dir, "xl.meta"), bytes);

        // Act / Assert
        var act = () => XlMetaFile.ReadFile(Path.Combine(_dir, "xl.meta"));
        act.Should().Throw<XlIntegrityException>();
    }

    [Fact]
    public void ReadFile_TruncatedPayload_ThrowsIntegrity()
    {
        // Arrange: обрыв данных в середине
        Directory.CreateDirectory(_dir);
        XlMetaFile.Write(_dir, SampleRecord());
        var bytes = File.ReadAllBytes(Path.Combine(_dir, "xl.meta"))[..30];
        File.WriteAllBytes(Path.Combine(_dir, "xl.meta"), bytes);

        // Act / Assert
        var act = () => XlMetaFile.ReadFile(Path.Combine(_dir, "xl.meta"));
        act.Should().Throw<XlIntegrityException>();
    }

    [Fact]
    public void ReadFile_TrailingBytes_ThrowsIntegrity()
    {
        // Arrange: остаточные байты после записи
        Directory.CreateDirectory(_dir);
        XlMetaFile.Write(_dir, SampleRecord());
        var bytes = File.ReadAllBytes(Path.Combine(_dir, "xl.meta"));
        File.WriteAllBytes(Path.Combine(_dir, "xl.meta"), [.. bytes, .. new byte[] { 1, 2, 3 }]);

        // Act / Assert
        var act = () => XlMetaFile.ReadFile(Path.Combine(_dir, "xl.meta"));
        act.Should().Throw<XlIntegrityException>();
    }

    [Fact]
    public void Read_MissingFiles_ThrowsFileNotFound()
    {
        // Arrange: пустой каталог объекта
        Directory.CreateDirectory(_dir);

        // Act / Assert: отсутствие обоих файлов — сигнал «нет объекта» (NoSuchKey у вызывающего)
        var act = () => XlMetaFile.Read(_dir, out _);
        act.Should().Throw<FileNotFoundException>();
    }

    // ── запись v2 (EC-поля + bitrot, канон 06 §5 в правке Task 0) ──

    private static XlMetaEcInfo SampleEc(byte ecIndex = 2) => new(
        EcAlgo: "reedsolomon",
        EcM: 3, EcN: 4, EcBSize: 1024 * 1024,
        EcIndex: ecIndex,
        EcDist: [2, 0, 3, 1],
        Parts:
        [
            new XlMetaPartBitrot(
                SourceSize: 1_500_000,
                BlockHashes:
                [
                    // шард-мажор: [shard][block] — уникальное значение на каждую пару
                    new[] { HashByte(0, 0), HashByte(0, 1) },
                    new[] { HashByte(1, 0), HashByte(1, 1) },
                    new[] { HashByte(2, 0), HashByte(2, 1) },
                    new[] { HashByte(3, 0), HashByte(3, 1) },
                ]),
        ]);

    private static byte[] HashByte(byte shard, byte block)
    {
        var hash = new byte[32];
        hash[0] = shard;
        hash[1] = block;
        return hash;
    }

    [Fact]
    public void RoundTrip_V2_AllEcFieldsSurvive()
    {
        // Arrange
        var record = SampleRecord() with { Ec = SampleEc() };
        Directory.CreateDirectory(_dir);
        XlMetaFile.Write(_dir, record);

        // Act
        var read = XlMetaFile.ReadFile(Path.Combine(_dir, "xl.meta"));

        // Assert — эквивалентность всех полей, включая полный 2D-набор хэшей
        read.Should().BeEquivalentTo(record, options => options.WithStrictOrdering());
        read.Ec.Should().NotBeNull();
        read.Ec!.Parts[0].SourceSize.Should().Be(1_500_000);
    }

    [Fact]
    public void RoundTrip_V2_PreservesShardMajorLayout()
    {
        // Arrange: ecN=4, blockCount=2 → 8 хэшей: раскладка побайтово воспроизведена
        var record = SampleRecord() with { Ec = SampleEc() };
        Directory.CreateDirectory(_dir);
        XlMetaFile.Write(_dir, record);

        // Act
        var read = XlMetaFile.ReadFile(Path.Combine(_dir, "xl.meta"));

        // Assert — уникальное значение на каждую пару (shard, block) в исходной раскладке
        var hashes = read.Ec!.Parts[0].BlockHashes;
        hashes.Length.Should().Be(4);
        hashes[0].Length.Should().Be(2);
        hashes[3][1][0].Should().Be(3);
        hashes[3][1][1].Should().Be(1);
        hashes[0][1][0].Should().Be(0);
        hashes[0][1][1].Should().Be(1);
    }

    [Fact]
    public void Write_V1_ReadableByNewReader()
    {
        // Arrange: запись без Ec (v1)
        var record = SampleRecord();
        Directory.CreateDirectory(_dir);
        XlMetaFile.Write(_dir, record);

        // Act
        var read = XlMetaFile.ReadFile(Path.Combine(_dir, "xl.meta"));

        // Assert
        read.Ec.Should().BeNull();
        read.Should().BeEquivalentTo(record);
    }

    [Fact]
    public void Write_NullEc_WritesV1()
    {
        // Arrange/Act: байты записи содержат recordVersion 1 (offset 5..6, LE)
        Directory.CreateDirectory(_dir);
        XlMetaFile.Write(_dir, SampleRecord());
        var bytes = File.ReadAllBytes(Path.Combine(_dir, "xl.meta"));

        // Assert
        bytes[5].Should().Be(1);
        bytes[6].Should().Be(0);
    }

    [Fact]
    public void Write_Ec_WritesV2()
    {
        // Arrange/Act
        Directory.CreateDirectory(_dir);
        XlMetaFile.Write(_dir, SampleRecord() with { Ec = SampleEc() });
        var bytes = File.ReadAllBytes(Path.Combine(_dir, "xl.meta"));

        // Assert — recordVersion 2
        bytes[5].Should().Be(2);
        bytes[6].Should().Be(0);
    }

    [Fact]
    public void Read_V2Truncated_Throws()
    {
        // Arrange: обрыв в bitrot-хвосте
        Directory.CreateDirectory(_dir);
        XlMetaFile.Write(_dir, SampleRecord() with { Ec = SampleEc() });
        var bytes = File.ReadAllBytes(Path.Combine(_dir, "xl.meta"));
        File.WriteAllBytes(Path.Combine(_dir, "xl.meta"), bytes[..^10]);

        // Act / Assert
        var act = () => XlMetaFile.ReadFile(Path.Combine(_dir, "xl.meta"));
        act.Should().Throw<XlIntegrityException>();
    }

    [Fact]
    public void Read_V2ResidualBytes_Throws()
    {
        // Arrange: лишние байты после bitrot-хвоста
        Directory.CreateDirectory(_dir);
        XlMetaFile.Write(_dir, SampleRecord() with { Ec = SampleEc() });
        var bytes = File.ReadAllBytes(Path.Combine(_dir, "xl.meta"));
        File.WriteAllBytes(Path.Combine(_dir, "xl.meta"), [.. bytes, .. new byte[] { 9 }]);

        // Act / Assert
        var act = () => XlMetaFile.ReadFile(Path.Combine(_dir, "xl.meta"));
        act.Should().Throw<XlIntegrityException>();
    }

    [Fact]
    public void RoundTrip_EmptyBlocks()
    {
        // Arrange: объект size 0 — partCount 1, blockCount 0 (канон 06 §4 «пустой объект»)
        var ec = SampleEc() with
        {
            Parts = [new XlMetaPartBitrot(0, [[], [], [], []])],
        };
        var record = SampleRecord() with { Size = 0, Ec = ec };
        Directory.CreateDirectory(_dir);
        XlMetaFile.Write(_dir, record);

        // Act
        var read = XlMetaFile.ReadFile(Path.Combine(_dir, "xl.meta"));

        // Assert
        read.Ec!.Parts[0].SourceSize.Should().Be(0);
        read.Ec!.Parts[0].BlockHashes.Should().HaveCount(4);
        read.Ec!.Parts[0].BlockHashes.Should().OnlyContain(shard => shard.Length == 0);
    }
}
