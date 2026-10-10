using System.Text.Json;
using OwnS3.Storage;

namespace OwnS3.UnitTests.Storage;

// Том xl: volume.json fail-fast, старт/фоновые чистки (tmp безусловно; .trash и
// orphan-dataDir — порог 1 ч), CheckHealth. Время — TimeProvider.System, возраст
// каталогов задаётся File.SetLastWriteTimeUtc (детерминированно, без sleep).
public class XlVolumeTests : IDisposable
{
    private readonly XlVolume _volume = new(Path.Combine(Path.GetTempPath(), "owns3-test-" + Guid.NewGuid().ToString("N")), TimeProvider.System);

    private string Root => _volume.Root;
    private XlVolume Volume => _volume;

    private static XlMetaRecord MetaRecord(Guid versionId) => new(
        versionId, 5, DateTimeOffset.FromUnixTimeMilliseconds(1_728_000_000_000),
        "5d41402abc4b2a76b9719d911017c592", "text/plain",
        new Dictionary<string, string>(), new Dictionary<string, string>(),
        "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824");

    public void Dispose()
    {
        // Teardown: temp-том удаляется при любом исходе прогона
        if (Directory.Exists(Root))
            Directory.Delete(Root, recursive: true);
    }

    [Fact]
    public void Initialize_EmptyRoot_CreatesSysLayout()
    {
        // Arrange / Act
        Volume.Initialize();

        // Assert: служебная структура + валидный volume.json
        Directory.Exists(Path.Combine(Root, ".owns3.sys", "tmp")).Should().BeTrue();
        Directory.Exists(Path.Combine(Root, ".owns3.sys", "multipart")).Should().BeTrue();
        Directory.Exists(Path.Combine(Root, ".owns3.sys", ".trash")).Should().BeTrue();
        Directory.Exists(Path.Combine(Root, ".owns3.sys", "buckets")).Should().BeTrue();
        Directory.Exists(Path.Combine(Root, ".owns3.sys", "config")).Should().BeTrue();
        var json = JsonDocument.Parse(File.ReadAllText(Path.Combine(Root, ".owns3.sys", "volume.json")));
        json.RootElement.GetProperty("magic").GetString().Should().Be("OWNS3-VOL");
        json.RootElement.GetProperty("formatVersion").GetInt32().Should().Be(1);
        json.RootElement.GetProperty("mode").GetString().Should().Be("XL Single");
        Guid.TryParseExact(json.RootElement.GetProperty("volumeId").GetString(), "N", out _).Should().BeTrue();
    }

    [Fact]
    public void Initialize_SecondRun_ValidatesAndKeeps()
    {
        // Arrange
        Volume.Initialize();
        var firstId = JsonDocument.Parse(File.ReadAllText(Volume.ConfigVolumeJsonPath))
            .RootElement.GetProperty("volumeId").GetString();

        // Act
        Volume.Initialize();

        // Assert: volumeId сохраняется, отказов нет
        var secondId = JsonDocument.Parse(File.ReadAllText(Volume.ConfigVolumeJsonPath))
            .RootElement.GetProperty("volumeId").GetString();
        secondId.Should().Be(firstId);
    }

    [Fact]
    public void Initialize_ForeignMagic_Throws()
    {
        // Arrange: чужой magic в volume.json
        Directory.CreateDirectory(Volume.SysDir);
        File.WriteAllText(Volume.ConfigVolumeJsonPath,
            """{"magic":"OTHER","formatVersion":1,"volumeId":"0102030405064708890a0b0c0d0e0f10","mode":"XL Single"}""");

        // Act / Assert: fail-fast старта
        var act = () => Volume.Initialize();
        act.Should().Throw<Exception>().WithMessage("*magic*");
    }

    [Fact]
    public void Initialize_IncompatibleVersion_Throws()
    {
        // Arrange: версия формата из будущего
        Directory.CreateDirectory(Volume.SysDir);
        File.WriteAllText(Volume.ConfigVolumeJsonPath,
            """{"magic":"OWNS3-VOL","formatVersion":2,"volumeId":"0102030405064708890a0b0c0d0e0f10","mode":"XL Single"}""");

        // Act / Assert
        var act = () => Volume.Initialize();
        act.Should().Throw<Exception>().WithMessage("*верс*");
    }

    [Fact]
    public void Initialize_WrongMode_Throws()
    {
        // Arrange: режим не «XL Single»
        Directory.CreateDirectory(Volume.SysDir);
        File.WriteAllText(Volume.ConfigVolumeJsonPath,
            """{"magic":"OWNS3-VOL","formatVersion":1,"volumeId":"0102030405064708890a0b0c0d0e0f10","mode":"XL Multi"}""");

        // Act / Assert
        var act = () => Volume.Initialize();
        act.Should().Throw<Exception>().WithMessage("*режим*");
    }

    [Fact]
    public void Initialize_MissingVolumeJsonOnDirtyVolume_Throws()
    {
        // Arrange: volume.json нет, но том не пуст
        Directory.CreateDirectory(Path.Combine(Root, "b"));

        // Act / Assert
        var act = () => Volume.Initialize();
        act.Should().Throw<Exception>();
    }

    [Fact]
    public void Initialize_ClearsTmpUnconditionally()
    {
        // Arrange: свежий мусор staging (возраст меньше порога — всё равно удаляется)
        Volume.Initialize();
        var staged = Path.Combine(Volume.TmpDir, Guid.NewGuid().ToString("N"), "data");
        Directory.CreateDirectory(staged);
        File.WriteAllText(Path.Combine(staged, "part.1"), "x");

        // Act
        Volume.Initialize();

        // Assert
        Directory.Exists(staged).Should().BeFalse();
        Directory.EnumerateFileSystemEntries(Volume.TmpDir).Should().BeEmpty();
    }

    [Fact]
    public async Task RunCleanup_DoesNotTouchInFlightStaging()
    {
        // Arrange: staging идущего PUT; безусловная очистка tmp — только на
        // старте (спека §4.7/канон 04 §6), фон tmp НЕ трогает
        Volume.Initialize();
        var staged = Path.Combine(Volume.TmpDir, Guid.NewGuid().ToString("N"), "data");
        Directory.CreateDirectory(staged);
        File.WriteAllText(Path.Combine(staged, "part.1"), "partial");
        File.SetLastWriteTimeUtc(staged, DateTime.UtcNow.AddHours(-2)); // даже «старый»

        // Act
        await Volume.RunCleanupAsync(CancellationToken.None);

        // Assert: in-flight staging не вытирается фоновой чисткой
        Directory.Exists(staged).Should().BeTrue();
        File.Exists(Path.Combine(staged, "part.1")).Should().BeTrue();
    }

    [Fact]
    public void Initialize_RemovesAgedTrash_KeepsFresh()
    {
        // Arrange
        Volume.Initialize();
        var aged = Path.Combine(Volume.TrashDir, Guid.NewGuid().ToString("N"));
        var fresh = Path.Combine(Volume.TrashDir, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(aged);
        Directory.CreateDirectory(fresh);
        File.SetLastWriteTimeUtc(aged, DateTime.UtcNow.AddHours(-2));
        File.SetLastWriteTimeUtc(fresh, DateTime.UtcNow.AddMinutes(-1));

        // Act
        Volume.Initialize();

        // Assert: порог 1 ч от mtime
        Directory.Exists(aged).Should().BeFalse();
        Directory.Exists(fresh).Should().BeTrue();
    }

    [Fact]
    public void Initialize_KeepsCurrentDataDir_MovesAgedOrphan()
    {
        // Arrange: живой объект b/k (xl.meta → dataDir V) + лишний orphan-каталог
        Volume.Initialize();
        var versionId = Guid.NewGuid();
        var objectDir = Path.Combine(Root, "b", "k");
        Directory.CreateDirectory(objectDir);
        XlMetaFile.Write(objectDir, MetaRecord(versionId));
        var currentDataDir = Path.Combine(objectDir, versionId.ToString("N"));
        Directory.CreateDirectory(currentDataDir);
        File.WriteAllText(Path.Combine(currentDataDir, "part.1"), "hello");
        var orphan = Path.Combine(objectDir, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(orphan);
        File.WriteAllText(Path.Combine(orphan, "part.1"), "old");
        File.SetLastWriteTimeUtc(orphan, DateTime.UtcNow.AddHours(-2));

        // Act
        Volume.Initialize();

        // Assert: текущий dataDir на месте, orphan переехал в .trash
        Directory.Exists(currentDataDir).Should().BeTrue();
        Directory.Exists(orphan).Should().BeFalse();
        Directory.EnumerateDirectories(Volume.TrashDir).Should().HaveCount(1);
    }

    [Fact]
    public async Task Cleanup_RemovesGuidDataDir_WithoutXlMeta()
    {
        // Arrange: краш Delete между двумя rename — dataDir остался, xl.meta уже в .trash
        Volume.Initialize();
        var objectDir = Path.Combine(Root, "b", "k");
        var orphan = Path.Combine(objectDir, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(orphan);
        File.WriteAllText(Path.Combine(orphan, "part.1"), "x");
        File.SetLastWriteTimeUtc(orphan, DateTime.UtcNow.AddHours(-2));

        // Act
        await Volume.RunCleanupAsync(CancellationToken.None);

        // Assert: отсутствие xl.meta не блокирует чистку Guid-подкаталогов
        Directory.Exists(orphan).Should().BeFalse();
        Directory.EnumerateDirectories(Volume.TrashDir).Should().HaveCount(1);
    }

    [Fact]
    public async Task Cleanup_KeepsNonGuidSubdirs()
    {
        // Arrange: не-Guid подкаталог — сегмент вложенного ключа
        Volume.Initialize();
        var nested = Path.Combine(Root, "b", "k", "nested-key");
        Directory.CreateDirectory(nested);
        XlMetaFile.Write(nested, MetaRecord(Guid.NewGuid()));
        File.SetLastWriteTimeUtc(nested, DateTime.UtcNow.AddHours(-2));

        // Act
        await Volume.RunCleanupAsync(CancellationToken.None);

        // Assert
        Directory.Exists(nested).Should().BeTrue();
    }

    [Theory]
    [InlineData("b")]        // ключ верхнего уровня из 32 hex
    [InlineData("nested")]   // вложенный ключ <префикс>/<32 hex>
    public async Task Cleanup_KeepsObjectWithHexGuidKey_AfterAging(string parent)
    {
        // Arrange: живой объект, чей ключ-сегмент — 32 hex (парсится
        // Guid.TryParseExact("N")); «старый» каталог с xl.meta внутри —
        // объект, а не orphan-dataDir-кандидат
        Volume.Initialize();
        const string hexKey = "0102030405064708890a0b0c0d0e0f10";
        var versionId = Guid.NewGuid();
        var objectDir = Path.Combine([Root, parent, hexKey]);
        Directory.CreateDirectory(objectDir);
        XlMetaFile.Write(objectDir, MetaRecord(versionId));
        var dataDir = Path.Combine(objectDir, versionId.ToString("N"));
        Directory.CreateDirectory(dataDir);
        File.WriteAllText(Path.Combine(dataDir, "part.1"), "x");
        File.SetLastWriteTimeUtc(objectDir, DateTime.UtcNow.AddHours(-2));
        File.SetLastWriteTimeUtc(dataDir, DateTime.UtcNow.AddHours(-2));

        // Act
        await Volume.RunCleanupAsync(CancellationToken.None);

        // Assert: объект жив — xl.meta и текущий dataDir на месте
        XlMetaFile.Read(objectDir, out _).DataDirName.Should().Be(versionId.ToString("N"));
        Directory.Exists(dataDir).Should().BeTrue();
    }

    [Fact]
    public async Task RunCleanup_SameThresholds_AsStartup()
    {
        // Arrange: тот же порог 1 ч в фоновом проходе
        Volume.Initialize();
        var aged = Path.Combine(Volume.TrashDir, Guid.NewGuid().ToString("N"));
        var fresh = Path.Combine(Volume.TrashDir, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(aged);
        Directory.CreateDirectory(fresh);
        File.SetLastWriteTimeUtc(aged, DateTime.UtcNow.AddHours(-2));
        File.SetLastWriteTimeUtc(fresh, DateTime.UtcNow.AddMinutes(-1));

        // Act
        await Volume.RunCleanupAsync(CancellationToken.None);

        // Assert
        Directory.Exists(aged).Should().BeFalse();
        Directory.Exists(fresh).Should().BeTrue();
    }

    [Fact]
    public void CheckHealth_TrueOnValidVolume()
    {
        // Arrange
        Volume.Initialize();

        // Act / Assert: volume.json валиден + touch-проба записи
        Volume.CheckHealth().Should().BeTrue();
    }

    [Fact]
    public void CheckHealth_FalseWhenTmpMissing()
    {
        // Arrange: каталог tmp недоступен — проба записи невозможна
        Volume.Initialize();
        Directory.Delete(Volume.TmpDir, recursive: true);

        // Act / Assert
        Volume.CheckHealth().Should().BeFalse();
    }
}
