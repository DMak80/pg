using System.Text;
using FluentAssertions;
using Metrics.SdGenerator;

namespace Metrics.SdGenerator.UnitTests;

// Атомарная запись file_sd при diff: tmp+rename, неизменённый контент — mtime
// не дёргается (spec §2 «запись файла минимальна»).
public class SdFileWriterTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "sd-writer-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        // Очистка temp-каталога при любом исходе теста.
        if (Directory.Exists(_dir))
            Directory.Delete(_dir, recursive: true);
    }

    private SdFileWriter NewWriter()
    {
        // Arrange: temp-каталог создаётся тестом (writer не создаёт иерархию — volume прометеуса существует).
        Directory.CreateDirectory(_dir);
        return new SdFileWriter(Path.Combine(_dir, "sd.json"));
    }

    [Fact]
    public void Write_CreatesFile_WhenAbsent()
    {
        // Arrange
        var writer = NewWriter();

        // Act
        var rewritten = writer.WriteIfChanged("""[{"targets":["h1:8008"]}]""");

        // Assert
        rewritten.Should().BeTrue();
        File.ReadAllText(Path.Combine(_dir, "sd.json"), Encoding.UTF8)
            .Should().Be("""[{"targets":["h1:8008"]}]""");
    }

    [Fact]
    public void Write_Skips_WhenUnchanged()
    {
        // Arrange
        var writer = NewWriter();
        writer.WriteIfChanged("""[{"targets":["h1:8008"]}]""");
        var path = Path.Combine(_dir, "sd.json");
        var mtimeBefore = File.GetLastWriteTimeUtc(path);

        // Act: второй вызов тем же контентом
        var rewritten = writer.WriteIfChanged("""[{"targets":["h1:8008"]}]""");

        // Assert: файл не тронут — mtime не изменился
        rewritten.Should().BeFalse();
        File.GetLastWriteTimeUtc(path).Should().Be(mtimeBefore);
    }

    [Fact]
    public void Write_Rewrites_WhenChanged()
    {
        // Arrange
        var writer = NewWriter();
        writer.WriteIfChanged("""[{"targets":["h1:8008"]}]""");

        // Act: контент A → B
        var rewritten = writer.WriteIfChanged("""[{"targets":["h2:8008"]}]""");

        // Assert: файл = B, tmp-хвостов нет
        rewritten.Should().BeTrue();
        File.ReadAllText(Path.Combine(_dir, "sd.json"), Encoding.UTF8)
            .Should().Be("""[{"targets":["h2:8008"]}]""");
        Directory.GetFiles(_dir, "*.tmp").Should().BeEmpty();
    }
}
