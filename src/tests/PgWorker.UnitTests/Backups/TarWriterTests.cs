using System.Security.Cryptography;
using System.Text;
using PgWorker.Backups.Tar;

namespace PgWorker.UnitTests.Backups;

// Запись tar-архива (spec t01 §3.2, §6.15): roundtrip с reader'ом, GNU
// longname, PAX size >8 ГиБ, добивка, счётчик байт и сквозной sha256.
public class TarWriterTests
{
    // AAA: короткое имя и данные проходят roundtrip writer→reader.
    [Fact]
    public async Task RoundTrip_ShortName()
    {
        // Arrange
        var ms = new MemoryStream();
        var data = Encoding.ASCII.GetBytes("hello, tar");
        using var writer = new TarStreamWriter(ms);

        // Act
        await writer.WriteEntryAsync(
            new TarEntry("a.txt", TarTypeFlags.File, data.Length, 0x1A4, ""),
            new MemoryStream(data), CancellationToken.None);
        await writer.FinishAsync(CancellationToken.None);
        var reader = new TarStreamReader(new MemoryStream(ms.ToArray()));
        var entry = await reader.ReadHeaderAsync(CancellationToken.None);
        var read = await reader.ReadAllDataAsync(CancellationToken.None);
        var end = await reader.ReadHeaderAsync(CancellationToken.None);

        // Assert
        entry!.Name.Should().Be("a.txt");
        entry.TypeFlag.Should().Be(TarTypeFlags.File);
        entry.Size.Should().Be(data.Length);
        entry.Mode.Should().Be(0x1A4);
        read.Should().Equal(data);
        end.Should().BeNull();
    }

    // AAA: имя 100<n≤255 разбивается на prefix/name и восстанавливается reader'ом.
    [Fact]
    public async Task RoundTrip_PrefixSplitName()
    {
        // Arrange — одно длинное имя, разложимое на prefix (≤155) + name (≤100).
        var name = "verylongdirectoryname/" + new string('f', 90);
        var ms = new MemoryStream();
        using var writer = new TarStreamWriter(ms);

        // Act
        await writer.WriteEntryAsync(
            new TarEntry(name, TarTypeFlags.File, 0, 0x1A4, ""), [], CancellationToken.None);
        await writer.FinishAsync(CancellationToken.None);
        var reader = new TarStreamReader(new MemoryStream(ms.ToArray()));
        var entry = await reader.ReadHeaderAsync(CancellationToken.None);

        // Assert
        entry!.Name.Should().Be(name);
    }

    // AAA: имя >100 без разложимого prefix пишется GNU longname и читается целиком.
    [Fact]
    public async Task RoundTrip_GnuLongName()
    {
        // Arrange — 140 байт без '/' (prefix-разбиение невозможно).
        var name = new string('n', 140);
        var data = Encoding.ASCII.GetBytes("payload");
        var ms = new MemoryStream();
        using var writer = new TarStreamWriter(ms);

        // Act
        await writer.WriteEntryAsync(
            new TarEntry(name, TarTypeFlags.File, data.Length, 0x1A4, ""), data, CancellationToken.None);
        await writer.FinishAsync(CancellationToken.None);
        var reader = new TarStreamReader(new MemoryStream(ms.ToArray()));
        var entry = await reader.ReadHeaderAsync(CancellationToken.None);
        var read = await reader.ReadAllDataAsync(CancellationToken.None);

        // Assert
        entry!.Name.Should().Be(name);
        read.Should().Equal(data);
    }

    // AAA: размер >8 ГиБ пишется PAX-записью (основной заголовок size=0),
    // reader возвращает size из PAX — без выделения 8 ГиБ данных.
    [Fact]
    public async Task PaxSize_Over8GiB_RestoredByReader()
    {
        // Arrange
        var ms = new MemoryStream();
        var size = 9L * 1024 * 1024 * 1024;
        using var writer = new TarStreamWriter(ms);

        // Act — только заголовки: данные такого размера в тесте не пишутся.
        await writer.WriteEntryHeaderAsync(
            new TarEntry("huge.bin", TarTypeFlags.File, size, 0x1A4, ""), CancellationToken.None);
        await writer.FinishAsync(CancellationToken.None);
        var reader = new TarStreamReader(new MemoryStream(ms.ToArray()));
        var entry = await reader.ReadHeaderAsync(CancellationToken.None);

        // Assert
        entry!.Name.Should().Be("huge.bin");
        entry.Size.Should().Be(size);
    }

    // AAA: FinishAsync дописывает ровно два нулевых блока, Position = длина выхода.
    [Fact]
    public async Task Finish_WritesTwoZeroBlocks_PositionMatchesOutput()
    {
        // Arrange
        var ms = new MemoryStream();
        using var writer = new TarStreamWriter(ms);
        var data = Encoding.ASCII.GetBytes("abc");

        // Act
        await writer.WriteEntryAsync(
            new TarEntry("a.txt", TarTypeFlags.File, data.Length, 0x1A4, ""), data, CancellationToken.None);
        writer.Position.Should().Be(1024); // заголовок + добивка данных
        await writer.FinishAsync(CancellationToken.None);
        var bytes = ms.ToArray();

        // Assert
        writer.Position.Should().Be(bytes.Length);
        writer.TotalBytes.Should().Be(bytes.Length);
        bytes[^1024..].Should().OnlyContain(b => b == 0);
    }

    // AAA: сквозной sha256 writer'а равен SHA256 всего выходного потока.
    [Fact]
    public async Task Sha256_MatchesWholeStream()
    {
        // Arrange
        var ms = new MemoryStream();
        using var writer = new TarStreamWriter(ms);
        await writer.WriteEntryAsync(
            new TarEntry("a.txt", TarTypeFlags.File, 3, 0x1A4, ""),
            Encoding.ASCII.GetBytes("abc"), CancellationToken.None);
        await writer.WriteEntryAsync(
            new TarEntry("dir", TarTypeFlags.Dir, 0, 0x1ED, ""), [], CancellationToken.None);

        // Act
        await writer.FinishAsync(CancellationToken.None);
        var hash = writer.Sha256Hash();

        // Assert
        hash.Should().Equal(SHA256.HashData(ms.ToArray()));
    }
}
