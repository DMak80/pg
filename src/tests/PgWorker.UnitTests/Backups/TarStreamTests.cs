using System.Text;
using PgWorker.Backups.Tar;

namespace PgWorker.UnitTests.Backups;

// Потоковое чтение tar (spec t01 §3.2, §6.15): ustar+prefix, GNU longname,
// PAX path/size, типы 0/5/2, валидация checksum/typeflag, обрыв данных.
public class TarStreamTests
{
    // AAA: запись-файл с коротким именем читается заголовком и данными.
    [Fact]
    public async Task ReadsShortNameFile()
    {
        // Arrange — один файл "hello.txt" с данными "abc".
        var tar = Concat(
            Entry("hello.txt", TarTypeFlags.File, Encoding.ASCII.GetBytes("abc")));
        var reader = new TarStreamReader(new MemoryStream(tar));

        // Act
        var entry = await reader.ReadHeaderAsync(CancellationToken.None);
        var data = await reader.ReadAllDataAsync(CancellationToken.None);

        // Assert
        entry.Should().NotBeNull();
        entry!.Name.Should().Be("hello.txt");
        entry.TypeFlag.Should().Be(TarTypeFlags.File);
        entry.Size.Should().Be(3);
        entry.Mode.Should().Be(0x1A4); // 0644
        Encoding.ASCII.GetString(data).Should().Be("abc");
    }

    // AAA: ustar-префикс склеивается с именем через '/'.
    [Fact]
    public async Task ReadsPrefixQualifiedName()
    {
        // Arrange — имя в prefix/name-полях.
        var tar = Concat(Entry("file.txt", TarTypeFlags.File, [], prefix: "dir/sub"));
        var reader = new TarStreamReader(new MemoryStream(tar));

        // Act
        var entry = await reader.ReadHeaderAsync(CancellationToken.None);

        // Assert
        entry!.Name.Should().Be("dir/sub/file.txt");
    }

    // AAA: GNU longname ('L') переопределяет имя следующей записи.
    [Fact]
    public async Task ReadsGnuLongName()
    {
        // Arrange — имя >100 байт в служебной L-записи, основная — урезанное.
        var longName = "very/long/" + new string('d', 120) + "/file.txt";
        var tar = Concat(
            Entry("longname", TarTypeFlags.GnuLongName, Encoding.ASCII.GetBytes(longName + "\0")),
            Entry("truncated", TarTypeFlags.File, Encoding.ASCII.GetBytes("x")));
        var reader = new TarStreamReader(new MemoryStream(tar));

        // Act
        var entry = await reader.ReadHeaderAsync(CancellationToken.None);

        // Assert
        entry!.Name.Should().Be(longName);
        entry.Size.Should().Be(1);
    }

    // AAA: PAX 'x' с path/size переопределяет имя и размер следующей записи.
    [Fact]
    public async Task ReadsPaxPathAndSize()
    {
        // Arrange — PAX-запись path/size, основная запись с size-полем 0.
        var tar = Concat(
            Entry("pax", TarTypeFlags.PaxHeader, Encoding.ASCII.GetBytes(PaxRecord("path", "pax/dir/file.txt")
                + PaxRecord("size", "5"))),
            Entry("ignored", TarTypeFlags.File, [], sizeOverride: 0),
            new byte[DataAndPadding(5)]);
        var reader = new TarStreamReader(new MemoryStream(tar));

        // Act
        var entry = await reader.ReadHeaderAsync(CancellationToken.None);

        // Assert
        entry!.Name.Should().Be("pax/dir/file.txt");
        entry.Size.Should().Be(5);
    }

    // AAA: каталог (typeflag '5') читается записью без данных.
    [Fact]
    public async Task ReadsDirectoryEntry()
    {
        // Arrange
        var tar = Concat(Entry("pgdata/sub", TarTypeFlags.Dir, []));
        var reader = new TarStreamReader(new MemoryStream(tar));

        // Act
        var entry = await reader.ReadHeaderAsync(CancellationToken.None);
        var data = await reader.ReadAllDataAsync(CancellationToken.None);

        // Assert
        entry!.TypeFlag.Should().Be(TarTypeFlags.Dir);
        entry.Size.Should().Be(0);
        data.Should().BeEmpty();
    }

    // AAA: symlink ('2') несёт linkname без данных.
    [Fact]
    public async Task ReadsSymlinkLinkName()
    {
        // Arrange
        var tar = Concat(Entry("pg_wal", TarTypeFlags.Symlink, [], linkname: "/pgdata/pg_wal"));
        var reader = new TarStreamReader(new MemoryStream(tar));

        // Act
        var entry = await reader.ReadHeaderAsync(CancellationToken.None);

        // Assert
        entry!.TypeFlag.Should().Be(TarTypeFlags.Symlink);
        entry.LinkName.Should().Be("/pgdata/pg_wal");
    }

    // AAA: нулевой заголовок — конец архива (null).
    [Fact]
    public async Task NullHeaderMeansEndOfArchive()
    {
        // Arrange
        var tar = Concat(
            Entry("a.txt", TarTypeFlags.File, Encoding.ASCII.GetBytes("a")),
            new byte[1024]);
        var reader = new TarStreamReader(new MemoryStream(tar));

        // Act
        var first = await reader.ReadHeaderAsync(CancellationToken.None);
        var second = await reader.ReadHeaderAsync(CancellationToken.None);
        var third = await reader.ReadHeaderAsync(CancellationToken.None);

        // Assert
        first!.Name.Should().Be("a.txt");
        second.Should().BeNull();
        third.Should().BeNull();
    }

    // AAA: битый checksum заголовка — TarFormatException (не «тихо пусто»).
    [Fact]
    public async Task BrokenChecksumThrows()
    {
        // Arrange — заголовок с испорченным name-байтом после подсчёта checksum.
        var header = Header("broken.txt", TarTypeFlags.File, 0);
        header[0] = (byte)'X';
        var reader = new TarStreamReader(new MemoryStream(Concat(header, new byte[512])));

        // Act
        var act = () => reader.ReadHeaderAsync(CancellationToken.None);

        // Assert
        await act.Should().ThrowAsync<TarFormatException>();
    }

    // AAA: чужой typeflag ('1' hardlink) — TarFormatException.
    [Fact]
    public async Task UnsupportedTypeFlagThrows()
    {
        // Arrange
        var tar = Concat(Entry("link", '1', [], linkname: "target"));
        var reader = new TarStreamReader(new MemoryStream(tar));

        // Act
        var act = () => reader.ReadHeaderAsync(CancellationToken.None);

        // Assert
        await act.Should().ThrowAsync<TarFormatException>();
    }

    // AAA: обрыв данных раньше Size — TarFormatException при дочитывании.
    [Fact]
    public async Task TruncatedDataThrows()
    {
        // Arrange — заголовок обещает 1000 байт, данных только 10 и EOF.
        var tar = Concat(
            Header("truncated.bin", TarTypeFlags.File, 1000),
            new byte[10]);
        var reader = new TarStreamReader(new MemoryStream(tar));
        await reader.ReadHeaderAsync(CancellationToken.None);

        // Act
        var act = () => reader.ReadAllDataAsync(CancellationToken.None);

        // Assert
        await act.Should().ThrowAsync<TarFormatException>();
    }

    // AAA: добивка до кратности 512 пропускается — следующая запись читается.
    [Fact]
    public async Task PaddingBetweenEntriesIsSkipped()
    {
        // Arrange — запись размером 3 (добивка 509) и следующая за ней.
        var tar = Concat(
            Entry("first.txt", TarTypeFlags.File, Encoding.ASCII.GetBytes("abc")),
            Entry("second.txt", TarTypeFlags.File, Encoding.ASCII.GetBytes("de")));
        var reader = new TarStreamReader(new MemoryStream(tar));

        // Act — поток первой записи открывается и НЕ дочитывается целиком.
        var first = await reader.ReadHeaderAsync(CancellationToken.None);
        var stream = reader.OpenData();
        var buffer = new byte[1];
        await stream.ReadExactlyAsync(buffer, CancellationToken.None);
        stream.Dispose();
        var second = await reader.ReadHeaderAsync(CancellationToken.None);
        var data = await reader.ReadAllDataAsync(CancellationToken.None);

        // Assert
        first!.Name.Should().Be("first.txt");
        second!.Name.Should().Be("second.txt");
        Encoding.ASCII.GetString(data).Should().Be("de");
    }

    // ── Билдер тестовых tar-байтов (заголовок+данные+добивка) ──

    private static byte[] Concat(params byte[][] parts)
    {
        using var ms = new MemoryStream();
        foreach (var part in parts)
            ms.Write(part);
        return ms.ToArray();
    }

    private static int DataAndPadding(long size) => (int)(size + (512 - size % 512) % 512);

    private static byte[] Entry(
        string name, char type, byte[] data,
        long? sizeOverride = null, string magic = "ustar\0", string prefix = "", string linkname = "")
    {
        var size = sizeOverride ?? data.Length;
        var padding = (int)((512 - size % 512) % 512);
        return Concat(Header(name, type, size, magic, prefix, linkname), data, new byte[padding]);
    }

    private static byte[] Header(
        string name, char type, long size, string magic = "ustar\0", string prefix = "", string linkname = "")
    {
        var header = new byte[512];
        WriteAscii(header, 0, 100, name);
        WriteOctal(header, 100, 8, 0x1A4); // mode 0644
        WriteOctal(header, 108, 8, 0);     // uid
        WriteOctal(header, 116, 8, 0);     // gid
        WriteOctal(header, 124, 12, size);
        WriteOctal(header, 136, 12, 0);    // mtime
        for (var i = 148; i < 156; i++)
            header[i] = 0x20;              // checksum считается по пробелам
        header[156] = (byte)type;
        WriteAscii(header, 157, 100, linkname);
        WriteAscii(header, 257, 6, magic);
        WriteAscii(header, 263, 2, "00");
        WriteAscii(header, 345, 155, prefix);
        var sum = 0L;
        foreach (var b in header)
            sum += b;
        WriteAscii(header, 148, 6, Convert.ToString(sum, 8).PadLeft(6, '0'));
        header[154] = 0;
        header[155] = 0x20;
        return header;
    }

    private static void WriteAscii(byte[] target, int offset, int length, string value)
    {
        var bytes = Encoding.ASCII.GetBytes(value);
        Array.Copy(bytes, 0, target, offset, Math.Min(bytes.Length, length));
    }

    private static void WriteOctal(byte[] target, int offset, int length, long value)
    {
        var digits = Convert.ToString(value, 8).PadLeft(length - 1, '0');
        WriteAscii(target, offset, length, digits);
        target[offset + length - 1] = 0;
    }

    // PAX-запись "<len> <key>=<value>\n" с корректной длиной-префиксом.
    private static string PaxRecord(string key, string value)
    {
        var body = $" {key}={value}\n";
        var length = body.Length + 1;
        while (body.Length + length.ToString().Length != length)
            length = body.Length + length.ToString().Length;
        return length + body;
    }
}
