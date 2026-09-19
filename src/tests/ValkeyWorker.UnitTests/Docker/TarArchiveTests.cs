using System.Text;
using ValkeyWorker.Docker.Engine;

namespace ValkeyWorker.UnitTests.Docker;

// ustar-писатель/читатель для Docker volume-archive API (spec §4.2):
// round-trip имя→данные, права ключа 0600 сохраняются в заголовке.
public sealed class TarArchiveTests
{
    [Fact]
    public void Build_Read_RoundTrip()
    {
        // Arrange — права ustar: 0o600 ключ, 0o644 публичные
        var entries = new[]
        {
            new TarArchive.Entry("node.key", 0b1_1000_0000, Encoding.UTF8.GetBytes("key-data")),
            new TarArchive.Entry("ca.pem", 0b1_1010_0100, Encoding.UTF8.GetBytes("ca-data")),
        };
        // Act
        var tar = TarArchive.Build(entries);
        var read = TarArchive.Read(tar);
        // Assert
        Assert.Equal("key-data", Encoding.UTF8.GetString(read["node.key"]));
        Assert.Equal("ca-data", Encoding.UTF8.GetString(read["ca.pem"]));
    }

    [Fact]
    public void Build_KeyHeaderMode0600()
    {
        // Arrange — права приватного ключа 0600 (arch/21 §2)
        var entries = new[]
        {
            new TarArchive.Entry("node.key", 0b1_1000_0000, [1, 2, 3]), // 0o600
        };
        // Act
        var tar = TarArchive.Build(entries);
        // Assert — mode в заголовке ustar: 0000600\0 на смещении 100
        var modeField = Encoding.ASCII.GetString(tar, 100, 8).TrimEnd('\0', ' ');
        Assert.Equal("0000600", modeField);
    }

    [Fact]
    public void Read_TrailingNullBlocks_Terminate()
    {
        // Arrange — один файл, хвост из нулевых блоков
        var tar = TarArchive.Build([new TarArchive.Entry("a", 0b1_1010_0100, "x"u8.ToArray())]); // 0o644
        // Act
        var read = TarArchive.Read(tar);
        // Assert
        Assert.Single(read);
        Assert.Equal("x", Encoding.UTF8.GetString(read["a"]));
    }
}
