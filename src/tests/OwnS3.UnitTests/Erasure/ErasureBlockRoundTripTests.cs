using System.Security.Cryptography;
using OwnS3.Storage;
using OwnS3.Storage.Erasure;

namespace OwnS3.UnitTests.Erasure;

// Тесты стримингового шардирования блоков 1 МиБ (t40, план Task 4).
public class ErasureBlockRoundTripTests
{
    private const int BlockSize = ReedSolomonCodec.ErasureBlockMaxSize;

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(1024 * 1024)]        // ровно блок
    [InlineData(1024 * 1024 + 1)]    // блок + хвост
    [InlineData(5 * 1024 * 1024 + 12345)]
    public async Task RoundTrip_ProducesOriginalBytes(int len)
    {
        // Arrange
        var data = RandomBytes(len);
        var codec = new ReedSolomonCodec(3, 1);
        var shardStreams = Enumerable.Range(0, 4).Select(_ => new MemoryStream()).ToArray();

        // Act
        ErasurePartResult result;
        using (var writer = new ErasureBlockWriter(codec, shardStreams.Cast<Stream>().ToArray()))
        {
            await writer.WriteAsync(new MemoryStream(data), data.Length, CancellationToken.None);
            result = await writer.CompleteAsync();
        }

        var readers = shardStreams.Select(s => (Stream?)new MemoryStream(s.ToArray())).ToArray();
        var assembled = new MemoryStream();
        using (var reader = new ErasureBlockReader(codec, readers, result, 0, data.LongLength))
        {
            await reader.CopyToAsync(assembled);
        }

        // Assert
        assembled.ToArray().Should().Equal(data);
        result.Sha256Hex.Should().Be(Convert.ToHexString(SHA256.HashData(data)).ToLowerInvariant());
        result.ETagHex.Should().Be(Convert.ToHexString(MD5.HashData(data)).ToLowerInvariant());
        result.SourceLength.Should().Be(len);
        result.BlockCount.Should().Be((len + BlockSize - 1) / BlockSize);
    }

    [Fact]
    public async Task RoundTrip_WithOfflineShard_Reconstructs()
    {
        // Arrange
        var data = RandomBytes(2 * BlockSize + 555);
        var codec = new ReedSolomonCodec(3, 1);
        var shardStreams = Enumerable.Range(0, 4).Select(_ => new MemoryStream()).ToArray();
        ErasurePartResult result;
        using (var writer = new ErasureBlockWriter(codec, shardStreams.Cast<Stream>().ToArray()))
        {
            await writer.WriteAsync(new MemoryStream(data), data.Length, CancellationToken.None);
            result = await writer.CompleteAsync();
        }

        // Act — шард-поток 2 недоступен (null)
        var readers = shardStreams.Select(s => (Stream?)new MemoryStream(s.ToArray())).ToArray();
        readers[2] = null;
        var assembled = new MemoryStream();
        using (var reader = new ErasureBlockReader(codec, readers, result, 0, data.LongLength))
        {
            await reader.CopyToAsync(assembled);
        }

        // Assert
        assembled.ToArray().Should().Equal(data);
    }

    [Fact]
    public async Task Read_RangeWindow_ReturnsSlice()
    {
        // Arrange
        var data = RandomBytes(3 * BlockSize + 777);
        var (codec, shardStreams, result) = await WriteSharded(data, 3, 1);

        // Act — окно внутри второго блока с заходом в третий
        var offset = (long)BlockSize + 12345;
        var length = (long)BlockSize + 100;
        var readers = shardStreams.Select(s => (Stream?)new MemoryStream(s.ToArray())).ToArray();
        var assembled = new MemoryStream();
        using (var reader = new ErasureBlockReader(codec, readers, result, offset, length))
        {
            await reader.CopyToAsync(assembled);
        }

        // Assert
        assembled.ToArray().Should().Equal(data[(int)offset..(int)(offset + length)]);
    }

    [Fact]
    public async Task BitrotCorruption_DetectedAndReconstructed()
    {
        // Arrange
        var data = RandomBytes(BlockSize + 4096);
        var (codec, shardStreams, result) = await WriteSharded(data, 3, 1);
        var shardCopies = shardStreams.Select(s => s.ToArray()).ToArray();

        // Act — порча байта в шард-потоке 1 (data-позиция)
        shardCopies[1][100] ^= 0xFF;
        var readers = shardCopies.Select(c => (Stream?)new MemoryStream(c)).ToArray();
        var assembled = new MemoryStream();
        using var reader = new ErasureBlockReader(codec, readers, result, 0, data.LongLength);
        {
            await reader.CopyToAsync(assembled);
        }

        // Assert — чтение сходится, позиция битого шарда зафиксирована
        assembled.ToArray().Should().Equal(data);
        reader.CorruptedShards.Should().Contain(1);
    }

    [Fact]
    public async Task TwoCorruptedShards_BeyondParity_Throws()
    {
        // Arrange — m=1: двух порченных шардов недостаточно живых для реконструкции
        var data = RandomBytes(BlockSize);
        var (codec, shardStreams, result) = await WriteSharded(data, 3, 1);
        var shardCopies = shardStreams.Select(s => s.ToArray()).ToArray();
        shardCopies[0][50] ^= 0xFF;
        shardCopies[2][150] ^= 0xFF;
        var readers = shardCopies.Select(c => (Stream?)new MemoryStream(c)).ToArray();

        // Act
        var assembled = new MemoryStream();
        var act = async () =>
        {
            using var reader = new ErasureBlockReader(codec, readers, result, 0, data.LongLength);
            await reader.CopyToAsync(assembled);
        };

        // Assert
        await act.Should().ThrowAsync<XlIntegrityException>();
    }

    [Fact]
    public async Task Writer_LengthMismatch_Throws()
    {
        // Arrange
        var codec = new ReedSolomonCodec(3, 1);
        var shardStreams = Enumerable.Range(0, 4).Select(_ => new MemoryStream()).Cast<Stream>().ToArray();
        using var writer = new ErasureBlockWriter(codec, shardStreams);
        var data = RandomBytes(1000);

        // Act — заявленная длина не совпадает с телом
        var act = () => writer.WriteAsync(new MemoryStream(data), data.Length + 1, CancellationToken.None);

        // Assert
        await act.Should().ThrowAsync<XlIntegrityException>();
    }

    // ── хелперы ──

    private static byte[] RandomBytes(int len, int seed = 7)
    {
        var rng = new Random(seed);
        var buf = new byte[len];
        rng.NextBytes(buf);
        return buf;
    }

    private static async Task<(ReedSolomonCodec Codec, MemoryStream[] ShardStreams, ErasurePartResult Result)> WriteSharded(
        byte[] data, int k, int m)
    {
        var codec = new ReedSolomonCodec(k, m);
        var shardStreams = Enumerable.Range(0, k + m).Select(_ => new MemoryStream()).ToArray();
        using var writer = new ErasureBlockWriter(codec, shardStreams.Cast<Stream>().ToArray());
        await writer.WriteAsync(new MemoryStream(data), data.Length, CancellationToken.None);
        var result = await writer.CompleteAsync();
        return (codec, shardStreams, result);
    }
}

// CopyToAsync-удобство для тестов: расширение читает ReadAsync до конца потока.
file static class ErasureBlockReaderExtensions
{
    public static async Task CopyToAsync(this ErasureBlockReader reader, Stream target)
    {
        var buffer = new byte[64 * 1024];
        int read;
        while ((read = await reader.ReadAsync(buffer, 0, buffer.Length, CancellationToken.None)) > 0)
        {
            await target.WriteAsync(buffer.AsMemory(0, read));
        }
    }
}
