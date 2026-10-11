using System.Security.Cryptography;
using OwnS3.Storage;

namespace OwnS3.Storage.Erasure;

// Итог шардирования одной части (для xl.meta v2 и дигестов записи).
// ShardBlockHashes[shardIndex][blockIndex] — 32 байта HighwayHash(MagicKey);
// полный 2D-набор ecN × blockCount, раскладка шард-мажор — канон 06 §5.
public sealed record ErasurePartResult(
    string ETagHex,
    string Sha256Hex,
    long SourceLength,
    int BlockCount,
    byte[][][] ShardBlockHashes);

// Запись: тело → k+m шард-потоков (позиция потока i = позиция диска i сета,
// ДО применения distribution — маппинг distribution[j] делает вызывающий,
// открывая потоки в нужном порядке). Копит bitrot-хэши per shard per block.
public sealed class ErasureBlockWriter : IDisposable
{
    private readonly ReedSolomonCodec _codec;
    private readonly Stream[] _shardStreams;
    private readonly IncrementalHash _md5 = IncrementalHash.CreateHash(HashAlgorithmName.MD5);
    private readonly IncrementalHash _sha256 = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
    private readonly List<byte[]>[] _hashesByShard;
    private readonly byte[] _blockBuffer = new byte[ReedSolomonCodec.ErasureBlockMaxSize];
    private long _sourceLength;
    private int _blockCount;
    private bool _completed;

    public ErasureBlockWriter(ReedSolomonCodec codec, Stream[] shardStreams)
    {
        ArgumentNullException.ThrowIfNull(codec);
        ArgumentNullException.ThrowIfNull(shardStreams);

        if (shardStreams.Length != codec.TotalShards)
        {
            throw new ArgumentException(
                $"ожидалось {codec.TotalShards} шард-потоков, получено {shardStreams.Length}", nameof(shardStreams));
        }

        _codec = codec;
        _shardStreams = shardStreams;
        _hashesByShard = Enumerable.Range(0, codec.TotalShards).Select(_ => new List<byte[]>()).ToArray();
    }

    public async Task WriteAsync(Stream body, long expectedLength, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(body);
        ObjectDisposedException.ThrowIf(_completed, this);

        var k = _codec.DataShards;
        var shardBuffers = new byte[_codec.TotalShards][];
        for (var j = 0; j < _codec.TotalShards; j++)
        {
            shardBuffers[j] = new byte[CeilDiv(ReedSolomonCodec.ErasureBlockMaxSize, k)];
        }

        while (true)
        {
            var blockLen = await ReadBlockAsync(body, _blockBuffer, ct);
            if (blockLen == 0)
            {
                break;
            }

            var shardLen = CeilDiv(blockLen, k);

            // нарезка блока на k data-шардов с нулевой добивкой в памяти
            for (var j = 0; j < k; j++)
            {
                var take = Math.Min(shardLen, Math.Max(0, blockLen - j * shardLen));
                Array.Clear(shardBuffers[j], 0, shardLen);
                if (take > 0)
                {
                    Array.Copy(_blockBuffer, j * shardLen, shardBuffers[j], 0, take);
                }
            }

            var parity = new byte[_codec.ParityShards][];
            _codec.Encode(shardBuffers.Take(k).Select(b => b[..shardLen]).ToArray(), parity);

            // запись шард-блоков + bitrot-хэши каждого шард-блока каждого шарда
            for (var j = 0; j < k; j++)
            {
                await WriteShardBlockAsync(j, shardBuffers[j], shardLen, ct);
            }

            for (var j = 0; j < _codec.ParityShards; j++)
            {
                await WriteShardBlockAsync(k + j, parity[j], shardLen, ct);
            }

            _md5.AppendData(_blockBuffer, 0, blockLen);
            _sha256.AppendData(_blockBuffer, 0, blockLen);
            _sourceLength += blockLen;
            _blockCount++;
        }

        if (_sourceLength != expectedLength)
        {
            throw new XlIntegrityException(
                $"фактическая длина тела {_sourceLength} не совпадает с заявленной {expectedLength}");
        }
    }

    // Флаш всех потоков (без dispose) + итоговые дайджесты исходного потока.
    public async Task<ErasurePartResult> CompleteAsync()
    {
        ObjectDisposedException.ThrowIf(_completed, this);

        foreach (var stream in _shardStreams)
        {
            await stream.FlushAsync(CancellationToken.None);
        }

        _completed = true;
        return new ErasurePartResult(
            Convert.ToHexString(_md5.GetHashAndReset()).ToLowerInvariant(),
            Convert.ToHexString(_sha256.GetHashAndReset()).ToLowerInvariant(),
            _sourceLength,
            _blockCount,
            _hashesByShard.Select(l => l.ToArray()).ToArray());
    }

    public void Dispose()
    {
        _md5.Dispose();
        _sha256.Dispose();
    }

    private async Task WriteShardBlockAsync(int shardIndex, byte[] buffer, int shardLen, CancellationToken ct)
    {
        await _shardStreams[shardIndex].WriteAsync(buffer.AsMemory(0, shardLen), ct);
        _hashesByShard[shardIndex].Add(HighwayHash256.Hash(HighwayHash256.MagicKey, buffer.AsSpan(0, shardLen)));
    }

    private static async Task<int> ReadBlockAsync(Stream body, byte[] buffer, CancellationToken ct)
    {
        var total = 0;
        while (total < buffer.Length)
        {
            var read = await body.ReadAsync(buffer.AsMemory(total, buffer.Length - total), ct);
            if (read == 0)
            {
                break;
            }

            total += read;
        }

        return total;
    }

    private static int CeilDiv(int value, int div) => (value + div - 1) / div;
}
