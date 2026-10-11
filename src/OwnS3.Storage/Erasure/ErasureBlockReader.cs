using OwnS3.Storage;

namespace OwnS3.Storage.Erasure;

// Чтение: сборка исходного потока из k+m шард-потоков (null = диск офлайн),
// bitrot-сверка каждого читаемого блока по хэшам из xl.meta; битый блок →
// реконструкция из k других; позиции битых/недостающих собираются в
// CorruptedShards. Выбор k живых источников блока — в порядке позиций
// (data-позиции 0..k−1 систематического кода раньше parity) — приоритет
// data-позиций чтения (канон 06 §8 п.2).
public sealed class ErasureBlockReader : IDisposable
{
    private readonly ReedSolomonCodec _codec;
    private readonly Stream?[] _shardStreams;
    private readonly ErasurePartResult _meta;
    private readonly long _offset;
    private readonly long _length;
    private readonly int _fullShardLen;
    private readonly HashSet<int> _corruptedShards = [];
    private readonly List<(int Index, Stream Stream)> _openStreams = [];

    private long _delivered;
    private int _currentBlockIndex = -1;
    private byte[]? _currentBlock;
    private int _currentBlockConsumed;

    public ErasureBlockReader(ReedSolomonCodec codec, Stream?[] shardStreams, ErasurePartResult meta,
        long offset, long length)
    {
        ArgumentNullException.ThrowIfNull(codec);
        ArgumentNullException.ThrowIfNull(shardStreams);
        ArgumentNullException.ThrowIfNull(meta);

        if (shardStreams.Length != codec.TotalShards)
        {
            throw new ArgumentException(
                $"ожидалось {codec.TotalShards} шард-потоков, получено {shardStreams.Length}", nameof(shardStreams));
        }

        if (offset < 0 || length < 0 || offset + length > meta.SourceLength)
        {
            throw new ArgumentOutOfRangeException(nameof(offset), "Range-окно вне границ исходного потока");
        }

        _codec = codec;
        _shardStreams = shardStreams;
        _meta = meta;
        _offset = offset;
        _length = length;
        _fullShardLen = CeilDiv(ReedSolomonCodec.ErasureBlockMaxSize, codec.DataShards);
    }

    // Позиции с несовпавшим bitrot (для пометки heal).
    public IReadOnlyList<int> CorruptedShards => _corruptedShards.ToList();

    public Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(buffer);

        if (count == 0)
        {
            return Task.FromResult(0);
        }

        return ReadInternalAsync(buffer, offset, count, ct);
    }

    private async Task<int> ReadInternalAsync(byte[] buffer, int offset, int count, CancellationToken ct)
    {
        var written = 0;
        while (written < count && _delivered < _length)
        {
            if (_currentBlock is null || !CurrentBlockHasMore)
            {
                if (!await LoadNextBlockAsync(ct))
                {
                    break;
                }
            }

            var blockOffset = _currentBlockConsumed;
            var take = Math.Min(count - written, _currentBlock!.Length - blockOffset);
            Array.Copy(_currentBlock, blockOffset, buffer, offset + written, take);
            _currentBlockConsumed += take;
            written += take;
            _delivered += take;
        }

        return written;
    }

    private bool CurrentBlockHasMore => _currentBlock is not null && _currentBlockConsumed < _currentBlock.Length;

    // Загружает следующий блок окна, декодируя его из шард-потоков.
    private async Task<bool> LoadNextBlockAsync(CancellationToken ct)
    {
        long windowEnd = _offset + _length;

        // первый блок окна: по offset; далее — следующий за текущим
        var blockIdx = _currentBlockIndex < 0
            ? (int)(_offset / ReedSolomonCodec.ErasureBlockMaxSize)
            : _currentBlockIndex + 1;

        if (blockIdx >= _meta.BlockCount || blockIdx * (long)ReedSolomonCodec.ErasureBlockMaxSize >= windowEnd)
        {
            return false;
        }

        var blockStart = blockIdx * (long)ReedSolomonCodec.ErasureBlockMaxSize;
        var blockLen = (int)Math.Min(ReedSolomonCodec.ErasureBlockMaxSize, _meta.SourceLength - blockStart);

        var decoded = await DecodeBlockAsync(blockIdx, blockLen, ct);

        // вырезаем из блока пересечение с окном чтения
        var from = (int)Math.Max(0, _offset - blockStart);
        var to = (int)Math.Min(blockLen, windowEnd - blockStart);
        _currentBlockIndex = blockIdx;
        _currentBlock = from < to ? decoded[from..to] : Array.Empty<byte>();
        _currentBlockConsumed = 0;
        return true;
    }

    // Чтение шард-блоков блока: k живых по порядку позиций (data-приоритет),
    // bitrot-сверка, реконструкция недостающих.
    private async Task<byte[]> DecodeBlockAsync(int blockIdx, int blockLen, CancellationToken ct)
    {
        var k = _codec.DataShards;
        var shardLen = CeilDiv(blockLen, k);
        var shards = new byte[]?[_codec.TotalShards];
        var valid = 0;

        for (var j = 0; j < _codec.TotalShards && valid < k; j++)
        {
            var stream = _shardStreams[j];
            if (stream is null)
            {
                continue;
            }

            var shardOffset = (long)blockIdx * _fullShardLen;
            var buffer = new byte[shardLen];
            await ReadExactlyAsync(stream, shardOffset, buffer, ct);

            // bitrot-сверка шард-блока
            var expected = BlockHash(j, blockIdx);
            if (expected is not null)
            {
                var actual = HighwayHash256.Hash(HighwayHash256.MagicKey, buffer);
                if (!actual.AsSpan().SequenceEqual(expected))
                {
                    _corruptedShards.Add(j);
                    continue;
                }
            }

            shards[j] = buffer;
            valid++;
        }

        if (valid < k)
        {
            throw new XlIntegrityException(
                $"блок {blockIdx}: живых шард-блоков {valid} меньше k={k} — bitrot/offline за пределами parity");
        }

        // все валидные — data-позиции без потерь: блок не декодируется вовсе
        if (valid == k && shards.Take(k).All(s => s is not null))
        {
            return JoinDataShards(shards, k, shardLen, blockLen);
        }

        _codec.Reconstruct(shards);
        return JoinDataShards(shards, k, shardLen, blockLen);
    }

    // Хэш шард-блока из меты (раскладка шард-мажор: [shard][block]); битых/коротких мет — null.
    private byte[]? BlockHash(int shardIndex, int blockIdx)
    {
        if (shardIndex >= _meta.ShardBlockHashes.Length)
        {
            return null;
        }

        var perShard = _meta.ShardBlockHashes[shardIndex];
        return blockIdx < perShard.Length ? perShard[blockIdx] : null;
    }

    private static byte[] JoinDataShards(byte[]?[] shards, int k, int shardLen, int blockLen)
    {
        var result = new byte[blockLen];
        for (var j = 0; j < k; j++)
        {
            var take = Math.Min(shardLen, Math.Max(0, blockLen - j * shardLen));
            if (take > 0)
            {
                Array.Copy(shards[j]!, 0, result, j * shardLen, take);
            }
        }

        return result;
    }

    private static async Task ReadExactlyAsync(Stream stream, long position, byte[] buffer, CancellationToken ct)
    {
        if (stream.CanSeek)
        {
            stream.Seek(position, SeekOrigin.Begin);
        }

        var total = 0;
        while (total < buffer.Length)
        {
            var read = await stream.ReadAsync(buffer.AsMemory(total, buffer.Length - total), ct);
            if (read == 0)
            {
                break;
            }

            total += read;
        }
    }

    private static int CeilDiv(int value, int div) => (value + div - 1) / div;

    public void Dispose()
    {
        // шард-потоки принадлежат вызывающему; закрываем только собственные клоны (нет)
        _openStreams.Clear();
    }
}
