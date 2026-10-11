using OwnS3.Storage.Erasure;

namespace OwnS3.UnitTests.Erasure;

// Тесты систематического RS-кодека над GF(2^8) (t40, план Task 2).
public class ReedSolomonCodecTests
{
    [Theory]
    [InlineData(0, 3, 1)]      // 0 байт
    [InlineData(1, 3, 1)]      // короче шард-блока
    [InlineData(64, 3, 1)]     // ровно k шард-блоков по 64
    [InlineData(1000, 3, 1)]   // с хвостом
    [InlineData(512, 4, 2)]    // m=2: одновременная потеря 2 шардов — C(6,2)=15 мульти-потерь
    [InlineData(1000, 6, 2)]   // m=2 при k+m=8: C(8,2)=28 мульти-потерь фактическим Reconstruct
    public void RoundTrip_AllLossCombinations_UpToParity(int len, int k, int m)
    {
        // Arrange
        var data = RandomBytes(len);
        var codec = new ReedSolomonCodec(k, m);
        var shardLen = CeilDiv(len, k);
        var shards = Shard(data, k, shardLen);
        var parity = new byte[m][];
        codec.Encode(shards, parity);
        var all = new byte[]?[k + m];
        for (var i = 0; i < k; i++)
        {
            all[i] = shards[i];
        }

        for (var i = 0; i < m; i++)
        {
            all[k + i] = parity[i];
        }

        // Act — каждая комбинация потерь до m
        foreach (var lost in Combinations(k + m, m))
        {
            var damaged = (byte[]?[])all.Clone();
            foreach (var i in lost)
            {
                damaged[i] = null;
            }

            codec.Reconstruct(damaged);

            // Assert — восстановленный поток равен исходным данным
            JoinShards(damaged, k, shardLen, len).Should().Equal(data);
        }
    }

    [Fact]
    public void Encode_IsSystematic_DataShardsUntouched()
    {
        // Arrange
        var data = RandomBytes(1000);
        var k = 3;
        var m = 2;
        var codec = new ReedSolomonCodec(k, m);
        var shardLen = CeilDiv(data.Length, k);
        var shards = Shard(data, k, shardLen);
        var before = shards.Select(s => s.ToArray()).ToArray();
        var parity = new byte[m][];

        // Act
        codec.Encode(shards, parity);

        // Assert — data-шарды побайтово равны входу (после нулевой добивки хвоста)
        for (var i = 0; i < k; i++)
        {
            shards[i].Should().Equal(before[i]);
        }
    }

    [Fact]
    public void Matrix_TopIsIdentity()
    {
        // Arrange
        var codec = new ReedSolomonCodec(6, 2);

        // Act
        var matrix = codec.Matrix;

        // Assert — верхние k×k строк — identity
        for (var i = 0; i < 6; i++)
        {
            for (var j = 0; j < 6; j++)
            {
                matrix[i, j].Should().Be((byte)(i == j ? 1 : 0));
            }
        }
    }

    [Theory]
    [InlineData(4, 2)]
    [InlineData(6, 2)]
    [InlineData(7, 1)]
    public void Matrix_AnyKRowsIndependent(int k, int m)
    {
        // Arrange — все C(k+m,k) подматрицы полного ранга по GF (k+m <= 8 перебором)
        var codec = new ReedSolomonCodec(k, m);

        // Act/Assert
        foreach (var rows in Combinations(k + m, k))
        {
            var sub = new byte[k, k];
            for (var r = 0; r < k; r++)
            {
                for (var c = 0; c < k; c++)
                {
                    sub[r, c] = codec.Matrix[rows[r], c];
                }
            }

            RankOverGf(sub, k).Should().Be(k, "любые k строк генерирующей матрицы обязаны быть независимы (rows={0})",
                string.Join(",", rows));
        }
    }

    [Fact]
    public void Matrix_StableAcrossInstances()
    {
        // Arrange
        var first = new ReedSolomonCodec(5, 3);
        var second = new ReedSolomonCodec(5, 3);

        // Act/Assert — конструирование детерминировано, матрицы равны
        for (var i = 0; i < first.TotalShards; i++)
        {
            for (var j = 0; j < first.DataShards; j++)
            {
                first.Matrix[i, j].Should().Be(second.Matrix[i, j]);
            }
        }
    }

    [Fact]
    public void Reconstruct_TooFewShards_Throws()
    {
        // Arrange
        var codec = new ReedSolomonCodec(3, 1);
        var shards = new byte[]?[]
        {
            new byte[] { 1, 2, 3 },
            null,
            null,
            null,
        };

        // Act
        var act = () => codec.Reconstruct(shards);

        // Assert — живых меньше k
        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void RoundTrip_K8M8()
    {
        // Arrange — крупный блочный прогон 1 МиБ; throughput в выводе — ориентир, не гейт (спека §7)
        const int k = 8, m = 8, blockSize = 1024 * 1024;
        var data = RandomBytes(blockSize);
        var codec = new ReedSolomonCodec(k, m);
        var shardLen = CeilDiv(data.Length, k);
        var shards = Shard(data, k, shardLen);
        var parity = new byte[m][];

        // Act
        var sw = System.Diagnostics.Stopwatch.StartNew();
        codec.Encode(shards, parity);
        sw.Stop();
        var encodeMs = sw.ElapsedMilliseconds;

        var damaged = new byte[]?[k + m];
        for (var i = 0; i < k; i++)
        {
            damaged[i] = shards[i];
        }

        for (var i = 0; i < m; i++)
        {
            damaged[k + i] = parity[i];
        }

        // теряем 8 из 16 так, чтобы живых осталось ровно k: data 0..7 потеряны, parity 8..15 живы
        for (var i = 0; i < k; i++)
        {
            damaged[i] = null;
        }

        sw.Restart();
        codec.Reconstruct(damaged);
        sw.Stop();
        var decodeMs = sw.ElapsedMilliseconds;

        // Assert
        JoinShards(damaged, k, shardLen, blockSize).Should().Equal(data);
        Console.WriteLine($"[K8M8] encode 1 МиБ: {encodeMs} мс; reconstruct 1 МиБ из k живых: {decodeMs} мс");
    }

    // ── хелперы ──

    private static byte[] RandomBytes(int len, int seed = 42)
    {
        var rng = new Random(seed);
        var buf = new byte[len];
        rng.NextBytes(buf);
        return buf;
    }

    private static int CeilDiv(int value, int div) => (value + div - 1) / div;

    // Нарезка потока на k равных шард-блоков с нулевой добивкой хвоста.
    private static byte[][] Shard(byte[] data, int k, int shardLen)
    {
        var shards = new byte[k][];
        for (var i = 0; i < k; i++)
        {
            shards[i] = new byte[shardLen];
            var take = Math.Min(shardLen, Math.Max(0, data.Length - i * shardLen));
            if (take > 0)
            {
                Array.Copy(data, i * shardLen, shards[i], 0, take);
            }
        }

        return shards;
    }

    private static byte[] JoinShards(byte[]?[] shards, int k, int shardLen, int totalLen)
    {
        var joined = new byte[k * shardLen];
        for (var i = 0; i < k; i++)
        {
            shards[i].Should().NotBeNull("data-шард обязан быть восстановлен");
            Array.Copy(shards[i]!, 0, joined, i * shardLen, shardLen);
        }

        // обрезаем нулевую добивку хвостового шарда до исходной длины потока
        return joined[..totalLen];
    }

    // Все сочетания из n по r (порядок возрастания).
    private static IEnumerable<int[]> Combinations(int n, int r)
    {
        var combo = Enumerable.Range(0, r).ToArray();
        while (true)
        {
            yield return (int[])combo.Clone();

            var i = r - 1;
            while (i >= 0 && combo[i] == n - r + i)
            {
                i--;
            }

            if (i < 0)
            {
                yield break;
            }

            combo[i]++;
            for (var j = i + 1; j < r; j++)
            {
                combo[j] = combo[j - 1] + 1;
            }
        }
    }

    // Ранг матрицы n×n над GF(2^8) гауссовым исключением.
    private static int RankOverGf(byte[,] matrix, int n)
    {
        var work = (byte[,])matrix.Clone();
        var rank = 0;
        for (var col = 0; col < n && rank < n; col++)
        {
            // поиск ведущего
            var pivot = -1;
            for (var row = rank; row < n; row++)
            {
                if (work[row, col] != 0)
                {
                    pivot = row;
                    break;
                }
            }

            if (pivot < 0)
            {
                continue;
            }

            // обмен строк
            for (var c = 0; c < n; c++)
            {
                (work[rank, c], work[pivot, c]) = (work[pivot, c], work[rank, c]);
            }

            // нормализация и исключение ниже
            var inv = GaloisField256.Inverse(work[rank, col]);
            for (var c = 0; c < n; c++)
            {
                work[rank, c] = GaloisField256.Multiply(work[rank, c], inv);
            }

            for (var row = 0; row < n; row++)
            {
                if (row != rank && work[row, col] != 0)
                {
                    var factor = work[row, col];
                    for (var c = 0; c < n; c++)
                    {
                        work[row, c] ^= GaloisField256.Multiply(factor, work[rank, c]);
                    }
                }
            }

            rank++;
        }

        return rank;
    }
}
