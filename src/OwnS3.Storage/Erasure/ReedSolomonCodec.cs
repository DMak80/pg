namespace OwnS3.Storage.Erasure;

// Систематический RS-кодек k data → k+m (канон 06 §4): матрица — Vandermonde,
// приведённая к identity в верхних k×k строках (гауссово исключение над GF(2⁸),
// схема референса buildMatrix: vm × inverse(vm.Top) — верх становится identity,
// свойство «любые k строк независимы» сохраняется).
// Оперирует равными шард-блоками одного erasure-блока (нарезку делает
// ErasureBlockWriter). Потокобезопасен после создания (матрица иммутабельна).
public sealed class ReedSolomonCodec
{
    // Максимальный erasure-блок исходных данных (канон 06 §4).
    public const int ErasureBlockMaxSize = 1024 * 1024;

    private readonly byte[,] _matrix;

    public ReedSolomonCodec(int dataShards, int parityShards)
    {
        if (dataShards < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(dataShards), "k >= 1 обязателен");
        }

        if (parityShards < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(parityShards), "m >= 1 обязателен");
        }

        if (dataShards + parityShards > 255)
        {
            throw new ArgumentOutOfRangeException(nameof(parityShards), "k + m <= 255 (размер поля GF(2^8))");
        }

        DataShards = dataShards;
        ParityShards = parityShards;
        _matrix = BuildGeneratorMatrix(dataShards, dataShards + parityShards);
    }

    public int DataShards { get; }

    public int ParityShards { get; }

    public int TotalShards => DataShards + ParityShards;

    // (k+m)×k, стабильна: верх k×k == identity. Копия — матрица иммутабельна внутри.
    public byte[,] Matrix => (byte[,])_matrix.Clone();

    // data[k] (len равные) → parity[m] (len). Заполняет parity-элементы (аллоцирует при null).
    public void Encode(byte[][] dataShards, byte[]?[] parityShards)
    {
        ArgumentNullException.ThrowIfNull(dataShards);
        ArgumentNullException.ThrowIfNull(parityShards);

        if (dataShards.Length != DataShards)
        {
            throw new ArgumentException($"ожидалось {DataShards} data-шардов, получено {dataShards.Length}", nameof(dataShards));
        }

        if (parityShards.Length != ParityShards)
        {
            throw new ArgumentException($"ожидалось {ParityShards} parity-шардов, получено {parityShards.Length}", nameof(parityShards));
        }

        if (dataShards.Length == 0)
        {
            return;
        }

        var shardLen = dataShards[0].Length;
        foreach (var shard in dataShards)
        {
            if (shard.Length != shardLen)
            {
                throw new ArgumentException("data-шарды обязаны быть равной длины", nameof(dataShards));
            }
        }

        for (var i = 0; i < ParityShards; i++)
        {
            if (parityShards[i] is null || parityShards[i]!.Length != shardLen)
            {
                parityShards[i] = new byte[shardLen];
            }

            // parity_i = Σ_j matrix[row][j] ⊗ data_j; единичные коэффициенты — прямой XOR
            EncodeRow(dataShards, shardLen, DataShards + i, parityShards[i]!);
        }
    }

    // Восстановление: shards[i] == null или короткий — потерян. Живых должно быть >= k,
    // иначе InvalidOperationException. Восстанавливает data и parity (все <= m потерь).
    public void Reconstruct(byte[]?[] shards)
    {
        ArgumentNullException.ThrowIfNull(shards);

        if (shards.Length != TotalShards)
        {
            throw new ArgumentException($"ожидалось {TotalShards} шардов, получено {shards.Length}", nameof(shards));
        }

        // ожидаемая длина — по первому живому шарду (шард-блоки одного блока равны)
        var shardLen = -1;
        for (var i = 0; i < TotalShards; i++)
        {
            if (shards[i] is { } valid)
            {
                shardLen = valid.Length;
                break;
            }
        }

        if (shardLen < 0)
        {
            throw new InvalidOperationException("нет ни одного живого шарда");
        }

        // короткий шард — тоже потерян (запись оборвалась)
        var alive = new List<int>(TotalShards);
        for (var i = 0; i < TotalShards; i++)
        {
            if (shards[i] is { Length: var len } && len == shardLen)
            {
                alive.Add(i);
            }
            else
            {
                shards[i] = null;
            }
        }

        if (alive.Count < DataShards)
        {
            throw new InvalidOperationException(
                $"живых шардов {alive.Count} меньше k={DataShards} — восстановление невозможно");
        }

        var firstAlive = alive.Take(DataShards).ToArray();
        var missing = Enumerable.Range(0, TotalShards).Except(firstAlive).ToArray();
        if (missing.Length == 0)
        {
            return;
        }

        // решающая система: D = строки генерирующей матрицы живых, D × x = y_alive
        var d = new byte[DataShards, DataShards];
        for (var r = 0; r < DataShards; r++)
        {
            for (var c = 0; c < DataShards; c++)
            {
                d[r, c] = _matrix[firstAlive[r], c];
            }
        }

        var dInv = InvertMatrix(d, DataShards);

        // x = D^-1 × y_alive — исходные data-шарды
        var x = new byte[DataShards][];
        var scaled = new byte[shardLen];
        for (var i = 0; i < DataShards; i++)
        {
            x[i] = new byte[shardLen];
            for (var r = 0; r < DataShards; r++)
            {
                var coef = dInv[i, r];
                if (coef == 0)
                {
                    continue;
                }

                var source = shards[firstAlive[r]]!;
                if (coef == 1)
                {
                    XorInPlace(x[i], source);
                }
                else
                {
                    source.AsSpan(0, shardLen).CopyTo(scaled);
                    GaloisField256.MultiplyInPlace(coef, scaled);
                    XorInPlace(x[i], scaled);
                }
            }
        }

        // потерянные data-позиции — напрямую из x (верх матрицы identity);
        // потерянные parity — строкой генерирующей матрицы от x
        foreach (var idx in missing)
        {
            if (shards[idx] != null)
            {
                continue;
            }

            if (idx < DataShards)
            {
                shards[idx] = x[idx].ToArray();
            }
            else
            {
                var restored = new byte[shardLen];
                EncodeRow(x, shardLen, idx, restored);
                shards[idx] = restored;
            }
        }
    }

    // target = Σ_j matrix[row][j] ⊗ data_j (строка генерирующей матрицы, коэффициент 0 пропускается).
    private void EncodeRow(byte[][] data, int shardLen, int row, byte[] target)
    {
        Array.Clear(target);
        var scaled = new byte[shardLen];
        for (var j = 0; j < DataShards; j++)
        {
            var coef = _matrix[row, j];
            if (coef == 0)
            {
                continue;
            }

            if (coef == 1)
            {
                XorInPlace(target, data[j]);
            }
            else
            {
                data[j].AsSpan(0, shardLen).CopyTo(scaled);
                GaloisField256.MultiplyInPlace(coef, scaled);
                XorInPlace(target, scaled);
            }
        }
    }

    private static void XorInPlace(Span<byte> target, ReadOnlySpan<byte> source)
    {
        for (var i = 0; i < target.Length; i++)
        {
            target[i] ^= source[i];
        }
    }

    // Генерирующая матрица: Vandermonde (total×k) со схемой референса klauspost —
    // vm[r][c] = r^c (узел x_r = r в строке, степень c в столбце): любая квадратная
    // подматрица строк — классическая Vandermonde с различными узлами, det != 0 —
    // MDS для всех k+m <= 255 (схема (c+1)^r таким свойством над GF(2^8) не обладает).
    // Приведение: vm × inverse(vm.Top) — верх становится identity, ранги строк сохраняются.
    private static byte[,] BuildGeneratorMatrix(int k, int total)
    {
        var vm = new byte[total, k];
        for (var r = 0; r < total; r++)
        {
            for (var c = 0; c < k; c++)
            {
                vm[r, c] = Pow((byte)r, c);
            }
        }

        var top = new byte[k, k];
        for (var r = 0; r < k; r++)
        {
            for (var c = 0; c < k; c++)
            {
                top[r, c] = vm[r, c];
            }
        }

        var topInv = InvertMatrix(top, k);

        // result = vm × topInv
        var result = new byte[total, k];
        for (var r = 0; r < total; r++)
        {
            for (var c = 0; c < k; c++)
            {
                byte acc = 0;
                for (var t = 0; t < k; t++)
                {
                    acc ^= GaloisField256.Multiply(vm[r, t], topInv[t, c]);
                }

                result[r, c] = acc;
            }
        }

        return result;
    }

    // Возведение в степень над GF(2^8).
    private static byte Pow(byte value, int exponent)
    {
        var result = (byte)1;
        for (var i = 0; i < exponent; i++)
        {
            result = GaloisField256.Multiply(result, value);
        }

        return result;
    }

    // Инверсия n×n матрицы над GF(2^8): расширенная [A|E] → гауссово исключение → [E|A^-1].
    private static byte[,] InvertMatrix(byte[,] source, int n)
    {
        var work = new byte[n, 2 * n];
        for (var r = 0; r < n; r++)
        {
            for (var c = 0; c < n; c++)
            {
                work[r, c] = source[r, c];
            }

            work[r, n + r] = 1;
        }

        for (var col = 0; col < n; col++)
        {
            // ведущая строка с ненулевым элементом в столбце
            var pivot = -1;
            for (var row = col; row < n; row++)
            {
                if (work[row, col] != 0)
                {
                    pivot = row;
                    break;
                }
            }

            if (pivot < 0)
            {
                throw new InvalidOperationException("матрица вырождена над GF(2^8)");
            }

            if (pivot != col)
            {
                for (var c = 0; c < 2 * n; c++)
                {
                    (work[col, c], work[pivot, c]) = (work[pivot, c], work[col, c]);
                }
            }

            var inv = GaloisField256.Inverse(work[col, col]);
            for (var c = 0; c < 2 * n; c++)
            {
                work[col, c] = GaloisField256.Multiply(work[col, c], inv);
            }

            for (var row = 0; row < n; row++)
            {
                if (row == col || work[row, col] == 0)
                {
                    continue;
                }

                var factor = work[row, col];
                for (var c = 0; c < 2 * n; c++)
                {
                    work[row, c] ^= GaloisField256.Multiply(factor, work[col, c]);
                }
            }
        }

        var inverse = new byte[n, n];
        for (var r = 0; r < n; r++)
        {
            for (var c = 0; c < n; c++)
            {
                inverse[r, c] = work[r, n + c];
            }
        }

        return inverse;
    }
}
