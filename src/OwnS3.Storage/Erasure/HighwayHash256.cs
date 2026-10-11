using System.Buffers.Binary;
using System.Numerics;

namespace OwnS3.Storage.Erasure;

// Порт HighwayHash-256 (github.com/minio/highwayhash v1.0.3, portable-ветка
// без SIMD — highwayhash.go + highwayhash_generic.go). Ключ — 32 байта.
// Используется для bitrot-хэшей шард-блоков (канон 06 §6). SIMD не используется:
// критерий — корректность (векторы Go), скорость достаточна на блоке 1 МиБ.
public static class HighwayHash256
{
    // Магический ключ bitrot-контроля референса (cmd/bitrot.go, magicHighwayHash256Key).
    private static readonly byte[] s_magicKey =
    {
        0x4b, 0xe7, 0x34, 0xfa, 0x8e, 0x23, 0x8a, 0xcd, 0x26, 0x3e, 0x83, 0xe6, 0xbb, 0x96, 0x85, 0x52,
        0x04, 0x0f, 0x93, 0x5d, 0xa3, 0x9f, 0x44, 0x14, 0x97, 0xe0, 0x9d, 0x13, 0x22, 0xde, 0x36, 0xa0,
    };

    // Инициализационные константы референса.
    private static readonly ulong[] s_init0 =
    {
        0xdbe6d5d5fe4cce2f, 0xa4093822299f31d0, 0x13198a2e03707344, 0x243f6a8885a308d3,
    };

    private static readonly ulong[] s_init1 =
    {
        0x3bd39e10cb0ef593, 0xc0acf169b5f18a8c, 0xbe5466cf34e90c6c, 0x452821e638d01377,
    };

    public static ReadOnlySpan<byte> MagicKey => s_magicKey;

    public static byte[] Hash(ReadOnlySpan<byte> key, ReadOnlySpan<byte> data)
    {
        var state = new ulong[16];
        Initialize(state, key);

        var fullBlocks = data.Length & ~31;
        if (fullBlocks > 0)
        {
            Update(state, data[..fullBlocks]);
            data = data[fullBlocks..];
        }

        if (data.Length > 0)
        {
            var block = new byte[32];
            data.CopyTo(block);
            HashBuffer(state, block, data.Length);
        }

        var hash = new byte[32];
        Finalize(hash, state);
        return hash;
    }

    public static byte[] HashStream(ReadOnlySpan<byte> key, Stream data)
    {
        ArgumentNullException.ThrowIfNull(data);

        var state = new ulong[16];
        Initialize(state, key);

        var buffer = new byte[32];
        int read;
        while ((read = data.Read(buffer, 0, 32)) == 32)
        {
            Update(state, buffer);
        }

        if (read > 0)
        {
            var block = new byte[32];
            new ReadOnlySpan<byte>(buffer, 0, read).CopyTo(block);
            HashBuffer(state, block, read);
        }

        var hash = new byte[32];
        Finalize(hash, state);
        return hash;
    }

    // Инициализация состояния из ключа (initializeGeneric).
    private static void Initialize(ulong[] state, ReadOnlySpan<byte> key)
    {
        if (key.Length != 32)
        {
            throw new ArgumentException("ключ HighwayHash — ровно 32 байта", nameof(key));
        }

        var k0 = BinaryPrimitives.ReadUInt64LittleEndian(key);
        var k1 = BinaryPrimitives.ReadUInt64LittleEndian(key[8..]);
        var k2 = BinaryPrimitives.ReadUInt64LittleEndian(key[16..]);
        var k3 = BinaryPrimitives.ReadUInt64LittleEndian(key[24..]);

        for (var i = 0; i < 4; i++)
        {
            state[8 + i] = s_init0[i];
            state[12 + i] = s_init1[i];
        }

        state[0] = s_init0[0] ^ k0;
        state[1] = s_init0[1] ^ k1;
        state[2] = s_init0[2] ^ k2;
        state[3] = s_init0[3] ^ k3;

        // byteswap 32-битных половин ключа
        var s0 = (k0 >> 32) | (k0 << 32);
        var s1 = (k1 >> 32) | (k1 << 32);
        var s2 = (k2 >> 32) | (k2 << 32);
        var s3 = (k3 >> 32) | (k3 << 32);

        state[4] = s_init1[0] ^ s0;
        state[5] = s_init1[1] ^ s1;
        state[6] = s_init1[2] ^ s2;
        state[7] = s_init1[3] ^ s3;
    }

    // Обработка 32-байтовых пакетов (updateGeneric): 4 параллельные линии
    // (v0/v1/mul0/mul1) + zipperMerge-перемешивания.
    private static void Update(ulong[] state, ReadOnlySpan<byte> msg)
    {
        while (msg.Length >= 32)
        {
            var m = msg[..32];

            for (var i = 0; i < 4; i++)
            {
                var mi = BinaryPrimitives.ReadUInt64LittleEndian(m[(i * 8)..]);

                // v1 += m + mul0; mul0 ^= lo32(v1) * hi32(v0); v0 += mul1; mul1 ^= lo32(v0) * hi32(v1)
                state[4 + i] += mi + state[8 + i];
                state[8 + i] ^= unchecked((uint)state[4 + i]) * (state[0 + i] >> 32);
                state[0 + i] += state[12 + i];
                state[12 + i] ^= unchecked((uint)state[0 + i]) * (state[4 + i] >> 32);
            }

            ZipperMergeAdd(state, 4, 5, state, 0, 1);
            ZipperMergeAdd(state, 6, 7, state, 2, 3);
            ZipperMergeAdd(state, 0, 1, state, 4, 5);
            ZipperMergeAdd(state, 2, 3, state, 6, 7);

            msg = msg[32..];
        }
    }

    // zipperMerge(v0, v1): пара результирующих слагаемых — добавляется к целям (инлайн референса).
    private static void ZipperMergeAdd(ulong[] source, int v0Idx, int v1Idx, ulong[] target, int d0Idx, int d1Idx)
    {
        var val0 = source[v0Idx];
        var val1 = source[v1Idx];

        var res = val0 & (0xffUL << (2 * 8));
        var res2 = (val0 & (0xffUL << (7 * 8))) + (val1 & (0xffUL << (2 * 8)));
        res += (val1 & (0xffUL << (7 * 8))) >> 8;
        res2 += (val0 & (0xffUL << (6 * 8))) >> 8;
        res += ((val0 & (0xffUL << (5 * 8))) + (val1 & (0xffUL << (6 * 8)))) >> 16;
        res2 += (val1 & (0xffUL << (5 * 8))) >> 16;
        res += ((val0 & (0xffUL << (3 * 8))) + (val1 & (0xffUL << (4 * 8)))) >> 24;
        res2 += ((val1 & (0xffUL << (3 * 8))) + (val0 & (0xffUL << (4 * 8)))) >> 24;
        res += (val0 & (0xffUL << (1 * 8))) << 32;
        res2 += (val1 & 0xff) << 48;
        res += val0 << 56;
        res2 += (val1 & (0xffUL << (1 * 8))) << 24;

        target[d0Idx] += res;
        target[d1Idx] += res2;
    }

    // Хвост < 32 байт с добивкой (hashBuffer референса).
    private static void HashBuffer(ulong[] state, byte[] buffer, int offset)
    {
        var block = new byte[32];
        var mod32 = ((ulong)offset << 32) + (ulong)offset;
        for (var i = 0; i < 4; i++)
        {
            state[i] += mod32;
        }

        for (var i = 0; i < 4; i++)
        {
            var t0 = (uint)state[4 + i];
            t0 = BitOperations.RotateLeft(t0, offset);

            var t1 = (uint)(state[4 + i] >> 32);
            t1 = BitOperations.RotateLeft(t1, offset);

            state[4 + i] = ((ulong)t1 << 32) | t0;
        }

        var mod4 = offset & 3;
        var remain = offset - mod4;

        Array.Copy(buffer, 0, block, 0, remain);
        if (offset >= 16)
        {
            Array.Copy(buffer, offset - 4, block, 28, 4);
        }
        else if (mod4 != 0)
        {
            uint last = buffer[remain];
            last += (uint)buffer[remain + (mod4 >> 1)] << 8;
            last += (uint)buffer[offset - 1] << 16;
            BinaryPrimitives.WriteUInt32LittleEndian(block.AsSpan(16), last);
        }

        Update(state, block);
    }

    // Финализация 32 байта: 10 раундов перестановки v0 + update, затем reduceMod (finalizeGeneric).
    private static void Finalize(byte[] output, ulong[] state)
    {
        var tmp = new byte[32];
        for (var round = 0; round < 10; round++)
        {
            var p0 = (state[2] >> 32) | (state[2] << 32);
            var p1 = (state[3] >> 32) | (state[3] << 32);
            var p2 = (state[0] >> 32) | (state[0] << 32);
            var p3 = (state[1] >> 32) | (state[1] << 32);

            BinaryPrimitives.WriteUInt64LittleEndian(tmp.AsSpan(0), p0);
            BinaryPrimitives.WriteUInt64LittleEndian(tmp.AsSpan(8), p1);
            BinaryPrimitives.WriteUInt64LittleEndian(tmp.AsSpan(16), p2);
            BinaryPrimitives.WriteUInt64LittleEndian(tmp.AsSpan(24), p3);

            Update(state, tmp);
        }

        var (h0, h1) = ReduceMod(
            state[0] + state[8 + 0], state[1] + state[8 + 1],
            state[4 + 0] + state[12 + 0], state[4 + 1] + state[12 + 1]);
        BinaryPrimitives.WriteUInt64LittleEndian(output.AsSpan(0), h0);
        BinaryPrimitives.WriteUInt64LittleEndian(output.AsSpan(8), h1);

        (h0, h1) = ReduceMod(
            state[2] + state[8 + 2], state[3] + state[8 + 3],
            state[4 + 2] + state[12 + 2], state[4 + 3] + state[12 + 3]);
        BinaryPrimitives.WriteUInt64LittleEndian(output.AsSpan(16), h0);
        BinaryPrimitives.WriteUInt64LittleEndian(output.AsSpan(24), h1);
    }

    // Редукция v = [v0, v1, v2, v3] по неприводимому полиному x^128 + x^2 + x.
    private static (ulong R0, ulong R1) ReduceMod(ulong v0, ulong v1, ulong v2, ulong v3)
    {
        v3 &= 0x3FFFFFFFFFFFFFFFUL;

        var r0 = v2;
        var r1 = v3;

        var v3Shifted = (v3 << 1) | (v2 >> 63);
        var v2Shifted = v2 << 1;
        r1 = (r1 << 2) | (r0 >> 62);
        r0 <<= 2;

        r0 ^= v0 ^ v2Shifted;
        r1 ^= v1 ^ v3Shifted;
        return (r0, r1);
    }
}
