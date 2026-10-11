namespace OwnS3.Storage.Erasure;

// SipHash-2-4 (референс Aumasson/DJB): 128-битный ключ, 64-битный выход.
// Используется SIPMOD-распределением объектов по сетам (канон 06 §3).
// Собственная реализация: пакет System.IO.Hashing SipHash не содержит;
// корректность — тест-векторами референса (ключ 000102..0f, входы 0..63 байт).
public static class SipHash24
{
    // Ключ — ровно 16 байт; data — произвольная длина.
    public static ulong Hash(ReadOnlySpan<byte> key, ReadOnlySpan<byte> data)
    {
        if (key.Length != 16)
            throw new ArgumentException("ключ SipHash — ровно 16 байт", nameof(key));

        var k0 = System.Runtime.InteropServices.MemoryMarshal.Read<ulong>(key[..8]);
        var k1 = System.Runtime.InteropServices.MemoryMarshal.Read<ulong>(key[8..]);

        var v0 = k0 ^ 0x736f6d6570736575UL;
        var v1 = k1 ^ 0x646f72616e646f6dUL;
        var v2 = k0 ^ 0x6c7967656e657261UL;
        var v3 = k1 ^ 0x7465646279746573UL;

        var remaining = data;
        while (remaining.Length >= 8)
        {
            var mi = System.Runtime.InteropServices.MemoryMarshal.Read<ulong>(remaining[..8]);
            v3 ^= mi;
            SipRound(ref v0, ref v1, ref v2, ref v3);
            SipRound(ref v0, ref v1, ref v2, ref v3);
            v0 ^= mi;
            remaining = remaining[8..];
        }

        // хвостовой < 8 байт: little-endian добивка + длина в старшем байте
        ulong last = (ulong)data.Length << 56;
        for (var i = 0; i < remaining.Length; i++)
        {
            last |= (ulong)remaining[i] << (8 * i);
        }

        v3 ^= last;
        SipRound(ref v0, ref v1, ref v2, ref v3);
        SipRound(ref v0, ref v1, ref v2, ref v3);
        v0 ^= last;
        v2 ^= 0xff;
        SipRound(ref v0, ref v1, ref v2, ref v3);
        SipRound(ref v0, ref v1, ref v2, ref v3);
        SipRound(ref v0, ref v1, ref v2, ref v3);
        SipRound(ref v0, ref v1, ref v2, ref v3);
        return v0 ^ v1 ^ v2 ^ v3;
    }

    private static void SipRound(ref ulong v0, ref ulong v1, ref ulong v2, ref ulong v3)
    {
        v0 += v1;
        v1 = (v1 << 13) | (v1 >> 51);
        v1 ^= v0;
        v0 = (v0 << 32) | (v0 >> 32);
        v2 += v3;
        v3 = (v3 << 16) | (v3 >> 48);
        v3 ^= v2;
        v0 += v3;
        v3 = (v3 << 21) | (v3 >> 43);
        v3 ^= v0;
        v2 += v1;
        v1 = (v1 << 17) | (v1 >> 47);
        v1 ^= v2;
        v2 = (v2 << 32) | (v2 >> 32);
    }
}
