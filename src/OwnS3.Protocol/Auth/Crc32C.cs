namespace OwnS3.Protocol.Auth;

// Инкрементальный CRC32C (Castagnoli, reflected 0x82F63B78, init/xorout
// 0xFFFFFFFF). Отдельная реализация: в System.IO.Hashing 10.x тип Crc32C
// отсутствует (удалён апстримом), а трейлер x-amz-checksum-crc32c — канон
// главы 03 §3.
public sealed class Crc32C
{
    private static readonly uint[] Table = BuildTable();

    private uint _hash = 0xFFFFFFFF;

    public void Append(ReadOnlySpan<byte> data)
    {
        foreach (var b in data)
            _hash = Table[(byte)_hash ^ b] ^ (_hash >> 8);
    }

    public byte[] GetCurrentHash()
    {
        // Канонический AWS-формат контрольных сумм — big-endian (network order).
        var final = ~_hash;
        return [(byte)(final >> 24), (byte)(final >> 16), (byte)(final >> 8), (byte)final];
    }

    public static byte[] Hash(ReadOnlySpan<byte> data)
    {
        var crc = new Crc32C();
        crc.Append(data);
        return crc.GetCurrentHash();
    }

    private static uint[] BuildTable()
    {
        var table = new uint[256];
        for (var i = 0; i < 256; i++)
        {
            var c = (uint)i;
            for (var k = 0; k < 8; k++)
                c = (c & 1) != 0 ? 0x82F63B78u ^ (c >> 1) : c >> 1;
            table[i] = c;
        }
        return table;
    }
}
