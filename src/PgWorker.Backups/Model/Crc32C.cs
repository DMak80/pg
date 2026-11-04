namespace PgWorker.Backups;

/// <summary>CRC-32C (Castagnoli) — сверка xl_crc записей WAL (spec §4.1, решение
/// №18): reflected-полином 0x82F63B78, init 0xFFFFFFFF, xorout 0xFFFFFFFF, БЕЗ
/// отражения байтов (зеркальный алгоритм); управляемая табличная реализация —
/// P/Invoke запрещён, пакет System.IO.Hashing не подключается. Инкрементальный:
/// Init → Update(куски) → Finish.</summary>
internal static class Crc32C
{
    private static readonly uint[] Table = BuildTable();

    internal static uint Init() => 0xFFFFFFFF;

    internal static uint Update(uint crc, byte[] b, int offset, int count)
    {
        for (var i = offset; i < offset + count; i++)
            crc = Table[(crc ^ b[i]) & 0xFF] ^ (crc >> 8);
        return crc;
    }

    internal static uint Finish(uint crc) => crc ^ 0xFFFFFFFF;

    internal static uint HashData(byte[] data) => Finish(Update(Init(), data, 0, data.Length));

    private static uint[] BuildTable()
    {
        var table = new uint[256];
        for (uint i = 0; i < 256; i++)
        {
            var crc = i;
            for (var k = 0; k < 8; k++)
                crc = (crc & 1) != 0 ? (crc >> 1) ^ 0x82F63B78 : crc >> 1;
            table[i] = crc;
        }
        return table;
    }
}
