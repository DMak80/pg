using FluentAssertions;
using PgWorker.Backups;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// ValidatedPrefixLength: границы записей + CRC xl_crc каждой записи (spec §4.1,
// решение №18). Фикстуры строятся валидными по CRC, затем портятся точечно.
public class WalPrefixScannerCrcTests
{
    private const int Page = WalPrefixScanner.PageBytes;

    /// <summary>Сегмент с long-заголовком (tli=1, pageaddr=0) и записями с
    /// корректным xl_crc: запись recordLen байт от позиции 40; тело после
    /// 4-байтового xl_crc заполнено паттерном; xl_crc = CRC-32C образа
    /// ([pos..pos+20) + [pos+24..recordEnd)).</summary>
    private static byte[] SegmentWithRecords(params int[] recordLengths)
    {
        var records = new List<(int Pos, int Len)>();
        var data = new byte[Page * 2];
        data[4] = 1; // tli=1 LE
        var pos = WalPrefixScanner.LongHeaderBytes;
        var rng = 1;
        foreach (var len in recordLengths)
        {
            if (pos % Page == 0)
                pos += WalPrefixScanner.ShortHeaderBytes;
            var end = pos + len;
            if (end > data.Length)
                Array.Resize(ref data, end + Page);
            records.Add((pos, len));
            // тело записи: ненулевой паттерн
            for (var i = pos + 24; i < end; i++)
                data[i] = (byte)(0x40 | (rng = rng * 5 + 1) % 0x3F);
            WriteLen(data, pos, len);
            WriteCrc(data, records[^1]);
            pos = (end + 7) & ~7; // MAXALIGN следующей записи
        }
        return data;
    }

    private static void WriteLen(byte[] data, int pos, int len)
    {
        data[pos] = (byte)len;
        data[pos + 1] = (byte)(len >> 8);
        data[pos + 2] = (byte)(len >> 16);
        data[pos + 3] = (byte)(len >> 24);
    }

    // xl_crc = CRC-32C образа записи: [pos..pos+20) + [pos+24..pos+len).
    private static void WriteCrc(byte[] data, (int Pos, int Len) r)
    {
        var crc = Crc32C.Init();
        crc = Crc32C.Update(crc, data, r.Pos, 20);
        crc = Crc32C.Update(crc, data, r.Pos + 24, r.Len - 24);
        var value = Crc32C.Finish(crc);
        data[r.Pos + 20] = (byte)value;
        data[r.Pos + 21] = (byte)(value >> 8);
        data[r.Pos + 22] = (byte)(value >> 16);
        data[r.Pos + 23] = (byte)(value >> 24);
    }

    [Fact]
    public void ValidRecords_FullPrefix()
    {
        // Arrange
        var data = SegmentWithRecords(64, 100);

        // Act
        var len = WalPrefixScanner.ValidatedPrefixLength(data, data.Length);

        // Assert — обе записи валидны: конец БАЙТОВ второй записи (первая [40..104), вторая [104..204))
        len.Should().Be(204);
    }

    [Fact]
    public void CorruptedBodyCrc_TruncatesToPreviousRecord()
    {
        // Arrange — портим тело ВТОРОЙ записи [104..204) (CRC перестанет сходиться)
        var data = SegmentWithRecords(64, 100);
        data[104 + 30] ^= 0xFF;

        // Act
        var len = WalPrefixScanner.ValidatedPrefixLength(data, data.Length);

        // Assert — валидна только первая запись: конец её байтов
        len.Should().Be(104);
    }

    [Fact]
    public void CorruptedCrcField_TruncatesToPreviousRecord()
    {
        // Arrange — портим сам xl_crc первой записи
        var data = SegmentWithRecords(64, 100);
        data[40 + 20] ^= 0xFF;

        // Act / Assert — ни одной CRC-валидной записи
        WalPrefixScanner.ValidatedPrefixLength(data, data.Length).Should().Be(0);
    }

    [Fact]
    public void RecordWithoutCrcField_StillValidByBoundaries_InValidPrefix()
    {
        // Arrange — ValidPrefixLength (границы) не проверяет CRC: обе записи целиком
        var data = SegmentWithRecords(64, 100);
        data[40 + 20] ^= 0xFF;

        // Act / Assert — старый сканер принимает обе (границы целы)
        WalPrefixScanner.ValidPrefixLength(data, data.Length).Should().Be(204);
    }

    [Fact]
    public void RecordSpanningPageBoundary_CrcSkipsPageHeader()
    {
        // Arrange — запись пересекает границу страницы: short-заголовок второй
        // страницы попадает внутрь байтов записи и не входит в CRC-образ
        const int pos = WalPrefixScanner.LongHeaderBytes;
        var len = Page - pos + 100; // логических байтов: 8152 до границы + 100 после
        var data = new byte[Page * 2];
        data[4] = 1; // tli=1 LE
        WriteSpanningRecord(data, pos, len);

        // Act
        var validated = WalPrefixScanner.ValidatedPrefixLength(data, data.Length);

        // Assert — физический конец записи: граница страницы + short-заголовок + 100
        validated.Should().Be(Page + WalPrefixScanner.ShortHeaderBytes + 100);
        WalPrefixScanner.ValidPrefixLength(data, data.Length).Should().Be(validated);
    }

    // Запись через границу страницы: логические байты пишутся в обход
    // short-заголовка следующей страницы; xl_crc — по логическому образу.
    private static void WriteSpanningRecord(byte[] data, int pos, int len)
    {
        for (var i = 0; i < 4; i++)
            data[Physical(pos, i)] = (byte)(len >> (8 * i));
        var rng = 7;
        for (var i = 24; i < len; i++)
            data[Physical(pos, i)] = (byte)(0x40 | (rng = rng * 5 + 1) % 0x3F);

        var crc = Crc32C.Init();
        crc = UpdateLogical(crc, data, pos, 0, 20, len);
        crc = UpdateLogical(crc, data, pos, 24, len, len);
        var value = Crc32C.Finish(crc);
        for (var i = 0; i < 4; i++)
            data[Physical(pos, 20 + i)] = (byte)(value >> (8 * i));
    }

    // CRC по логическому диапазону записи [from..to): физические байты идут
    // в обход short-заголовков страниц.
    private static uint UpdateLogical(uint crc, byte[] data, int pos, int from, int to, int totalLen)
    {
        long cur = pos;
        long consumed = 0;
        while (consumed < totalLen)
        {
            if (cur % Page == 0)
                cur += WalPrefixScanner.ShortHeaderBytes;
            var room = Page - cur % Page;
            var take = (int)Math.Min(room, totalLen - consumed);
            var a = Math.Max(consumed, (long)from);
            var b = Math.Min(consumed + take, (long)to);
            if (b > a)
                crc = Crc32C.Update(crc, data, (int)(cur + (a - consumed)), (int)(b - a));
            consumed += take;
            cur += take;
        }
        return crc;
    }

    // Физическая позиция логического байта записи (в обход short-заголовков).
    private static long Physical(int pos, long logical)
    {
        long cur = pos;
        long consumed = 0;
        while (true)
        {
            if (cur % Page == 0)
                cur += WalPrefixScanner.ShortHeaderBytes;
            var room = Page - cur % Page;
            if (logical < consumed + room)
                return cur + (logical - consumed);
            consumed += room;
            cur += room;
        }
    }

    [Fact]
    public void Crc32C_ReferenceVector()
    {
        // Arrange / Act / Assert — эталон RFC 3720: "123456789" → 0xE3069283
        Crc32C.HashData("123456789"u8.ToArray()).Should().Be(0xE3069283);
    }
}
