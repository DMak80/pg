using FluentAssertions;
using PgWorker.Backups;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Страничный парсер валидного префикса незакрытого WAL-сегмента (arch/19 §3):
// 8 КиБ страницы, long-заголовок 40 (первая) / short 24 (прочие), запись может
// начинаться в хвосте страницы (влезает xl_tot_len), заголовок продолжается на
// следующей; нули-добивка и незавершённые записи отсекаются; сегмент может
// начинаться продолжением записи из предыдущего (XLP_FIRST_IS_CONTRECORD).
public class WalPrefixScannerTests
{
    // Локальный билдер (в классе теста): страница 8192; long-заголовок первой
    // страницы (tli LE u32 @4, pageaddr LE u64 @8 = 0 — сканеру достаточно полей);
    // записи: [xl_tot_len LE u32 @0][xl_xid @4][полезные байты до totalLen],
    // следующая запись — с выравниванием длины по 8 (MAXALIGN, как в WAL).
    private static (byte[] Buffer, int Count) Page(params int[] recordLengths)
    {
        var buffer = new byte[WalPrefixScanner.PageBytes];
        // long-заголовок: tli=1 @4 (ненулевой — валиден), pageaddr @8 = 0
        buffer[4] = 1;
        var pos = WalPrefixScanner.LongHeaderBytes;
        foreach (var len in recordLengths)
        {
            WriteRecord(buffer, pos, len);
            pos += (len + 7) & ~7; // MAXALIGN
        }
        return (buffer, pos);
    }

    // Запись на позиции pos: xl_tot_len LE u32 @0 (поле первое в XLogRecord),
    // xl_xid @4 (отличен от длины — регресс-гвард), полезный байт @8.
    private static void WriteRecord(byte[] buffer, int pos, int totalLen)
    {
        var l = (uint)totalLen;
        buffer[pos] = (byte)l;
        buffer[pos + 1] = (byte)(l >> 8);
        buffer[pos + 2] = (byte)(l >> 16);
        buffer[pos + 3] = (byte)(l >> 24);
        buffer[pos + 4] = 0xB3;
        buffer[pos + 5] = 0x03;
        buffer[pos + 8] = 0xAB; // полезный байт записи (отличен от нуля)
    }

    [Fact]
    public void EmptyBuffer_Zero()
    {
        // Arrange
        var buffer = new byte[10];

        // Act
        var prefix = WalPrefixScanner.ValidPrefixLength(buffer, 10);

        // Assert
        prefix.Should().Be(0);
    }

    [Fact]
    public void OnlyLongHeader_Zero()
    {
        // Arrange — ровно long-заголовок, записей нет
        var buffer = new byte[WalPrefixScanner.PageBytes];

        // Act
        var prefix = WalPrefixScanner.ValidPrefixLength(buffer, WalPrefixScanner.LongHeaderBytes);

        // Assert
        prefix.Should().Be(0);
    }

    [Fact]
    public void Empty_AfterLongHeader_Zero()
    {
        // Arrange — long-заголовок + нули до конца накопленного буфера (Review Focus 1)
        var buffer = new byte[WalPrefixScanner.PageBytes];
        buffer[4] = 1;

        // Act
        var prefix = WalPrefixScanner.ValidPrefixLength(buffer, 200);

        // Assert
        prefix.Should().Be(0);
    }

    [Fact]
    public void SingleRecord_PrefixToEnd()
    {
        // Arrange
        var (buffer, _) = Page(100);
        const int count = 40 + 100;

        // Act
        var prefix = WalPrefixScanner.ValidPrefixLength(buffer, count);

        // Assert
        prefix.Should().Be(140);
    }

    [Fact]
    public void TwoRecords_PrefixToSecondEnd()
    {
        // Arrange
        var (buffer, count) = Page(100, 50);

        // Act
        var prefix = WalPrefixScanner.ValidPrefixLength(buffer, count);

        // Assert — конец второй записи (144 + 50), паддинг выравнивания не входит
        prefix.Should().Be(194);
    }

    [Fact]
    public void TrailingZeros_Cut()
    {
        // Arrange — запись + нули-добивка до конца страницы
        var (buffer, _) = Page(100);

        // Act
        var prefix = WalPrefixScanner.ValidPrefixLength(buffer, WalPrefixScanner.PageBytes);

        // Assert
        prefix.Should().Be(140);
    }

    [Fact]
    public void UnfinishedRecord_CutToPrevious()
    {
        // Arrange — вторая запись начата, но принята только часть заголовка
        var (buffer, _) = Page(100, 50);

        // Act
        var prefix = WalPrefixScanner.ValidPrefixLength(buffer, 40 + 100 + 10);

        // Assert
        prefix.Should().Be(140);
    }

    [Fact]
    public void ZeroTotLen_Cut()
    {
        // Arrange — после полной записи идут нули (totalLen=0), count захватывает их
        var (buffer, _) = Page(100);

        // Act
        var prefix = WalPrefixScanner.ValidPrefixLength(buffer, 200);

        // Assert
        prefix.Should().Be(140);
    }

    [Fact]
    public void RecordSpanningPage_Continuation()
    {
        // Arrange — запись 8153 байта не влезает в остаток страницы (8152):
        // начало 8152 байта + продолжение 1 байт после short-заголовка страницы 2;
        // затем MAXALIGN-добивка 7 и следующая запись 40.
        var buffer = new byte[2 * WalPrefixScanner.PageBytes];
        buffer[4] = 1;
        WriteRecord(buffer, WalPrefixScanner.LongHeaderBytes, 8153);
        var continuation = WalPrefixScanner.PageBytes + WalPrefixScanner.ShortHeaderBytes;
        var secondRecord = continuation + 1 + 7; // +1 байт продолжения, +7 добивки
        WriteRecord(buffer, secondRecord, 40);
        var fullCount = secondRecord + 40;

        // Act — запись целиком; запись продолжена, но следующая ещё не начата
        var prefixFull = WalPrefixScanner.ValidPrefixLength(buffer, fullCount);
        var prefixRecordOnly = WalPrefixScanner.ValidPrefixLength(buffer, continuation + 1);

        // Assert — конец второй записи / конец записи-продолжения
        prefixFull.Should().Be(fullCount);
        prefixRecordOnly.Should().Be(continuation + 1);
    }

    [Fact]
    public void RecordFillsPage_NextPageRead()
    {
        // Arrange — запись ровно до границы страницы, вторая — на следующей
        var buffer = new byte[2 * WalPrefixScanner.PageBytes];
        buffer[4] = 1;
        WriteRecord(buffer, WalPrefixScanner.LongHeaderBytes,
            WalPrefixScanner.PageBytes - WalPrefixScanner.LongHeaderBytes);
        WriteRecord(buffer, WalPrefixScanner.PageBytes + WalPrefixScanner.ShortHeaderBytes, 40);

        // Act
        var prefix = WalPrefixScanner.ValidPrefixLength(
            buffer, WalPrefixScanner.PageBytes + WalPrefixScanner.ShortHeaderBytes + 40);

        // Assert
        prefix.Should().Be(WalPrefixScanner.PageBytes + WalPrefixScanner.ShortHeaderBytes + 40);
    }

    [Fact]
    public void SecondPageHeaderOnly_CutAtPageEnd()
    {
        // Arrange — первая страница заполнена записью ровно до границы,
        // вторая накоплена только по short-заголовок
        var buffer = new byte[2 * WalPrefixScanner.PageBytes];
        buffer[4] = 1;
        WriteRecord(buffer, WalPrefixScanner.LongHeaderBytes,
            WalPrefixScanner.PageBytes - WalPrefixScanner.LongHeaderBytes);

        // Act
        var prefix = WalPrefixScanner.ValidPrefixLength(
            buffer, WalPrefixScanner.PageBytes + WalPrefixScanner.ShortHeaderBytes);

        // Assert
        prefix.Should().Be(WalPrefixScanner.PageBytes);
    }

    [Fact]
    public void RecordLengthFromOffsetZero_NotXid()
    {
        // Arrange — реальная раскладка XLogRecord: xl_tot_len=46 @0, xl_xid=947 @4.
        // Чтение длины со смещения 4 (xl_xid) дало бы 947 и префикс 987 —
        // обрезка посреди следующей записи (факт E2E: CRC-ошибка восстановления).
        var buffer = new byte[WalPrefixScanner.PageBytes];
        buffer[4] = 1;
        WriteRecord(buffer, WalPrefixScanner.LongHeaderBytes, 46);
        buffer[WalPrefixScanner.LongHeaderBytes + 4] = 947 & 0xFF;
        buffer[WalPrefixScanner.LongHeaderBytes + 5] = (947 >> 8) & 0xFF;
        const int count = 40 + 46;

        // Act
        var prefix = WalPrefixScanner.ValidPrefixLength(buffer, count);

        // Assert — конец единственной полной записи (40 + 46), не 987
        prefix.Should().Be(86);
    }

    [Theory]
    [InlineData(8)]
    [InlineData(16)]
    public void RecordStartsInPageTail_HeaderContinuesNextPage(int tailFree)
    {
        // Arrange — запись A заполняет страницу до (8192 − tailFree); запись B
        // НАЧИНАЕТСЯ в хвосте: xl_tot_len помещается, остаток заголовка
        // продолжается после short-заголовка следующей страницы; затем запись C.
        // PG гарантирует в хвосте страницы место под xl_tot_len (4 байта).
        var buffer = new byte[2 * WalPrefixScanner.PageBytes];
        buffer[4] = 1;
        WriteRecord(buffer, WalPrefixScanner.LongHeaderBytes,
            WalPrefixScanner.PageBytes - WalPrefixScanner.LongHeaderBytes - tailFree);
        var recordBStart = WalPrefixScanner.PageBytes - tailFree;
        WriteRecord(buffer, recordBStart, 100);
        var continuation = WalPrefixScanner.PageBytes + WalPrefixScanner.ShortHeaderBytes;
        var recordBEnd = continuation + (100 - tailFree);
        var recordCStart = (recordBEnd + 7) & ~7; // MAXALIGN
        WriteRecord(buffer, recordCStart, 40);
        var count = recordCStart + 40;

        // Act
        var prefix = WalPrefixScanner.ValidPrefixLength(buffer, count);

        // Assert — конец последней полной записи (C): разделённый заголовок B
        // прочитан верно, хвост страницы не принят за добивку
        prefix.Should().Be(count);
    }

    [Fact]
    public void SegmentStartsWithContinuation_SkipsRemainder()
    {
        // Arrange — первая страница сегмента: long-заголовок,
        // XLP_FIRST_IS_CONTRECORD @2, xlp_rem_len=60 @16: продолжение записи,
        // начатой в предыдущем сегменте; далее — обычная полная запись.
        var buffer = new byte[2 * WalPrefixScanner.PageBytes];
        buffer[2] = 0x01;               // xlp_info = XLP_FIRST_IS_CONTRECORD
        buffer[16] = 60;                // xlp_rem_len (LE u32)
        var continuationEnd = WalPrefixScanner.LongHeaderBytes + 60;
        var nextRecord = (continuationEnd + 7) & ~7; // MAXALIGN
        WriteRecord(buffer, nextRecord, 40);
        var count = nextRecord + 40;

        // Act
        var prefix = WalPrefixScanner.ValidPrefixLength(buffer, count);
        var incomplete = WalPrefixScanner.ValidPrefixLength(buffer, continuationEnd - 1);

        // Assert — префикс до конца полной записи после продолжения; пока
        // продолжение не принято целиком, валидной границы нет
        prefix.Should().Be(count);
        incomplete.Should().Be(0);
    }
}
