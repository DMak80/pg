using FluentAssertions;
using PgWorker.Backups;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Страничный парсер валидного префикса незакрытого WAL-сегмента (arch/19 §3):
// 8 КиБ страницы, long-заголовок 40 (первая) / short 24 (прочие), записи не
// пересекают границу страницы, нули-добивка и незавершённые записи отсекаются.
public class WalPrefixScannerTests
{
    // Локальный билдер (в классе теста): страница 8192; long-заголовок первой
    // страницы (tli LE u32 @4, pageaddr LE u64 @8 = 0 — сканеру достаточно полей);
    // записи: [зарезерв 4 байта нулей][totalLen LE u32 @4][полезные байты до totalLen].
    private static (byte[] Buffer, int Count) Page(params int[] recordLengths)
    {
        var buffer = new byte[WalPrefixScanner.PageBytes];
        // long-заголовок: tli=1 @4 (ненулевой — валиден), pageaddr @8 = 0
        buffer[4] = 1;
        var pos = WalPrefixScanner.LongHeaderBytes;
        foreach (var len in recordLengths)
        {
            WriteRecord(buffer, pos, len);
            pos += len;
        }
        return (buffer, pos);
    }

    // Запись на позиции pos: маркер 4 байта, totalLen LE u32 @4, полезный байт @8.
    private static void WriteRecord(byte[] buffer, int pos, int totalLen)
    {
        var l = (uint)totalLen;
        buffer[pos + 4] = (byte)l;
        buffer[pos + 5] = (byte)(l >> 8);
        buffer[pos + 6] = (byte)(l >> 16);
        buffer[pos + 7] = (byte)(l >> 24);
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

        // Assert
        prefix.Should().Be(190);
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
    public void RecordNotFittingPage_MovedToNext()
    {
        // Arrange — запись длиннее остатка страницы (8153 > 8152) — стоп;
        // вторая страница: short-заголовок 24 + запись 40 — читается.
        var notFitting = new byte[2 * WalPrefixScanner.PageBytes];
        notFitting[4] = 1;
        WriteRecord(notFitting, WalPrefixScanner.LongHeaderBytes,
            WalPrefixScanner.PageBytes - WalPrefixScanner.LongHeaderBytes + 1);

        var nextPage = new byte[2 * WalPrefixScanner.PageBytes];
        nextPage[4] = 1;
        WriteRecord(nextPage, WalPrefixScanner.LongHeaderBytes,
            WalPrefixScanner.PageBytes - WalPrefixScanner.LongHeaderBytes);
        WriteRecord(nextPage,
            WalPrefixScanner.PageBytes + WalPrefixScanner.ShortHeaderBytes, 40);

        // Act
        var prefixNotFitting = WalPrefixScanner.ValidPrefixLength(notFitting, 8193);
        var prefixNextPage = WalPrefixScanner.ValidPrefixLength(
            nextPage, WalPrefixScanner.PageBytes + WalPrefixScanner.ShortHeaderBytes + 40);

        // Assert
        prefixNotFitting.Should().Be(0);
        prefixNextPage.Should().Be(
            WalPrefixScanner.PageBytes + WalPrefixScanner.ShortHeaderBytes + 40);
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
}
