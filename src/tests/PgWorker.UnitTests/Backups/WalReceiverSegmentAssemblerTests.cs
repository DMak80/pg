using FluentAssertions;
using PgWorker.WalReceiver;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Математика сегментов от LSN (t27, arch/19 §3): границы/имена 1:1 с WalFileName,
// отбрасывание неполного головного сегмента, TLI из long page header, разрыв LSN —
// внутренняя ошибка (протокол физической репликации дыр не даёт).
public class WalReceiverSegmentAssemblerTests
{
    private const int SegmentBytes = 16 * 1024 * 1024; // 16 MiB (WalFileName.SegmentBytes)
    private const int Mib = 1024 * 1024;

    /// <summary>Байты с синтетическим long page header: tli (LE u32 @4) и
    /// xlp_pageaddr (LE u64 @8) == boundary — ровно то, что читает ассемблер.</summary>
    private static byte[] SegmentBytesWithHeader(uint tli, ulong boundary, int length)
    {
        var data = new byte[length];
        if (length >= 16)
        {
            data[4] = (byte)tli;
            data[5] = (byte)(tli >> 8);
            data[6] = (byte)(tli >> 16);
            data[7] = (byte)(tli >> 24);
            for (var i = 0; i < 8; i++)
                data[8 + i] = (byte)(boundary >> (8 * i));
        }

        return data;
    }

    [Fact]
    public void Один_сегмент_по_кускам_закрывается_последним_чанком()
    {
        // Arrange — 4 чанка по 4 MiB от границы LSN 0; header первого сегмента tli=1
        var assembler = new SegmentAssembler(0);
        var closed = new List<ClosedSegment>();

        // Act
        for (var i = 0; i < 4; i++)
        {
            var data = i == 0
                ? SegmentBytesWithHeader(1, 0, 4 * Mib)
                : new byte[4 * Mib];
            var chunkClosed = assembler.Append(new XLogChunk((ulong)(i * 4 * Mib), data));
            // Assert — до последнего чанка ничего не закрыто
            if (i < 3)
                chunkClosed.Should().BeEmpty($"после {i + 1}-го из 4 чанков сегмент не закрыт");
            closed.AddRange(chunkClosed);
        }

        // Assert — ровно один закрытый сегмент, имя сверено с WalFileName, EndLsn
        closed.Should().HaveCount(1);
        closed[0].Tli.Should().Be(1);
        var expected = PgWorker.Backups.WalFileName.FromLsn(1, "0/0");
        closed[0].Name.Should().Be(expected.Name);
        closed[0].Name.Should().Be("000000010000000000000000");
        closed[0].EndLsn.Should().Be((ulong)SegmentBytes);
        closed[0].Data.Length.Should().Be(SegmentBytes);
        assembler.LastAppendedLsn.Should().Be((ulong)SegmentBytes);
    }

    [Fact]
    public void Чанк_пересекает_границу_сплит_и_добивка()
    {
        // Arrange — ассемблер от 0; один чанк 20 MiB
        var assembler = new SegmentAssembler(0);
        var big = new byte[20 * Mib];
        SegmentBytesWithHeader(1, 0, 16).AsSpan().CopyTo(big.AsSpan());

        // Act
        var first = assembler.Append(new XLogChunk(0, big));

        // Assert — 1 закрытый (16 MiB) + буфер 4 MiB (LastAppended = 20 MiB)
        first.Should().HaveCount(1);
        first[0].EndLsn.Should().Be((ulong)SegmentBytes);
        assembler.LastAppendedLsn.Should().Be((ulong)(20 * Mib));

        // Arrange — добивающий чанк 12 MiB
        // Act — второй закрытый
        var second = assembler.Append(new XLogChunk((ulong)(20 * Mib), new byte[12 * Mib]));

        // Assert
        second.Should().HaveCount(1);
        second[0].Name.Should().Be("000000010000000000000001");
        second[0].EndLsn.Should().Be((ulong)(2 * SegmentBytes));
        assembler.LastAppendedLsn.Should().Be((ulong)(32 * Mib));
    }

    [Fact]
    public void Старт_с_середины_сегмента_головной_отброшен_имя_от_границы()
    {
        // Arrange — startLsn = 5 MiB (середина сегмента [0, 16 MiB)); до ближайшей
        // границы 16 MiB — 11 MiB отбрасывания
        var assembler = new SegmentAssembler(5UL * Mib);

        // Act — чанк 5 MiB: целиком до границы → отброшен
        var afterDiscard = assembler.Append(new XLogChunk(5UL * Mib, new byte[5 * Mib]));

        // Assert
        afterDiscard.Should().BeEmpty();
        assembler.LastAppendedLsn.Should().Be((ulong)(10 * Mib));

        // Act — чанк 22 MiB: 6 MiB до добора (отброс до 16 MiB) + 16 MiB полного сегмента
        var chunk = new byte[22 * Mib];
        SegmentBytesWithHeader(1, (ulong)SegmentBytes, 16).AsSpan().CopyTo(chunk.AsSpan(6 * Mib));
        var closed = assembler.Append(new XLogChunk(10UL * Mib, chunk));

        // Assert — имя СЛЕДУЮЩЕГО полного сегмента — от границы 16 MiB (seg 1), не от startLsn
        closed.Should().HaveCount(1);
        closed[0].Name.Should().Be("000000010000000000000001");
        closed[0].EndLsn.Should().Be((ulong)(2 * SegmentBytes));
        closed[0].Tli.Should().Be(1);
        assembler.LastAppendedLsn.Should().Be((ulong)(32 * Mib));
    }

    [Fact]
    public void TLI_из_заголовка_и_наследование_следующим_сегментом()
    {
        // Arrange — сегмент с header tli=2 на границе 0
        var assembler = new SegmentAssembler(0);
        var tli2 = SegmentBytesWithHeader(2, 0, SegmentBytes);

        // Act / Assert — TLI прочитан из header
        var first = assembler.Append(new XLogChunk(0, tli2));
        first.Should().HaveCount(1);
        first[0].Tli.Should().Be(2);
        first[0].Name.Should().Be("000000020000000000000000");

        // Act — следующий сегмент без валидного header (pageaddr ≠ границе) — наследует tli
        var noHeader = new byte[SegmentBytes];
        var second = assembler.Append(new XLogChunk((ulong)SegmentBytes, noHeader));

        // Assert
        second.Should().HaveCount(1);
        second[0].Tli.Should().Be(2);
        second[0].Name.Should().Be("000000020000000000000001");
    }

    [Fact]
    public void Разрыв_WalStart_внутренняя_ошибка()
    {
        // Arrange — ассемблер от 0, первый чанк принят
        var assembler = new SegmentAssembler(0);
        assembler.Append(new XLogChunk(0, SegmentBytesWithHeader(1, 0, 4 * Mib)));

        // Act — чанк с WalStart, не равным LastAppendedLsn
        var act = () => assembler.Append(new XLogChunk(99UL * Mib, new byte[Mib]));

        // Assert
        act.Should().Throw<ApplicationException>();
    }

    [Fact]
    public void Математика_имён_сверена_с_WalFileName_на_переходе_log()
    {
        // Arrange — граница сегмента seg=0xFF: LSN 0/FF000000; следующий сегмент —
        // log+1, seg 0 (0x100000000). startLsn ровно на границе.
        var boundary = 0xFF000000UL;
        var assembler = new SegmentAssembler(boundary);

        // Act — один полный сегмент с валидным header
        var closed = assembler.Append(
            new XLogChunk(boundary, SegmentBytesWithHeader(1, boundary, SegmentBytes)));

        // Assert — имя сверено с FromLsn-математикой
        closed.Should().HaveCount(1);
        closed[0].Name.Should().Be("0000000100000000000000ff");
        PgWorker.Backups.WalFileName.FromLsn(1, "0/FF000000").Name.Should().Be(closed[0].Name);
    }
}
