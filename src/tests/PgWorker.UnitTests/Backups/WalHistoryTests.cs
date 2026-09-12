using FluentAssertions;
using PgWorker.Backups;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Парсер .history (arch/19 §3, t04): строки "parentTLI switchWALLSN [reason]";
// последняя запись — сам TLI файла (ответвление от parentTLI в switchWALLSN).
public class WalHistoryTests
{
    // AAA: валидный history — записи в порядке строк, LSN без изменений
    [Fact]
    public void Parse_Валидный_возвращаетЗаписи()
    {
        // Arrange — 00000003.history: линия 3 ответвилась от 2, 2 — от 1
        const string content = "1\t0/2000000\tunknown\n2\t16/37D3000\tno promotion\n";

        // Act
        var entries = WalHistory.Parse(content);

        // Assert
        entries.Should().NotBeNull().And.HaveCount(2);
        entries![0].Should().Be(new WalHistoryEntry(1, "0/2000000"));
        entries[1].Should().Be(new WalHistoryEntry(2, "16/37D3000"));
    }

    // AAA: строки-мусор (не 2 первых поля hex/LSN) пропускаются, не роняют разбор
    [Fact]
    public void Parse_МусорныеСтроки_Пропускаются()
    {
        // Arrange
        const string content = "garbage line\n\n1\t0/2000000\n";

        // Act
        var entries = WalHistory.Parse(content);

        // Assert
        entries.Should().ContainSingle().Which.Should().Be(new WalHistoryEntry(1, "0/2000000"));
    }

    // AAA: пустой/полностью битый файл — null (строгий разбор не состоялся)
    [Fact]
    public void Parse_ПустойИлиБитый_null()
    {
        // Arrange / Act / Assert
        WalHistory.Parse("").Should().BeNull();
        WalHistory.Parse("no records here").Should().BeNull();
    }

    // AAA: parentTLI hex (формат PG — hex без 0x), LSN строго X/Y
    [Fact]
    public void Parse_HexParentИПлохойLsn_СтрогаяВалидация()
    {
        // Arrange — parentTLI "00000001" (hex-вид, как пишет PG), битый LSN в другой строке
        const string content = "00000001\t0/2000000\n2\tNOT_AN_LSN\n";

        // Act
        var entries = WalHistory.Parse(content);

        // Assert — битая строка пропущена, hex-parent распознан
        entries.Should().ContainSingle().Which.ParentTli.Should().Be(1);
    }
}
