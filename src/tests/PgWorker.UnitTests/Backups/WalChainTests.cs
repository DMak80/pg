using FluentAssertions;
using PgWorker.Backups;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Gap-детектор непрерывности WAL-цепочки (arch/19 §3): последовательность Next()
// без пропусков; TLI-переход валиден при history + первом сегменте нового TLI
// ∈ {последний старого, Next(последний)}; .partial игнорируется.
public class WalChainTests
{
    private static WalFileName Seg(uint tli, uint log, uint seg) => new(tli, log, seg);

    [Fact]
    public void Сплошная_цепочка_непрерывна()
    {
        // Arrange
        var objects = new[]
        {
            "000000010000000000000001", "000000010000000000000002", "000000010000000000000003",
        };

        // Act
        var result = WalChain.Check(Seg(1, 0, 1), objects);

        // Assert
        result.IsContinuous.Should().BeTrue();
        result.GapError.Should().BeNull();
        result.LastSegment!.Value.Name.Should().Be("000000010000000000000003");
    }

    [Fact]
    public void Дыра_внутри_цепочки_дает_разрыв_с_границами()
    {
        // Arrange — нет 000000010000000000000002
        var objects = new[] { "000000010000000000000001", "000000010000000000000003" };

        // Act
        var result = WalChain.Check(Seg(1, 0, 1), objects);

        // Assert
        result.IsContinuous.Should().BeFalse();
        result.GapError.Should().Contain("000000010000000000000002")
            .And.Contain("000000010000000000000003");
    }

    [Fact]
    public void Первый_объект_выше_старта_дает_дыру_от_стартовой_точки()
    {
        // Arrange — цепочка начинается с chainStart, а первый объект позже
        var objects = new[] { "000000010000000000000005" };

        // Act
        var result = WalChain.Check(Seg(1, 0, 1), objects);

        // Assert
        result.IsContinuous.Should().BeFalse();
        result.GapError.Should().Contain("000000010000000000000001");
    }

    [Fact]
    public void Сегменты_раньше_старта_игнорируются()
    {
        // Arrange — хвост до chain_start (ретенция/сдвиг старта полным бэкапом)
        var objects = new[]
        {
            "000000010000000000000001", "000000010000000000000005", "000000010000000000000006",
        };

        // Act
        var result = WalChain.Check(Seg(1, 0, 5), objects);

        // Assert
        result.IsContinuous.Should().BeTrue();
    }

    [Fact]
    public void Partial_сегменты_игнорируются()
    {
        // Arrange
        var objects = new[]
        {
            "000000010000000000000001", "000000010000000000000002.partial",
            "000000010000000000000002",
        };

        // Act
        var result = WalChain.Check(Seg(1, 0, 1), objects);

        // Assert
        result.IsContinuous.Should().BeTrue();
    }

    [Fact]
    public void TLI_переход_с_history_и_повтором_последнего_сегмента_валиден()
    {
        // Arrange — failover в середине сегмента: PG перезаписывает сегмент с нового TLI
        var objects = new[]
        {
            "000000010000000000000001", "000000010000000000000002",
            "00000002.history",
            "000000020000000000000002", // == последнему старого TLI
        };

        // Act
        var result = WalChain.Check(Seg(1, 0, 1), objects);

        // Assert
        result.IsContinuous.Should().BeTrue();
        result.LastSegment!.Value.Tli.Should().Be(2);
    }

    [Fact]
    public void TLI_переход_с_history_и_следующим_сегментом_валиден()
    {
        // Arrange — переключение на границе сегмента: первый новый = Next(последнего)
        var objects = new[]
        {
            "000000010000000000000001",
            "00000002.history",
            "000000020000000000000002",
        };

        // Act
        var result = WalChain.Check(Seg(1, 0, 1), objects);

        // Assert
        result.IsContinuous.Should().BeTrue();
    }

    [Fact]
    public void TLI_переход_без_history_разрыв()
    {
        // Arrange
        var objects = new[]
        {
            "000000010000000000000001", "000000020000000000000002",
        };

        // Act
        var result = WalChain.Check(Seg(1, 0, 1), objects);

        // Assert
        result.IsContinuous.Should().BeFalse();
        result.GapError.Should().Contain("00000002.history");
    }

    [Fact]
    public void TLI_переход_со_скачком_мимо_точки_переключения_разрыв()
    {
        // Arrange — первый сегмент нового TLI ≠ последнему/next (skip позиции)
        var objects = new[]
        {
            "000000010000000000000001", "00000002.history", "000000020000000000000005",
        };

        // Act
        var result = WalChain.Check(Seg(1, 0, 1), objects);

        // Assert
        result.IsContinuous.Should().BeFalse();
    }

    [Fact]
    public void History_без_перехода_не_влияет()
    {
        // Arrange — лишний history (файл от старого failover) — не ошибка
        var objects = new[] { "00000002.history", "000000010000000000000001", "000000010000000000000002" };

        // Act
        var result = WalChain.Check(Seg(1, 0, 1), objects);

        // Assert
        result.IsContinuous.Should().BeTrue();
    }

    [Fact]
    public void Дыра_после_перехода_на_новый_TLI_ловится()
    {
        // Arrange
        var objects = new[]
        {
            "000000010000000000000001", "00000002.history", "000000020000000000000002",
            "000000020000000000000004", // пропущен 3-й
        };

        // Act
        var result = WalChain.Check(Seg(1, 0, 1), objects);

        // Assert
        result.IsContinuous.Should().BeFalse();
        result.GapError.Should().Contain("000000020000000000000003");
    }
}
