using PgWorker.Core.Model;

namespace PgWorker.UnitTests.Provisioning;

// Парсер заявок ресурсов request_{cpu,mem} (rework №5): панель пишет
// инвариант-десятичные ядра («2»/«0.5») и память с суффиксами («8Gi», «4G»);
// нечитаемое — толерантный null (заявка — не контракт, кластер обязан
// подняться и без лимита).
public class NodeResourcesTests
{
    [Theory]
    [InlineData("2", 2.0)]
    [InlineData("0.5", 0.5)]
    [InlineData(" 1,25 ", 1.25)] // запятая → инвариантная точка
    [InlineData("4", 4.0)]
    public void ParseCpu_InvariantDecimal_Cores(string raw, double expected)
    {
        // Arrange + Act
        var cores = NodeResourcesParser.ParseCpu(raw);

        // Assert — число ядер как есть (лимит задаёт движок: cores*1e9 наносек)
        cores.Should().Be(expected);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("abc")]
    [InlineData("-1")]
    [InlineData("0")]
    public void ParseCpu_UnreadableOrNonPositive_Null(string? raw)
    {
        // Arrange + Act + Assert — без лимита, а не ошибка provisioning
        NodeResourcesParser.ParseCpu(raw).Should().BeNull();
    }

    [Theory]
    [InlineData("8Gi", 8L << 30)]
    [InlineData("8GiB", 8L << 30)]
    [InlineData("4G", 4_000_000_000L)]
    [InlineData("512Mi", 512L << 20)]
    [InlineData("1024", 1024L)]
    [InlineData("1.5Gi", (long)(1.5 * (1L << 30)))]
    [InlineData("16 GB ", 16_000_000_000L)]
    public void ParseMem_Suffixes_Bytes(string raw, long expected)
    {
        // Arrange + Act
        var bytes = NodeResourcesParser.ParseMem(raw);

        // Assert — двоичные суффиксы (Ki/Mi/Gi) — 2^10.., десятичные (K/M/G) — 10^3..
        bytes.Should().Be(expected);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("Gi")]
    [InlineData("8gg")]
    [InlineData("memory")]
    [InlineData("0G")]
    [InlineData("-5Gi")]
    public void ParseMem_Unreadable_Null(string? raw)
    {
        // Arrange + Act + Assert — без лимита, а не ошибка provisioning
        NodeResourcesParser.ParseMem(raw).Should().BeNull();
    }

    [Fact]
    public void Parse_BothKeys_NodeResourcesRecord()
    {
        // Arrange — значения обоих ключей /service/<scope>/request_*
        // Act
        var resources = NodeResourcesParser.Parse("0.5", "8Gi");

        // Assert — готовая заявка для драйвера
        resources.Should().Be(new NodeResources(0.5, 8L << 30));
    }

    [Fact]
    public void Parse_NoReadableValues_Null()
    {
        // Arrange — ключей нет (или значения мусорные)
        // Act + Assert — заявки нет: null, а не пустой record
        NodeResourcesParser.Parse(null, null).Should().BeNull();
        NodeResourcesParser.Parse("abc", "xyz").Should().BeNull();
    }
}
