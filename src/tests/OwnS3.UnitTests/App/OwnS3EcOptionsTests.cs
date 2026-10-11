using OwnS3.Storage.Erasure;

namespace OwnS3.UnitTests.App;

// Тесты парсинга/валидаций EC-конфигурации (t40, план Task 14).
public class OwnS3EcOptionsTests
{
    [Fact]
    public void ParseDataDirs_ValidList_TrimsAndKeepsOrder()
    {
        // Arrange/Act
        var dirs = OwnS3.App.OwnS3EcOptions.ParseDataDirs(" /d1 , /d2 ,/d3 ");

        // Assert — порядок = канонический порядок дисков
        dirs.Should().Equal("/d1", "/d2", "/d3");
    }

    [Fact]
    public void ParseDataDirs_EmptyElement_Fails()
    {
        // Arrange/Act/Assert — пустой элемент в списке — отказ старта
        var act = () => OwnS3.App.OwnS3EcOptions.ParseDataDirs("/d1,,/d2");
        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void ParseDataDirs_Duplicate_Fails()
    {
        // Arrange/Act/Assert
        var act = () => OwnS3.App.OwnS3EcOptions.ParseDataDirs("/d1,/d2,/d1");
        act.Should().Throw<InvalidOperationException>().WithMessage("*дубликат*");
    }

    [Theory]
    [InlineData(4, null, 4)]
    [InlineData(12, null, 4)]
    [InlineData(32, null, 16)]
    public void BuildTopology_DefaultSetSize(int drives, int? setSize, int expected)
    {
        // Arrange/Act
        var topology = OwnS3.App.OwnS3EcOptions.BuildTopology(drives, setSize, parity: 1);

        // Assert
        topology.SetSize.Should().Be(expected);
        topology.DriveCount.Should().Be(drives);
        topology.SetCount.Should().Be(drives / expected);
    }

    [Fact]
    public void BuildTopology_NoDivisorDefault_Fails()
    {
        // Arrange/Act/Assert — 5 путей: нет делителя из 4/8/16 и setSize не задан
        var act = () => OwnS3.App.OwnS3EcOptions.BuildTopology(5, null, parity: 1);
        act.Should().Throw<InvalidOperationException>().WithMessage("*авто-дефолт*");
    }

    [Theory]
    [InlineData(4, 1, 2)]   // parity > setSize/2
    [InlineData(4, 4, 0)]   // parity < 1
    [InlineData(6, 4, 1)]   // N % setSize != 0
    [InlineData(4, 3, 1)]   // setSize вне 4–16 (меньше)
    [InlineData(18, 4, 1)]  // setSize вне 4–16 (больше)
    public void BuildTopology_OutOfRange_Fails(int drives, int? setSize, int parity)
    {
        // Arrange/Act/Assert — валидации канона 06 §1
        var act = () => OwnS3.App.OwnS3EcOptions.BuildTopology(drives, setSize, parity);
        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void BuildTopology_DrivesBelowTwo_Fails()
    {
        // Arrange/Act/Assert — N ≥ 2 обязателен
        var act = () => OwnS3.App.OwnS3EcOptions.BuildTopology(1, null, 1);
        act.Should().Throw<InvalidOperationException>().WithMessage("*меньше 2*");
    }
}
