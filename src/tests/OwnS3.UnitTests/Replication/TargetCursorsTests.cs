using OwnS3.Replication.Cursors;

namespace OwnS3.UnitTests.Replication;

// Watermark-курсоры приёма (t35, глава 07 §5): независимость соседей, персист
// Flush'ом, монотонность (назад не двигается), дебаунс — ручной Flush (без
// FakeTimeProvider-пакета: TimeProvider инжектится, тесты персиста — ручной Flush).
public sealed class TargetCursorsFixture : IDisposable
{
    public string Dir { get; } = Path.Combine(Path.GetTempPath(), "owns3-test-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        // Teardown: temp-каталог удаляется при любом исходе
        if (Directory.Exists(Dir))
            Directory.Delete(Dir, recursive: true);
    }
}

public class TargetCursorsTests : IClassFixture<TargetCursorsFixture>
{
    private readonly TargetCursorsFixture _fixture = new();

    public TargetCursorsTests()
    {
        // Изоляция кейсов: temp-каталог на кейс
        if (Directory.Exists(_fixture.Dir))
            Directory.Delete(_fixture.Dir, recursive: true);
    }

    private string Dir => _fixture.Dir;

    [Fact]
    public void Advance_TwoTargets_Independent()
    {
        // Arrange — курсоры на temp-каталоге
        using var cursors = new TargetCursors(Dir, TimeProvider.System);

        // Act — advance двух соседей независимо
        cursors.Advance("peer-a", 5);
        cursors.Advance("peer-b", 9);

        // Assert — watermark каждого свой
        cursors.GetAccepted("peer-a").Should().Be(5);
        cursors.GetAccepted("peer-b").Should().Be(9);
    }

    [Fact]
    public void Flush_PersistsForNewInstance()
    {
        // Arrange — advance + Flush (синхронный персист)
        using (var cursors = new TargetCursors(Dir, TimeProvider.System))
        {
            cursors.Advance("peer-a", 42);
            cursors.SetMode("peer-a", "reconciling");
            cursors.Flush();
        }

        // Act — новый экземпляр того же каталога (рестарт источника)
        using var reloaded = new TargetCursors(Dir, TimeProvider.System);

        // Assert — позиция приёма пережила рестарт (глава 07 §5)
        reloaded.GetAccepted("peer-a").Should().Be(42);
    }

    [Fact]
    public void GetAbsent_ReturnsMinusOne()
    {
        // Arrange — курсор без файлов
        using var cursors = new TargetCursors(Dir, TimeProvider.System);

        // Act / Assert — файла нет → -1 (новый сосед → notifier с конца журнала)
        cursors.GetAccepted("unknown").Should().Be(-1);
    }

    [Fact]
    public void Advance_Backwards_Ignored()
    {
        // Arrange — watermark 10
        using var cursors = new TargetCursors(Dir, TimeProvider.System);
        cursors.Advance("peer-a", 10);

        // Act — попытка назад
        cursors.Advance("peer-a", 4);

        // Assert — монотонность: назад не двигается
        cursors.GetAccepted("peer-a").Should().Be(10);
    }

    [Fact]
    public void Flush_DebouncedTail_NotVisibleUntilFlush()
    {
        // Arrange — advance без Flush (грязный хвост)
        using var cursors = new TargetCursors(Dir, TimeProvider.System);
        cursors.Advance("peer-a", 7);

        // Act — другой экземпляр до персиста
        using var early = new TargetCursors(Dir, TimeProvider.System);
        var before = early.GetAccepted("peer-a");

        // Act — после Flush
        cursors.Flush();
        using var after = new TargetCursors(Dir, TimeProvider.System);

        // Assert — до Flush -1 (дебаунс не прошёл), после — 7
        before.Should().Be(-1);
        after.GetAccepted("peer-a").Should().Be(7);
    }

    [Fact]
    public void Dispose_PersistsTail_Graceful()
    {
        // Arrange
        using (var cursors = new TargetCursors(Dir, TimeProvider.System))
        {
            // Act — advance и Dispose (graceful-shutdown) без явного Flush
            cursors.Advance("peer-a", 11);
        }

        // Assert — graceful-персист хвоста (глава 07 §5)
        using var reloaded = new TargetCursors(Dir, TimeProvider.System);
        reloaded.GetAccepted("peer-a").Should().Be(11);
    }
}
