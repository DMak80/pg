using OwnS3.Replication;

namespace OwnS3.UnitTests.Replication;

// Маркер чистого завершения (t35, глава 07 §7): отсутствие/битость → грязный старт,
// MarkCleanAndPersist переживает рестарт, MarkDirty.
public sealed class ReplicationStateFixture : IDisposable
{
    public string Dir { get; } = Path.Combine(Path.GetTempPath(), "owns3-test-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        // Teardown: temp-каталог удаляется при любом исходе
        if (Directory.Exists(Dir))
            Directory.Delete(Dir, recursive: true);
    }
}

public class ReplicationStateTests : IClassFixture<ReplicationStateFixture>
{
    private readonly ReplicationStateFixture _fixture = new();

    private string StatePath => Path.Combine(_fixture.Dir, "state.json");

    [Fact]
    public void MissingFile_DirtyStart()
    {
        // Arrange — state.json нет (пустой том / первый старт)
        var state = new ReplicationState(StatePath);

        // Assert — WasCleanShutdown == false → стартовый reconcile (глава 07 §9)
        state.WasCleanShutdown().Should().BeFalse();
    }

    [Fact]
    public void MarkCleanAndPersist_SurvivesNewInstance()
    {
        // Arrange — graceful stop: drain → fsync-точка → clean
        var state = new ReplicationState(StatePath);
        state.MarkDirty();

        // Act
        state.MarkCleanAndPersist();

        // Assert — новый экземпляр (рестарт) видит чистое завершение
        var reloaded = new ReplicationState(StatePath);
        reloaded.WasCleanShutdown().Should().BeTrue();
    }

    [Fact]
    public void MarkDirty_StartsFresh()
    {
        // Arrange — чистый маркер от прошлого запуска
        var state = new ReplicationState(StatePath);
        state.MarkCleanAndPersist();

        // Act — новый старт сбрасывает маркер ДО готовности
        var restarted = new ReplicationState(StatePath);
        restarted.MarkDirty();

        // Assert — маркер грязный (крах до graceful-stop даст стартовый reconcile)
        var check = new ReplicationState(StatePath);
        check.WasCleanShutdown().Should().BeFalse();
    }

    [Fact]
    public void CorruptedFile_DirtyStart()
    {
        // Arrange — битый state.json
        Directory.CreateDirectory(_fixture.Dir);
        File.WriteAllText(StatePath, "{not-json");

        // Act / Assert — битый маркер = грязный старт (глава 07 §7)
        var state = new ReplicationState(StatePath);
        state.WasCleanShutdown().Should().BeFalse();
    }
}
