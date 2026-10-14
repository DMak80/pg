using System.Collections.Concurrent;
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

    [Fact]
    public void ConcurrentMarking_NoCrash_FileConsistent()
    {
        // Arrange — MarkDirty (ExecuteAsync старта) и MarkCleanAndPersist (StopAsync)
        // при быстрой остановке заходят concurrently: tmp+Move без сериализации
        // ронял второй File.Move с FileNotFoundException
        Directory.CreateDirectory(_fixture.Dir);
        var state = new ReplicationState(StatePath);
        var errors = new ConcurrentQueue<Exception>();

        // Act — 200 пар конкурентных записей с барьером старта (максимальный нахлёст)
        using var barrier = new Barrier(2);
        var worker = (Func<int> mark) =>
        {
            for (var i = 0; i < 200; i++)
            {
                try
                {
                    mark();
                }
                catch (Exception ex)
                {
                    errors.Enqueue(ex);
                }
                barrier.SignalAndWait(TimeSpan.FromSeconds(5));
            }
        };
        var threads = new[]
        {
            new Thread(() => worker(() => { state.MarkDirty(); return 0; })),
            new Thread(() => worker(() => { state.MarkCleanAndPersist(); return 0; })),
        };
        foreach (var thread in threads) thread.Start();
        foreach (var thread in threads) thread.Join(TimeSpan.FromSeconds(30));

        // Assert — ни одного исключения; файл читается (последний завершённый
        // Move победил — clean или dirty, оба валидны)
        errors.Should().BeEmpty();
        var read = () => new ReplicationState(StatePath).WasCleanShutdown();
        read.Should().NotThrow();
    }
}
