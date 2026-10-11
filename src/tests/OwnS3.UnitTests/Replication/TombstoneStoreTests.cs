using OwnS3.Replication.Lww;

namespace OwnS3.UnitTests.Replication;

// Tombstone-хранилище (t35, глава 07 §7): максимум времени удаления, снятие,
// retention-чистка (зовёт цикл обслуживания §8-а).
public sealed class TombstoneStoreFixture : IDisposable
{
    public string Dir { get; } = Path.Combine(Path.GetTempPath(), "owns3-test-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        // Teardown: temp-каталог удаляется при любом исходе
        if (Directory.Exists(Dir))
            Directory.Delete(Dir, recursive: true);
    }
}

public class TombstoneStoreTests : IClassFixture<TombstoneStoreFixture>
{
    private readonly TombstoneStoreFixture _fixture = new();

    public TombstoneStoreTests()
    {
        // Изоляция кейсов: temp-каталог на кейс
        if (Directory.Exists(_fixture.Dir))
            Directory.Delete(_fixture.Dir, recursive: true);
    }

    private TombstoneStore CreateStore() => new(_fixture.Dir);

    [Fact]
    public void PutGetRemove_Roundtrip()
    {
        // Arrange — хранилище
        var store = CreateStore();

        // Act — запись, чтение, снятие
        store.Put("b", "k", 1000);
        var afterPut = store.GetDeleteTime("b", "k");
        store.Remove("b", "k");
        var afterRemove = store.GetDeleteTime("b", "k");

        // Assert — tombstone жив после Put, знания нет после Remove
        afterPut.Should().Be(1000);
        afterRemove.Should().BeNull();
    }

    [Fact]
    public void Put_OlderTime_DoesNotRollbackNewer()
    {
        // Arrange — свежий tombstone
        var store = CreateStore();
        store.Put("b", "k", 2000);

        // Act — опоздавший старый delete-репликой
        store.Put("b", "k", 1000);

        // Assert — максимум (новейшее) время сохранено
        store.GetDeleteTime("b", "k").Should().Be(2000);
    }

    [Fact]
    public void Cleanup_RemovesExpired_KeepsFresh()
    {
        // Arrange — старый (вне retention) и свежий tombstones
        var store = CreateStore();
        var now = DateTimeOffset.FromUnixTimeMilliseconds(1_000_000_000);
        store.Put("b", "old", now.AddHours(-200).ToUnixTimeMilliseconds());
        store.Put("b", "fresh", now.AddHours(-1).ToUnixTimeMilliseconds());

        // Act — retention 168 ч (дефолт канона)
        var removed = store.Cleanup(now, retentionHours: 168);

        // Assert — старый удалён, свежий остался
        removed.Should().Be(1);
        store.GetDeleteTime("b", "old").Should().BeNull();
        store.GetDeleteTime("b", "fresh").Should().NotBeNull();
    }
}
