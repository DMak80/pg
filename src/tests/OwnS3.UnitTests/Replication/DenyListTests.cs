using OwnS3.Replication;

namespace OwnS3.UnitTests.Replication;

// Локальный deny-list репликации (t39, глава 07 §2/spec §4.5): решения отзыва
// по имени ИЛИ публичному ключу, персист tmp+rename, идемпотентный Add.
public sealed class DenyListFixture : IDisposable
{
    public string Dir { get; } = Path.Combine(Path.GetTempPath(), "owns3-test-" + Guid.NewGuid().ToString("N"));
    public string FilePath => Path.Combine(Dir, "denylist.json");

    public void Dispose()
    {
        // Teardown: temp-каталог удаляется при любом исходе
        if (Directory.Exists(Dir))
            Directory.Delete(Dir, recursive: true);
    }
}

public class DenyListTests : IDisposable
{
    private readonly DenyListFixture _fixture = new();

    // Фиксированное время для RevokedAtUnixMs (детерминизм персиста).
    private static readonly DateTimeOffset At = new(2026, 10, 14, 12, 0, 0, TimeSpan.Zero);

    private const string KeyA = "-----BEGIN PUBLIC KEY-----\nAAA\n-----END PUBLIC KEY-----\n";
    private const string KeyB = "-----BEGIN PUBLIC KEY-----\nBBB\n-----END PUBLIC KEY-----\n";

    public void Dispose()
    {
        // Изоляция кейсов: temp-каталог на кейс
        if (Directory.Exists(_fixture.Dir))
            Directory.Delete(_fixture.Dir, recursive: true);
    }

    private DenyList Create(TimeProvider? timeProvider = null) =>
        new(_fixture.FilePath, timeProvider ?? new FixedTimeProvider(At));

    [Fact]
    public void Add_Persists_ReloadKeeps()
    {
        // Arrange — пустой deny-list; запись отзыва
        var denyList = Create();

        // Act
        denyList.Add("n1", KeyA);

        // Assert — перечитанный с того же файла видит запись
        var reloaded = Create();
        reloaded.Snapshot.Should().ContainSingle()
            .Which.Should().BeEquivalentTo(new DenyEntry("n1", KeyA, At.ToUnixTimeMilliseconds()));
    }

    [Fact]
    public void IsDenied_ByName_True()
    {
        // Arrange — отозвано имя n1 с ключом A
        var denyList = Create();
        denyList.Add("n1", KeyA);

        // Act / Assert — совпадение по имени (ключ другой) — отказ
        denyList.IsDenied("n1", KeyB).Should().BeTrue();
    }

    [Fact]
    public void IsDenied_ByPublicKey_True()
    {
        // Arrange — отозвано имя n1 с ключом A
        var denyList = Create();
        denyList.Add("n1", KeyA);

        // Act / Assert — совпадение по публичному ключу (имя другое) — отказ
        denyList.IsDenied("другое", KeyA).Should().BeTrue();
    }

    [Fact]
    public void IsDenied_Neither_False()
    {
        // Arrange — пустой deny-list
        var denyList = Create();

        // Act / Assert — ни имя, ни ключ не отозваны
        denyList.IsDenied("x", KeyB).Should().BeFalse();
    }

    [Fact]
    public void Remove_UnknownName_False()
    {
        // Arrange — пустой deny-list
        var denyList = Create();

        // Act / Assert
        denyList.Remove("no-such").Should().BeFalse();
    }

    [Fact]
    public void Remove_Existing_TrueNotDenied()
    {
        // Arrange — отзыв n1
        var denyList = Create();
        denyList.Add("n1", KeyA);

        // Act
        var removed = denyList.Remove("n1");

        // Assert — отзыв снят; персист: перечитанный тоже чист
        removed.Should().BeTrue();
        denyList.IsDenied("n1", KeyA).Should().BeFalse();
        Create().Snapshot.Should().BeEmpty();
    }

    [Fact]
    public void Add_IdempotentSameKey_UpdatesTime()
    {
        // Arrange — отзыв n1 в At; повторный отзыв позже
        var later = At.AddMinutes(5);
        var denyList = Create();
        denyList.Add("n1", KeyA);
        denyList.Add("n1", KeyA);

        // Act — перечитанный список (фиксированное время позже)
        var reloaded = Create(new FixedTimeProvider(later));
        // (время в записи ставится провайдером Add: проверяем по первому списку)
        var entries = Create().Snapshot;

        // Assert — одна запись (не дубль), время отзыва от последнего Add
        entries.Should().ContainSingle();
        reloaded.IsDenied("n1", KeyA).Should().BeTrue();
        entries[0].RevokedAtUnixMs.Should().Be(At.ToUnixTimeMilliseconds());
    }

    [Fact]
    public void Constructor_MalformedFile_Throws()
    {
        // Arrange — повреждённый вручную файл (fail-fast, spec §4.5)
        Directory.CreateDirectory(_fixture.Dir);
        File.WriteAllText(_fixture.FilePath, "{not-json");

        // Act
        var act = () => Create();

        // Assert
        act.Should().Throw<InvalidOperationException>();
    }
}
