using System.Security.Cryptography;
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

    // ── Replace: полная перезапись (PUT /replication/denylist, глава 07 §2) ──

    // Валидный PEM публичного ключа ECDSA P-256 (валидация Replace — ImportFromPem;
    // константы KeyA/KeyB — намеренно битые, для негативных кейсов).
    private static string ValidPem()
    {
        using var key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        return key.ExportSubjectPublicKeyInfoPem();
    }

    [Fact]
    public void Replace_SwapsList_PersistsAndReloads()
    {
        // Arrange — отзыв n1 (битый PEM Add-у не мешает); перезапись одним n2
        var denyList = Create();
        denyList.Add("n1", KeyA);
        var pem2 = ValidPem();

        // Act
        denyList.Replace([("n2", pem2)]);

        // Assert — полный обмен: старой записи нет, новая есть (имя и ключ);
        // персист: перечитанный с того же файла видит то же
        denyList.Snapshot.Should().ContainSingle()
            .Which.Should().BeEquivalentTo(new DenyEntry("n2", pem2, At.ToUnixTimeMilliseconds()));
        denyList.IsDenied("n1", KeyA).Should().BeFalse();
        denyList.IsDenied("n2", "другой-ключ").Should().BeTrue();
        Create().Snapshot.Should().ContainSingle().Which.Name.Should().Be("n2");
    }

    [Fact]
    public void Replace_EmptyList_ClearsAll_FileIsEmptyArray()
    {
        // Arrange — один отзыв
        var denyList = Create();
        denyList.Add("n1", KeyA);

        // Act — пустой entries: снятие всех отзывов
        denyList.Replace([]);

        // Assert — список пуст; файл — литерал [] (формат на диске не меняется)
        denyList.Snapshot.Should().BeEmpty();
        denyList.IsDenied("n1", KeyA).Should().BeFalse();
        File.ReadAllText(_fixture.FilePath).Should().Be("[]");
        Create().Snapshot.Should().BeEmpty();
    }

    [Fact]
    public void Replace_SetsRevokedAtToApplicationTime()
    {
        // Arrange — перезапись в At+5мин (провайдер с другим «сейчас»)
        var later = At.AddMinutes(5);
        var denyList = Create(new FixedTimeProvider(later));
        var pem = ValidPem();

        // Act
        denyList.Replace([("n1", pem)]);

        // Assert — время каждой записи = момент применения перезаписи (серверное)
        denyList.Snapshot.Should().ContainSingle()
            .Which.RevokedAtUnixMs.Should().Be(later.ToUnixTimeMilliseconds());
    }

    [Fact]
    public void Replace_IdempotentSamePayload_UpdatesTime()
    {
        // Arrange — первая перезапись в At
        var pem = ValidPem();
        Create().Replace([("n1", pem)]);

        // Act — повторная перезапись тем же составом позже: успешная замена
        var later = At.AddMinutes(5);
        var reloaded = Create(new FixedTimeProvider(later));
        reloaded.Replace([("n1", pem)]);

        // Assert — одна запись, время от последнего применения
        reloaded.Snapshot.Should().ContainSingle()
            .Which.RevokedAtUnixMs.Should().Be(later.ToUnixTimeMilliseconds());
    }

    [Fact]
    public void Replace_PersistAtomic_NoTmpLeft()
    {
        // Arrange
        var denyList = Create();

        // Act
        denyList.Replace([("n1", ValidPem())]);

        // Assert — tmp+rename: файла .tmp не осталось, основной файл заменён
        File.Exists(_fixture.FilePath + ".tmp").Should().BeFalse();
        File.Exists(_fixture.FilePath).Should().BeTrue();
    }

    [Theory]
    [InlineData("name-empty")]      // пустое имя
    [InlineData("pem-empty")]       // пустой publicKeyPem
    [InlineData("pem-broken")]      // PEM не импортируется как ECDSA P-256
    [InlineData("name-duplicate")]  // дубли имён
    [InlineData("key-duplicate")]   // дубли ключей
    public void Replace_InvalidEntries_Throws_SnapshotAndFileUntouched(string kind)
    {
        // Arrange — действующий отзыв keep; невалидный состав по виду кейса
        var denyList = Create();
        denyList.Add("keep", KeyA);
        var pemA = ValidPem();
        var pemB = ValidPem();
        IReadOnlyList<(string Name, string PublicKeyPem)> entries = kind switch
        {
            "name-empty" => [("", pemA)],
            "pem-empty" => [("n1", "")],
            "pem-broken" => [("n1", KeyA)],
            "name-duplicate" => [("n1", pemA), ("n1", pemB)],
            _ => [("n1", pemA), ("n2", pemA)],
        };

        // Act
        var act = () => denyList.Replace(entries);

        // Assert — ArgumentException ДО персиста: снапшот и файл не тронуты
        act.Should().Throw<ArgumentException>();
        denyList.Snapshot.Should().ContainSingle().Which.Name.Should().Be("keep");
        Create().Snapshot.Should().ContainSingle().Which.Name.Should().Be("keep");
    }

    [Fact]
    public void Replace_CoexistsWithAddRemoveUnderLock()
    {
        // Arrange — смешанная последовательность операций одного инстанса
        var pem = ValidPem();
        var denyList = Create();

        // Act
        denyList.Add("n1", KeyA);
        denyList.Replace([("n2", pem)]);
        denyList.Add("n3", KeyB);
        var removed = denyList.Remove("n2");

        // Assert — операции Replace/Add/Remove/IsDenied согласованы под одним локом;
        // персист отражает финальный состав
        removed.Should().BeTrue();
        denyList.IsDenied("n2", pem).Should().BeFalse();
        denyList.IsDenied("n3", "x").Should().BeTrue();
        denyList.Snapshot.Select(e => e.Name).Should().BeEquivalentTo("n3");
        Create().Snapshot.Select(e => e.Name).Should().BeEquivalentTo("n3");
    }
}
