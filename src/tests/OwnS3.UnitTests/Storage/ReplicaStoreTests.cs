using Microsoft.Extensions.Logging.Abstractions;
using OwnS3.Storage;

namespace OwnS3.UnitTests.Storage;

// Фикстура: temp-том + SingleXlDrive + ReplicaStore (полный teardown);
// бакетов заранее НЕ создаёт — применение бакетов проверяет сам кейс.
public sealed class ReplicaStoreFixture : IAsyncLifetime
{
    public string Root { get; } = Path.Combine(Path.GetTempPath(), "owns3-test-" + Guid.NewGuid().ToString("N"));
    public XlVolume Volume { get; }
    public SingleXlDrive Drive { get; }
    public ReplicaStore Store { get; }

    public ReplicaStoreFixture()
    {
        Volume = new XlVolume(Root, TimeProvider.System);
        Volume.Initialize();
        Drive = new SingleXlDrive(Volume, new FixedTimeProvider(TestVectors.FixedTime));
        Store = new ReplicaStore(Drive, new FixedTimeProvider(TestVectors.FixedTime));
    }

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public ValueTask DisposeAsync()
    {
        // Teardown: temp-том удаляется при любом исходе
        if (Directory.Exists(Root))
            Directory.Delete(Root, recursive: true);
        return ValueTask.CompletedTask;
    }
}

// IReplicaStore (t35, глава 07 §11): применение реплики штатным xl-коммитом
// с ИСХОДНЫМИ метаданными (LWW-сходимость), sha-сверка до Commit,
// репликационные чтения (состояние ключа / листинг / fetch-отдача), бакеты.
public class ReplicaStoreTests(ReplicaStoreFixture fixture) : IClassFixture<ReplicaStoreFixture>
{
    private static readonly DateTimeOffset SourceModTime = new(2026, 10, 1, 12, 0, 0, TimeSpan.Zero);

    private ReplicaStore Store => fixture.Store;
    private string Root => fixture.Root;

    private static string Sha256HexOf(byte[] payload) =>
        Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(payload)).ToLowerInvariant();

    private static ReplicaObjectMetadata MetaOf(byte[] payload) =>
        new(SourceModTime, "abcd0123", Sha256HexOf(payload), "application/x-test",
            new Dictionary<string, string> { ["origin"] = "mesh-1" });

    [Fact]
    public async Task ApplyObject_PreservesSourceMetadata()
    {
        // Arrange — реплика объекта с фиксированным modTime источника
        var payload = "replica-body"u8.ToArray();
        var meta = MetaOf(payload);

        // Act
        await Store.ApplyObjectAsync("b", "k", new MemoryStream(payload), payload.Length, meta,
            meta.ContentSha256Hex, TestContext.Current.CancellationToken);

        // Assert — xl-запись несёт ИСХОДНЫЕ modTime/etag/sha/userMeta (LWW-сходимость)
        var state = Store.ReadKeyState("b", "k").Throw().Value is ReplicaKeyLookup.Found f0 ? f0.State
            : throw new InvalidOperationException("запись не найдена после Apply");
        state.ModTime.Should().Be(SourceModTime);
        state.ContentSha256Hex.Should().Be(meta.ContentSha256Hex);
        state.Size.Should().Be(payload.Length);
        var (metadata, body) = Store.OpenObjectForReplication("b", "k").Throw().Value;
        using (body)
        {
            metadata.ContentType.Should().Be("application/x-test");
            metadata.UserMetadata.Should().ContainKey("origin").WhoseValue.Should().Be("mesh-1");
            using var reader = new StreamReader(body);
            (await reader.ReadToEndAsync(TestContext.Current.CancellationToken)).Should().Be("replica-body");
        }

        // Повторное применение идемпотентно (LWW-идемпотентность реплики)
        await Store.ApplyObjectAsync("b", "k", new MemoryStream(payload), payload.Length, meta,
            meta.ContentSha256Hex, TestContext.Current.CancellationToken);
        var state2 = Store.ReadKeyState("b", "k").Throw().Value is ReplicaKeyLookup.Found f1 ? f1.State
            : throw new InvalidOperationException("запись не найдена после повтора");
        state2.Should().BeEquivalentTo(state);
    }

    [Fact]
    public async Task ApplyObject_ChecksumMismatch_RejectedWithoutCommit()
    {
        // Arrange — expectedSha не совпадает с телом
        var payload = "mismatch-body"u8.ToArray();
        var meta = MetaOf(payload);

        // Act
        var result = await Store.ApplyObjectAsync("b", "bad", new MemoryStream(payload), payload.Length,
            meta, "deadbeef", TestContext.Current.CancellationToken);

        // Assert — мисматч — Failed ДО коммита, ключа нет, staging снесён (tmp пуст)
        (!result.IsSuccess).Should().BeTrue();
        result.Error.Should().BeOfType<ReplicationChecksumMismatchException>();
        var state = Store.ReadKeyState("b", "bad");
        state.IsSuccess.Should().BeTrue();
        state.Value.Should().BeOfType<ReplicaKeyLookup.Missing>();
        Directory.Exists(Path.Combine(Root, ".owns3.sys", "tmp")).Should().BeTrue();
        Directory.EnumerateFileSystemEntries(Path.Combine(Root, ".owns3.sys", "tmp")).Should().BeEmpty();
    }

    [Fact]
    public async Task ListReplicationEntries_PagesByMarker_Utf8Order()
    {
        // Arrange — три ключа в СВОЁМ бакете (изоляция от других кейсов фикстуры)
        const string bucket = "blist";
        var payload = "page-body"u8.ToArray();
        var meta = MetaOf(payload);
        foreach (var key in new[] { "a", "m", "z" })
            await Store.ApplyObjectAsync(bucket, key, new MemoryStream(payload), payload.Length, meta,
                meta.ContentSha256Hex, TestContext.Current.CancellationToken);

        // Act — страница по maxKeys=2 от начала
        var page1 = Store.ListReplicationEntries(bucket, null, 2).Throw().Value;
        var nextMarker = page1.NextMarker;

        // Assert — первая страница в Utf8-порядке, nextMarker — последний отданный
        page1.Entries.Select(e => e.Key).Should().ContainInOrder("a", "m");
        page1.Entries[0].ModTime.Should().Be(SourceModTime);
        page1.Entries[0].ContentSha256Hex.Should().Be(meta.ContentSha256Hex);
        page1.Entries[0].Size.Should().Be(payload.Length);
        nextMarker.Should().Be("m");

        // Act — вторая страница от маркера
        var page2 = Store.ListReplicationEntries(bucket, nextMarker, 2).Throw().Value;

        // Assert — остаток без повторов; конец листинга — null-маркер
        page2.Entries.Select(e => e.Key).Should().ContainInOrder("z");
        page2.NextMarker.Should().BeNull();
    }

    [Fact]
    public async Task ApplyBucketCreate_Idempotent_KeepsFirstCreationDate()
    {
        // Arrange — создание бакета репликой с CreationDate соседа
        var t1 = new DateTimeOffset(2026, 9, 1, 0, 0, 0, TimeSpan.Zero);
        var t2 = new DateTimeOffset(2026, 10, 1, 0, 0, 0, TimeSpan.Zero);

        // Act — повторный create другого времени
        Store.ApplyBucketCreate("bf", t1);
        Store.ApplyBucketCreate("bf", t2);

        // Assert — CreationDate осталась первой (идемпотентность)
        fixture.Drive.ListBuckets().Throw().Value.Single(e => e.Name == "bf").CreationDate.Should().Be(t1);
    }

    [Fact]
    public async Task ApplyBucketDelete_EmptyDeleted_NonEmptyRejected()
    {
        // Arrange — пустой и непустой бакеты
        Store.ApplyBucketCreate("bempty", SourceModTime);
        Store.ApplyBucketCreate("bfull", SourceModTime);
        var payload = "del-body"u8.ToArray();
        var meta = MetaOf(payload);
        await Store.ApplyObjectAsync("bfull", "k", new MemoryStream(payload), payload.Length, meta,
            meta.ContentSha256Hex, TestContext.Current.CancellationToken);

        // Act — удаление пустого; удаление непустого
        Store.ApplyBucketDelete("bempty");
        var result = Store.ApplyBucketDelete("bfull");

        // Assert — пустой удалён (и повтор идемпотентен), непустой — Failed(BucketNotEmpty)
        fixture.Drive.BucketExists("bempty").Throw().Value.Should().BeFalse();
        Store.ApplyBucketDelete("bempty");
        (!result.IsSuccess).Should().BeTrue();
        result.Error.Should().BeOfType<ObjectStoreException>()
            .Which.Code.Should().Be(ObjectStoreErrorCode.BucketNotEmpty);
    }

    [Fact]
    public void ReadKeyState_ОтсутствующийКлюч_TерминальныйSuccessMissing()
    {
        // Act
        var lookup = Store.ReadKeyState("b", "no-such-repl-key");
        // Assert — «записи нет» — терминальный Success (конвенция 01 §4)
        lookup.IsSuccess.Should().BeTrue();
        lookup.Value.Should().BeOfType<ReplicaKeyLookup.Missing>();
    }

    [Fact]
    public async Task DeleteObject_Idempotent_RemovesKey()
    {
        // Arrange — ключ от реплики
        var payload = "tomb-body"u8.ToArray();
        await Store.ApplyObjectAsync("b", "tbd", new MemoryStream(payload), payload.Length, MetaOf(payload),
            MetaOf(payload).ContentSha256Hex, TestContext.Current.CancellationToken);

        // Act — удаление дважды
        Store.DeleteObject("b", "tbd");
        Store.DeleteObject("b", "tbd");

        // Assert — ключа нет (идемпотентность 204-семантики Delete; Missing — Success)
        var state = Store.ReadKeyState("b", "tbd");
        state.IsSuccess.Should().BeTrue();
        state.Value.Should().BeOfType<ReplicaKeyLookup.Missing>();
    }
}
