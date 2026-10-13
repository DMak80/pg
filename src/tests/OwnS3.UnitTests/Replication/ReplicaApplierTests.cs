using System.Text;
using OwnS3.Replication.Lww;
using OwnS3.Storage;

namespace OwnS3.UnitTests.Replication;

// ReplicaApplier (t35, глава 07 §7): модтайм-сохранение применения, tombstone-защита
// от воскрешения, LWW-исходы, отложенные бакетные удаления (RetryQueue).
public sealed class ReplicaApplierFixture : IDisposable
{
    public string VolumeDir { get; } = Path.Combine(Path.GetTempPath(), "owns3-test-" + Guid.NewGuid().ToString("N"));
    public string TombstonesDir { get; } = Path.Combine(Path.GetTempPath(), "owns3-test-" + Guid.NewGuid().ToString("N"));
    public SingleXlDrive Drive { get; }
    public ReplicaStore Store { get; }
    public TombstoneStore Tombstones { get; }
    public ReplicaApplier Applier { get; }

    public ReplicaApplierFixture()
    {
        var volume = new XlVolume(VolumeDir, TimeProvider.System);
        volume.Initialize();
        Drive = new SingleXlDrive(volume, new FixedTimeProvider(TestVectors.FixedTime));
        Store = new ReplicaStore(Drive, new FixedTimeProvider(TestVectors.FixedTime));
        Tombstones = new TombstoneStore(TombstonesDir);
        Applier = new ReplicaApplier(Store, Tombstones, new FixedTimeProvider(TestVectors.FixedTime));
    }

    public void Dispose()
    {
        // Teardown: temp-каталоги удаляются при любом исходе
        foreach (var dir in new[] { VolumeDir, TombstonesDir })
            if (Directory.Exists(dir))
                Directory.Delete(dir, recursive: true);
    }
}

public class ReplicaApplierTests : IClassFixture<ReplicaApplierFixture>
{
    private static readonly DateTimeOffset SourceModTime = new(2026, 10, 1, 12, 0, 0, TimeSpan.Zero);

    private readonly ReplicaApplierFixture _fixture;

    public ReplicaApplierTests(ReplicaApplierFixture fixture)
    {
        // Фикстура per-class: кейсы используют УНИКАЛЬНЫЕ бакеты/ключи (изоляция)
        _fixture = fixture;
    }

    private ReplicaApplier Applier => _fixture.Applier;
    private IReplicaStore Store => _fixture.Store;
    private TombstoneStore Tombstones => _fixture.Tombstones;

    private static byte[] Body(string content) => Encoding.UTF8.GetBytes(content);

    private static ReplicaObjectMetadata MetaOf(string content, DateTimeOffset? modTime = null,
        string? sha = null) =>
        new(modTime ?? SourceModTime, "abcd0123", sha ?? Sha256Hex(content), "application/x-test",
            new Dictionary<string, string>());

    private static string Sha256Hex(byte[] payload) =>
        Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(payload)).ToLowerInvariant();

    private static string Sha256Hex(string content) => Sha256Hex(Body(content));

    [Fact]
    public async Task ApplyObject_NewKey_AppliedWithSourceModTime()
    {
        // Arrange — ключа и tombstone нет
        // Act
        var result = await Applier.ApplyObjectAsync("bap", "new", new MemoryStream(Body("v1")),
            2, MetaOf("v1"), Sha256Hex("v1"), TestContext.Current.CancellationToken);

        // Assert — применено (Applied), xl-запись несёт modTime источника (LWW-сходимость)
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be(ReplicaApplyOutcome.Applied);
        var appliedState = Store.ReadKeyState("bap", "new").Throw().Value is ReplicaKeyLookup.Found ra
            ? ra.State
            : throw new InvalidOperationException("запись не найдена после Apply");
        appliedState.ModTime.Should().Be(SourceModTime);
    }

    [Fact]
    public async Task ApplyObject_OlderPutAfterTombstone_Skipped_NotResurrected()
    {
        // Arrange — tombstone новее входящего PUT
        const string bucket = "btomb", key = "old";
        Tombstones.Put(bucket, key, SourceModTime.ToUnixTimeMilliseconds() + 10_000);

        // Act — входящий PUT старее tombstone
        var result = await Applier.ApplyObjectAsync(bucket, key, new MemoryStream(Body("stale")),
            5, MetaOf("stale"), Sha256Hex("stale"), TestContext.Current.CancellationToken);

        // Assert — skip (Skipped): ключ не воскрешён, tombstone на месте
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be(ReplicaApplyOutcome.Skipped);
        Store.ReadKeyState(bucket, key).Value.Should().BeOfType<ReplicaKeyLookup.Missing>();
        Tombstones.GetDeleteTime(bucket, key).Should().NotBeNull();
    }

    [Fact]
    public async Task ApplyObject_NewerPutAfterTombstone_Applied_TombstoneRemoved()
    {
        // Arrange — tombstone старее входящего PUT
        const string bucket = "brev", key = "revived";
        Tombstones.Put(bucket, key, SourceModTime.ToUnixTimeMilliseconds() - 10_000);

        // Act — входящий PUT новее tombstone
        var result = await Applier.ApplyObjectAsync(bucket, key, new MemoryStream(Body("fresh")),
            5, MetaOf("fresh"), Sha256Hex("fresh"), TestContext.Current.CancellationToken);

        // Assert — применён (Applied), tombstone снят (запись жива — негативное знание снято)
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be(ReplicaApplyOutcome.Applied);
        Store.ReadKeyState(bucket, key).Should().NotBeNull();
        Tombstones.GetDeleteTime(bucket, key).Should().BeNull();
    }

    [Fact]
    public async Task ApplyObject_LocalNewer_Skipped()
    {
        // Arrange — локальная запись новее
        const string bucket = "bloc", key = "local";
        var newerMod = SourceModTime.AddSeconds(60);
        await Applier.ApplyObjectAsync(bucket, key, new MemoryStream(Body("local-new")),
            9, MetaOf("local-new", newerMod), Sha256Hex("local-new"), TestContext.Current.CancellationToken);

        // Act — входящий PUT старее локального
        var result = await Applier.ApplyObjectAsync(bucket, key, new MemoryStream(Body("incoming-old")),
            12, MetaOf("incoming-old"), Sha256Hex("incoming-old"), TestContext.Current.CancellationToken);

        // Assert — skip (Skipped): локальная версия не перезаписана
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be(ReplicaApplyOutcome.Skipped);
        var keptState = Store.ReadKeyState(bucket, key).Throw().Value is ReplicaKeyLookup.Found rb
            ? rb.State
            : throw new InvalidOperationException("запись не найдена после skip");
        keptState.ModTime.Should().Be(newerMod);
    }

    [Fact]
    public async Task ApplyObject_Conflict_ResultFlagAndEvent()
    {
        // Arrange — равные modTime, разные sha
        const string bucket = "bcfl", key = "conflict";
        var shaA = new string('a', 64);
        await Applier.ApplyObjectAsync(bucket, key, new MemoryStream(Body("local-body")),
            10, MetaOf("local-body", sha: shaA), Sha256Hex("local-body"), TestContext.Current.CancellationToken);
        (string? Bucket, string? Key, long InMod, string? InSha, long LocalMod, string? LocalSha) raised = default;
        Applier.ConflictDetected += (b, k, inMod, inSha, localMod, localSha) => raised = (b, k, inMod, inSha, localMod, localSha);
        var incomingSha = new string('b', 64); // лексикографически больше "aaa…a"

        // Act — входящий с равным modTime и большим sha
        var result = await Applier.ApplyObjectAsync(bucket, key, new MemoryStream(Body("incoming-body")),
            13, MetaOf("incoming-body", sha: incomingSha), Sha256Hex("incoming-body"),
            TestContext.Current.CancellationToken);

        // Assert — ConflictApplied в результате СВОЕГО вызова (конфликт И применено —
        // канон 07 §10) + событие с обеими версиями (только лог)
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be(ReplicaApplyOutcome.ConflictApplied);
        raised.Key.Should().Be(key);
        raised.InSha.Should().Be(incomingSha);
        raised.LocalSha.Should().Be(shaA);
    }

    [Fact]
    public async Task ApplyDelete_LocalNewer_Skipped()
    {
        // Arrange — локальная запись новее tombstone-времени
        const string bucket = "bdel", key = "keep";
        var localMod = SourceModTime.AddSeconds(30);
        Applier.ApplyBucketCreate(bucket, SourceModTime.ToUnixTimeMilliseconds());
        await Store.ApplyObjectAsync(bucket, key, new MemoryStream(Body("local")),
            5, MetaOf("local", localMod), Sha256Hex("local"), TestContext.Current.CancellationToken);

        // Act — delete-реплика старее локального
        Applier.ApplyDelete(bucket, key, SourceModTime.ToUnixTimeMilliseconds());

        // Assert — ключ жив (локальная новее), tombstone не поставлен
        Store.ReadKeyState(bucket, key).Should().NotBeNull();
        Tombstones.GetDeleteTime(bucket, key).Should().BeNull();
    }

    [Fact]
    public void ApplyDelete_MissingKey_PutsTombstone()
    {
        // Arrange — ключа нет
        const string bucket = "bmiss", key = "gone";

        // Act
        Applier.ApplyDelete(bucket, key, 5000);

        // Assert — tombstone записан (защита от воскрешения)
        Tombstones.GetDeleteTime(bucket, key).Should().Be(5000);
    }

    [Fact]
    public async Task ApplyBucketDelete_NonEmpty_DeferredThenSucceedsAfterRetryTick()
    {
        // Arrange — непустой бакет
        const string bucket = "bdef";
        Applier.ApplyBucketCreate(bucket, SourceModTime.ToUnixTimeMilliseconds());
        await Store.ApplyObjectAsync(bucket, "obj", new MemoryStream(Body("x")), 1, MetaOf("x"),
            Sha256Hex("x"), TestContext.Current.CancellationToken);

        // Act — delete непустого (откладывается), затем удаление объекта и ретрай-тик
        Applier.ApplyBucketDelete(bucket);
        Store.DeleteObject(bucket, "obj");
        Applier.RetryTick();

        // Assert — бакет удалён отложенным ретраем (до тика он был жив)
        _fixture.Drive.BucketExists(bucket).Throw().Value.Should().BeFalse();
    }
}
