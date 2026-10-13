using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using OwnS3.Replication;
using OwnS3.Replication.Inbound;
using OwnS3.Replication.Lww;
using OwnS3.Storage;
using OwnS3.Replication.Transport;

namespace OwnS3.UnitTests.Replication;

// Приёмник уведомлений (t35, глава 07 §6): дедуп (повторный ACK), самодостаточные
// применяются, LWW-предфильтр object-put, переполнение очереди → null (429),
// дыры seqNo закрывает reconcile (применяются как есть).
public sealed class NotificationInboxFixture : IDisposable
{
    public string VolumeDir { get; } = Path.Combine(Path.GetTempPath(), "owns3-test-" + Guid.NewGuid().ToString("N"));
    public string TombstonesDir { get; } = Path.Combine(Path.GetTempPath(), "owns3-test-" + Guid.NewGuid().ToString("N"));
    public ReplicaStore Store { get; }
    public TombstoneStore Tombstones { get; }
    public ReplicaApplier Applier { get; }
    public CountingCounters Counters { get; } = new();

    public NotificationInboxFixture()
    {
        var volume = new XlVolume(VolumeDir, TimeProvider.System);
        volume.Initialize();
        var drive = new SingleXlDrive(volume, new FixedTimeProvider(TestVectors.FixedTime));
        Store = new ReplicaStore(drive, new FixedTimeProvider(TestVectors.FixedTime));
        Tombstones = new TombstoneStore(TombstonesDir);
        Applier = new ReplicaApplier(Store, Tombstones, new FixedTimeProvider(TestVectors.FixedTime));
    }

    public NotificationInbox CreateInbox(ReplicaApplier? applier = null, int queueCapacity = 100,
        IReplicaStore? store = null) =>
        new(new FetchQueueRegistry(new ReplicationOptions { FetchQueueMax = queueCapacity }),
            applier ?? Applier, store ?? Store, Tombstones, Counters);

    public void Dispose()
    {
        // Teardown: temp-каталоги удаляются при любом исходе
        foreach (var dir in new[] { VolumeDir, TombstonesDir })
            if (Directory.Exists(dir))
                Directory.Delete(dir, recursive: true);
    }
}

// Счётчик-заглушка IOutcomeCounters (ассерты точек инкремента inbox).
public sealed class CountingCounters : IOutcomeCounters
{
    public int AppliedCount, SkippedCount, ConflictCount, FetchedCount;

    public void Applied(string sourceName) => AppliedCount++;
    public void Skipped(string sourceName) => SkippedCount++;
    public void Conflict(string sourceName) => ConflictCount++;
    public void Fetched(string sourceName) => FetchedCount++;

    public void Reset() => (AppliedCount, SkippedCount, ConflictCount, FetchedCount) = (0, 0, 0, 0);
}

public class NotificationInboxTests : IClassFixture<NotificationInboxFixture>
{
    private static readonly DateTimeOffset Mod = new(2026, 10, 1, 12, 0, 0, TimeSpan.Zero);

    private readonly NotificationInboxFixture _fixture;

    public NotificationInboxTests(NotificationInboxFixture fixture)
    {
        // Fixture per-class: счётчики сбрасываются на каждый тест (изоляция ассертов)
        _fixture = fixture;
        _fixture.Counters.Reset();
    }

    private static ReplicationEvent PutEvent(long seqNo, string bucket = "b", string key = "k", long modMs = 1000) =>
        new(seqNo, ReplicationEventType.ObjectPut, bucket, key, modMs, "\"e\"", 1, "sha", "origin");

    private static byte[] Body(string content) => Encoding.UTF8.GetBytes(content);

    [Fact]
    public void Handle_DuplicateBatch_SkipsReapplication_AndRepeatsAck()
    {
        // Arrange — батч bucket-create + object-delete seqNo 1..2 от источника "peer-a"
        var inbox = _fixture.CreateInbox();
        var batch = new NotificationBatchRequest("mesh-a",
        [
            new ReplicationEvent(1, ReplicationEventType.BucketCreate, "bdedup", "", 100, "", 0, "", "mesh-a"),
            new ReplicationEvent(2, ReplicationEventType.ObjectDelete, "bdedup", "gone", 200, "", 0, "", "mesh-a"),
        ]);

        // Act — Handle дважды одним и тем же батчем
        var first = inbox.Handle("peer-a", batch);
        var appliedAfterFirst = _fixture.Counters.AppliedCount;
        var second = inbox.Handle("peer-a", batch);

        // Assert — тот же acceptedToSeqNo=2; применение не продублировано
        // (Applied не растёт; повторные события — дедуп-пропуск → Skipped)
        first.Should().Be(2);
        second.Should().Be(2);
        appliedAfterFirst.Should().Be(2);
        _fixture.Counters.AppliedCount.Should().Be(2);
        _fixture.Counters.SkippedCount.Should().Be(2);
    }

    // Фейк-стор: ReadKeyState возвращает кворумный отказ — отказ применения Delete
    // в inbox моделируется без порчи фикстурного тома.
    private sealed class FailingReadStore(IReplicaStore inner) : IReplicaStore
    {
        public Task<Result> ApplyObjectAsync(string bucket, string key, Stream body, long contentLength,
            ReplicaObjectMetadata metadata, string expectedSha256Hex, CancellationToken ct) =>
            inner.ApplyObjectAsync(bucket, key, body, contentLength, metadata, expectedSha256Hex, ct);
        public Result DeleteObject(string bucket, string key) => inner.DeleteObject(bucket, key);
        public Result<ReplicaKeyLookup> ReadKeyState(string bucket, string key) =>
            Result<ReplicaKeyLookup>.Failed(new ObjectStoreException(ObjectStoreErrorCode.InsufficientReadQuorum));
        public Result<ReplicaListingPage> ListReplicationEntries(string bucket, string? marker, int maxKeys) =>
            inner.ListReplicationEntries(bucket, marker, maxKeys);
        public Result<(ObjectMetadata Metadata, Stream Body)> OpenObjectForReplication(string bucket, string key) =>
            inner.OpenObjectForReplication(bucket, key);
        public Result ApplyBucketCreate(string bucket, DateTimeOffset creationDate) =>
            inner.ApplyBucketCreate(bucket, creationDate);
        public Result ApplyBucketDelete(string bucket) => inner.ApplyBucketDelete(bucket);
        public Result<IReadOnlyList<BucketEntry>> ListBuckets() => inner.ListBuckets();
    }

    [Fact]
    public void Handle_ApplyDeleteFails_ThrowsOut_NoAck()
    {
        // Arrange — аплаер со стором, ReadKeyState которого отказывает (кворум):
        // ApplyDelete вернёт Failed → мост бросает → Handle — исключение наружу
        var failingStore = new FailingReadStore(_fixture.Store);
        var failingApplier = new ReplicaApplier(failingStore, _fixture.Tombstones,
            new FixedTimeProvider(TestVectors.FixedTime));
        var inbox = _fixture.CreateInbox(applier: failingApplier, store: failingStore);
        var batch = new NotificationBatchRequest("mesh-a",
            [new ReplicationEvent(1, ReplicationEventType.ObjectDelete, "bfail", "gone", 200, "", 0, "", "mesh-a")]);

        // Act — Handle бросает доменный отказ (не ACK)
        var thrown = Assert.Throws<ObjectStoreException>(() => inbox.Handle("peer-a", batch));

        // Assert — исключение уходит хендлеру (→ 503/400+XML соседу → повтор батча);
        // событие НЕ засчитано применённым и НЕ ACK-нуто (Applied=0, lastAccepted не сдвинут)
        thrown.Code.Should().Be(ObjectStoreErrorCode.InsufficientReadQuorum);
        _fixture.Counters.AppliedCount.Should().Be(0);
        // Повтор того же батча на ЗДОРОВОМ inbox'е применяется (at-least-once: событие не потеряно)
        var healthy = _fixture.CreateInbox();
        healthy.Handle("peer-a", batch).Should().Be(1);
        _fixture.Counters.AppliedCount.Should().Be(1);
    }

    [Fact]
    public void Handle_SelfSufficientApplied_IncrementsAppliedCounter()
    {
        // Arrange — батч bucket-create (самодостаточное) + счётчик-заглушка
        var inbox = _fixture.CreateInbox();
        var batch = new NotificationBatchRequest("mesh-a",
            [new ReplicationEvent(1, ReplicationEventType.BucketCreate, "bself", "", 100, "", 0, "", "mesh-a")]);

        // Act
        var accepted = inbox.Handle("peer-a", batch);

        // Assert — counters.Applied == 1 (точка инкремента inbox — гарантия ассертов Task 14)
        accepted.Should().Be(1);
        _fixture.Counters.AppliedCount.Should().Be(1);
    }

    [Fact]
    public async Task Handle_LwwPrefilterSkip_IncrementsSkippedCounter()
    {
        // Arrange — локальный ключ новее уведомления object-put
        const string bucket = "bpre", key = "local";
        var body = Body("newer");
        var sha = Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(body)).ToLowerInvariant();
        _fixture.Store.ApplyBucketCreate(bucket, Mod);
        await _fixture.Store.ApplyObjectAsync(bucket, key, new MemoryStream(body), body.Length,
            new ReplicaObjectMetadata(Mod.AddSeconds(60), "e", sha, "text/plain", new Dictionary<string, string>()),
            sha, TestContext.Current.CancellationToken);
        var inbox = _fixture.CreateInbox();
        var batch = new NotificationBatchRequest("mesh-a", [PutEvent(1, bucket, key, modMs: 100)]);

        // Act
        var accepted = inbox.Handle("peer-a", batch);

        // Assert — counters.Skipped == 1; ключ не перезаписан
        accepted.Should().Be(1);
        _fixture.Counters.SkippedCount.Should().Be(1);
        // Assert — ключ не перезаписан (размер исходного тела)
        var state = _fixture.Store.ReadKeyState(bucket, key).Throw().Value;
        (state is ReplicaKeyLookup.Found f ? f.State.Size : -1).Should().Be(body.Length);
    }

    [Fact]
    public void Handle_GapInSeqNo_AppliesAnyway()
    {
        // Arrange — seqNo 5 после lastAccepted=1 (дыра 2..4 — закроет reconcile)
        var inbox = _fixture.CreateInbox();
        inbox.Handle("peer-a", new NotificationBatchRequest("mesh-a",
            [new ReplicationEvent(1, ReplicationEventType.BucketCreate, "bgap", "", 100, "", 0, "", "mesh-a")]));

        // Act
        var accepted = inbox.Handle("peer-a", new NotificationBatchRequest("mesh-a",
            [new ReplicationEvent(5, ReplicationEventType.BucketCreate, "bgap2", "", 200, "", 0, "", "mesh-a")]));

        // Assert — применяется, acceptedToSeqNo=5 (пропущенные закроет reconcile)
        accepted.Should().Be(5);
        _fixture.Counters.AppliedCount.Should().Be(2);
    }

    [Fact]
    public void Handle_FetchQueueFull_ReturnsNull()
    {
        // Arrange — capacity=1; два put-ключа старее локальных → второй не влез
        var inbox = _fixture.CreateInbox(queueCapacity: 1);
        var batch = new NotificationBatchRequest("mesh-a",
        [
            PutEvent(1, "bfull", "k1", modMs: 100),
            PutEvent(2, "bfull", "k2", modMs: 200),
        ]);

        // Act
        var accepted = inbox.Handle("peer-a", batch);

        // Assert — null → ReplicationHandlers отвечает 429 (глава 07 §6)
        accepted.Should().BeNull();
    }

    [Fact]
    public void Handle_ObjectPut_StaleQueuedForFetch_NotApplied()
    {
        // Arrange — локального ключа нет; object-put старее → fetch-очередь (без применения)
        var inbox = _fixture.CreateInbox();
        var batch = new NotificationBatchRequest("mesh-a", [PutEvent(3, "bq", "queued", modMs: 100)]);

        // Act
        var accepted = inbox.Handle("peer-a", batch);

        // Assert — ключ в очереди фетчей, применение не выполнялось ( applied не инкремент)
        accepted.Should().Be(3);
        _fixture.Counters.AppliedCount.Should().Be(0);
        _fixture.Counters.SkippedCount.Should().Be(0);
    }
}
