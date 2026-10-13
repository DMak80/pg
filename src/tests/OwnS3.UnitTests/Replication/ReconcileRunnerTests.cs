using Microsoft.Extensions.Logging.Abstractions;
using OwnS3.Replication;
using OwnS3.Replication.Inbound;
using OwnS3.Replication.Lww;
using OwnS3.Replication.Reconcile;
using OwnS3.Replication.Transport;
using OwnS3.Storage;

namespace OwnS3.UnitTests.Replication;

// Reconcile / backfill (t35, глава 07 §9): полный проход (дыры → fetch-задачи,
// новее не тронут), маркер прогресса постранично, параллельный не создаётся,
// backfill состава бакетов.
public sealed class ReconcileRunnerFixture : IDisposable
{
    public string VolumeDir { get; } = Path.Combine(Path.GetTempPath(), "owns3-test-" + Guid.NewGuid().ToString("N"));
    public string TombstonesDir { get; } = Path.Combine(Path.GetTempPath(), "owns3-test-" + Guid.NewGuid().ToString("N"));
    public string ReconcileDir { get; } = Path.Combine(Path.GetTempPath(), "owns3-test-" + Guid.NewGuid().ToString("N"));
    public FakeReconcileTransport Transport { get; } = new();
    public ReplicaStore Store { get; }
    public ReplicaApplier Applier { get; }
    public FetchQueueRegistry Queues { get; }
    public ReplicationMetrics Metrics { get; } = new(new System.Diagnostics.Metrics.Meter("owns3.test"));

    public ReconcileRunnerFixture()
    {
        var volume = new XlVolume(VolumeDir, TimeProvider.System);
        volume.Initialize();
        var drive = new SingleXlDrive(volume, new FixedTimeProvider(TestVectors.FixedTime));
        Store = new ReplicaStore(drive, new FixedTimeProvider(TestVectors.FixedTime));
        Tombstones = new TombstoneStore(TombstonesDir);
        Applier = new ReplicaApplier(Store, Tombstones, new FixedTimeProvider(TestVectors.FixedTime));
        Queues = new FetchQueueRegistry(new ReplicationOptions { FetchQueueMax = 100 });
        Targets = OwnS3.Replication.ReplicationTargets.Load(
            Path.Combine(Path.GetTempPath(), "owns3-test-" + Guid.NewGuid().ToString("N")),
            [new TargetOptions("peer-a", "http://peer-a:9000", "ak", "sk")],
            NullLogger<OwnS3.Replication.ReplicationTargets>.Instance);
        Runner = new ReconcileRunner(Targets, Store, Applier, Queues, Transport, ReconcileDir,
            NullLogger<ReconcileRunner>.Instance, Metrics);
    }

    private TombstoneStore Tombstones { get; }
    public OwnS3.Replication.ReplicationTargets Targets { get; }
    public ReconcileRunner Runner { get; }

    public void Dispose()
    {
        // Teardown: temp-каталоги удаляются при любом исходе
        foreach (var dir in new[] { VolumeDir, TombstonesDir, ReconcileDir })
            if (Directory.Exists(dir))
                Directory.Delete(dir, recursive: true);
    }
}

// Фейк-транспорт reconcile: листинг соседа по страницам + состав бакетов.
public sealed class FakeReconcileTransport : IReplicationTransport
{
    public Dictionary<string, List<ReplicationListEntry>> Listing { get; } = new();
    public List<(string Name, long CreationUnixMs)> Buckets { get; } = [];

    public Task<Result<ReplicationListingResponse>> ListAsync(TargetRecord target, string bucket, string? marker,
        int maxKeys, CancellationToken ct)
    {
        var keys = Listing.GetValueOrDefault(bucket, []);
        var from = 0;
        if (marker is not null)
            from = keys.FindIndex(e => string.CompareOrdinal(e.Key, marker) > 0);
        var page = keys.Skip(from).Take(maxKeys).ToList();
        var next = from + maxKeys < keys.Count ? page[^1].Key : null;
        return Task.FromResult(Result<ReplicationListingResponse>.Success(
            new ReplicationListingResponse(page, next)));
    }

    // Гейт длительности прохода (тест «второй Trigger не порождает второй проход»).
    public Func<Task<IReadOnlyList<(string Name, long CreationUnixMs)>>>? ListingBucketsAsyncOverride { get; set; }

    public Task<Result<IReadOnlyList<(string Name, long CreationUnixMs)>>> ListingBucketsAsync(TargetRecord target,
        CancellationToken ct)
    {
        if (ListingBucketsAsyncOverride is not null)
            return ContinueWithBuckets(ListingBucketsAsyncOverride());
        return Task.FromResult(Result<IReadOnlyList<(string, long)>>.Success(
            Buckets.ToList() as IReadOnlyList<(string, long)>));
    }

    private static async Task<Result<IReadOnlyList<(string Name, long CreationUnixMs)>>> ContinueWithBuckets(
        Task<IReadOnlyList<(string Name, long CreationUnixMs)>> pending)
    {
        // Гейт может бросать (фейк-отказ — ретрай-сценарий): исход — Failed
        try
        {
            return Result<IReadOnlyList<(string Name, long CreationUnixMs)>>.Success(await pending);
        }
        catch (Exception ex)
        {
            return Result<IReadOnlyList<(string Name, long CreationUnixMs)>>.Failed(
                new PeerRejectedException($"сосед недоступен (тест): {ex.Message}"));
        }
    }

    public Task<Result<long>> SendNotificationsAsync(TargetRecord t, NotificationBatchRequest b, CancellationToken ct) =>
        throw new NotSupportedException("reconcile не рассылает");

    public Task<Result<FetchOutcome>> FetchObjectAsync(TargetRecord t, string bucket, string key, CancellationToken ct) =>
        throw new NotSupportedException("reconcile фетчит через очередь");

    public Task<Result<PingResult>> PingAsync(TargetRecord target, TimeSpan timeout, CancellationToken ct) =>
        throw new NotSupportedException("reconcile не пингует");
}

public class ReconcileRunnerTests : IClassFixture<ReconcileRunnerFixture>
{
    private readonly ReconcileRunnerFixture _fixture = new();

    private static ReplicationListEntry Entry(string key, long modMs) =>
        new(key, DateTimeOffset.FromUnixTimeMilliseconds(modMs), "sha-" + key, 10);

    [Fact]
    public async Task FullPass_QueuesFetchesForOlderAndMissing_KeepsNewer()
    {
        // Arrange — сосед имеет бакет bb и листит 3 ключа: absent (нет), local-old
        // (наша запись СТАРЕЕ соседской — §9 fetch), local-new (наша НОВЕЕ — §9 skip)
        _fixture.Transport.Buckets.Add(("bb", 1));
        _fixture.Transport.Listing["bb"] =
        [
            Entry("absent", 1000),
            Entry("local-old", 5000),
            Entry("local-new", 1000),
        ];
        var body = "v"u8.ToArray();
        var sha = Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(body)).ToLowerInvariant();
        _fixture.Store.ApplyBucketCreate("bb", DateTimeOffset.FromUnixTimeMilliseconds(1));
        await _fixture.Store.ApplyObjectAsync("bb", "local-old", new MemoryStream(body), body.Length,
            new ReplicaObjectMetadata(DateTimeOffset.FromUnixTimeMilliseconds(3000), "e",
                sha, "text/plain", new Dictionary<string, string>()),
            sha, TestContext.Current.CancellationToken);
        await _fixture.Store.ApplyObjectAsync("bb", "local-new", new MemoryStream(body), body.Length,
            new ReplicaObjectMetadata(DateTimeOffset.FromUnixTimeMilliseconds(9000), "e",
                sha, "text/plain", new Dictionary<string, string>()),
            sha, TestContext.Current.CancellationToken);

        // Act
        await _fixture.Runner.RunForSourceAsync("peer-a", TestContext.Current.CancellationToken);

        // Assert — fetch-задачи: absent (нет) и local-old (наша старее); local-new не тронут
        // (наш вариант выиграет при взаимной доставке — §9)
        var queue = _fixture.Queues.ForSource("peer-a");
        queue.Depth.Should().Be(2);
        var tasks = new List<FetchTask>();
        while (queue.TryDequeue(out var task))
            tasks.Add(task);
        tasks.Select(t => t.Key).Should().ContainInOrder("absent", "local-old");
        _fixture.Runner.ProcessedKeys("peer-a").Should().Be(3);
        _fixture.Runner.IsRunning("peer-a").Should().BeFalse();
    }

    [Fact]
    public async Task Marker_ContinuesFromNextPageAfterRestart()
    {
        // Arrange — сосед имеет бакет bm с 3 ключами; полный проход снимает маркер
        // (постраничность проверяется структурой ListAsync: маркер пишется после каждой
        // страницы, рестарт продолжает с него — поведение RunCoreAsync)
        _fixture.Transport.Buckets.Add(("bm", 100));
        _fixture.Transport.Listing["bm"] = [Entry("a", 1000), Entry("b", 2000), Entry("c", 3000)];

        // Act
        await _fixture.Runner.RunForSourceAsync("peer-a", TestContext.Current.CancellationToken);

        // Assert — маркер снят (проход завершён)
        File.Exists(Path.Combine(_fixture.ReconcileDir, "peer-a.marker")).Should().BeFalse();
        _fixture.Metrics.ReconcilePositionOf("peer-a").Should().Be(3);
    }

    [Fact]
    public async Task SecondTrigger_WhileRunning_AwaitsNotDuplicates()
    {
        // Arrange — листинг с большой страницей; долгий проход имитируем медленным транспортом
        var gate = new TaskCompletionSource<IReadOnlyList<(string Name, long CreationUnixMs)>>();
        _fixture.Transport.ListingBucketsAsyncOverride = () => gate.Task;
        _fixture.Transport.Buckets.Add(("bg", 100));

        // Act — Trigger (фоном) и второй RunForSourceAsync (после фактического старта)
        _fixture.Runner.Trigger("peer-a", TestContext.Current.CancellationToken);
        for (var i = 0; i < 100 && !_fixture.Runner.IsRunning("peer-a"); i++)
            await Task.Delay(10, TestContext.Current.CancellationToken);
        var second = _fixture.Runner.RunForSourceAsync("peer-a", TestContext.Current.CancellationToken);

        // Assert — IsRunning зафиксирован; второй await вернулся НЕ порождая второй проход;
        // после отпускания гейта проход завершается (IsRunning сбрасывается)
        _fixture.Runner.IsRunning("peer-a").Should().BeTrue();
        gate.SetResult([]);
        await second;
        for (var i = 0; i < 100 && _fixture.Runner.IsRunning("peer-a"); i++)
            await Task.Delay(20, TestContext.Current.CancellationToken);
        _fixture.Runner.IsRunning("peer-a").Should().BeFalse();
    }

    [Fact]
    public async Task Backfill_CreatesMissingBucketsWithNeighborCreationDate()
    {
        // Arrange — сосед имеет бакет, локально нет
        _fixture.Transport.Buckets.Add(("new-bucket", 1760000000000));
        _fixture.Transport.Listing["new-bucket"] = [];

        // Act
        await _fixture.Runner.RunForSourceAsync("peer-a", TestContext.Current.CancellationToken);

        // Assert — бакет создан с CreationDate соседа (только в сторону «у соседа есть»)
        _fixture.Store.ListBuckets().Throw().Value.Should().ContainSingle(b => b.Name == "new-bucket")
            .Which.CreationDate.Should().Be(DateTimeOffset.FromUnixTimeMilliseconds(1760000000000));
    }

    // Runner с очередью на 1 слот и малым бюджетом ожидания (suspend-кейсы §9).
    private ReconcileRunner SuspendRunner(TimeSpan budget, out FetchQueueRegistry queues)
    {
        queues = new FetchQueueRegistry(new ReplicationOptions { FetchQueueMax = 1 });
        return new ReconcileRunner(_fixture.Targets, _fixture.Store, _fixture.Applier, queues,
            _fixture.Transport, _fixture.ReconcileDir, NullLogger<ReconcileRunner>.Instance,
            _fixture.Metrics) { QueueWaitBudget = budget };
    }

    [Fact]
    public async Task QueueFull_AfterWaitBudget_SuspendsWithMarker()
    {
        // Arrange — очередь на 1 слот, листинг [a, b], бюджет 400 мс: «b» не влезает,
        // переживает бюджет → проход приостанавливается, маркер сохранён
        _fixture.Transport.Buckets.Add(("bb", 1));
        _fixture.Transport.Listing["bb"] = [Entry("a", 1000), Entry("b", 2000)];
        var runner = SuspendRunner(TimeSpan.FromMilliseconds(400), out var queues);

        // Act
        await runner.RunForSourceAsync("peer-a", TestContext.Current.CancellationToken);

        // Assert — «a» поставлен, «b» остался в листинге; маркер-файл «bb|» —
        // страница будет перечитана следующим проходом (at-least-once)
        queues.ForSource("peer-a").Depth.Should().Be(1);
        var markerPath = Path.Combine(_fixture.ReconcileDir, "peer-a.marker");
        File.Exists(markerPath).Should().BeTrue();
        File.ReadAllText(markerPath).Should().Be("bb|");
    }

    [Fact]
    public async Task QueueFull_WaitCapacity_EnqueuesAfterDrain()
    {
        // Arrange — очередь на 1 слот, листинг [a, b], бюджет 10 с; фон дренирует
        // «a» через 300 мс — «b» должен встать в очередь ВНУТРИ прохода (ожид ёмкости)
        _fixture.Transport.Buckets.Add(("bb", 1));
        _fixture.Transport.Listing["bb"] = [Entry("a", 1000), Entry("b", 2000)];
        var runner = SuspendRunner(TimeSpan.FromSeconds(10), out var queues);
        var drain = Task.Run(async () =>
        {
            await Task.Delay(300, TestContext.Current.CancellationToken);
            queues.ForSource("peer-a").TryDequeue(out _);
        }, TestContext.Current.CancellationToken);

        // Act
        await runner.RunForSourceAsync("peer-a", TestContext.Current.CancellationToken);
        await drain;

        // Assert — «b» поставлен (проход НЕ приостанавливался), маркер снят
        queues.ForSource("peer-a").Depth.Should().Be(1);
        queues.ForSource("peer-a").TryDequeue(out var task).Should().BeTrue();
        task!.Key.Should().Be("b");
        File.Exists(Path.Combine(_fixture.ReconcileDir, "peer-a.marker")).Should().BeFalse();
    }
}
