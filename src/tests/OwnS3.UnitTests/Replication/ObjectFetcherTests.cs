using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using OwnS3.Replication;
using OwnS3.Replication.Fetching;
using OwnS3.Replication.Inbound;
using OwnS3.Replication.Lww;
using OwnS3.Replication.Transport;
using OwnS3.Storage;

namespace OwnS3.UnitTests.Replication;

// Фетчер per-source (t35, глава 07 §7): применение с исходными метаданными, sha-
// мисматч → повтор, tombstone → удаление, 404 → пропуск, пауза по месту, drain,
// счётчики исходов IOutcomeCounters по результату СВОЕГО применения.
public sealed class ObjectFetcherFixture : IDisposable
{
    public string VolumeDir { get; } = Path.Combine(Path.GetTempPath(), "owns3-test-" + Guid.NewGuid().ToString("N"));
    public string TombstonesDir { get; } = Path.Combine(Path.GetTempPath(), "owns3-test-" + Guid.NewGuid().ToString("N"));
    public FakeFetchTransport Transport { get; } = new();
    public CountingCounters Counters { get; } = new();
    public ReplicaStore Store { get; }
    public TombstoneStore Tombstones { get; }

    public ObjectFetcherFixture()
    {
        var volume = new XlVolume(VolumeDir, TimeProvider.System);
        volume.Initialize();
        var drive = new SingleXlDrive(volume, new FixedTimeProvider(TestVectors.FixedTime));
        Store = new ReplicaStore(drive, new FixedTimeProvider(TestVectors.FixedTime));
        Tombstones = new TombstoneStore(TombstonesDir);
        Applier = new ReplicaApplier(Store, Tombstones, new FixedTimeProvider(TestVectors.FixedTime));
    }

    public ReplicaApplier Applier { get; }

    // Очередь фетчей fixture — тесты enqueue'ат задачи (фетчер их разбирает).
    public FetchQueue Queue { get; } = new(100);

    public ReplicationMetrics Metrics { get; } = new(new System.Diagnostics.Metrics.Meter("owns3.test.fetcher"));

    // Переключаемое свободное место (тест DiskPause: пауза → резюм в рантайме)
    private long _freeDisk = long.MaxValue;

    public void SetFreeDisk(long bytes) => Interlocked.Exchange(ref _freeDisk, bytes);

    public ObjectFetcher Create(long freeDisk = long.MaxValue, long minDisk = 0, int pollMs = 5)
    {
        SetFreeDisk(freeDisk);
        return new ObjectFetcher(new TargetRecord("peer-a", "http://peer-a:9000", "ak", "sk"),
            Queue, Transport, Applier, Counters,
            () => Interlocked.Read(ref _freeDisk),
            new ReplicationOptions { DiskFreeMinBytes = minDisk, Parallelism = 1, BackoffMaxSec = 60 },
            TimeProvider.System, NullLogger<ObjectFetcher>.Instance, Metrics,
            diskPoll: TimeSpan.FromMilliseconds(pollMs), backoffBase: TimeSpan.FromMilliseconds(10));
    }

    public void Dispose()
    {
        // Teardown: temp-каталоги удаляются при любом исходе
        foreach (var dir in new[] { VolumeDir, TombstonesDir })
            if (Directory.Exists(dir))
                Directory.Delete(dir, recursive: true);
    }
}

// Фейк-транспорт фетча: сценарии 200-объект / tombstone / 404 / ошибка.
public sealed class FakeFetchTransport : IReplicationTransport
{
    // Ответ на следующий фетч: исключение, tombstone (unix-ms), 404 (null) или объект.
    public Queue<object?> Outcomes { get; } = new();

    // Первые N фетчей — сетевой сбой (HttpClientException), затем обычные исходы.
    public int FailuresBeforeSuccess { get; set; }

    public static FetchResult BodyResult(byte[] body, long modMs, string sha) =>
        new(new MemoryStream(body), body.Length, modMs, sha, "\"etag\"", "application/x-test",
            new Dictionary<string, string>(), TombstoneUnixMs: null);

    public Task<FetchResult> FetchObjectAsync(TargetRecord target, string bucket, string key, CancellationToken ct)
    {
        if (FailuresBeforeSuccess > 0)
        {
            FailuresBeforeSuccess--;
            throw new System.Net.Http.HttpRequestException("источник offline (тест)");
        }
        if (Outcomes.Count == 0)
        {
            var defaultBody = "default"u8.ToArray();
            return Task.FromResult(BodyResult(defaultBody, 1, Sha(defaultBody)));
        }
        var outcome = Outcomes.Dequeue();
        return Task.FromResult(outcome switch
        {
            Exception ex => throw ex,
            long tomb => new FetchResult(null, 0, 0, "", "", "", new Dictionary<string, string>(), tomb),
            null => new FetchResult(null, 0, 0, "", "", "", new Dictionary<string, string>(), null),
            FetchResult r => r,
            _ => throw new InvalidOperationException("неизвестный исход"),
        });
    }

    public static string Sha(byte[] body) =>
        Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(body)).ToLowerInvariant();

    public Task<long?> SendNotificationsAsync(TargetRecord t, NotificationBatchRequest b, CancellationToken ct) =>
        throw new NotSupportedException("фетчер не рассылает");

    public Task<ReplicationListingResponse> ListAsync(TargetRecord t, string bucket, string? marker, int maxKeys,
        CancellationToken ct) => throw new NotSupportedException("фетчер не листит");

    public Task<IReadOnlyList<(string Name, long CreationUnixMs)>> ListingBucketsAsync(TargetRecord t,
        CancellationToken ct) => throw new NotSupportedException("фетчер не листит");
}

public class ObjectFetcherTests : IClassFixture<ObjectFetcherFixture>
{
    private readonly ObjectFetcherFixture _fixture = new();

    private static byte[] Body(string content) => Encoding.UTF8.GetBytes(content);

    [Fact]
    public async Task FetchBody_AppliedWithSourceMetadata()
    {
        // Arrange — фейк-транспорт отдаёт тело+заголовки (modTime/sha источника)
        const string content = "fetch-body";
        _fixture.Transport.Outcomes.Enqueue(
            FakeFetchTransport.BodyResult(Body(content), 1760000000000, FakeFetchTransport.Sha(Body(content))));
        _fixture.Queue.TryEnqueue(new FetchTask("b", "k", 1760000000000));
        var fetcher = _fixture.Create();

        // Act
        using var cts = new CancellationTokenSource();
        await fetcher.DrainAsync(cts.Token);

        // Assert — применено с ИСХОДНЫМИ метаданными (xl-запись несёт modTime источника)
        var state = _fixture.Store.ReadKeyState("b", "k");
        state.Should().NotBeNull();
        state!.ModTime.Should().Be(DateTimeOffset.FromUnixTimeMilliseconds(1760000000000));
        state.ContentSha256Hex.Should().Be(FakeFetchTransport.Sha(Body(content)));
        _fixture.Counters.AppliedCount.Should().Be(1);
        _fixture.Counters.FetchedCount.Should().Be(1);
    }

    [Fact]
    public async Task FetchChecksumMismatch_RequeuedNotApplied()
    {
        // Arrange — sha заголовка не совпадает с телом (повтор), затем корректное тело
        _fixture.Transport.Outcomes.Enqueue(FakeFetchTransport.BodyResult(Body("corrupt"), 100, "deadbeef"));
        _fixture.Transport.Outcomes.Enqueue(FakeFetchTransport.BodyResult(Body("good"), 100,
            FakeFetchTransport.Sha(Body("good"))));
        _fixture.Queue.TryEnqueue(new FetchTask("b", "k", 100));
        var fetcher = _fixture.Create();

        // Act — воркер: мисматч → повтор той же задачи (бэкофф), затем успех
        using var cts = new CancellationTokenSource();
        var run = fetcher.RunAsync(cts.Token);
        var applied = false;
        for (var i = 0; i < 100 && !applied; i++)
        {
            await Task.Delay(20, TestContext.Current.CancellationToken);
            applied = _fixture.Store.ReadKeyState("b", "k") is not null;
        }

        // Assert — локально применено ТОЛЬКО корректное тело (мисматч не прошёл до Commit)
        applied.Should().BeTrue("повтор после sha-мисматча применяет корректное тело");
        _fixture.Store.ReadKeyState("b", "k")!.ContentSha256Hex
            .Should().Be(FakeFetchTransport.Sha(Body("good")));
        cts.Cancel();
        await run;
    }

    [Fact]
    public async Task FetchTombstone_DeletesKey_PutsTombstone()
    {
        // Arrange — локальный ключ (от реплики), tombstone-ответ
        _fixture.Store.ApplyBucketCreate("bt", DateTimeOffset.FromUnixTimeMilliseconds(100));
        await _fixture.Store.ApplyObjectAsync("bt", "gone", new MemoryStream(Body("v")), 1,
            new ReplicaObjectMetadata(DateTimeOffset.FromUnixTimeMilliseconds(100), "e",
                FakeFetchTransport.Sha(Body("v")), "text/plain", new Dictionary<string, string>()),
            FakeFetchTransport.Sha(Body("v")), TestContext.Current.CancellationToken);
        _fixture.Transport.Outcomes.Enqueue(5000L); // tombstone deleteTime=5000
        _fixture.Queue.TryEnqueue(new FetchTask("bt", "gone", 4000));
        var fetcher = _fixture.Create();

        // Act
        using var cts = new CancellationTokenSource();
        await fetcher.DrainAsync(cts.Token);

        // Assert — ключ удалён, tombstone записан (защита от воскрешения)
        _fixture.Store.ReadKeyState("bt", "gone").Should().BeNull();
        _fixture.Tombstones.GetDeleteTime("bt", "gone").Should().Be(5000);
        _fixture.Counters.AppliedCount.Should().Be(1);
    }

    [Fact]
    public async Task Drain_SourceOffline_TaskNotLost_RetryAfterRecovery()
    {
        // Arrange — транспорт падает (источник offline): drain НЕ теряет задачу,
        // ретрай после восстановления (finding 2/1)
        var payload = "drain-body"u8.ToArray();
        var sha = Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(payload)).ToLowerInvariant();
        _fixture.Transport.FailuresBeforeSuccess = int.MaxValue; // источник offline
        _fixture.Transport.Outcomes.Enqueue(FakeFetchTransport.BodyResult(payload, 100, sha));
        _fixture.Queue.TryEnqueue(new FetchTask("b", "drain", 100));
        var fetcher = _fixture.Create();

        // Act — drain с недоступным источником (бюджет ограничен токеном)
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));
        try
        {
            await fetcher.DrainAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            // бюджет истёк на ретраях — ожидаемо приoffline
        }

        // Assert — задача НЕ потеряна (HasUnapplied true — чистый маркер не ставить, R2)
        fetcher.HasUnapplied.Should().BeTrue();

        // Восстановление: транспорт вернулся — ретрай (RunAsync) применяет задачу
        _fixture.Transport.FailuresBeforeSuccess = 0;
        using var runCts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        await fetcher.RunAsync(runCts.Token);
        _fixture.Store.ReadKeyState("b", "drain")!.ModTime.Should().Be(DateTimeOffset.FromUnixTimeMilliseconds(100));
        fetcher.HasUnapplied.Should().BeFalse("после успешного ретрая незавершённой работы нет");
    }

    [Fact]
    public async Task Fetch404_SkippedWithoutChanges()
    {
        // Arrange — 404 (ключ неизвестен)
        _fixture.Transport.Outcomes.Enqueue(null);
        _fixture.Queue.TryEnqueue(new FetchTask("b", "none", 100));
        var fetcher = _fixture.Create();

        // Act
        using var cts = new CancellationTokenSource();
        await fetcher.DrainAsync(cts.Token);

        // Assert — состояние приёмника не менялось, счётчики применения не инкремент
        _fixture.Store.ReadKeyState("b", "k").Should().BeNull();
        _fixture.Counters.AppliedCount.Should().Be(0);
        _fixture.Counters.FetchedCount.Should().Be(0);
    }

    [Fact]
    public async Task DiskPause_FetchesHalted_ResumeWithSpace()
    {
        // Arrange — места меньше минимума: фетчи не идут; локальный ключ новее
        // уведомления → предфильтр skip (без сети); транспорт с исходом после паузы
        _fixture.Transport.Outcomes.Enqueue(FakeFetchTransport.BodyResult(Body("v"), 100,
            FakeFetchTransport.Sha(Body("v"))));
        var fetcher = _fixture.Create(freeDisk: 100, minDisk: 1000);
        _fixture.Store.ApplyBucketCreate("bp", DateTimeOffset.FromUnixTimeMilliseconds(100));
        var localBody = Body("newer");
        await _fixture.Store.ApplyObjectAsync("bp", "local-newer", new MemoryStream(localBody), localBody.Length,
            new ReplicaObjectMetadata(DateTimeOffset.FromUnixTimeMilliseconds(200), "e",
                FakeFetchTransport.Sha(localBody), "text/plain", new Dictionary<string, string>()),
            FakeFetchTransport.Sha(localBody), TestContext.Current.CancellationToken);
        _fixture.Queue.TryEnqueue(new FetchTask("bp", "local-newer", 100));

        // Act фаза 1 — воркер в фоне: пауза по месту
        using var cts = new CancellationTokenSource();
        var run = fetcher.RunAsync(cts.Token);
        await Task.Delay(50, TestContext.Current.CancellationToken);

        // Assert фаза 1 — пауза активна, транспорт не вызывался
        fetcher.Paused.Should().BeTrue();
        _fixture.Transport.Outcomes.Should().HaveCount(1, "фетчи на паузе — транспорт не тронут");

        // Act фаза 2 — место появилось
        _fixture.SetFreeDisk(long.MaxValue);

        // Assert фаза 2 — пауза снята, задача из очереди обработана (skip-ветка:
        // локальная новее — транспорт тоже не нужен, но фазы прошли)
        await Task.Delay(120, TestContext.Current.CancellationToken);
        fetcher.Paused.Should().BeFalse("место появилось — фетчи продолжаются");

        cts.Cancel();
        await run;
    }

    [Fact]
    public async Task DiskPause_Resume_AppliesQueuedKey()
    {
        // Arrange — пауза, затем место: ключ из очереди применяется (резюм по канону §7)
        var payload = "resume-body"u8.ToArray();
        var sha = Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(payload)).ToLowerInvariant();
        _fixture.Transport.Outcomes.Enqueue(FakeFetchTransport.BodyResult(payload, 100, sha));
        _fixture.Queue.TryEnqueue(new FetchTask("br", "resumed", 100));
        var fetcher = _fixture.Create(freeDisk: 100, minDisk: 1000);

        using var cts = new CancellationTokenSource();
        var run = fetcher.RunAsync(cts.Token);
        await Task.Delay(50, TestContext.Current.CancellationToken);
        fetcher.Paused.Should().BeTrue();

        // Act — место появилось
        _fixture.SetFreeDisk(long.MaxValue);
        await Task.Delay(150, TestContext.Current.CancellationToken);

        // Assert — пауза снята, ключ из очереди применён с исходным sha
        fetcher.Paused.Should().BeFalse();
        var state = _fixture.Store.ReadKeyState("br", "resumed");
        state.Should().NotBeNull();
        state!.ContentSha256Hex.Should().Be(sha);
        _fixture.Counters.AppliedCount.Should().BeGreaterThanOrEqualTo(1);
        cts.Cancel();
        await run;
    }

    [Fact]
    public async Task Drain_EmptiesQueue()
    {
        // Arrange — 3 фетч-задачи (по ключу на задачу, у транспорта свой исход на каждую)
        var fetcher = _fixture.Create();
        var queue = new FetchQueue(10);
        var fetcherWithQueue = new ObjectFetcher(new TargetRecord("peer-a", "http://a:9000", "ak", "sk"),
            queue, _fixture.Transport, _fixture.Applier, _fixture.Counters, () => long.MaxValue,
            new ReplicationOptions { DiskFreeMinBytes = 0 }, TimeProvider.System,
            NullLogger<ObjectFetcher>.Instance, _fixture.Metrics);
        var bucket = "bdrain";
        _fixture.Store.ApplyBucketCreate(bucket, DateTimeOffset.FromUnixTimeMilliseconds(100));
        foreach (var key in new[] { "d1", "d2", "d3" })
        {
            _fixture.Transport.Outcomes.Enqueue(FakeFetchTransport.BodyResult(Body(key), 100,
                FakeFetchTransport.Sha(Body(key))));
            queue.TryEnqueue(new FetchTask(bucket, key, 100));
        }

        // Act
        using var cts = new CancellationTokenSource();
        await fetcherWithQueue.DrainAsync(cts.Token);

        // Assert — очередь пуста, все три ключа применены
        queue.Depth.Should().Be(0);
        foreach (var key in new[] { "d1", "d2", "d3" })
            _fixture.Store.ReadKeyState(bucket, key).Should().NotBeNull();
    }
}
