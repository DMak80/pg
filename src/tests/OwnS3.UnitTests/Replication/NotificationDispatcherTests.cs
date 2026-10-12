using System.Diagnostics;
using Microsoft.Extensions.Logging.Abstractions;
using OwnS3.Replication;
using OwnS3.Replication.Cursors;
using OwnS3.Replication.Notifier;
using OwnS3.Replication.Outbox;
using OwnS3.Replication.Transport;

namespace OwnS3.UnitTests.Replication;

// Notifier per-target (t35, глава 07 §8): окно in-flight (backpressure), бэкофф
// эксп с повтором от watermark (at-least-once), рестарт с watermark, новый сосед —
// с конца журнала, метрики queue_depth/delivery_lag.
public sealed class NotificationDispatcherFixture : IDisposable
{
    public string OutboxDir { get; } = Path.Combine(Path.GetTempPath(), "owns3-test-" + Guid.NewGuid().ToString("N"));
    public FakeTransport Transport { get; } = new();
    public ReplicationMetrics Metrics { get; } = new(new System.Diagnostics.Metrics.Meter("owns3.test"));

    private readonly List<string> _tempDirs = [];

    // СВОИ курсоры на кейс (watermark монотонен — кейсы не должны делить его);
    // для рестарт-кейсов тест создаёт курсоры заранее и передаёт оба dispatchers.
    internal TargetCursors NewCursors()
    {
        var dir = Path.Combine(Path.GetTempPath(), "owns3-test-" + Guid.NewGuid().ToString("N"));
        _tempDirs.Add(dir);
        return new TargetCursors(dir, TimeProvider.System);
    }

    // Малые паузы — детерминизм без ожиданий (поллинг 5 мс, база бэкоффа 10 мс).
    // Journal передаётся тестом (ЕДИНСТВЕННЫЙ инстанс на каталог — как в DI прода).
    internal NotificationDispatcher Create(OutboxJournal journal, TargetCursors cursors,
        TargetRecord? target = null, int window = 64) =>
        new(target ?? new TargetRecord("peer-a", "http://peer-a:9000", "ak", "sk"),
            journal, cursors, Transport,
            new ReplicationOptions { InflightWindow = window, BackoffMaxSec = 60 }, Metrics,
            "test-host", TimeProvider.System, NullLogger<NotificationDispatcher>.Instance,
            pollDelay: TimeSpan.FromMilliseconds(5), backoffBase: TimeSpan.FromMilliseconds(10));

    public void Dispose()
    {
        // Teardown: temp-каталоги (курсоры кейсов + outbox) удаляются при любом исходе
        foreach (var dir in _tempDirs)
            if (Directory.Exists(dir))
                Directory.Delete(dir, recursive: true);
        if (Directory.Exists(OutboxDir))
            Directory.Delete(OutboxDir, recursive: true);
    }
}

// Фейк-транспорт: сценарии ответов + журнал отправленных батчей.
public sealed class FakeTransport : IReplicationTransport
{
    // Очередь исходов: null = 429; long = accepted; исключение — Throw.
    public Queue<object?> Outcomes { get; } = new();
    public List<IReadOnlyList<ReplicationEvent>> Sent { get; } = [];
    public Exception? Throw { get; set; }

    public Task<long?> SendNotificationsAsync(TargetRecord target, NotificationBatchRequest batch,
        CancellationToken ct)
    {
        if (Throw is not null)
        {
            var ex = Throw;
            Throw = null;
            throw ex;
        }
        Sent.Add(batch.Events);
        if (Outcomes.Count == 0)
            return Task.FromResult<long?>(batch.Events.Count > 0 ? batch.Events[^1].SeqNo : 0);
        var outcome = Outcomes.Dequeue();
        return Task.FromResult(outcome as long?);
    }

    public Task<FetchResult> FetchObjectAsync(TargetRecord target, string bucket, string key, CancellationToken ct) =>
        throw new NotSupportedException("notifier не фетчит");

    public Task<ReplicationListingResponse> ListAsync(TargetRecord target, string bucket, string? marker,
        int maxKeys, CancellationToken ct) => throw new NotSupportedException("notifier не листит");

    public Task<IReadOnlyList<(string Name, long CreationUnixMs)>> ListingBucketsAsync(TargetRecord target,
        CancellationToken ct) => throw new NotSupportedException("notifier не листит");
}

public class NotificationDispatcherTests : IClassFixture<NotificationDispatcherFixture>
{
    private readonly NotificationDispatcherFixture _fixture = new();

    public NotificationDispatcherTests()
    {
        // Изоляция кейсов: temp-каталоги/фейк на кейс
        if (Directory.Exists(_fixture.OutboxDir))
            Directory.Delete(_fixture.OutboxDir, recursive: true);
    }

    private OutboxJournal Journal(int events, long startModMs = 1000)
    {
        var journal = new OutboxJournal(_fixture.OutboxDir, long.MaxValue,
            NullLogger<OutboxJournal>.Instance);
        for (var i = 0; i < events; i++)
            journal.Append(ReplicationEventType.ObjectPut, "b", $"k{i}", startModMs + i, "e", 1, "s", "h");
        return journal;
    }

    private static async Task RunUntilAsync(Func<bool> condition, int timeoutMs = 5000)
    {
        var sw = Stopwatch.StartNew();
        while (!condition() && sw.ElapsedMilliseconds < timeoutMs)
            await Task.Delay(10);
        condition().Should().BeTrue($"условие не выполнено за {timeoutMs} мс");
    }

    [Fact]
    public async Task InflightWindow_BatchesSplit_SecondFromAck()
    {
        // Arrange — журнал 100 событий, окно 64; ACK за пределами окна не приходит (429)
        var journal = Journal(100);
        var cursors = _fixture.NewCursors();
        cursors.Advance("peer-a", 0); // известный сосед: рассылка от начала журнала
        _fixture.Transport.Outcomes.Enqueue(64L); // ACK ровно окна (64)
        _fixture.Transport.Outcomes.Enqueue(null); // следующий батч — 429: пауза бэкоффа до Cancel
        var dispatcher = _fixture.Create(journal, cursors, window: 64);
        using var cts = new CancellationTokenSource();

        // Act — первый батч: ровно окно (64), ACK 64 двигает watermark
        var run = dispatcher.RunAsync(cts.Token);
        await RunUntilAsync(() => _fixture.Transport.Sent.Count >= 1);

        // Assert — первый батч ровно окно (64)
        _fixture.Transport.Sent[0].Should().HaveCount(64);
        cts.Cancel();
        await run;

        // Act — рестарт на тех же курсорах: watermark=64 → второй батч 36; ack=100 → тишина
        _fixture.Transport.Sent.Clear();
        using var cts2 = new CancellationTokenSource();
        var run2 = dispatcher.RunAsync(cts2.Token);
        await RunUntilAsync(() => _fixture.Transport.Sent.Count >= 1);
        _fixture.Transport.Sent[0].Should().HaveCount(36);
        _fixture.Transport.Sent[0][0].SeqNo.Should().Be(65);
        cts2.Cancel();
        await run2;
        _fixture.Transport.Sent.Should().HaveCount(1, "после полного ACK новых событий нет — тишина");
    }

    [Fact]
    public async Task Backoff_RepeatsFromWatermark_ResetOnSuccess()
    {
        // Arrange — 3 события; сценарий: 429, 429, успех
        var journal = Journal(3);
        var cursors = _fixture.NewCursors();
        cursors.Advance("peer-a", 0);
        _fixture.Transport.Outcomes.Enqueue(null);
        _fixture.Transport.Outcomes.Enqueue(null);
        var dispatcher = _fixture.Create(journal, cursors);
        using var cts = new CancellationTokenSource();

        // Act
        var run = dispatcher.RunAsync(cts.Token);
        await RunUntilAsync(() => _fixture.Transport.Sent.Count >= 3);
        await RunUntilAsync(() => dispatcher.Online);

        // Assert — все три повтора ОДНОГО батча от watermark (at-least-once);
        // после успеха Online=true и бэкофф сброшен (метрика backoff-исходов 2)
        _fixture.Transport.Sent.Should().HaveCount(3);
        _fixture.Transport.Sent[1].Should().BeEquivalentTo(_fixture.Transport.Sent[0]);
        _fixture.Transport.Sent[2].Should().BeEquivalentTo(_fixture.Transport.Sent[0]);
        _fixture.Metrics.QueueDepthOf("peer-a").Should().Be(0);
        cts.Cancel();
        await run;
    }

    [Fact]
    public async Task Restart_ContinuesFromPersistedWatermark()
    {
        // Arrange — 5 событий; dispatcher ACKнул 3 и остановлен (рестарт источника)
        var journal = Journal(5);
        var cursors = _fixture.NewCursors();
        cursors.Advance("peer-a", 0);
        _fixture.Transport.Outcomes.Enqueue(3L);
        _fixture.Transport.Outcomes.Enqueue(null); // после ACK 3 — 429: пауза бэкоффа до Cancel
        var first = _fixture.Create(journal, cursors);
        using var cts = new CancellationTokenSource();
        var run = first.RunAsync(cts.Token);
        await RunUntilAsync(() => _fixture.Transport.Sent.Count >= 1);
        cts.Cancel();
        await run;
        _fixture.Transport.Sent.Clear();

        // Act — новый dispatcher на тех же journal/cursors (рестарт)
        using var cts2 = new CancellationTokenSource();
        var restarted = _fixture.Create(journal, cursors);
        var run2 = restarted.RunAsync(cts2.Token);
        await RunUntilAsync(() => _fixture.Transport.Sent.Count >= 1);

        // Assert — повтор с watermark=3: только события 4,5
        _fixture.Transport.Sent[0].Select(e => e.SeqNo).Should().ContainInOrder(4, 5);
        cts2.Cancel();
        await run2;
    }

    [Fact]
    public async Task NewNeighbor_StartsFromJournalTail()
    {
        // Arrange — курсора нет (−1); журнал уже наполнен 3 событиями (существующее
        // соседу НЕ рассылается — он «знает» его с конца журнала)
        var journal = Journal(3);
        var dispatcher = _fixture.Create(journal, _fixture.NewCursors());
        using var cts = new CancellationTokenSource();
        var run = dispatcher.RunAsync(cts.Token);
        await Task.Delay(50, TestContext.Current.CancellationToken); // старт: watermark = LastSeqNo = 3, батч пуст — поллинг

        // Act — ПОСЛЕ старта появляется новое событие
        journal.Append(ReplicationEventType.ObjectPut, "b", "new", 5000, "e", 1, "s", "h");
        await RunUntilAsync(() => _fixture.Transport.Sent.Count >= 1);

        // Assert — соседу уходит ТОЛЬКО новое событие (рассылка с конца журнала, §8)
        _fixture.Transport.Sent[0].Select(e => e.SeqNo).Should().ContainInOrder(4);
        cts.Cancel();
        await run;
    }

    [Fact]
    public async Task Metrics_QueueDepthAndDeliveryLag_ReflectWatermark()
    {
        // Arrange — 5 событий; ACK только 2 → queue_depth = 3
        var journal = Journal(5, startModMs: TimeProvider.System.GetUtcNow().ToUnixTimeMilliseconds() - 10_000);
        var cursors = _fixture.NewCursors();
        cursors.Advance("peer-a", 0);
        _fixture.Transport.Outcomes.Enqueue(2L);
        _fixture.Transport.Outcomes.Enqueue(null); // после ACK 2 — 429: watermark замирает на 2
        var dispatcher = _fixture.Create(journal, cursors);
        using var cts = new CancellationTokenSource();

        // Act
        var run = dispatcher.RunAsync(cts.Token);
        await RunUntilAsync(() => _fixture.Transport.Sent.Count >= 1);
        await RunUntilAsync(() => _fixture.Metrics.QueueDepthOf("peer-a") == 3);

        // Assert — queue_depth = LastSeqNo − watermark = 5 − 2; delivery_lag > 0
        // (старейшее неподтверждённое событие — 10 с назад)
        _fixture.Metrics.QueueDepthOf("peer-a").Should().Be(3);
        _fixture.Metrics.DeliveryLagOf("peer-a").Should().BeGreaterThan(0);
        cts.Cancel();
        await run;
    }
}
