using System.Text;
using System.Text.RegularExpressions;
using FluentAssertions;
using PgWorker.Backups;
using PgWorker.WalReceiver;
using Shared.Core;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Ядро приёмника как управляемая сессия (t25, arch/19 §3, spec §3.1 п.6):
// инвариант подтверждения — SetConfirmed ТОЛЬКО после успешного S3-put
// (feedback-инвариант AC1), backpressure при отказе S3 (AC6-ядро), рестарт от
// хвоста S3, TLI/.history-fallback ФС ноды, исходы сессии (WaitingSlot /
// FailedPermanent / RoleChanged) вместо exit-кодов, факты tli_jump /
// history_missing — события/исход вместо stdout-маркеров.
public class WalReceiverCoreTests
{
    private const int SegmentBytes = 16 * 1024 * 1024;
    private const int Mib = 1024 * 1024;
    private static readonly TimeSpan FastRetry = TimeSpan.FromMilliseconds(20);

    private static WalReceiverOptions Options() => new(
        "s1a", 5432, "backup_exec", "pw", "postgres",
        "pgw_bkp_c1_shard1", "c1", "shard1",
        ["http://s3a:9000"], ["http://s3a:9000"], "backups", "ak", "sk");

    /// <summary>Сегментные байты с long page header (tli, pageaddr=boundary).</summary>
    private static byte[] SegmentData(uint tli, ulong boundary)
    {
        var data = new byte[SegmentBytes];
        data[4] = (byte)tli;
        data[5] = (byte)(tli >> 8);
        data[6] = (byte)(tli >> 16);
        data[7] = (byte)(tli >> 24);
        for (var i = 0; i < 8; i++)
            data[8 + i] = (byte)(boundary >> (8 * i));
        return data;
    }

    private static XLogChunk Chunk(ulong boundary, uint tli = 1)
        => new(boundary, boundary + SegmentBytes, SegmentData(tli, boundary));

    private static ulong EndLsn(uint segId) => (segId + 1UL) << 24;

    /// <summary>Мини-фейк IBackupS3: put в словарь, ListWalAsync от словаря;
    /// FailKeys — УСТОЙЧИВЫЙ отказ (до ручного снятия — backpressure-тест).</summary>
    private sealed class FakeS3 : IBackupS3
    {
        public readonly Dictionary<string, byte[]> Objects = [];
        public readonly HashSet<string> FailKeys = [];

        public Task<Result> PutObjectAsync(string key, byte[] data, string? sha256, CancellationToken ct = default)
        {
            if (!FailKeys.Contains(key))
            {
                // Сегментные буферы берутся из пула и возвращаются после put:
                // фейк копирует байты, как реальный S3 (после Result данные
                // владельцем могут быть переиспользованы).
                Objects[key] = data.ToArray();
                return Task.FromResult(Result.Success());
            }

            return Task.FromResult(Result.Failed(
                new ApplicationException($"S3 put {key}: test transport failure")));
        }

        public Task<Result<IReadOnlyList<WalObject>>> ListWalAsync(
            string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default)
        {
            // WalObject.Name — последний компонент ключа (контракт IBackupS3.ListAsync)
            var objects = Objects.Keys
                .Where(k => k.StartsWith($"{cluster}/{shard}/wal/"))
                .Select(k => new WalObject(k[(k.LastIndexOf('/') + 1)..], DateTimeOffset.UnixEpoch))
                .ToList();
            return Task.FromResult(Result<IReadOnlyList<WalObject>>.Success(objects));
        }

        public Task<Result<bool>> BucketExistsAsync(CancellationToken ct) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<S3ObjectInfo>>> ListPrefixAsync(
            string prefix, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result> DeleteKeysAsync(IReadOnlyList<string> keys, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<WalObject>>> ListAsync(
            string cluster, string shard, string prefix, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<string>> GetObjectAsync(
            string cluster, string shard, string key, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<string>>> ListFullsAsync(
            string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<string>> DownloadTextAsync(
            string cluster, string shard, string objectKey, CancellationToken ct = default) => throw new NotSupportedException();
    }

    /// <summary>Программируемый фейк транспорта репликации: очередь чанков, счётчики,
    /// журнал подтверждений (LSN + размер S3 в момент подтверждения).</summary>
    private sealed class FakeXLogSource : IXLogReplicationSource
    {
        public readonly Queue<XLogChunk> Chunks = new();
        public int MoveNextCalls;
        public int OpenCalls;
        public int StartCalls;
        public ulong? StartLsn;
        public uint? LastTimeline;

        // tli-механика (58P01): политика старта (startLsn, timeline) → результат;
        // null — успех (прежнее поведение). Identify — Identity или Failed при null.
        // OpenFailuresRemaining — программный отказ процедурного open (счётчик
        // оставшихся открытий, которые должны упасть).
        public Func<ulong, uint?, Result>? StartPolicy;
        public readonly List<(ulong Lsn, uint? Timeline)> StartLog = [];
        public ServerIdentity? Identity = new(1, 0);
        public int OpenFailuresRemaining;

        // ПЕРВАЯ позиция START_REPLICATION: transient-переподключение стартует
        // заново от свежего хвоста S3 и перезаписывает StartLsn — ассерты старта
        // (от хвоста/слота) проверяют начальную позицию, а не исход гонки
        // «переподключение vs отмена теста».
        public ulong? FirstStartLsn;
        public ulong? ConfirmedLsn;
        public bool SegmentSizeMismatch;
        public bool ThrowOnStreamExhausted;
        public bool ThrownOnce;
        public readonly Dictionary<uint, byte[]?> History = [];
        public readonly List<string> ConfirmJournal = [];

        public Task<Result> OpenAsync(CancellationToken ct)
        {
            OpenCalls++;
            // Отказ — ТОЛЬКО процедурных открытий (после первого старта: основной
            // open цикла всегда предшествует START_REPLICATION; точка отказа
            // процедурного открытия — open досыпки/прыжка tli-процедуры).
            if (StartCalls > 0 && OpenFailuresRemaining > 0)
            {
                OpenFailuresRemaining--;
                return Task.FromResult(Result.Failed(
                    new ApplicationException("fake: open failed")));
            }

            return Task.FromResult(SegmentSizeMismatch
                ? Result.Failed(new ApplicationException("wal_segment_size 32MB ≠ 16 MiB"))
                : Result.Success());
        }

        public Task<Result> StartReplicationAsync(string slot, ulong startLsn, uint? timeline, CancellationToken ct)
        {
            StartCalls++;
            StartLsn = startLsn;
            LastTimeline = timeline;
            FirstStartLsn ??= startLsn;
            StartLog.Add((startLsn, timeline));
            if (StartPolicy is { } policy)
                return Task.FromResult(policy(startLsn, timeline));
            return Task.FromResult(Result.Success());
        }

        public Task<Result<ServerIdentity>> IdentifySystemAsync(CancellationToken ct)
            => Task.FromResult(Identity is { } id
                ? Result<ServerIdentity>.Success(id)
                : Result<ServerIdentity>.Failed(new ApplicationException("identify unavailable")));

        public IAsyncEnumerable<XLogChunk> Stream => StreamImpl();

        private async IAsyncEnumerable<XLogChunk> StreamImpl()
        {
            while (true)
            {
                MoveNextCalls++;
                if (Chunks.Count > 0)
                {
                    yield return Chunks.Dequeue();
                    continue;
                }

                if (ThrowOnStreamExhausted && !ThrownOnce)
                {
                    ThrownOnce = true;
                    throw new IOException("test transport drop");
                }

                yield break; // сервер закрыл поток — transient-переподключение
            }
        }

        public Task<Result> SetConfirmedAsync(ulong confirmedLsn, CancellationToken ct)
        {
            ConfirmedLsn = confirmedLsn;
            ConfirmJournal.Add($"{confirmedLsn}");
            return Task.FromResult(Result.Success());
        }

        public Task<Result<byte[]?>> ReadTimelineHistoryAsync(uint tli, CancellationToken ct)
            => Task.FromResult(History.TryGetValue(tli, out var content)
                ? Result<byte[]?>.Success(content)
                : Result<byte[]?>.Success(null));

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    /// <summary>Слот-ридер: фиксируемое значение или Failed («слот отсутствует»).</summary>
    private sealed class FakeSlots(ulong? restartLsn) : ISlotPositionReader
    {
        public Task<Result<ulong>> ReadRestartLsnAsync(string slot, CancellationToken ct)
            => Task.FromResult(restartLsn is { } lsn
                ? Result<ulong>.Success(lsn)
                : Result<ulong>.Failed(new ApplicationException($"slot {slot} is missing")));
    }

    /// <summary>Прогон одной сессии ядра: исход + diagnostic-журнал (колбэк
    /// Diagnostic вместо stdout) + собранные факты tli-прыжков (живые события
    /// TliJump — факт публикуется сразу при прыжке, до исхода сессии). Отмена —
    /// нормальный финал теста (SIGTERM-семантика): Outcome=null, факты/журнал
    /// сохранены.</summary>
    private static async Task<(WalSessionOutcome? Outcome, string Output, List<WalReceiverTliJump> Jumps)> RunSession(
        FakeXLogSource source, FakeS3 s3, FakeSlots slots, WalReceiverOptions options,
        CancellationToken ct)
    {
        // Arrange —Diagnostic-журнал + подписка на факты прыжков
        var output = new StringWriter();
        var jumps = new List<WalReceiverTliJump>();
        var events = new WalSessionEvents
        {
            Diagnostic = output.WriteLine,
            TliJump = jumps.Add,
        };

        // Act — сессия до исхода или до отмены теста
        try
        {
            var outcome = await WalReceiverCore.RunSessionAsync(options, source, slots, s3, events, ct, FastRetry);
            return (outcome, output.ToString(), jumps);
        }
        catch (OperationCanceledException)
        {
            return (null, output.ToString(), jumps);
        }
    }

    /// <summary>Ожидание условия с бюджетом (таймауты тестов короткие).</summary>
    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan budget, Func<string>? debug = null)
    {
        var deadline = DateTime.UtcNow + budget;
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
                return;
            await Task.Delay(10, TestContext.Current.CancellationToken);
        }

        Assert.Fail($"the condition was not met within the wait budget; debug: {debug?.Invoke() ?? "none"}");
    }

    [Fact]
    public async Task Feedback_Invariant_ConfirmationOnlyAfterPut()
    {
        // Arrange — 3 сегмента; отмена после появления 3-го в S3
        var source = new FakeXLogSource();
        foreach (var boundary in new[] { 0UL, (ulong)SegmentBytes, 2UL * SegmentBytes })
            source.Chunks.Enqueue(Chunk(boundary));
        var s3 = new FakeS3();
        var slots = new FakeSlots(0);
        using var cts = new CancellationTokenSource();

        var run = RunSession(source, s3, slots, Options(), cts.Token);
        await WaitUntilAsync(() => s3.Objects.ContainsKey("c1/shard1/wal/000000010000000000000002"),
            TimeSpan.FromSeconds(10),
            () => $"keys=[{string.Join(",", s3.Objects.Keys)}] open={source.OpenCalls} start={source.StartCalls} " +
                  $"moveNext={source.MoveNextCalls} confirmed={source.ConfirmedLsn} journal=[{string.Join(",", source.ConfirmJournal)}]");
        cts.Cancel();
        var (outcome, _, _) = await run;

        // Assert — чистая отмена (outcome не случился); подтверждение = конец сегмента ...000002
        outcome.Should().BeNull("graceful cancellation is the normal end of a session");
        source.ConfirmedLsn.Should().Be(EndLsn(2));
        source.FirstStartLsn.Should().Be(0); // пустой префикс → старт от restart_lsn=0

        // Assert — журнал подтверждений: ровно по одному на сегмент, строго по порядку
        // (confirm k происходит сразу после успешного put k — никогда не опережает)
        source.ConfirmJournal.Should().Equal(
            $"{EndLsn(0)}", $"{EndLsn(1)}", $"{EndLsn(2)}");
    }

    [Fact]
    public async Task Backpressure_S3Failure_FreezesStream_AndFinishesInFlight()
    {
        // Arrange — 3 сегмента; УСТОЙЧИВЫЙ отказ put сегмента ...000001 (до починки)
        var source = new FakeXLogSource();
        foreach (var boundary in new[] { 0UL, (ulong)SegmentBytes, 2UL * SegmentBytes })
            source.Chunks.Enqueue(Chunk(boundary));
        var s3 = new FakeS3();
        const string stuckKey = "c1/shard1/wal/000000010000000000000001";
        s3.FailKeys.Add(stuckKey);
        var slots = new FakeSlots(0);
        using var cts = new CancellationTokenSource();

        var run = RunSession(source, s3, slots, Options(), cts.Token);

        // Assert — до сбоя: сегмент ...000000 доставлен и подтверждён
        await WaitUntilAsync(() => source.ConfirmedLsn == EndLsn(0), TimeSpan.FromSeconds(10));
        var frozen = source.MoveNextCalls;

        // Act/Assert — backpressure: enumerator не двигается (put ...000001 в ретрае),
        // подтверждение не уходит за доставленное
        await Task.Delay(150, TestContext.Current.CancellationToken); // несколько retry-интервалов (20 мс)
        source.MoveNextCalls.Should().Be(frozen, "stream reading is stopped before delivering segment ...000001");
        source.ConfirmedLsn.Should().Be(EndLsn(0), "confirmation never gets ahead of what was delivered");
        s3.Objects.Keys.Should().NotContain(stuckKey);

        // Act — починка S3 (снятие отказа): доставка доигрывается
        s3.FailKeys.Clear();
        await WaitUntilAsync(() => source.ConfirmedLsn == EndLsn(2), TimeSpan.FromSeconds(10));
        cts.Cancel();
        var (outcome, _, _) = await run;

        // Assert — сессия прервана отменой теста; сегмент доставлен
        outcome.Should().BeNull();
        s3.Objects.Should().ContainKey(stuckKey);
    }

    [Fact]
    public async Task Restart_ResendsFromS3Tail()
    {
        // Arrange — в S3 уже есть сегменты ...000000–...000001 (от предыдущего запуска)
        var s3 = new FakeS3();
        s3.Objects["c1/shard1/wal/000000010000000000000000"] = SegmentData(1, 0);
        s3.Objects["c1/shard1/wal/000000010000000000000001"] = SegmentData(1, SegmentBytes);
        var source = new FakeXLogSource();
        source.Chunks.Enqueue(Chunk(2UL * SegmentBytes));
        var slots = new FakeSlots(0);
        using var cts = new CancellationTokenSource();

        var run = RunSession(source, s3, slots, Options(), cts.Token);
        await WaitUntilAsync(() => s3.Objects.ContainsKey("c1/shard1/wal/000000010000000000000002"),
            TimeSpan.FromSeconds(10));
        cts.Cancel();
        var (outcome, _, _) = await run;

        // Assert — старт НЕ от слота (0), а от конца непрерывного хвоста S3
        // (первая START_REPLICATION: reconnect после отмены не участвует)
        outcome.Should().BeNull();
        source.FirstStartLsn.Should().Be(EndLsn(1));
    }

    [Fact]
    public async Task TliSwitch_HistoryPut()
    {
        // Arrange — сегмент ...000000 (TLI 1), сегмент ...000001 с header TLI 2, история доступна
        var source = new FakeXLogSource();
        source.Chunks.Enqueue(Chunk(0, tli: 1));
        source.Chunks.Enqueue(Chunk(SegmentBytes, tli: 2));
        source.History[2] = [1, 2, 3];
        var s3 = new FakeS3();
        var slots = new FakeSlots(0);
        using var cts = new CancellationTokenSource();

        var run = RunSession(source, s3, slots, Options(), cts.Token);
        await WaitUntilAsync(() => s3.Objects.ContainsKey("c1/shard1/wal/000000020000000000000001"),
            TimeSpan.FromSeconds(10));
        cts.Cancel();
        var (outcome, _, _) = await run;

        // Assert — .history загружен в префикс
        outcome.Should().BeNull();
        s3.Objects.Should().ContainKey("c1/shard1/wal/00000002.history");
    }

    [Fact]
    public async Task TliSwitch_WithoutHistory_Fact_AndNoPut()
    {
        // Arrange — история рантаймом не экспонируется (null); ФС ноды файла не
        // имеет (HistoryFilePath не задан — прод-дефолт pg_wal тоже пуст)
        var source = new FakeXLogSource();
        source.Chunks.Enqueue(Chunk(0, tli: 1));
        source.Chunks.Enqueue(Chunk(SegmentBytes, tli: 2));
        var s3 = new FakeS3();
        var slots = new FakeSlots(0);
        using var cts = new CancellationTokenSource();

        var run = RunSession(source, s3, slots, Options(), cts.Token);
        await WaitUntilAsync(() => s3.Objects.ContainsKey("c1/shard1/wal/000000020000000000000001"),
            TimeSpan.FromSeconds(10));
        cts.Cancel();
        var (outcome, output, _) = await run;

        // Assert — put истории не было; факт history_missing в диагностике сессии
        // (текст уходит в error статус-ключа, spec §3.1 п.6)
        outcome.Should().BeNull();
        s3.Objects.Keys.Should().NotContain("c1/shard1/wal/00000002.history");
        output.Should().Contain("history_missing tli=2");
    }

    [Fact]
    public async Task HistoryFallback_Filesystem()
    {
        // Arrange — рантайм истории не отдаёт; файл pg_wal ноды существует
        // (HistoryFilePath — подмена прод-пути pg_wal на tmpdir теста)
        var source = new FakeXLogSource();
        source.Chunks.Enqueue(Chunk(0, tli: 1));
        source.Chunks.Enqueue(Chunk(SegmentBytes, tli: 2));
        var s3 = new FakeS3();
        var slots = new FakeSlots(0);
        var historyPath = Path.Combine(Path.GetTempPath(), $"pgw-hist-{Guid.NewGuid():N}", "00000002.history");
        Directory.CreateDirectory(Path.GetDirectoryName(historyPath)!);
        await File.WriteAllTextAsync(historyPath, "1/0 2/100\n", TestContext.Current.CancellationToken);
        using var cts = new CancellationTokenSource();

        var output = new StringWriter();
        var events = new WalSessionEvents
        {
            Diagnostic = output.WriteLine,
            HistoryFilePath = tli => tli == 2 ? historyPath : null,
        };
        var session = WalReceiverCore.RunSessionAsync(
            Options(), source, slots, s3, events, cts.Token, FastRetry);
        try
        {
            await WaitUntilAsync(() => s3.Objects.ContainsKey("c1/shard1/wal/000000020000000000000001"),
                TimeSpan.FromSeconds(10));
        }
        finally
        {
            cts.Cancel(); // чистая отмена при любом исходе ожидания
        }

        try
        {
            await session;
        }
        catch (OperationCanceledException)
        {
            // чистая отмена — нормальный финал
        }

        // Assert — история взята из ФС ноды и положена в S3 (fallback §3.1 п.6)
        s3.Objects.Should().ContainKey("c1/shard1/wal/00000002.history");
    }

    [Fact]
    public async Task Permanent_SegmentSizeMismatch_FailedPermanent()
    {
        // Arrange
        var source = new FakeXLogSource { SegmentSizeMismatch = true };
        var s3 = new FakeS3();
        var slots = new FakeSlots(0);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        // Act
        var (outcome, output, _) = await RunSession(source, s3, slots, Options(), cts.Token);

        // Assert — исход FailedPermanent (НЕ exit процесса), текст ошибки —
        // в исходе сессии (состояние статус-ключа, spec §3.1 п.6)
        outcome.Should().NotBeNull();
        outcome!.Kind.Should().Be(WalSessionKind.FailedPermanent);
        outcome.Error.Should().Contain("wal_segment_size");
        source.OpenCalls.Should().Be(1, "permanent — no retries");
    }

    [Fact]
    public async Task SlotMissing_WaitingSlot_Transient()
    {
        // Arrange — пустой S3 и слота нет
        var source = new FakeXLogSource();
        var s3 = new FakeS3();
        var slots = new FakeSlots(null);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        // Act
        var (outcome, _, _) = await RunSession(source, s3, slots, Options(), cts.Token);

        // Assert — слота нет: transient-ожидание (ensure делает воркер),
        // НЕ permanent; старт репликации не начинался
        outcome.Should().NotBeNull();
        outcome!.Kind.Should().Be(WalSessionKind.WaitingSlot);
        source.StartCalls.Should().Be(0);
    }

    [Fact]
    public async Task Reconnect_AfterStreamDrop()
    {
        // Arrange — сегмент ...000000, обрыв, затем сегмент ...000001 в очереди;
        // tail-резолв после reconnect
        var source = new FakeXLogSource();
        source.Chunks.Enqueue(Chunk(0));
        source.ThrowOnStreamExhausted = true;
        var s3 = new FakeS3();
        var slots = new FakeSlots(0);
        using var cts = new CancellationTokenSource();

        var run = RunSession(source, s3, slots, Options(), cts.Token);

        // Act — сегмент ...000001 подкладывается: он может быть доставлен только после
        // повторного OpenAsync (tail от хвоста S3 = конец сегмента ...000000)
        await WaitUntilAsync(() => source.OpenCalls >= 2, TimeSpan.FromSeconds(10));
        source.Chunks.Enqueue(Chunk(SegmentBytes));
        await WaitUntilAsync(() => s3.Objects.ContainsKey("c1/shard1/wal/000000010000000000000001"),
            TimeSpan.FromSeconds(10));
        cts.Cancel();
        var (outcome, output, _) = await run;

        // Assert
        outcome.Should().BeNull();
        source.OpenCalls.Should().BeGreaterThanOrEqualTo(2);
    }

    [Fact]
    public async Task StreamLsnGap_Permanent()
    {
        // Arrange — чанки с дырой в WalStart
        var source = new FakeXLogSource();
        source.Chunks.Enqueue(Chunk(0));
        source.Chunks.Enqueue(new XLogChunk(99UL * Mib, 100UL * Mib, new byte[Mib]));
        var s3 = new FakeS3();
        var slots = new FakeSlots(0);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        // Act
        var (outcome, output, _) = await RunSession(source, s3, slots, Options(), cts.Token);

        // Assert — разрыв LSN протокола: FailedPermanent с текстом ошибки
        outcome.Should().NotBeNull();
        outcome!.Kind.Should().Be(WalSessionKind.FailedPermanent);
        outcome.Error.Should().Contain("gap");
    }

    [Fact]
    public async Task TliBackfill_ClosesGap_NoRpoLoss()
    {
        // Arrange — в S3 сегменты TLI1 №0–1; старт без TIMELINE от хвоста → 58P01
        var source = new FakeXLogSource();
        SeedChain(out var s3, new[] { 0U, 1U }, tli: 1);
        var refusalsRemaining = 1;
        source.StartPolicy = (lsn, timeline) => timeline is null && refusalsRemaining-- > 0
            ? Result.Failed(new WalStartRefused("fake: requested WAL segment has already been removed"))
            : Result.Success();
        source.Identity = new ServerIdentity(2, 3UL * SegmentBytes + 5UL * Mib);
        source.History[2] = [1, 2, 3];
        // досыпка timeline:1 от того же хвоста: сегмент TLI1 №2; после конца потока —
        // переподключение от выросшего хвоста: сегмент TLI2 №3
        source.Chunks.Enqueue(Chunk(2UL * SegmentBytes));
        source.Chunks.Enqueue(Chunk(3UL * SegmentBytes, tli: 2));
        var slots = new FakeSlots(0);
        using var cts = new CancellationTokenSource();

        var run = RunSession(source, s3, slots, Options(), cts.Token);
        await WaitUntilAsync(() => s3.Objects.ContainsKey("c1/shard1/wal/000000020000000000000003"),
            TimeSpan.FromSeconds(10),
            () => $"keys=[{string.Join(",", s3.Objects.Keys)}] open={source.OpenCalls} " +
                  $"start=[{string.Join(";", source.StartLog.Select(e => $"{e.Lsn}/{e.Timeline}"))}]");
        cts.Cancel();
        var (outcome, output, jumps) = await run;

        // Assert — досыпка закрыла зазор: диагностический лог backfill в журнале,
        // .history в S3, факта прыжка НЕТ (прыжок-факт — только при fallback);
        // OpenCalls >= 2 (стартовый open цикла + процедурный open перед досыпкой —
        // процедура НЕ стартует на погашенном соединении).
        outcome.Should().BeNull();
        output.Should().Contain("tli_procedure: backfill");
        output.Should().NotContain("tli_procedure: jump");
        jumps.Should().BeEmpty();
        s3.Objects.Should().ContainKey("c1/shard1/wal/00000002.history");
        source.OpenCalls.Should().BeGreaterThanOrEqualTo(2);

        // Assert — ветка досыпки: подтверждение слота строго после put сегмента —
        // журнал ровно по одному на доставленный сегмент, строго по порядку
        // (confirm k — сразу после успешного put k, значение — конец только что
        // положенного сегмента: досыпка TLI1 №2 → EndLsn(2), затем TLI2 №3 → EndLsn(3))
        source.ConfirmJournal.Should().Equal($"{EndLsn(2)}", $"{EndLsn(3)}");
    }

    [Fact]
    public async Task TliBackfillRefused_FallsBackToJump()
    {
        // Arrange — отказ старта и без TIMELINE, и с timeline:1 (сегменты срезаны)
        var source = new FakeXLogSource();
        SeedChain(out var s3, new[] { 0U, 1U }, tli: 1);
        source.StartPolicy = (_, _) =>
            Result.Failed(new WalStartRefused("fake: requested WAL segment has already been removed"));
        source.Identity = new ServerIdentity(2, 3UL * SegmentBytes + 5UL * Mib);
        source.History[2] = [1, 2, 3];
        var slots = new FakeSlots(0);
        using var cts = new CancellationTokenSource();

        var run = RunSession(source, s3, slots, Options(), cts.Token);
        await WaitUntilAsync(() => source.StartLog.Any(e => e.Timeline == 2),
            TimeSpan.FromSeconds(10),
            () => $"open={source.OpenCalls} start=[{string.Join(";", source.StartLog.Select(e => $"{e.Lsn}/{e.Timeline}"))}]");
        cts.Cancel();
        var (outcome, output, jumps) = await run;

        // Assert — факт прыжка опубликован событием (живая публикация, до исхода
        // сессии): прыжок ровно от границы сегмента xlogpos (3*SegmentBytes) с
        // timeline:2; диагностический журнал несёт обе ветки (backfill → jump);
        // OpenCalls >= 3 (стартовый + open перед досыпкой + open перед прыжком —
        // отказ досыпки гасит соединение).
        outcome.Should().BeNull();
        jumps.Should().Contain(j => j.To == "000000020000000000000003" && j.Tli == 2 && j.From == "000000010000000000000001");
        output.Should().Contain("tli_procedure: backfill");
        output.Should().Contain("tli_procedure: jump");
        source.StartLog.Should().Contain(e => e.Lsn == 3UL * SegmentBytes && e.Timeline == 2);
        source.OpenCalls.Should().BeGreaterThanOrEqualTo(3);
    }

    [Fact]
    public async Task TliJump_SameTli_NoBackfillNoHistory()
    {
        // Arrange — сервер на том же TLI, что хвост: зазор внутри одного таймлайна
        var source = new FakeXLogSource();
        SeedChain(out var s3, new[] { 0U, 1U }, tli: 1);
        source.StartPolicy = (_, _) =>
            Result.Failed(new WalStartRefused("fake: requested WAL segment has already been removed"));
        source.Identity = new ServerIdentity(1, 3UL * SegmentBytes + 5UL * Mib);
        var slots = new FakeSlots(0);
        using var cts = new CancellationTokenSource();

        var run = RunSession(source, s3, slots, Options(), cts.Token);
        await WaitUntilAsync(() => source.StartLog.Any(e => e.Timeline == 1),
            TimeSpan.FromSeconds(10),
            () => $"open={source.OpenCalls} start=[{string.Join(";", source.StartLog.Select(e => $"{e.Lsn}/{e.Timeline}"))}]");
        cts.Cancel();
        var (outcome, output, jumps) = await run;

        // Assert — сразу прыжок (timeline:1 от границы): нет досыпки,
        // нет put .history (TLI 1 истории не имеет — не запрашивается);
        // факт прыжка с from_tli == to_tli == 1.
        outcome.Should().BeNull();
        jumps.Should().Contain(j => j.Tli == 1 && j.From == "000000010000000000000001");
        output.Should().NotContain("tli_procedure: backfill");
        s3.Objects.Keys.Should().NotContain("c1/shard1/wal/00000001.history");
    }

    [Fact]
    public async Task TliProcedure_IdentifyFails_PlainRetry()
    {
        // Arrange — отказ старта + отказ IDENTIFY_SYSTEM
        var source = new FakeXLogSource();
        SeedChain(out var s3, new[] { 0U, 1U }, tli: 1);
        source.StartPolicy = (_, _) =>
            Result.Failed(new WalStartRefused("fake: requested WAL segment has already been removed"));
        source.Identity = null; // IdentifySystemAsync → Failed
        var slots = new FakeSlots(0);
        using var cts = new CancellationTokenSource();

        var run = RunSession(source, s3, slots, Options(), cts.Token);
        await WaitUntilAsync(() => source.OpenCalls >= 2, TimeSpan.FromSeconds(10),
            () => $"open={source.OpenCalls} start={source.StartCalls}");
        cts.Cancel();
        var (outcome, output, jumps) = await run;

        // Assert — ретраи transient-цикла; ни одной ветки процедуры (identify
        // падает до любых веток), факта прыжка нет.
        outcome.Should().BeNull();
        source.OpenCalls.Should().BeGreaterThanOrEqualTo(2);
        output.Should().NotContain("tli_procedure: backfill");
        output.Should().NotContain("tli_procedure: jump");
        jumps.Should().BeEmpty();
    }

    [Fact]
    public async Task TliProcedure_HistoryMissing_StillBackfills()
    {
        // Arrange — история нового TLI рантаймом не отдаётся (null); файла ФС нет
        var source = new FakeXLogSource();
        SeedChain(out var s3, new[] { 0U, 1U }, tli: 1);
        var refusalsRemaining = 1;
        source.StartPolicy = (lsn, timeline) => timeline is null && refusalsRemaining-- > 0
            ? Result.Failed(new WalStartRefused("fake: requested WAL segment has already been removed"))
            : Result.Success();
        source.Identity = new ServerIdentity(2, 3UL * SegmentBytes + 5UL * Mib);
        // History пусто: ReadTimelineHistoryAsync → Success(null)
        source.Chunks.Enqueue(Chunk(2UL * SegmentBytes));
        source.Chunks.Enqueue(Chunk(3UL * SegmentBytes, tli: 2));
        var slots = new FakeSlots(0);
        using var cts = new CancellationTokenSource();

        var run = RunSession(source, s3, slots, Options(), cts.Token);
        await WaitUntilAsync(() => s3.Objects.ContainsKey("c1/shard1/wal/000000020000000000000003"),
            TimeSpan.FromSeconds(10),
            () => $"keys=[{string.Join(",", s3.Objects.Keys)}] open={source.OpenCalls}");
        cts.Cancel();
        var (outcome, output, jumps) = await run;

        // Assert — факт history_missing в диагностике + досыпка продолжается штатно
        outcome.Should().BeNull();
        output.Should().Contain("history_missing tli=2");
        output.Should().Contain("tli_procedure: backfill");
        output.Should().NotContain("tli_procedure: jump");
        jumps.Should().BeEmpty();
        s3.Objects.Should().ContainKey("c1/shard1/wal/000000020000000000000003");
    }

    [Fact]
    public async Task TliProcedure_OpenFails_PlainRetry()
    {
        // Arrange — отказ старта 58P01, процедурный OpenAsync досыпки → Failed
        // (счётчик OpenFailuresRemaining в фейке): процедура не стартовала —
        // обычный transient-цикл, ни досыпка, ни прыжок не стартовали (каждый
        // старт процедуры — на СВЕЖЕМ соединении; после отказа open вернуться
        // на погашенное нельзя).
        var source = new FakeXLogSource();
        SeedChain(out var s3, new[] { 0U, 1U }, tli: 1);
        var refusalsRemaining = 1;
        source.StartPolicy = (lsn, timeline) => timeline is null && refusalsRemaining-- > 0
            ? Result.Failed(new WalStartRefused("fake: requested WAL segment has already been removed"))
            : Result.Success();
        source.Identity = new ServerIdentity(2, 3UL * SegmentBytes + 5UL * Mib);
        source.History[2] = [1, 2, 3];
        source.OpenFailuresRemaining = 1;
        source.Chunks.Enqueue(Chunk(2UL * SegmentBytes));
        var slots = new FakeSlots(0);
        using var cts = new CancellationTokenSource();

        var run = RunSession(source, s3, slots, Options(), cts.Token);
        await WaitUntilAsync(() => s3.Objects.ContainsKey("c1/shard1/wal/000000010000000000000002"),
            TimeSpan.FromSeconds(10),
            () => $"keys=[{string.Join(",", s3.Objects.Keys)}] open={source.OpenCalls} " +
                  $"start=[{string.Join(";", source.StartLog.Select(e => $"{e.Lsn}/{e.Timeline}"))}]");
        cts.Cancel();
        var (outcome, output, jumps) = await run;

        // Assert — OpenCalls >= 2 (стартовый + отказавший процедурный); прыжка
        // нет; стартов с явным TIMELINE нет (отказ open досыпки предотвратил и
        // досыпку, и прыжок этой итерации).
        outcome.Should().BeNull();
        source.OpenCalls.Should().BeGreaterThanOrEqualTo(2);
        output.Should().NotContain("tli_procedure: jump");
        jumps.Should().BeEmpty();
        source.StartLog.Should().OnlyContain(e => e.Timeline == null);
    }

    [Fact]
    public async Task TliProcedure_RePromoteDuringProcedure_Converges()
    {
        // Arrange — сервер промоутнулся на T=3 между итерациями процедуры:
        // 1-й цикл — identify=(2,…), досыпка timeline:1 → 58P01, прыжок
        // timeline:2 → снова 58P01 → Success(null) → transient retry-цикл;
        // 2-й цикл — identify=(3,…), .history(3) put, досыпка timeline:1 → успех.
        var source = new FakeXLogSource();
        SeedChain(out var s3, new[] { 0U, 1U }, tli: 1);
        source.Identity = new ServerIdentity(2, 3UL * SegmentBytes + 5UL * Mib);
        source.History[2] = [1, 2, 3];
        source.History[3] = [1, 2, 3, 4];
        var attempts = 0;
        source.StartPolicy = (_, timeline) =>
        {
            attempts++;
            // Смена истины между итерациями: повторный промоут на T=3
            // (первый identify=2 уже случился — прыжок timeline:2 отказывает)
            if (attempts == 3)
                source.Identity = new ServerIdentity(3, 3UL * SegmentBytes + 5UL * Mib);
            return attempts switch
            {
                1 or 2 or 3 or 4 => Result.Failed(
                    new WalStartRefused("fake: requested WAL segment has already been removed")),
                _ => Result.Success(),
            };
        };
        // Досыпка 2-го цикла: сегмент TLI1 №2; после CopyDone — переподключение
        // от выросшего хвоста: сегмент TLI3 №3 (история не перечитывается —
        // prevTli сессии инициализируется первым сегментом)
        source.Chunks.Enqueue(Chunk(2UL * SegmentBytes));
        source.Chunks.Enqueue(Chunk(3UL * SegmentBytes, tli: 3));
        var slots = new FakeSlots(0);
        using var cts = new CancellationTokenSource();

        var run = RunSession(source, s3, slots, Options(), cts.Token);
        await WaitUntilAsync(() => s3.Objects.ContainsKey("c1/shard1/wal/000000030000000000000003"),
            TimeSpan.FromSeconds(10),
            () => $"keys=[{string.Join(",", s3.Objects.Keys)}] open={source.OpenCalls} " +
                  $"start=[{string.Join(";", source.StartLog.Select(e => $"{e.Lsn}/{e.Timeline}"))}]");
        cts.Cancel();
        var (outcome, output, jumps) = await run;

        // Assert — две итерации процедуры (два старта досыпки в журнале; паттерн
        // «backfill segment» — именно старт досыпки, отказ досыпки пишет строку
        // «backfill refused» — в счёт не попадает), факт прыжка первого цикла
        // (to_tli:2); конвергенция без счётчиков попыток: оба
        // .history в S3, досыпка доставила TLI1 №2, доставка продолжилась TLI3
        // №3; каждый старт процедуры — на свежем соединении (OpenCalls >= 5:
        // стартовый цикл + open досыпки + open прыжка + стартовый + open
        // досыпки 2-го цикла).
        outcome.Should().BeNull();
        Regex.Matches(output, "tli_procedure: backfill segment")
            .Should().HaveCount(2, "two procedure iterations (T=2 refused, T=3 backfilled)");
        jumps.Should().Contain(j => j.Tli == 2 && j.To == "000000020000000000000003");
        s3.Objects.Should().ContainKey("c1/shard1/wal/00000002.history");
        s3.Objects.Should().ContainKey("c1/shard1/wal/00000003.history");
        s3.Objects.Should().ContainKey("c1/shard1/wal/000000010000000000000002");
        source.StartLog.Should().Contain(e => e.Lsn == 3UL * SegmentBytes && e.Timeline == 2);
        source.OpenCalls.Should().BeGreaterThanOrEqualTo(5);
    }

    [Fact]
    public async Task WalStartRefused_EmptyS3Tail_PlainRetry()
    {
        // Arrange — пустой S3, старт от restart_lsn слота; процедура неприменима
        // (tailSegment == null — вход не определён)
        var source = new FakeXLogSource();
        var s3 = new FakeS3();
        source.StartPolicy = (_, _) =>
            Result.Failed(new WalStartRefused("fake: requested WAL segment has already been removed"));
        source.Identity = new ServerIdentity(2, 3UL * SegmentBytes + 5UL * Mib);
        var slots = new FakeSlots(5 << 24);
        using var cts = new CancellationTokenSource();

        var run = RunSession(source, s3, slots, Options(), cts.Token);
        await WaitUntilAsync(() => source.StartCalls >= 2, TimeSpan.FromSeconds(10),
            () => $"open={source.OpenCalls} start={source.StartCalls}");
        cts.Cancel();
        var (outcome, output, jumps) = await run;

        // Assert — ретраи БЕЗ tli-веток (transient retry-цикл).
        outcome.Should().BeNull();
        source.StartCalls.Should().BeGreaterThanOrEqualTo(2);
        output.Should().NotContain("tli_procedure: backfill");
        output.Should().NotContain("tli_procedure: jump");
        jumps.Should().BeEmpty();
    }

    /// <summary>Посев непрерывной цепочки сегментов TLI 1 в мини-S3 фейка.</summary>
    private static void SeedChain(out FakeS3 s3, uint[] segments, uint tli)
    {
        s3 = new FakeS3();
        foreach (var segId in segments)
            s3.Objects[$"c1/shard1/wal/00000001{segId / WalFileName.SegsPerLog:x8}{segId % WalFileName.SegsPerLog:x8}"]
                = SegmentData(tli, segId * SegmentBytes);
    }
}
