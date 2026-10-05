using System.Text;
using FluentAssertions;
using PgWorker.Backups;
using PgWorker.WalReceiver;
using Shared.Core;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Главный цикл приёмника (t27, arch/19 §3, spec §3.1): инвариант подтверждения —
// SetConfirmed ТОЛЬКО после успешного S3-put (feedback-инвариант AC1), backpressure
// при отказе S3 (AC6-ядро), рестарт от хвоста S3, TLI/.history, permanent-коды
// (3 = segment-size, 4 = слот отсутствует, 6 = разрыв LSN), transient-reconnect.
public class WalReceiverCoreTests
{
    private const int SegmentBytes = 16 * 1024 * 1024;
    private const int Mib = 1024 * 1024;
    private static readonly TimeSpan FastRetry = TimeSpan.FromMilliseconds(20);

    private static WalReceiverOptions Options() => new(
        "s1a", 5432, "backup_exec", "pw", "postgres",
        "pgw_bkp_c1_shard1", "c1", "shard1",
        "http://minio:9000", null, "backups", "ak", "sk", true);

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
        => new(boundary, SegmentData(tli, boundary));

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
                Objects[key] = data;
                return Task.FromResult(Result.Success());
            }

            return Task.FromResult(Result.Failed(
                new ApplicationException($"S3 put {key}: тестовый отказ транспорта")));
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
        public ulong? ConfirmedLsn;
        public bool SegmentSizeMismatch;
        public bool ThrowOnStreamExhausted;
        public bool ThrownOnce;
        public readonly Dictionary<uint, byte[]?> History = [];
        public readonly List<string> ConfirmJournal = [];

        public Task<Result> OpenAsync(CancellationToken ct)
        {
            OpenCalls++;
            return Task.FromResult(SegmentSizeMismatch
                ? Result.Failed(new ApplicationException("wal_segment_size 32MB ≠ 16 MiB"))
                : Result.Success());
        }

        public Task<Result> StartReplicationAsync(string slot, ulong startLsn, CancellationToken ct)
        {
            StartCalls++;
            StartLsn = startLsn;
            return Task.FromResult(Result.Success());
        }

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
                    throw new IOException("тестовый обрыв транспорта");
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
                : Result<ulong>.Failed(new ApplicationException($"слот {slot} отсутствует")));
    }

    private static async Task<(int Code, string Output)> RunAsync(
        FakeXLogSource source, FakeS3 s3, FakeSlots slots, WalReceiverOptions options,
        CancellationToken ct)
    {
        var stdout = new StringWriter();
        var code = await WalReceiverCore.RunAsync(options, source, slots, s3, stdout, ct, FastRetry);
        return (code, stdout.ToString());
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

        Assert.Fail($"условие не выполнилось за бюджет ожидания; debug: {debug?.Invoke() ?? "нет"}");
    }

    [Fact]
    public async Task Feedback_инвариант_подтверждение_только_после_put()
    {
        // Arrange — 3 сегмента; отмена после появления 3-го в S3
        var source = new FakeXLogSource();
        foreach (var boundary in new[] { 0UL, (ulong)SegmentBytes, 2UL * SegmentBytes })
            source.Chunks.Enqueue(Chunk(boundary));
        var s3 = new FakeS3();
        var slots = new FakeSlots(0);
        using var cts = new CancellationTokenSource();

        var run = RunAsync(source, s3, slots, Options(), cts.Token);
        await WaitUntilAsync(() => s3.Objects.ContainsKey("c1/shard1/wal/000000010000000000000002"),
            TimeSpan.FromSeconds(10),
            () => $"keys=[{string.Join(",", s3.Objects.Keys)}] open={source.OpenCalls} start={source.StartCalls} " +
                  $"moveNext={source.MoveNextCalls} confirmed={source.ConfirmedLsn} journal=[{string.Join(",", source.ConfirmJournal)}]");
        cts.Cancel();
        var (code, _) = await run;

        // Assert — код чистой отмены; подтверждение = конец сегмента ...000002
        code.Should().Be(0);
        source.ConfirmedLsn.Should().Be(EndLsn(2));
        source.StartLsn.Should().Be(0); // пустой префикс → старт от restart_lsn=0

        // Assert — журнал подтверждений: ровно по одному на сегмент, строго по порядку
        // (confirm k происходит сразу после успешного put k — никогда не опережает)
        source.ConfirmJournal.Should().Equal(
            $"{EndLsn(0)}", $"{EndLsn(1)}", $"{EndLsn(2)}");
    }

    [Fact]
    public async Task Backpressure_отказ_S3_замирает_поток_и_доигрывается()
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

        var run = RunAsync(source, s3, slots, Options(), cts.Token);

        // Assert — до сбоя: сегмент ...000000 доставлен и подтверждён
        await WaitUntilAsync(() => source.ConfirmedLsn == EndLsn(0), TimeSpan.FromSeconds(10));
        var frozen = source.MoveNextCalls;

        // Act/Assert — backpressure: enumerator не двигается (put ...000001 в ретрае),
        // подтверждение не уходит за доставленное
        await Task.Delay(150, TestContext.Current.CancellationToken); // несколько retry-интервалов (20 мс)
        source.MoveNextCalls.Should().Be(frozen, "чтение потока остановлено до доставки сегмента ...000001");
        source.ConfirmedLsn.Should().Be(EndLsn(0), "подтверждение не опережает доставленное");
        s3.Objects.Keys.Should().NotContain(stuckKey);

        // Act — починка S3 (снятие отказа): доставка доигрывается
        s3.FailKeys.Clear();
        await WaitUntilAsync(() => source.ConfirmedLsn == EndLsn(2), TimeSpan.FromSeconds(10));
        cts.Cancel();
        var (code, _) = await run;

        // Assert
        code.Should().Be(0);
        s3.Objects.Should().ContainKey(stuckKey);
    }

    [Fact]
    public async Task Рестарт_дозасылает_от_хвоста_S3()
    {
        // Arrange — в S3 уже есть сегменты ...000000–...000001 (от предыдущего запуска)
        var s3 = new FakeS3();
        s3.Objects["c1/shard1/wal/000000010000000000000000"] = SegmentData(1, 0);
        s3.Objects["c1/shard1/wal/000000010000000000000001"] = SegmentData(1, SegmentBytes);
        var source = new FakeXLogSource();
        source.Chunks.Enqueue(Chunk(2UL * SegmentBytes));
        var slots = new FakeSlots(0);
        using var cts = new CancellationTokenSource();

        var run = RunAsync(source, s3, slots, Options(), cts.Token);
        await WaitUntilAsync(() => s3.Objects.ContainsKey("c1/shard1/wal/000000010000000000000002"),
            TimeSpan.FromSeconds(10));
        cts.Cancel();
        var (code, _) = await run;

        // Assert — старт НЕ от слота (0), а от конца непрерывного хвоста S3
        code.Should().Be(0);
        source.StartLsn.Should().Be(EndLsn(1));
    }

    [Fact]
    public async Task TLI_переход_history_put_и_history_missing()
    {
        // Arrange — сегмент ...000000 (TLI 1), сегмент ...000001 с header TLI 2, история доступна
        var source = new FakeXLogSource();
        source.Chunks.Enqueue(Chunk(0, tli: 1));
        source.Chunks.Enqueue(Chunk(SegmentBytes, tli: 2));
        source.History[2] = [1, 2, 3];
        var s3 = new FakeS3();
        var slots = new FakeSlots(0);
        using var cts = new CancellationTokenSource();

        var run = RunAsync(source, s3, slots, Options(), cts.Token);
        await WaitUntilAsync(() => s3.Objects.ContainsKey("c1/shard1/wal/000000020000000000000001"),
            TimeSpan.FromSeconds(10));
        cts.Cancel();
        var (code, _) = await run;

        // Assert — .history загружен в префикс
        code.Should().Be(0);
        s3.Objects.Should().ContainKey("c1/shard1/wal/00000002.history");
    }

    [Fact]
    public async Task TLI_переход_без_history_маркер_и_без_put()
    {
        // Arrange — история рантаймом не экспонируется (null)
        var source = new FakeXLogSource();
        source.Chunks.Enqueue(Chunk(0, tli: 1));
        source.Chunks.Enqueue(Chunk(SegmentBytes, tli: 2));
        var s3 = new FakeS3();
        var slots = new FakeSlots(0);
        using var cts = new CancellationTokenSource();

        var run = RunAsync(source, s3, slots, Options(), cts.Token);
        await WaitUntilAsync(() => s3.Objects.ContainsKey("c1/shard1/wal/000000020000000000000001"),
            TimeSpan.FromSeconds(10));
        cts.Cancel();
        var (code, output) = await run;

        // Assert — put истории не было; маркер history_missing в stdout
        code.Should().Be(0);
        s3.Objects.Keys.Should().NotContain("c1/shard1/wal/00000002.history");
        output.Should().Contain("\"history_missing\":\"2\"");
    }

    [Fact]
    public async Task Permanent_segment_size_mismatch_код_3()
    {
        // Arrange
        var source = new FakeXLogSource { SegmentSizeMismatch = true };
        var s3 = new FakeS3();
        var slots = new FakeSlots(0);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        // Act
        var (code, output) = await RunAsync(source, s3, slots, Options(), cts.Token);

        // Assert
        code.Should().Be(3);
        output.Should().Contain("wal_segment_size");
        source.OpenCalls.Should().Be(1, "permanent — без ретраев");
    }

    [Fact]
    public async Task Permanent_слот_отсутствует_код_4()
    {
        // Arrange — пустой S3 и слота нет
        var source = new FakeXLogSource();
        var s3 = new FakeS3();
        var slots = new FakeSlots(null);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        // Act
        var (code, output) = await RunAsync(source, s3, slots, Options(), cts.Token);

        // Assert
        code.Should().Be(4);
        output.Should().Contain("pgw_bkp_c1_shard1");
        source.StartCalls.Should().Be(0);
    }

    [Fact]
    public async Task Reconnect_после_обрыва_потока()
    {
        // Arrange — сегмент ...000000, обрыв, затем сегмент ...000001 в очереди;
        // tail-резолв после reconnect
        var source = new FakeXLogSource();
        source.Chunks.Enqueue(Chunk(0));
        source.ThrowOnStreamExhausted = true;
        var s3 = new FakeS3();
        var slots = new FakeSlots(0);
        using var cts = new CancellationTokenSource();

        var run = RunAsync(source, s3, slots, Options(), cts.Token);

        // Act — сегмент ...000001 подкладывается: он может быть доставлен только после
        // повторного OpenAsync (tail от хвоста S3 = конец сегмента ...000000)
        await WaitUntilAsync(() => source.OpenCalls >= 2, TimeSpan.FromSeconds(10));
        source.Chunks.Enqueue(Chunk(SegmentBytes));
        await WaitUntilAsync(() => s3.Objects.ContainsKey("c1/shard1/wal/000000010000000000000001"),
            TimeSpan.FromSeconds(10));
        cts.Cancel();
        var (code, output) = await run;

        // Assert
        code.Should().Be(0);
        source.OpenCalls.Should().BeGreaterThanOrEqualTo(2);
        output.Should().Contain("000000010000000000000001");
    }

    [Fact]
    public async Task Разрыв_LSN_потока_перманентный_код_6()
    {
        // Arrange — чанки с дырой в WalStart
        var source = new FakeXLogSource();
        source.Chunks.Enqueue(Chunk(0));
        source.Chunks.Enqueue(new XLogChunk(99UL * Mib, new byte[Mib]));
        var s3 = new FakeS3();
        var slots = new FakeSlots(0);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        // Act
        var (code, output) = await RunAsync(source, s3, slots, Options(), cts.Token);

        // Assert
        code.Should().Be(6);
        output.Should().Contain("разрыв");
    }
}
