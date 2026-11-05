using PgWorker.Backups;
using PgWorker.PgNode.BaseBackup;

namespace PgWorker.UnitTests.PgNode;

// Нарезка tar-потока на multipart-части (spec t01 §3.4 п.2): части 16 МиБ,
// ≤4 в полёте, финальный остаток — CompleteAsync, первая ошибка части гасит
// все последующие Write/Complete.
public class MultipartPartBufferStreamTests
{
    private const int MiB = 1024 * 1024;

    // AAA: 40 МиБ → две полных части 16 МиБ + финальный остаток 8 МиБ;
    // BytesDone — все записанные байты, PartsDone — завершённые полные части.
    [Fact]
    public async Task Writes40MiB_TwoFullPartsPlusTail()
    {
        // Arrange
        var session = new FakeSession();
        var stream = new MultipartPartBufferStream(session);
        var chunk = new byte[MiB];

        // Act
        for (var written = 0; written < 40 * MiB; written += chunk.Length)
            await stream.WriteAsync(chunk, CancellationToken.None);
        var bytesDone = stream.BytesDone;
        var partsDone = stream.PartsDone;
        var completed = await stream.CompleteAsync(CancellationToken.None);

        // Assert
        completed.IsSuccess.Should().BeTrue();
        completed.Value.Should().Be("etag-object");
        bytesDone.Should().Be(40 * MiB);
        partsDone.Should().Be(2);
        session.Parts.Select(p => p.PartNumber).Should().Equal(1, 2, 3);
        session.Parts[0].Size.Should().Be(16 * MiB);
        session.Parts[1].Size.Should().Be(16 * MiB);
        session.Parts[2].Size.Should().Be(8 * MiB); // хвост < 16 МиБ
        session.CompletedWith.Should().NotBeNull();
        session.CompletedWith!.Keys.Should().BeEquivalentTo(new[] { 1, 2, 3 });
    }

    // AAA: отказ 2-й части → CompleteAsync Failed с сообщением фейка;
    // AbortAsync вызывается вручную и поглощает отказ abort'а.
    [Fact]
    public async Task PartFailure_CompleteReturnsFailed_AbortSwallowsFailure()
    {
        // Arrange
        var session = new FakeSession(failPartNumber: 2);
        var stream = new MultipartPartBufferStream(session);
        var chunk = new byte[MiB];

        // Act — ровно две полных части: отказ второй, новых Write нет.
        for (var written = 0; written < 32 * MiB; written += chunk.Length)
            await stream.WriteAsync(chunk, CancellationToken.None);
        var completed = await stream.CompleteAsync(CancellationToken.None);
        await stream.AbortAsync();

        // Assert
        completed.IsSuccess.Should().BeFalse();
        completed.Error!.Message.Should().Contain("part 2 rejected");
        stream.PartsDone.Should().Be(1); // успела только первая
        session.Aborted.Should().BeTrue();
    }

    // AAA: Write после ошибки части бросает ApplicationException.
    [Fact]
    public async Task WriteAfterPartFailure_Throws()
    {
        // Arrange — первая же часть отвергнута фейком.
        var session = new FakeSession(failPartNumber: 1);
        var stream = new MultipartPartBufferStream(session);

        // Act
        await stream.WriteAsync(new byte[16 * MiB], CancellationToken.None);
        var act = async () => await stream.WriteAsync(new byte[1], CancellationToken.None);

        // Assert
        var thrown = await act.Should().ThrowAsync<ApplicationException>();
        thrown.Which.Message.Should().Contain("part 1 rejected");
    }

    // AAA (#24): backpressure — копия части делается ТОЛЬКО после занятия слота:
    // в полёте не больше 4 частей (5-я ждёт освобождения), память ≤5×part.
    [Fact]
    public async Task MaxFourPartsInFlight_Backpressure()
    {
        // Arrange — сессия блокирует заливку до ReleaseAll.
        var session = new GatedSession();
        var stream = new MultipartPartBufferStream(session);
        var chunk = new byte[MiB];
        var ct = TestContext.Current.CancellationToken;
        var writeTask = Task.Run(async () =>
        {
            for (var written = 0; written < 5 * 16 * MiB; written += chunk.Length)
                await stream.WriteAsync(chunk, ct);
        }, ct);

        // Act/Assert — 5-я часть ждёт слота: в полёте ровно 4 (сигнал из
        // сессии — без поллинга: ожидание ограничено 5 с и не является
        // длительным ожиданием юнит-теста).
        await session.ReachedFour.Task.WaitAsync(
            TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        session.MaxInFlight.Should().Be(4, "no more than 4 parts are in flight at once");

        // Act 2 — отпускаем заливку, поток дозаписывается и завершается.
        session.ReleaseAll();
        await writeTask;
        (await stream.CompleteAsync(CancellationToken.None)).IsSuccess.Should().BeTrue();
    }

    // AAA (#10): отмена во время заливки части не глотается в Failed —
    // OperationCanceledException уходит наружу (операция → failed cancelled,
    // не s3-pool-exhausted).
    [Fact]
    public async Task CancellationDuringPartUpload_PropagatesOce()
    {
        // Arrange — сессия висит до отмены токена.
        using var cts = new CancellationTokenSource();
        var stream = new MultipartPartBufferStream(new HangingSession());

        // Act — полная часть уходит в фоновую заливку, затем отмена.
        await stream.WriteAsync(new byte[16 * MiB], cts.Token);
        await cts.CancelAsync();
        var act = async () => await stream.CompleteAsync(cts.Token);

        // Assert — наружу OCE (не Result.Failed).
        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    // AAA: нулевые байты — части не создаются, Complete получает пустой список
    // (сессия отвергает по контракту; буфер не падает на пустом потоке).
    [Fact]
    public async Task ZeroBytes_NoParts_CompleteWithEmptyList()
    {
        // Arrange
        var session = new FakeSession();
        var stream = new MultipartPartBufferStream(session);

        // Act
        var completed = await stream.CompleteAsync(CancellationToken.None);

        // Assert — частей нет, Complete вызван с пустым словарём.
        session.Parts.Should().BeEmpty();
        session.CompletedWith.Should().NotBeNull();
        session.CompletedWith!.Should().BeEmpty();
        completed.IsSuccess.Should().BeTrue();
    }

    // Фейк-сессия с блокировкой заливки (наблюдение числа частей в полёте).
    private sealed class GatedSession : IMultipartUploadSession
    {
        private readonly TaskCompletionSource _gate = new();
        private readonly TaskCompletionSource _reachedFour = new();
        private int _inFlight;
        private int _maxInFlight;

        public string UploadId => "upload-gated";

        public int InFlight => Volatile.Read(ref _inFlight);

        public int MaxInFlight => Volatile.Read(ref _maxInFlight);

        // Сигнал «4 части в полёте» — детерминированное ожидание теста.
        public TaskCompletionSource ReachedFour => _reachedFour;

        public void ReleaseAll() => _gate.TrySetResult();

        public async Task<Result<string>> UploadPartAsync(
            int partNumber, byte[] data, CancellationToken ct)
        {
            var current = Interlocked.Increment(ref _inFlight);
            int seen;
            while (current > (seen = Volatile.Read(ref _maxInFlight)))
            {
                if (Interlocked.CompareExchange(ref _maxInFlight, current, seen) == seen)
                    break;
            }

            if (current >= 4)
                _reachedFour.TrySetResult();

            try
            {
                await _gate.Task.WaitAsync(ct);
                return Result<string>.Success($"etag-{partNumber}");
            }
            finally
            {
                Interlocked.Decrement(ref _inFlight);
            }
        }

        public Task<Result<string>> CompleteAsync(
            IReadOnlyDictionary<int, string> partETags, CancellationToken ct)
            => Task.FromResult(Result<string>.Success("etag-object"));

        public Task<Result> AbortAsync(CancellationToken ct) => Task.FromResult(Result.Success());
    }

    // Фейк-сессия, висящая до отмены токена (проверка проброса OCE).
    private sealed class HangingSession : IMultipartUploadSession
    {
        public string UploadId => "upload-hanging";

        public async Task<Result<string>> UploadPartAsync(
            int partNumber, byte[] data, CancellationToken ct)
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, ct);
            return Result<string>.Success("unreachable");
        }

        public Task<Result<string>> CompleteAsync(
            IReadOnlyDictionary<int, string> partETags, CancellationToken ct)
            => Task.FromResult(Result<string>.Success("unreachable"));

        public Task<Result> AbortAsync(CancellationToken ct) => Task.FromResult(Result.Success());
    }

    // Фейк-сессия: копит части, управляемо отказывает одной, фиксирует Abort.
    private sealed class FakeSession(int failPartNumber = 0) : IMultipartUploadSession
    {
        private readonly object _gate = new();

        public string UploadId => "upload-1";

        public List<(int PartNumber, int Size)> Parts { get; } = [];

        public IReadOnlyDictionary<int, string>? CompletedWith { get; private set; }

        public bool Aborted { get; private set; }

        public Task<Result<string>> UploadPartAsync(
            int partNumber, byte[] data, CancellationToken ct)
        {
            lock (_gate)
                Parts.Add((partNumber, data.Length));
            return Task.FromResult(partNumber == failPartNumber
                ? Result<string>.Failed(new ApplicationException($"part {partNumber} rejected"))
                : Result<string>.Success($"etag-{partNumber}"));
        }

        public Task<Result<string>> CompleteAsync(
            IReadOnlyDictionary<int, string> partETags, CancellationToken ct)
        {
            CompletedWith = partETags;
            return Task.FromResult(Result<string>.Success("etag-object"));
        }

        public Task<Result> AbortAsync(CancellationToken ct)
        {
            Aborted = true;
            // Отказ abort'а глотается вызывающим (best-effort уборка).
            return Task.FromResult(Result.Failed(new ApplicationException("abort failed")));
        }
    }
}
