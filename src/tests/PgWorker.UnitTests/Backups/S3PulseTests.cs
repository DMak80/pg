using PgWorker.Backups;
using Shared.Core.Hosting;

namespace PgWorker.UnitTests.Backups;

// «Пульс конечного внешнего вызова» (arch/14 §6, arch/19 §5): на время
// ОДНОГО S3-вызова Mark-отметки идут по расписанию (период инъектируется —
// реальные 10 с в тестах не ждём); завершение вызова гасит пульс.
public sealed class S3PulseTests
{
    // Мок прогресса: считает Mark-отметки (потокобезопасно — Timer-колбэки).
    private sealed class CountingProgress : ILoopProgress
    {
        private int _marks;
        public int Marks => Volatile.Read(ref _marks);
        public void Mark() => Interlocked.Increment(ref _marks);
    }

    private static async Task WaitForAsync(Func<bool> condition, string what)
    {
        for (var i = 0; i < 100 && !condition(); i++)
            await Task.Delay(20, TestContext.Current.CancellationToken);
        if (!condition())
            throw new Xunit.Sdk.XunitException($"не дождались: {what}");
    }

    // AAA: висящий вызов — пульс Mark'ает по расписанию (≥2 отметки при
    // периоде 20 мс); период инъектирован, реального 10-с дефолта нет.
    [Fact]
    public async Task CallAsync_HangingCall_MarksWhileInFlight()
    {
        // Arrange: вызов не завершается (TCS), период 20 мс, прогресс-счётчик.
        var progress = new CountingProgress();
        var tcs = new TaskCompletionSource<Result<int>>(TaskCreationOptions.RunContinuationsAsynchronously);

        // Act: запускаем и ждём двух отметок (≤~2 с бюджет полла)
        var task = S3Pulse.CallAsync(
            progress, _ => tcs.Task, CancellationToken.None, TimeSpan.FromMilliseconds(20));
        await WaitForAsync(() => progress.Marks >= 2, "пульс обязан Mark'ать ≥2 раз за висящий вызов");

        // Assert: вызов ещё в полёте, завершаем — результат проходит насквозь.
        tcs.SetResult(Result<int>.Success(42));
        var result = await task;
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be(42);
    }

    // AAA: завершение вызова гасит пульс — после возврата отметки не растут
    // (слепое окно = одна операция, «вечного» пульса нет).
    [Fact]
    public async Task CallAsync_AfterReturn_PulseStops()
    {
        // Arrange: быстрый вызов (мгновенный результат), период 20 мс.
        var progress = new CountingProgress();
        var result = await S3Pulse.CallAsync(
            progress,
            _ => Task.FromResult(Result<int>.Success(1)),
            CancellationToken.None,
            TimeSpan.FromMilliseconds(20));
        var atReturn = progress.Marks; // ≥1 (немедленный первый тик Timer'а)

        // Act: ждём втрое дольше периода после возврата.
        await Task.Delay(80, TestContext.Current.CancellationToken);

        // Assert: новых отметок нет — пульс умер вместе с вызовом.
        result.IsSuccess.Should().BeTrue();
        progress.Marks.Should().Be(atReturn);
    }

    // AAA: негенерик-перегрузка (DeleteKeysAsync → Result) — тот же пульс.
    [Fact]
    public async Task CallAsync_NonGeneric_PulseAlsoWorks()
    {
        // Arrange: висящий негенерик-вызов, период 20 мс.
        var progress = new CountingProgress();
        var tcs = new TaskCompletionSource<Result>(TaskCreationOptions.RunContinuationsAsynchronously);

        // Act
        var task = S3Pulse.CallAsync(
            progress, _ => tcs.Task, CancellationToken.None, TimeSpan.FromMilliseconds(20));
        await WaitForAsync(() => progress.Marks >= 2, "негенерик-пульс тоже Mark'ает");
        tcs.SetResult(Result.Success());

        // Assert
        (await task).IsSuccess.Should().BeTrue();
    }

    // AAA: null-прогресс — вызов работает без пульса (тесты/без DI).
    [Fact]
    public async Task CallAsync_NoProgress_CallUnaffected()
    {
        // Arrange / Act
        var result = await S3Pulse.CallAsync<int>(
            null, _ => Task.FromResult(Result<int>.Success(7)), CancellationToken.None,
            TimeSpan.FromMilliseconds(10));

        // Assert
        result.Value.Should().Be(7);
    }
}
