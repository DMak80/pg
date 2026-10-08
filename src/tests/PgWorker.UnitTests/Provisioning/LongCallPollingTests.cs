using Microsoft.Extensions.Logging;
using PgWorker.App;
using PgWorker.UnitTests.App;
using Shared.Core.Hosting;
using LongCallPolling = PgWorker.Provisioning.Processes.LongCallPolling;

namespace PgWorker.UnitTests.Provisioning;

// Поллинг-инвариант долгих одиночных вызовов: вызов драйвера не молчит дольше
// окна проверки watchdog — итерации с таймаутом короче окна, Mark ПО ФАКТУ
// итерации + лог elapsed (включая завершающую успешную); отмена по
// итерационному таймауту (брошенная вызовом ИЛИ проглоченная им в
// Result.Failed) — повтор до бюджета (spec §1.2 п.4); ошибка вызова
// неотменного характера — наверх без повторов; внешний ct — проброс OCE;
// бюджет фазы — граница (PatroniBootSec-семантика).
public sealed class LongCallPollingTests
{
    private sealed class MarkCounter : Shared.Core.Hosting.ILoopProgress
    {
        public int Marks;
        public void Mark() => Marks++;
    }

    private sealed class CollectingLogger : ILogger
    {
        public List<string> Messages { get; } = [];
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel level, EventId eventId, TState state, Exception? e,
            Func<TState, Exception?, string> formatter) => Messages.Add(formatter(state, e));
    }

    [Fact]
    public async Task ДолгийВызов_ИтерацииИдут_MarkПоФактуЛогElapsed()
    {
        // Arrange: вызов «висит» 3 c (медленный daemon — дольше порога сноса
        // в масштабе юнита; в проде аналогично фазе > 30 c), таймаут итерации
        // 0.5 c, бюджет 10 c; завершается успехом на 4-й попытке
        var progress = new MarkCounter();
        var logger = new CollectingLogger();
        var calls = 0;

        // Act
        var result = await LongCallPolling.EnsureAsync(
            "create/start ноды shard1a",
            async token =>
            {
                calls++;
                if (calls < 4)
                    await Task.Delay(TimeSpan.FromSeconds(3), token); // «висит» — итерация снята таймаутом 0.5 c
                return Result.Success(); // 4-я попытка — успех (медленный daemon отпустил)
            },
            progress, logger,
            TimeSpan.FromMilliseconds(500), TimeSpan.FromSeconds(10), CancellationToken.None);

        // Assert: итерации поллинга шли (первые 3 попытки сняты таймаутом),
        // каждая — Mark ПО ФАКТУ + лог elapsed (включая завершающую успешную);
        // итог — успех
        result.IsSuccess.Should().BeTrue();
        calls.Should().BeGreaterThanOrEqualTo(4);
        progress.Marks.Should().BeGreaterThanOrEqualTo(calls, "отметка на каждую итерацию");
        logger.Messages.Count(m => m.Contains("уже") && m.Contains("shard1a"))
            .Should().BeGreaterThanOrEqualTo(calls,
                "elapsed-лог на КАЖДОЙ итерации, включая успешную (буква приказа п.4)");
        logger.Messages.Should().Contain(m => m.Contains("успех"),
            "завершающая успешная итерация тоже логируется с elapsed");
    }

    [Fact]
    public async Task ОшибкаДрайвера_НаверхБезПоллинга()
    {
        // Arrange: драйвер отвечает быстрой ошибкой — поллинг не крутится
        var progress = new MarkCounter();
        var calls = 0;

        // Act
        var result = await LongCallPolling.EnsureAsync(
            "create/start ноды shard1a",
            _ => { calls++; return Task.FromResult(Result.Failed(new ApplicationException("docker down"))); },
            progress, new CollectingLogger(),
            TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(5), CancellationToken.None);

        // Assert: ошибка — наверх первым вызовом (тир повторит), отметка одна — по факту итерации
        result.IsSuccess.Should().BeFalse();
        calls.Should().Be(1);
        progress.Marks.Should().Be(1, "отметка не ставится без фактической итерации");
    }

    [Fact]
    public async Task БюджетИсчерпан_FailedНаверх()
    {
        // Arrange: вызов никогда не завершается, бюджет 2 c < вечности
        var progress = new MarkCounter();

        // Act
        var result = await LongCallPolling.EnsureAsync(
            "create/start ноды shard1a",
            async token => { await Task.Delay(TimeSpan.FromHours(1), token); return Result.Success(); },
            progress, new CollectingLogger(),
            TimeSpan.FromMilliseconds(300), TimeSpan.FromSeconds(2), CancellationToken.None);

        // Assert: существующий бюджет — граница фазы (PatroniBootSec-семантика),
        // ошибка — не раньше исчерпания (несколько итераций успели пройти)
        result.IsSuccess.Should().BeFalse();
        result.Error!.Message.Should().Contain("бюджет");
        progress.Marks.Should().BeGreaterThanOrEqualTo(2,
            "ошибка бюджета — только после нескольких таймаут-итераций, не раньше");
    }

    // AAA (spec §1.2 п.4, разбор E2E-маркера «SQL-скаляр не выполнен …
    // The operation was canceled»): вызов-обёртка ПРОГЛОТИЛА отмену
    // итерационного таймаута в Result.Failed (OCE в цепочке ошибки) —
    // это «итерация не уложилась», а не ошибка: повтор до успеха на 2-й.
    [Fact]
    public async Task ПроглоченнаяОтмена_FailedСОЧЕ_ПовторяетсяДоУспеха()
    {
        // Arrange: таймаут итерации 300 мс; 1-й вызов «висит», ловит отмену
        // токена и возвращает Failed с OCE в цепочке (как DatabaseProvisioner);
        // 2-й вызов — успех
        var progress = new MarkCounter();
        var logger = new CollectingLogger();
        var calls = 0;

        // Act
        var result = await LongCallPolling.EnsureAsync(
            "sql шарда shard3",
            async token =>
            {
                calls++;
                if (calls >= 2)
                    return Result.Success(); // 2-я итерация — «медленный daemon отпустил»
                try
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(600), token); // дольше таймаута 300 мс
                }
                catch (OperationCanceledException)
                {
                    return Result.Failed(new ApplicationException(
                        "SQL-скаляр не выполнен [dsn]: The operation was canceled",
                        new OperationCanceledException()));
                }

                return Result.Success();
            },
            progress, logger,
            TimeSpan.FromMilliseconds(300), TimeSpan.FromSeconds(5), CancellationToken.None);

        // Assert: успех (ошибка отмены НЕ ушла наверх), ровно 2 итерации,
        // elapsed-логи: таймаут-повтор + завершающая успешная
        result.IsSuccess.Should().BeTrue();
        calls.Should().Be(2);
        logger.Messages.Should().Contain(m => m.Contains("отменена по таймауту"),
            "итерация с проглоченной отменой логируется как таймаут-повтор");
        logger.Messages.Should().Contain(m => m.Contains("успех"),
            "завершающая успешная итерация логируется с elapsed");
    }

    // AAA (бюджет, проглоченная отмена): вызов вечно возвращает Failed с OCE —
    // повтор идёт до исчерпания бюджета, ошибка бюджета — не раньше.
    [Fact]
    public async Task ВечнаяПроглоченнаяОтмена_БюджетИсчерпан_НеРаньше()
    {
        // Arrange: таймаут 200 мс, бюджет 1 с (≈5 итераций); каждый вызов
        // «висит» до таймаута и возвращает Failed с OCE в цепочке
        var progress = new MarkCounter();
        var calls = 0;

        // Act
        var result = await LongCallPolling.EnsureAsync(
            "sql шарда shard3",
            async token =>
            {
                calls++;
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(10), token);
                }
                catch (OperationCanceledException)
                {
                }

                return Result.Failed(new ApplicationException(
                    "SQL-скаляр не выполнен [dsn]: The operation was canceled",
                    new OperationCanceledException()));
            },
            progress, new CollectingLogger(),
            TimeSpan.FromMilliseconds(200), TimeSpan.FromSeconds(1), CancellationToken.None);

        // Assert: ошибка бюджета (обработка процесса — прежняя, A5 идемпотентен),
        // не раньше нескольких итераций
        result.IsSuccess.Should().BeFalse();
        result.Error!.Message.Should().Contain("бюджет");
        calls.Should().BeGreaterThanOrEqualTo(3,
            "повторы до бюджета — ошибка не раньше исчерпания (spec §1.2 п.4)");
    }

    // AAA (внешний ct, брошенная отмена): отмена внешнего токена пробрасывает
    // OperationCanceledException наружу немедленно — не трактуется как таймаут.
    [Fact]
    public async Task ВнешняяОтмена_ПробросOceНаружу()
    {
        // Arrange: внешний токен отменён до вызова; вызов уважает токен
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var calls = 0;

        // Act + Assert: OCE наружу с первой итерации
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            LongCallPolling.EnsureAsync(
                "create/start ноды shard1a",
                async token =>
                {
                    calls++;
                    await Task.Delay(TimeSpan.FromSeconds(10), token);
                    return Result.Success();
                },
                new MarkCounter(), new CollectingLogger(),
                TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(30), cts.Token));
        calls.Should().Be(1, "внешняя отмена — немедленный проброс, без повторов");
    }

    // AAA (внешний ct, проглоченная отмена): вызов вернул Failed с OCE при
    // отменённом внешнем токене — проброс OCE, не «ошибка» и не повтор.
    [Fact]
    public async Task ПроглоченнаяВнешняяОтмена_ПробросOceНаружу()
    {
        // Arrange: внешний токен отменён; вызов глотает OCE в Result.Failed
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var calls = 0;

        // Act + Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            LongCallPolling.EnsureAsync(
                "sql шарда shard3",
                async token =>
                {
                    calls++;
                    try
                    {
                        await Task.Delay(TimeSpan.FromSeconds(10), token);
                    }
                    catch (OperationCanceledException)
                    {
                    }

                    return Result.Failed(new ApplicationException(
                        "SQL-скаляр не выполнен [dsn]: The operation was canceled",
                        new OperationCanceledException()));
                },
                new MarkCounter(), new CollectingLogger(),
                TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(30), cts.Token));
        calls.Should().Be(1);
    }

    // AAA (контр-кейс «(а)»): ошибка вызова НЕотменного характера при
    // сработавшем итерационном таймауте — наверх как есть, без повторов
    // (поллинг ловит молчание, не сбои).
    [Fact]
    public async Task ОшибкаНеотменногоХарактера_НаверхБезПовторов()
    {
        // Arrange: таймаут 200 мс; вызов «висит», ловит отмену токена, но
        // возвращает FAILED с ошибкой валидации (OCE в цепочке НЕТ)
        var calls = 0;

        // Act
        var result = await LongCallPolling.EnsureAsync(
            "sql шарда shard3",
            async token =>
            {
                calls++;
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(10), token);
                }
                catch (OperationCanceledException)
                {
                }

                return Result.Failed(new ApplicationException("хост не в таблице Docker:Hosts"));
            },
            new MarkCounter(), new CollectingLogger(),
            TimeSpan.FromMilliseconds(200), TimeSpan.FromSeconds(5), CancellationToken.None);

        // Assert: ошибка наверх первым вызовом (тик повторит), поллинг не крутится
        result.IsSuccess.Should().BeFalse();
        result.Error!.Message.Should().Contain("хост не в таблице");
        calls.Should().Be(1, "неотменная ошибка — наверх без повторов (spec §1.2 п.4а)");
    }

    // Локальная копия образца Shared.Core.UnitTests: счётчик StopApplication.
    private sealed class FakeLifetime : Microsoft.Extensions.Hosting.IHostApplicationLifetime
    {
        public int StopCalls;
        public CancellationToken ApplicationStarted => CancellationToken.None;
        public CancellationToken ApplicationStopping => CancellationToken.None;
        public CancellationToken ApplicationStopped => CancellationToken.None;
        public void StopApplication() => StopCalls++;
    }

    [Fact]
    public async Task Связка_ПоллингИдёт_WatchdogНеСносит_ВисящийВызовБезИтераций_Сносит()
    {
        // Arrange: HealthState + виталити PgWorker (порог от опций: 2×1=2 c —
        // масштабированный аналог продового 2×15=30 c) + LoopWatchdog (механика
        // прежняя: снос при возрасте активности > StaleAfter)
        var health = new PgWorker.App.HealthState(TimeProvider.System);
        var options = new FixedOptionsMonitor(new PgWorkerOptions
        {
            Loops = new LoopsOptions
            {
                ScanIntervalSec = 1, KeepaliveSec = 1, SnapshotIntervalMin = 360,
                Watchdog = new Shared.Core.Hosting.WatchdogOptions { Multiplier = 2, CheckIntervalSec = 1 },
            },
        });
        var vitality = new PgWorker.App.PgWorkerLoopsVitality(options, health);
        var lifetime = new FakeLifetime();
        using var cts = new CancellationTokenSource();
        // опции для watchdog — отдельным экземпляром (класс, не record: без with)
        using var watchdog = new Shared.Core.Hosting.LoopWatchdog(
            vitality, lifetime, Microsoft.Extensions.Logging.Abstractions.NullLogger<Shared.Core.Hosting.LoopWatchdog>.Instance,
            TimeProvider.System,
            new Shared.Core.Hosting.WatchdogOptions { Enabled = true, Multiplier = 2, CheckIntervalSec = 1, StopDelaySec = 0 });

        // Act 1: «долгая фаза» 3 c (дольше порога 2 c; внутри grace null-активности
        // snapshot 2×StaleAfter=4 c — иначе гонка с ним) — итерации поллинга дают
        // отметки чаще порога: живая фаза любой длительности неуязвима
        await watchdog.StartAsync(cts.Token);
        var phaseEnd = DateTimeOffset.UtcNow.AddSeconds(3);
        while (DateTimeOffset.UtcNow < phaseEnd)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(300), TestContext.Current.CancellationToken);
            health.Mark(); // итерация поллинга ПО ФАКТУ
        }

        // Assert 1: сноса нет — активность обновляется, возраст < StaleAfter
        lifetime.StopCalls.Should().Be(0, "итерации поллинга — не зависание (критерий 8)");

        // Act 2: «висящий вызов» — отметок нет 5 c (возраст snapshot > grace 4 c +
        // окно; reconcile последней отметки 3 c назад у порога — первым сработает
        // снос наблюдением активности в целом)
        await Task.Delay(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        // Assert 2: снос при возрасте активности > StaleAfter (механика прежняя)
        lifetime.StopCalls.Should().Be(1, "висящий вызов без итераций — снос (критерий 8)");
        cts.Cancel();
    }
}
