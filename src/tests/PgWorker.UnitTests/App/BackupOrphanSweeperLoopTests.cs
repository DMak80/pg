using Microsoft.Extensions.Logging.Abstractions;
using PgWorker.App;
using PgWorker.App.Loops;
using PgWorker.Backups.Supervisor;
using PgWorker.UnitTests.Provisioning;
using Shared.Core.Hosting;
using System.Diagnostics.Metrics;
using PgWorker.Etcd.S3;

namespace PgWorker.UnitTests.App;

// Тики живости BackupOrphanSweeperLoop: каждая итерация отмечается в HealthState
// (порог watchdog у sweeper'а — как у быстрых циклов: сон Supervisor:IntervalSec
// тикает чанками, спящий лидер не «stale»).
public sealed class BackupOrphanSweeperLoopTests
{
    private static (BackupOrphanSweeperLoop Loop, HealthState Health, CancellationTokenSource Cts) Create(
        int scanIntervalSec, int supervisorIntervalSec)
    {
        var etcd = new Fakes.FakeEtcd();
        var options = new FixedOptionsMonitor(new PgWorkerOptions
        {
            Etcd = new EtcdOptions { Endpoints = ["http://etcd:2379"] },
            Loops = new LoopsOptions { ScanIntervalSec = scanIntervalSec },
            Backups = new BackupsOptions { Enabled = false }, // не-лидер/выключено — тихий цикл
        });
        options.CurrentValue.Backups.Supervisor.IntervalSec = supervisorIntervalSec;
        var health = new HealthState(TimeProvider.System);
        var claims = new ClaimStore(
            "/pgworker", options.CurrentValue.Etcd.Endpoints, etcd, TimeProvider.System);
        var sweeper = new BackupOrphanSweeper(
            etcd, options.CurrentValue.Etcd.Endpoints,
            UnitPoolFactory.Disabled(), UnitPoolFactory.Store(), claims,
            () => null, TimeProvider.System, NullLogger<BackupOrphanSweeper>.Instance);
        var metrics = new Shared.Metrics.Worker.WorkerMetricsInstrumentation(
            new Meter("TestOrphanSweep"), TimeProvider.System);
        var loop = new BackupOrphanSweeperLoop(options, claims, sweeper, metrics, health,
            NullLogger<BackupOrphanSweeperLoop>.Instance);
        return (loop, health, new CancellationTokenSource());
    }

    [Fact]
    public async Task ExecuteAsync_MarksOrphanSweepTick()
    {
        // Arrange: цикл с нулевым интервалом скана (тик каждой итерацией)
        var (loop, health, cts) = Create(scanIntervalSec: 0, supervisorIntervalSec: 600);

        // Act: запускаем и ждём первой отметки (поллинг ≤5 с, шаг 50 мс)
        await loop.StartAsync(cts.Token);
        for (var i = 0; i < 100 && health.Snapshot().LastOrphanSweepTick is null; i++)
            await Task.Delay(50, TestContext.Current.CancellationToken);
        cts.Cancel();

        // Assert: sweeper отмечает тики живости (невидим до задачи — AC5)
        health.Snapshot().LastOrphanSweepTick.Should().NotBeNull();
    }

    [Fact]
    public async Task DelayTickingAsync_TicksInChunks_DoesNotSleepWholeInterval()
    {
        // Arrange: сон лидера sweeper'а — общий хелпер пульсирующего сна
        // (PulsingDelay): Supervisor:IntervalSec 600 c чанками короче окна —
        // тики живости без 600-с паузы
        var health = new HealthState(TimeProvider.System);

        // Act: 2,5 c сна чанками по 1 c (окно 2 c → чанк 1 c) → ≥2 отметки, затем отмена
        var task = PulsingDelay.SleepAsync(
            TimeSpan.FromSeconds(600), TimeSpan.FromSeconds(2),
            health.MarkOrphanSweepTick, TestContext.Current.CancellationToken);
        for (var i = 0; i < 50; i++)
            await Task.Delay(50, TestContext.Current.CancellationToken);
        var ticksInWindow = health.Snapshot().LastOrphanSweepTick;

        // Assert: за окно ≪ 600 c отметки уже есть — сон не глушит живость
        ticksInWindow.Should().NotBeNull();
    }

    // S3-стаб с висящим list-ом (bad-S3-контур): проход sweeper'а не завершается
    // до отмены — пульс активности обязан работать (арch/14 §6).
    private sealed class HangingS3 : IBackupS3
    {
        public TaskCompletionSource<Result<IReadOnlyList<PgWorker.Backups.S3ObjectInfo>>> Gate { get; }
            = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task<Result<bool>> BucketExistsAsync(CancellationToken ct) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<PgWorker.Backups.WalObject>>> ListWalAsync(
            string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default)
            => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<PgWorker.Backups.S3ObjectInfo>>> ListPrefixAsync(
            string prefix, int? maxKeysPerTest = null, CancellationToken ct = default)
        {
            // Отмена внешним токеном гасит «висящий» вызов (иначе тест не завершится).
            ct.Register(() => Gate.TrySetCanceled(ct));
            return Gate.Task;
        }
        public Task<Result> DeleteKeysAsync(IReadOnlyList<string> keys, CancellationToken ct = default)
            => throw new NotSupportedException();
        public Task<Result> PutObjectAsync(string key, byte[] data, string? sha256, CancellationToken ct = default)
            => throw new NotSupportedException();
        public Task<Result<string>> DownloadTextAsync(
            string cluster, string shard, string objectKey, CancellationToken ct = default)
            => throw new NotSupportedException();
        public Task<Result<string>> GetObjectAsync(
            string cluster, string shard, string key, CancellationToken ct = default)
            => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<PgWorker.Backups.WalObject>>> ListAsync(
            string cluster, string shard, string prefix, int? maxKeysPerTest = null, CancellationToken ct = default)
            => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<string>>> ListFullsAsync(
            string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default)
            => throw new NotSupportedException();
    }

    // AAA: лидерский проход с висящим S3 (CheckIntervalSec=1) — пульс АКТИВНОСТИ
    // (MarkOrphanSweepActivity) живёт во время прохода, новее последнего тика:
    // тики ≠ активность, watchdog-канал orphan-sweep не глушится долгим S3-проходом.
    [Fact]
    public async Task ExecuteAsync_LeaderHangingSweep_PulsesActivityBeyondTick()
    {
        // Arrange: Enabled=true, лидерство возьмёт FakeEtcd-txn; S3 висит.
        var etcd = new Fakes.FakeEtcd();
        var options = new FixedOptionsMonitor(new PgWorkerOptions
        {
            Etcd = new EtcdOptions { Endpoints = ["http://etcd:2379"] },
            Loops = new LoopsOptions { ScanIntervalSec = 0, Watchdog = new WatchdogOptions { CheckIntervalSec = 1 } },
            Backups = new BackupsOptions { Enabled = true },
        });
        var health = new HealthState(TimeProvider.System);
        var claims = new ClaimStore(
            "/pgworker", options.CurrentValue.Etcd.Endpoints, etcd, TimeProvider.System);
        var s3 = new HangingS3();
        var sweeper = new BackupOrphanSweeper(
            etcd, options.CurrentValue.Etcd.Endpoints,
            UnitPoolFactory.Wrap(s3), UnitPoolFactory.Store(), claims,
            () => options.CurrentValue.Backups.ToRuntime(), TimeProvider.System,
            NullLogger<BackupOrphanSweeper>.Instance);
        var metrics = new Shared.Metrics.Worker.WorkerMetricsInstrumentation(
            new Meter("TestOrphanSweep"), TimeProvider.System);
        var loop = new BackupOrphanSweeperLoop(options, claims, sweeper, metrics, health,
            NullLogger<BackupOrphanSweeperLoop>.Instance);
        using var cts = new CancellationTokenSource();

        // Act: цикл идёт; ждём активности пульса новее тика (период 1 c; ≤~6 с).
        await loop.StartAsync(cts.Token);
        DateTimeOffset? activity = null;
        for (var i = 0; i < 120; i++)
        {
            var snap = health.Snapshot();
            if (snap.LastOrphanSweepActivity is { } a
                && snap.LastOrphanSweepTick is { } t && a > t)
            {
                activity = a;
                break;
            }
            await Task.Delay(50, TestContext.Current.CancellationToken);
        }
        cts.Cancel();

        // Assert: пульс Activity отмечен И новее единственного тика итерации —
        // S3-проход висит, но канал активен (арх/14 §6, зеркально snapshot).
        activity.Should().NotBeNull("a hanging S3 pass must pulse activity");
    }

    // AAA: лидерский проход с Backups:Enabled=true и живым S3 — по итогам SweepAsync
    // инструментация пишет тик цикла orphan-sweep с ok=true (arch/18 §2.2; ок
    // последнего успешного тика двигает last_success — серия появляется).
    [Fact]
    public async Task ExecuteAsync_LeaderSweep_TicksOrphanSweepOkTrue()
    {
        // Arrange: Enabled=true, лидерство возьмёт FakeEtcd-txn; S3 отвечает пустыми
        // list'ами мгновенно; MeterListener собирает counter worker.loop.ticks.
        var etcd = new Fakes.FakeEtcd();
        var options = new FixedOptionsMonitor(new PgWorkerOptions
        {
            Etcd = new EtcdOptions { Endpoints = ["http://etcd:2379"] },
            Loops = new LoopsOptions { ScanIntervalSec = 0 },
            Backups = new BackupsOptions { Enabled = true },
        });
        var health = new HealthState(TimeProvider.System);
        var claims = new ClaimStore(
            "/pgworker", options.CurrentValue.Etcd.Endpoints, etcd, TimeProvider.System);
        using var meter = new Meter("TestOrphanSweep");
        var metrics = new Shared.Metrics.Worker.WorkerMetricsInstrumentation(meter, TimeProvider.System);
        var seen = new List<(string Loop, bool Ok)>();
        using var listener = new MeterListener();
        listener.SetMeasurementEventCallback<long>((instrument, measurement, tags, _) =>
        {
            if (instrument.Name != "worker.loop.ticks")
                return;
            var pairs = tags.ToArray();
            seen.Add((
                pairs.First(t => t.Key == "loop").Value!.ToString()!,
                (bool)pairs.First(t => t.Key == "ok").Value!));
        });
        listener.InstrumentPublished = (instrument, enable) =>
        {
            if (instrument.Meter.Name == "TestOrphanSweep")
                enable.EnableMeasurementEvents(instrument);
        };
        listener.Start();
        var sweeper = new BackupOrphanSweeper(
            etcd, options.CurrentValue.Etcd.Endpoints,
            UnitPoolFactory.Wrap(new EmptyS3()), UnitPoolFactory.Store(), claims,
            () => options.CurrentValue.Backups.ToRuntime(), TimeProvider.System,
            NullLogger<BackupOrphanSweeper>.Instance);
        var loop = new BackupOrphanSweeperLoop(options, claims, sweeper, metrics, health,
            NullLogger<BackupOrphanSweeperLoop>.Instance);
        using var cts = new CancellationTokenSource();

        // Act: цикл идёт; ждём первого завершённого прохода лидера (поллинг ≤5 с).
        await loop.StartAsync(cts.Token);
        for (var i = 0; i < 100 && seen.All(s => s.Loop != "orphan-sweep"); i++)
            await Task.Delay(50, TestContext.Current.CancellationToken);
        cts.Cancel();

        // Assert: успешный проход отметил тик цикла orphan-sweep с ok=true.
        seen.Should().Contain(s => s.Loop == "orphan-sweep" && s.Ok);
    }

    // AAA: провальный проход лидера (list пулов S3 вернул Result-ошибку) — тик цикла
    // orphan-sweep с ok=false; last_success не двигает (семантика закреплена
    // Shared.Metrics.UnitTests LoopTick_OkFalse_DoesNotMoveLastSuccess).
    [Fact]
    public async Task ExecuteAsync_LeaderSweepFails_TicksOrphanSweepOkFalse()
    {
        // Arrange: Enabled=true, лидерство возьмёт FakeEtcd-txn; list пулов S3
        // возвращает Result-ошибку → SweepAsync проваливается на первом шаге.
        var etcd = new Fakes.FakeEtcd();
        var options = new FixedOptionsMonitor(new PgWorkerOptions
        {
            Etcd = new EtcdOptions { Endpoints = ["http://etcd:2379"] },
            Loops = new LoopsOptions { ScanIntervalSec = 0 },
            Backups = new BackupsOptions { Enabled = true },
        });
        var health = new HealthState(TimeProvider.System);
        var claims = new ClaimStore(
            "/pgworker", options.CurrentValue.Etcd.Endpoints, etcd, TimeProvider.System);
        using var meter = new Meter("TestOrphanSweep");
        var metrics = new Shared.Metrics.Worker.WorkerMetricsInstrumentation(meter, TimeProvider.System);
        var seen = new List<(string Loop, bool Ok)>();
        using var listener = new MeterListener();
        listener.SetMeasurementEventCallback<long>((instrument, measurement, tags, _) =>
        {
            if (instrument.Name != "worker.loop.ticks")
                return;
            var pairs = tags.ToArray();
            seen.Add((
                pairs.First(t => t.Key == "loop").Value!.ToString()!,
                (bool)pairs.First(t => t.Key == "ok").Value!));
        });
        listener.InstrumentPublished = (instrument, enable) =>
        {
            if (instrument.Meter.Name == "TestOrphanSweep")
                enable.EnableMeasurementEvents(instrument);
        };
        listener.Start();
        var sweeper = new BackupOrphanSweeper(
            etcd, options.CurrentValue.Etcd.Endpoints,
            UnitPoolFactory.Disabled(), new FaultingPoolStore(), claims,
            () => options.CurrentValue.Backups.ToRuntime(), TimeProvider.System,
            NullLogger<BackupOrphanSweeper>.Instance);
        var loop = new BackupOrphanSweeperLoop(options, claims, sweeper, metrics, health,
            NullLogger<BackupOrphanSweeperLoop>.Instance);
        using var cts = new CancellationTokenSource();

        // Act: цикл идёт; ждём первого завершённого (провального) прохода лидера
        // (поллинг ≤5 с).
        await loop.StartAsync(cts.Token);
        for (var i = 0; i < 100 && seen.All(s => s.Loop != "orphan-sweep"); i++)
            await Task.Delay(50, TestContext.Current.CancellationToken);
        cts.Cancel();

        // Assert: провальный проход отметил тик цикла orphan-sweep с ok=false.
        seen.Should().Contain(s => s.Loop == "orphan-sweep" && !s.Ok);
    }

    // S3-стаб с мгновенными пустыми list-ответами: проход sweeper'а завершается
    // сразу (HangingS3 не подходит — нужен ЗАВЕРШАЮЩИЙСЯ фейк для тика ok=true).
    private sealed class EmptyS3 : IBackupS3
    {
        public Task<Result<IReadOnlyList<PgWorker.Backups.S3ObjectInfo>>> ListPrefixAsync(
            string prefix, int? maxKeysPerTest = null, CancellationToken ct = default)
            => Task.FromResult(Result<IReadOnlyList<PgWorker.Backups.S3ObjectInfo>>.Success(
                (IReadOnlyList<PgWorker.Backups.S3ObjectInfo>)[]));
        public Task<Result<bool>> BucketExistsAsync(CancellationToken ct) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<PgWorker.Backups.WalObject>>> ListWalAsync(
            string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default)
            => throw new NotSupportedException();
        public Task<Result> DeleteKeysAsync(IReadOnlyList<string> keys, CancellationToken ct = default)
            => throw new NotSupportedException();
        public Task<Result> PutObjectAsync(string key, byte[] data, string? sha256, CancellationToken ct = default)
            => throw new NotSupportedException();
        public Task<Result<string>> DownloadTextAsync(
            string cluster, string shard, string objectKey, CancellationToken ct = default)
            => throw new NotSupportedException();
        public Task<Result<string>> GetObjectAsync(
            string cluster, string shard, string key, CancellationToken ct = default)
            => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<PgWorker.Backups.WalObject>>> ListAsync(
            string cluster, string shard, string prefix, int? maxKeysPerTest = null, CancellationToken ct = default)
            => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<string>>> ListFullsAsync(
            string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default)
            => throw new NotSupportedException();
    }

    // Пул-стор S3 с ошибкой list пулов: SweepAsync сразу возвращает Result.Failed —
    // проход лидера провальный (ok=false; ListPrefixAsync-ошибку проход поглотил бы).
    private sealed class FaultingPoolStore : IS3PoolStore
    {
        public Task<Result<S3Pool?>> ReadAsync(string pool, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<string>>> ListPoolNamesAsync(CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<string>>.Failed(
                new ApplicationException("pool store unreachable")));
    }
}
