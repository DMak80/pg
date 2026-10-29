using Microsoft.Extensions.Logging.Abstractions;
using PgWorker.App;
using PgWorker.App.Loops;
using PgWorker.Backups;
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
            NullLogger<BackupOrphanSweeperLoop>.Instance,
            etcd, options.CurrentValue.Etcd.Endpoints);
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
// файловые методы интерфейса в этом тесте не используются —
        // заглушка (фейк нужен только перечисленным выше операциям).
        public Task<Result> PutFileAsync(string key, string filePath, CancellationToken ct = default)
            => Task.FromResult(Result.Failed(new ApplicationException("put-file: not supported by this fake")));

        public Task<Result> GetObjectToFileAsync(string cluster, string shard, string key, string destFilePath, CancellationToken ct = default)
            => Task.FromResult(Result.Failed(new ApplicationException("get-to-file: not supported by this fake")));
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
            NullLogger<BackupOrphanSweeperLoop>.Instance,
            etcd, options.CurrentValue.Etcd.Endpoints);
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
            NullLogger<BackupOrphanSweeperLoop>.Instance,
            etcd, options.CurrentValue.Etcd.Endpoints);
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
            NullLogger<BackupOrphanSweeperLoop>.Instance,
            etcd, options.CurrentValue.Etcd.Endpoints);
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

    // ── : лидерская поддержка/стирание стоп-ключа /pgworker/backups-disabled ──

    // Сборка цикла с управляемыми режимом/лидерством (FakeEtcd-Store — истина).
    private static (BackupOrphanSweeperLoop Loop, Fakes.FakeEtcd Etcd, CancellationTokenSource Cts) CreateGateLoop(
        bool enabled, string? seedLeader = null, bool seedDisabledKey = false)
    {
        var etcd = new Fakes.FakeEtcd();
        if (seedLeader is not null)
            etcd.Store["/pgworker/leader"] = new Fakes.FakeEtcd.Entry(seedLeader, 1, 1);
        if (seedDisabledKey)
            etcd.Store[BackupsDisabledGate.Key] = new Fakes.FakeEtcd.Entry("""{"updated_unix":1}""", 2, 1);
        var options = new FixedOptionsMonitor(new PgWorkerOptions
        {
            Etcd = new EtcdOptions { Endpoints = ["http://etcd:2379"] },
            Loops = new LoopsOptions { ScanIntervalSec = 0 },
            Backups = new BackupsOptions { Enabled = enabled },
        });
        var health = new HealthState(TimeProvider.System);
        var claims = new ClaimStore(
            "/pgworker", options.CurrentValue.Etcd.Endpoints, etcd, TimeProvider.System);
        var sweeper = new BackupOrphanSweeper(
            etcd, options.CurrentValue.Etcd.Endpoints,
            UnitPoolFactory.Disabled(), UnitPoolFactory.Store(), claims,
            () => options.CurrentValue.Backups.ToRuntime(), TimeProvider.System,
            NullLogger<BackupOrphanSweeper>.Instance);
        var metrics = new Shared.Metrics.Worker.WorkerMetricsInstrumentation(
            new Meter("TestOrphanSweep"), TimeProvider.System);
        var loop = new BackupOrphanSweeperLoop(options, claims, sweeper, metrics, health,
            NullLogger<BackupOrphanSweeperLoop>.Instance,
            etcd, options.CurrentValue.Etcd.Endpoints);
        return (loop, etcd, new CancellationTokenSource());
    }

    private static async Task<string?> DisabledKeyRaw(Fakes.FakeEtcd etcd)
        => etcd.Store.TryGetValue(BackupsDisabledGate.Key, out var e) ? e.Value : null;

    // AAA (arch/19 §4): лидер при Backups:Enabled=false поддерживает
    // /pgworker/backups-disabled на КАЖДОМ тике лидера — updated_unix ключа
    // растёт между тиками (не разовая запись).
    [Fact]
    public async Task LeaderDisabled_PutsBackupsDisabled_EveryTick()
    {
        // Arrange: Enabled=false, лидерство возьмёт свободный txn (ScanIntervalSec=0 —
        // итерации без паузы)
        var (loop, etcd, cts) = CreateGateLoop(enabled: false);

        // Act: цикл идёт — ждём появления ключа, затем РОСТА updated_unix (второй тик)
        await loop.StartAsync(cts.Token);
        string? first = null;
        for (var i = 0; i < 200 && first is null; i++)
        {
            first = await DisabledKeyRaw(etcd);
            if (first is null)
                await Task.Delay(25, TestContext.Current.CancellationToken);
        }
        string? second = first;
        for (var i = 0; i < 200 && second == first; i++)
        {
            second = await DisabledKeyRaw(etcd);
            if (second == first)
                await Task.Delay(25, TestContext.Current.CancellationToken);
        }
        cts.Cancel();

        // Assert: ключ жив и перезаписывается каждым тиком лидера
        first.Should().NotBeNull("the leader with Enabled=false must put the disabled key");
        second.Should().NotBeNull().And.NotBe(first, "each leader tick refreshes updated_unix");
    }

    // AAA (arch/19 §4): лидер при Backups:Enabled=true стирает
    // /pgworker/backups-disabled идемпотентно в каждом тике — stale-ключ от
    // прежнего лидера/режима не выживает takeover/reload.
    [Fact]
    public async Task LeaderEnabled_DeletesBackupsDisabled_EachTick()
    {
        // Arrange: Enabled=true, стоп-ключ уже стоит (stale от прежнего режима)
        var (loop, etcd, cts) = CreateGateLoop(enabled: true, seedDisabledKey: true);

        // Act: цикл идёт — лидерская del-ветка снимает ключ
        await loop.StartAsync(cts.Token);
        for (var i = 0; i < 200 && await DisabledKeyRaw(etcd) is not null; i++)
            await Task.Delay(25, TestContext.Current.CancellationToken);
        cts.Cancel();

        // Assert: ключ снят (del идемпотентен — отсутствие ключа тоже успех)
        (await DisabledKeyRaw(etcd)).Should().BeNull(
            "the leader with Enabled=true must idempotently delete the stale disabled key");
    }

    // AAA (arch/19 §4): не-лидер стоп-ключ НЕ пишет и НЕ стирает —
    // единственный писатель/стирающий глобального ключа — держатель лидера.
    [Fact]
    public async Task NonLeader_DoesNotTouchBackupsDisabled()
    {
        // Arrange: лидер занят другим инстансом (txn NotExists провалится);
        // стоп-ключ отсутствует
        var (loop, etcd, cts) = CreateGateLoop(enabled: false, seedLeader: """{"instance":"other"}""");

        // Act: цикл идёт несколько тиков (живость — отметками HealthState)
        await loop.StartAsync(cts.Token);
        for (var i = 0; i < 20; i++)
            await Task.Delay(50, TestContext.Current.CancellationToken);
        cts.Cancel();

        // Assert: ключа нет — не-лидер не пишет (и не мешает чужому лидерству)
        (await DisabledKeyRaw(etcd)).Should().BeNull("a non-leader must not put the disabled key");
        etcd.Store["/pgworker/leader"].Value.Should().Be("""{"instance":"other"}""",
            "a non-leader must not take over the occupied leadership");
    }

    // AAA (решение spec §7 №7): инстанс при СТАРТЕ с Enabled=true снимает
    // /pgworker/backups-disabled сразу (до захвата лидерства) — включение
    // подсистемы не ждёт тика лидера.
    [Fact]
    public async Task StartupEnabled_DeletesBackupsDisabled_BeforeLeadership()
    {
        // Arrange: Enabled=true, стоп-ключ стоит, лидерство НЕ наше (занято)
        var (loop, etcd, cts) = CreateGateLoop(enabled: true, seedLeader: """{"instance":"other"}""", seedDisabledKey: true);

        // Act: старт цикла — стартовый del выполняется до первого тика
        await loop.StartAsync(cts.Token);
        for (var i = 0; i < 200 && await DisabledKeyRaw(etcd) is not null; i++)
            await Task.Delay(25, TestContext.Current.CancellationToken);
        cts.Cancel();

        // Assert: ключ снят стартовым del даже без лидерства этого инстанса
        (await DisabledKeyRaw(etcd)).Should().BeNull(
            "an instance starting with Enabled=true must delete the disabled key at startup");
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
// файловые методы интерфейса в этом тесте не используются —
        // заглушка (фейк нужен только перечисленным выше операциям).
        public Task<Result> PutFileAsync(string key, string filePath, CancellationToken ct = default)
            => Task.FromResult(Result.Failed(new ApplicationException("put-file: not supported by this fake")));

        public Task<Result> GetObjectToFileAsync(string cluster, string shard, string key, string destFilePath, CancellationToken ct = default)
            => Task.FromResult(Result.Failed(new ApplicationException("get-to-file: not supported by this fake")));
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
