using Microsoft.Extensions.Logging.Abstractions;
using FluentAssertions;
using KafkaWorker.App;
using KafkaWorker.App.Loops;
using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using KafkaWorker.Etcd;
using KafkaWorker.Etcd.Coordination;
using KafkaWorker.Provisioning.Kafka;
using KafkaWorker.UnitTests.Provisioning;
using Xunit;

namespace KafkaWorker.UnitTests.App;

// Живой-Ф7 (t09; spec §3.1): StatusError = «последний тик» — провальный тик
// зажигает ошибку healthz, успешный гасит. Липкая ошибка = вечный 503
// «<Loop> service has error» при живых тиках — дефект наблюдаемости 2026-08-31.
public class LoopsHealthResetTests
{
    private static FixedOptionsMonitor Options(
        int scanSec = 0, int errorDelayMs = 200, int snapshotMin = 0, int keepaliveSec = 0) =>
        new(new KafkaWorkerOptions
        {
            Etcd = new EtcdOptions { Endpoints = ["http://etcd:2379"] },
            Loops = new LoopsOptions
            {
                ScanIntervalSec = scanSec, ErrorDelayMs = errorDelayMs,
                SnapshotIntervalMin = snapshotMin, KeepaliveSec = keepaliveSec,
            },
        });

    private static async Task WaitUntilAsync(Func<bool> done)
    {
        for (var i = 0; i < 300 && !done(); i++)
            await Task.Delay(10, TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task ReconcileLoop_StatusErrorStickyUntilNextSuccessfulTick()
    {
        // Arrange: цикл с нулевым интервалом; etcd «падает» на range (RangeFault →
        // тик проваливается, ErrorDelayMs=200 — детерминированное окно), затем оживает.
        var etcd = new Fakes.FakeEtcd();
        etcd.RangeFault = _ => new ApplicationException("etcd недоступен");
        var loop = new ReconcileLoop(
            Options(), etcd,
            new ClaimStore(["http://etcd:2379"], etcd, TimeProvider.System),
            new FakeProcesses(),
            new WorkJournal(etcd, ["http://etcd:2379"]),
            NullLogger<ReconcileLoop>.Instance, new HealthState(TimeProvider.System),
            new Shared.Metrics.Worker.WorkerMetricsInstrumentation(
                new System.Diagnostics.Metrics.Meter("TestLoops"), TimeProvider.System),
            new KafkaClusterBackoff(TimeProvider.System));
        using var cts = new CancellationTokenSource();
        await loop.StartAsync(cts.Token);

        // Act 1: ждём провального тика.
        await WaitUntilAsync(() => !loop.StatusError.IsSuccess);

        // Assert 1: ошибка последнего тика видна (unhealthy).
        loop.StatusError.IsSuccess.Should().BeFalse();

        // Act 2: etcd оживает → следующий тик успешен.
        etcd.RangeFault = null;
        await WaitUntilAsync(() => loop.StatusError.IsSuccess);

        // Assert 2: успешный тик погасил ошибку (healthz = «последний тик»).
        loop.StatusError.IsSuccess.Should().BeTrue(loop.StatusError.Error?.ToString());
        await loop.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task SnapshotLoop_StatusErrorResetBySuccessfulTake()
    {
        // Arrange: лидер (одиночный инстанс), снапшот-джоб во временной папке;
        // снятие снапшота падает (SnapshotFault), затем оживает.
        var etcd = new Fakes.FakeEtcd();
        etcd.SnapshotFault = () => new ApplicationException("snapshot failed");
        var options = Options(snapshotMin: 0);
        var job = new SnapshotJob(
            etcd, ["http://etcd:2379"],
            Path.Combine(Path.GetTempPath(), $"kfw-health-{Guid.NewGuid():N}"), 10, 60);
        var loop = new SnapshotLoop(
            options, new ClaimStore(["http://etcd:2379"], etcd, TimeProvider.System), job,
            NullLogger<SnapshotLoop>.Instance, new HealthState(TimeProvider.System), TimeProvider.System,
            new Shared.Metrics.Worker.WorkerMetricsInstrumentation(
                new System.Diagnostics.Metrics.Meter("TestLoops"), TimeProvider.System));
        using var cts = new CancellationTokenSource();
        await loop.StartAsync(cts.Token);

        // Act 1: лидерство захвачено, первый TakeAsync провален.
        await WaitUntilAsync(() => !loop.StatusError.IsSuccess);
        loop.StatusError.IsSuccess.Should().BeFalse();

        // Act 2: снапшот оживает → успешный TakeAsync.
        etcd.SnapshotFault = null;
        await WaitUntilAsync(() => loop.StatusError.IsSuccess);

        // Assert: успешный снимок погасил ошибку (живой-Ф7).
        loop.StatusError.IsSuccess.Should().BeTrue(loop.StatusError.Error?.ToString());
        await loop.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task SnapshotLoop_NotLeader_KeepsStatusErrorSuccess()
    {
        // Arrange: лидерство занято другим инстансом (version>0 у /kafkaworker/leader —
        // txn NotExists проигран) — цикл живёт в ветке не-лидера (MarkSnapshotTick,
        // без TakeAsync; spec §3.1: у не-лидера ошибок взятия не бывает).
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/kafkaworker/leader", """{"instance":"other","since_unix":1}""");
        var job = new SnapshotJob(
            etcd, ["http://etcd:2379"],
            Path.Combine(Path.GetTempPath(), $"kfw-health-{Guid.NewGuid():N}"), 10, 60);
        var loop = new SnapshotLoop(
            Options(), new ClaimStore(["http://etcd:2379"], etcd, TimeProvider.System), job,
            NullLogger<SnapshotLoop>.Instance, new HealthState(TimeProvider.System), TimeProvider.System,
            new Shared.Metrics.Worker.WorkerMetricsInstrumentation(
                new System.Diagnostics.Metrics.Meter("TestLoops"), TimeProvider.System));
        using var cts = new CancellationTokenSource();
        await loop.StartAsync(cts.Token);

        // Act: не-лидер тикает (попытки захвата проиграны) несколько проходов.
        await WaitUntilAsync(() => loop.Inited && loop.Working);
        await Task.Delay(100, TestContext.Current.CancellationToken);

        // Assert: ошибок нет и не было — StatusError Success (сброс/фейл — только
        // ветка лидера с реальным TakeAsync).
        loop.StatusError.IsSuccess.Should().BeTrue();
        await loop.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task KeepaliveLoop_PassKeepsStatusErrorSuccess()
    {
        // Arrange: у KeepaliveLoop фейлящих тиков нет — контракт цикла: пока проходы
        // контура живы, StatusError остаётся Success (сброс каждым проходом).
        var etcd = new Fakes.FakeEtcd();
        var loop = new KeepaliveLoop(
            Options(keepaliveSec: 0), new ClaimStore(["http://etcd:2379"], etcd, TimeProvider.System),
            NullLogger<KeepaliveLoop>.Instance, new HealthState(TimeProvider.System),
            new Shared.Metrics.Worker.WorkerMetricsInstrumentation(
                new System.Diagnostics.Metrics.Meter("TestLoops"), TimeProvider.System));
        using var cts = new CancellationTokenSource();

        // Act: цикл жив несколько проходов.
        await loop.StartAsync(cts.Token);
        await WaitUntilAsync(() => loop.Inited && loop.Working);
        await Task.Delay(100, TestContext.Current.CancellationToken);

        // Assert: ошибки нет — healthz цикла Healthy.
        loop.StatusError.IsSuccess.Should().BeTrue();
        await loop.StopAsync(CancellationToken.None);
    }

    // Пустые процессы: тик ReconcileLoop без кластеров — успех.
    private sealed class FakeProcesses : IKafkaClusterProcesses
    {
        public Task<Result> ProvisionAsync(KafkaClusterSnapshot snap, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result> DeprovisionAsync(KafkaClusterSnapshot snap, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result> ActiveAsync(KafkaClusterSnapshot snap, CancellationToken ct)
            => Task.FromResult(Result.Success());
    }
}
