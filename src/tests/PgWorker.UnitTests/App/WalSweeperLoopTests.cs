using Microsoft.Extensions.Logging.Abstractions;
using PgWorker.App;
using PgWorker.App.Loops;
using PgWorker.Backups;
using PgWorker.Backups.Supervisor;
using PgWorker.UnitTests.Provisioning;
using Shared.Etcd.Coordination;
using System.Diagnostics.Metrics;
using Xunit;

namespace PgWorker.UnitTests.App;

// Цикл чистки wal/ (arch/backups/05-etcd-contract.md, arch/metrics/02-dictionary.md §2): при Backups:Enabled=false
// серия loop="wal-sweeper" не пишется (выключенная подсистема не алертится),
// цикл тикает без исключений; лидерный путь покрывается E2E.
public sealed class WalSweeperLoopTests
{
    [Fact]
    public async Task WalSweeperLoop_DisabledQuiet_RunsWithoutLoopSeries()
    {
        // Arrange — выключенная подсистема, быстрый скан
        var etcd = new Fakes.FakeEtcd();
        var endpoints = new[] { "http://etcd:2379" };
        var options = new FixedOptionsMonitor(new PgWorkerOptions
        {
            Etcd = new EtcdOptions { Endpoints = endpoints },
            Loops = new LoopsOptions { ScanIntervalSec = 1 },
            Backups = new BackupsOptions { Enabled = false },
        });
        var claims = new ClaimStore("/pgworker", endpoints, etcd, TimeProvider.System);
        var sweeper = new WalSweeperProcess(
            etcd, endpoints, UnitPoolFactory.Disabled(), claims,
            new WorkJournal("/pgworker", etcd, endpoints),
            () => new BackupsRuntimeOptions(Enabled: true), TimeProvider.System,
            NullLogger<WalSweeperProcess>.Instance);
        using var meter = new Meter("TestWalSweepLoop");
        var metrics = new Shared.Metrics.Worker.WorkerMetricsInstrumentation(meter, TimeProvider.System);
        var loop = new WalSweeperLoop(options, claims, sweeper, metrics,
            NullLogger<WalSweeperLoop>.Instance);
        using var cts = new CancellationTokenSource();

        // Наблюдатель экспортируемой серии loop="wal-sweeper" (DebugSnapshot —
        // internal другого тест-проекта; проверяем сам экспорт инструмента).
        var measuredLoops = new List<string>();
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter == meter)
                    l.EnableMeasurementEvents(instrument);
            },
        };
        listener.SetMeasurementEventCallback<long>((instrument, _, tags, _) =>
        {
            if (instrument.Name != "worker.loop.last_success_timestamp_seconds")
                return;
            foreach (var tag in tags)
                if (tag.Key == "loop" && tag.Value is string loopName)
                    lock (measuredLoops)
                        measuredLoops.Add(loopName);
        });
        listener.Start();

        // Act — цикл живёт два скана и останавливается
        await loop.StartAsync(cts.Token);
        await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        await cts.CancelAsync();
        await loop.StopAsync(CancellationToken.None);
        listener.RecordObservableInstruments();

        // Assert — серия живости выключенной подсистемы не эмитится
        lock (measuredLoops)
            measuredLoops.Should().NotContain("wal-sweeper");
    }
}
