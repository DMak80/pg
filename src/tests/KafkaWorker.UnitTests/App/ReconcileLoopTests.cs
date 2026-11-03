using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using KafkaWorker.App;
using KafkaWorker.App.Loops;
using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using KafkaWorker.Provisioning.Kafka;
using KafkaWorker.UnitTests.Provisioning;
using Xunit;

namespace KafkaWorker.UnitTests.App;

// ReconcileLoop на моках, чтение состояния из слепка опросного кэша
// (arch/17 S10, реестр E10): тик работает КАЖДЫЙ раз (self-healing ждёт
// не-etcd события — гейтов по дельте нет), etcd в тике не читается;
// обработки идут по текущему слепку кэша.
public class ReconcileLoopTests
{
    private readonly Fakes.FakeEtcd _etcd = new();

    private readonly FixedOptionsMonitor _options = new(new KafkaWorkerOptions
    {
        Etcd = new EtcdOptions { Endpoints = ["http://etcd:2379"] },
        Loops = new LoopsOptions { ScanIntervalSec = 5, ErrorDelayMs = 10 },
        Parallelism = new ParallelismOptions { MaxClusters = 4 },
    });

    private ReconcileLoop CreateLoop(FakeProcesses processes, IPrefixPollSources? sources = null)
        => new(
            _options, sources ?? new FakeSources().Reload(_etcd),
            new ClaimStore("/kafkaworker", _options.CurrentValue.Etcd.Endpoints, _etcd, TimeProvider.System),
            processes,
            new WorkJournal("/kafkaworker", _etcd, _options.CurrentValue.Etcd.Endpoints),
            NullLogger<ReconcileLoop>.Instance, new HealthState(TimeProvider.System),
            new Shared.Metrics.Worker.WorkerMetricsInstrumentation(
                new System.Diagnostics.Metrics.Meter("TestReconcile"), TimeProvider.System),
            new KafkaClusterBackoff(TimeProvider.System));

    // Active-кластер (state отсутствует = Active); при state != null — заявка.
    private void SeedCluster(string name, string? state)
    {
        var config = state is null
            ? """{"brokers":1,"replication_factor":1,"min_insync_replicas":1,"default_partitions":1,"default_retention_ms":1}"""
            : $$"""{"brokers":1,"replication_factor":1,"min_insync_replicas":1,"default_partitions":1,"default_retention_ms":1,"state":"{{state}}"}""";
        _etcd.Seed($"/kafka/clusters/{name}/config", config);
        _etcd.Seed($"/kafka/clusters/{name}/brokers/broker1/state", "RUNNING");
    }

    // AAA (arch/17 S10): тик работает КАЖДЫЙ раз — между тиками состояния не
    // менялось, но self-healing не гейтится: processes зовутся каждым тиком.
    [Fact]
    public async Task Tick_ProcessesEveryTick()
    {
        // Arrange — стартовый слепок
        SeedCluster("events", null);
        var processes = new FakeProcesses();
        var loop = CreateLoop(processes);

        // Act — два тика подряд, между ними состояние не менялось
        var tick1 = await loop.TickAsync(TestContext.Current.CancellationToken);
        var tick2 = await loop.TickAsync(TestContext.Current.CancellationToken);

        // Assert — оба тика обработали кластер (гейта по дельте нет)
        tick1.IsSuccess.Should().BeTrue();
        tick2.IsSuccess.Should().BeTrue();
        processes.Active.Should().Equal("events", "events");
    }

    // AAA (arch/17 S10, реестр E10): etcd-чтений в тике ноль — Range делают
    // только старт кэша и его фоновый опрос; тик читает слепок кэша, не etcd.
    [Fact]
    public async Task Tick_ReadsSnapshotFromCache_NoEtcdRangeInTick()
    {
        // Arrange — реальный опросный кэш на фейке etcd (RangeCalls считает range)
        SeedCluster("events", null);
        var processes = new FakeProcesses();
        var sources = new EtcdPrefixSources(_options, _etcd);
        var loop = CreateLoop(processes, sources);

        // Act 1 — первый тик: старт кэша (первичная загрузка) + обработка.
        // Фоновый опрос кэша делает первый проход сразу после старта (цикл
        // «опрос → пауза») — ждём стабилизации счётчика, чтобы мерить именно тики.
        var tick1 = await loop.TickAsync(TestContext.Current.CancellationToken);
        var baselineRanges = await WaitStableAsync(() => _etcd.RangeCalls);

        // Act 2 — второй тик: кэш уже запущен, слепок из памяти
        var tick2 = await loop.TickAsync(TestContext.Current.CancellationToken);

        // Assert — оба тика обработали; тики range не добавили
        tick1.IsSuccess.Should().BeTrue();
        tick2.IsSuccess.Should().BeTrue();
        processes.Active.Should().Equal("events", "events");
        baselineRanges.Should().BeGreaterThanOrEqualTo(1, "первичная загрузка кэша — Range");
        _etcd.RangeCalls.Should().Be(baselineRanges, "тик читает слепок кэша, etcd не читает");
        await sources.StopAsync(CancellationToken.None);
    }

    // Ждёт стабилизации счётчика (два равных чтения с шагом 50 мс, бюджет 2 с):
    // первый проход фонового опроса кэша успел завершиться, следующий — за 5 с
    // (за пределами теста).
    private static async Task<int> WaitStableAsync(Func<int> count)
    {
        var last = -1;
        var deadline = DateTime.UtcNow.AddSeconds(2);
        while (DateTime.UtcNow < deadline)
        {
            var current = count();
            if (current == last)
                return current;
            last = current;
            await Task.Delay(50, TestContext.Current.CancellationToken);
        }

        return last;
    }

    // AAA (arch/17 S10): изменение домена доезжает до тика через слепок кэша —
    // обработка по НОВОМУ состоянию (заявка provisioning вместо Active).
    [Fact]
    public async Task Tick_DomainChange_ProcessesNewStateFromSnapshot()
    {
        // Arrange — первый тик обрабатывает Active-кластер
        SeedCluster("events", null);
        var processes = new FakeProcesses();
        var sources = new FakeSources().Reload(_etcd);
        var loop = CreateLoop(processes, sources);
        var tick1 = await loop.TickAsync(TestContext.Current.CancellationToken);
        tick1.IsSuccess.Should().BeTrue();
        processes.Active.Should().Equal("events");

        // Act — панель заявила NOT_INITIALIZED; опрос кэша принёс изменение в слепок
        SeedCluster("events", "NOT_INITIALIZED");
        sources.Reload(_etcd);
        var tick2 = await loop.TickAsync(TestContext.Current.CancellationToken);

        // Assert — тик обработал новое состояние: provisioning
        tick2.IsSuccess.Should().BeTrue();
        processes.Active.Should().HaveCount(1);
        processes.Provisioned.Should().Equal("events");
    }

    // Пустые процессы-дабл: фиксируют, какой процесс и для какого кластера звался.
    private sealed class FakeProcesses : IKafkaClusterProcesses
    {
        public List<string> Provisioned { get; } = [];

        public List<string> Deprovisioned { get; } = [];

        public List<string> Active { get; } = [];

        public Task<Result> ProvisionAsync(KafkaClusterSnapshot snap, CancellationToken ct)
        {
            Provisioned.Add(snap.Cluster);
            return Task.FromResult(Result.Success());
        }

        public Task<Result> DeprovisionAsync(KafkaClusterSnapshot snap, CancellationToken ct)
        {
            Deprovisioned.Add(snap.Cluster);
            return Task.FromResult(Result.Success());
        }

        public Task<Result> ActiveAsync(KafkaClusterSnapshot snap, CancellationToken ct)
        {
            Active.Add(snap.Cluster);
            return Task.FromResult(Result.Success());
        }
    }

    // Управляемый источник контроль-плейна тика (замена EtcdPrefixSources на
    // фейках, arch/17 S10): слепок правит тест — «опрос принёс изменение»
    // эмулируется перечитыванием Store (реальный кэш обновляет слепок своим
    // фоновым опросом); «сбой опроса» — слепок остаётся прежним.
    private sealed class FakeSources : IPrefixPollSources
    {
        public bool Started { get; private set; }

        private readonly Dictionary<string, (string Value, long ModRevision)> Clusters = new();

        IReadOnlyDictionary<string, (string Value, long ModRevision)> IPrefixPollSources.ClustersSnapshot => Clusters;

        public Task StartAsync(CancellationToken ct)
        {
            Started = true;
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken ct)
        {
            Started = false;
            return Task.CompletedTask;
        }

        // Успешный опрос: слепок домена перечитан из Store.
        public FakeSources Reload(Fakes.FakeEtcd etcd)
        {
            Clusters.Clear();
            foreach (var pair in etcd.Store)
                if (pair.Key.StartsWith("/kafka/clusters/", StringComparison.Ordinal))
                    Clusters[pair.Key] = (pair.Value.Value, pair.Value.ModRevision);
            return this;
        }
    }
}
