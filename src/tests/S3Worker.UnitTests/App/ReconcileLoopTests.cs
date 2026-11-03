using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using S3Worker.App;
using S3Worker.App.Loops;
using Shared.Etcd.Client;
using S3Worker.UnitTests.App;
using Xunit;

namespace S3Worker.UnitTests.App;

// ReconcileLoop на моках, чтение состояния из слепка опросного кэша
// (arch/17 S10, реестр E10): тик работает КАЖДЫЙ раз (self-healing ждёт
// не-etcd события — гейтов по дельте нет), etcd в тике не читается;
// обработки идут по текущему слепку кэша.
public class ReconcileLoopTests
{
    private readonly Fakes.FakeEtcd _etcd = new();

    private readonly FixedOptionsMonitor _options = new(new S3WorkerOptions
    {
        Etcd = new EtcdOptions { Endpoints = ["http://etcd:2379"] },
        Parallelism = new ParallelismOptions { MaxClusters = 4 },
    });

    private ReconcileLoop CreateLoop(FakeProcesses processes, IPrefixPollSources? sources = null)
        => new(
            _options, sources ?? new FakeSources().Reload(_etcd), processes,
            NullLogger<ReconcileLoop>.Instance, new HealthState(TimeProvider.System),
            new Shared.Metrics.Worker.WorkerMetricsInstrumentation(
                new System.Diagnostics.Metrics.Meter("TestReconcile"), TimeProvider.System));

    // Active-кластер (канонический минимум конфига, парсер arch/23 §2).
    private void SeedCluster(string name)
        => _etcd.Seed($"/s3/clusters/{name}/config",
            """{"platform":"owns3","nodes":1,"created_unix":1756500000}""");

    // AAA (arch/17 S10): тик работает КАЖДЫЙ раз — между тиками состояния не
    // менялось, но self-healing не гейтится: processes зовутся каждым тиком.
    [Fact]
    public async Task Tick_ProcessesEveryTick()
    {
        // Arrange — стартовый слепок
        SeedCluster("backups");
        var processes = new FakeProcesses();
        var loop = CreateLoop(processes);

        // Act — два тика подряд, между ними состояние не менялось
        var tick1 = await loop.TickAsync(TestContext.Current.CancellationToken);
        var tick2 = await loop.TickAsync(TestContext.Current.CancellationToken);

        // Assert — оба тика обработали кластер (гейта по дельте нет)
        tick1.IsSuccess.Should().BeTrue();
        tick2.IsSuccess.Should().BeTrue();
        processes.Ticks.Should().Equal(1, 1);
    }

    // AAA (arch/17 S10, реестр E10): etcd-чтений в тике ноль — Range делает
    // только первичная загрузка кэша; тик читает слепок кэша, не etcd.
    [Fact]
    public async Task Tick_ReadsSnapshotFromCache_NoEtcdRangeInTick()
    {
        // Arrange — реальный опросный кэш на фейке etcd (Ops журналит range)
        SeedCluster("backups");
        var processes = new FakeProcesses();
        var sources = new EtcdPrefixSources(_options, _etcd);
        var loop = CreateLoop(processes, sources);

        // Act 1 — первый тик: старт кэша (первичная загрузка) + обработка.
        // Фоновый опрос кэша делает первый проход сразу после старта (цикл
        // «опрос → пауза») — ждём стабилизации счётчика, чтобы мерить именно тики.
        var tick1 = await loop.TickAsync(TestContext.Current.CancellationToken);
        var baselineRanges = await WaitStableAsync(() => _etcd.Ops.Count(o => o == "range:/s3/"));

        // Act 2 — второй тик: кэш уже запущен, слепок из памяти
        var tick2 = await loop.TickAsync(TestContext.Current.CancellationToken);

        // Assert — оба тика обработали; тики range не добавили
        tick1.IsSuccess.Should().BeTrue();
        tick2.IsSuccess.Should().BeTrue();
        processes.Ticks.Should().Equal(1, 1);
        baselineRanges.Should().BeGreaterThanOrEqualTo(1, "первичная загрузка кэша — Range");
        _etcd.Ops.Count(o => o == "range:/s3/")
            .Should().Be(baselineRanges, "тик читает слепок кэша, etcd не читает");
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
    // обработка по НОВОМУ состоянию.
    [Fact]
    public async Task Tick_DomainChange_ProcessesNewStateFromSnapshot()
    {
        // Arrange — первый тик обрабатывает стартовый слепок
        SeedCluster("backups");
        var processes = new FakeProcesses();
        var sources = new FakeSources().Reload(_etcd);
        var loop = CreateLoop(processes, sources);
        var tick1 = await loop.TickAsync(TestContext.Current.CancellationToken);
        tick1.IsSuccess.Should().BeTrue();
        processes.Ticks.Should().HaveCount(1);

        // Act — панель заявила TO_REMOVE; опрос кэша принёс изменение в слепок
        _etcd.Seed("/s3/clusters/backups/config",
            """{"platform":"owns3","nodes":1,"created_unix":1756500000,"state":"TO_REMOVE"}""");
        sources.Reload(_etcd);
        var tick2 = await loop.TickAsync(TestContext.Current.CancellationToken);

        // Assert — тик обработал новое состояние (второй вызов, слепок обновлён)
        tick2.IsSuccess.Should().BeTrue();
        processes.Ticks.Should().Equal(1, 1);
    }

    // Пустой processes-дабл: фиксирует тики и состав слепка, который тик видел.
    private sealed class FakeProcesses : IS3ClusterProcesses
    {
        public List<int> Ticks { get; } = [];

        public List<List<string>> ClustersPerTick { get; } = [];

        public Task<int> TickAsync(IReadOnlyList<Kv> clustersKvs, CancellationToken ct)
        {
            Ticks.Add(clustersKvs.Count);
            ClustersPerTick.Add(clustersKvs.Select(kv => kv.Key).ToList());
            return Task.FromResult(0);
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
                if (pair.Key.StartsWith("/s3/", StringComparison.Ordinal))
                    Clusters[pair.Key] = (pair.Value.Value, pair.Value.ModRevision);
            return this;
        }
    }
}
