using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using S3Worker.App;
using S3Worker.App.Loops;
using Shared.Etcd.Client;
using S3Worker.UnitTests.App;
using Xunit;

namespace S3Worker.UnitTests.App;

// ReconcileLoop на моках с гейтом тика по дельте (arch/17 S10): тик применяется
// только при факте изменения наблюдаемых префиксов; тик без изменений не
// работает (processes не зовутся), но heartbeat/loops-alive отмечаются всегда
// (watchdog жив, arch/24 §6).
public class ReconcileLoopTests
{
    private readonly Fakes.FakeEtcd _etcd = new();

    private readonly FixedOptionsMonitor _options = new(new S3WorkerOptions
    {
        Etcd = new EtcdOptions { Endpoints = ["http://etcd:2379"] },
        Parallelism = new ParallelismOptions { MaxClusters = 4 },
    });

    private ReconcileLoop CreateLoop(
        FakeProcesses processes, FakeSources? sources = null, HealthState? health = null)
        => new(
            _options, sources ?? new FakeSources().Reload(_etcd), processes,
            NullLogger<ReconcileLoop>.Instance, health ?? new HealthState(TimeProvider.System),
            new Shared.Metrics.Worker.WorkerMetricsInstrumentation(
                new System.Diagnostics.Metrics.Meter("TestReconcile"), TimeProvider.System));

    // Active-кластер (канонический минимум конфига, парсер arch/23 §2).
    private void SeedCluster(string name)
        => _etcd.Seed($"/s3/clusters/{name}/config",
            """{"platform":"owns3","nodes":1,"created_unix":1756500000}""");

    // AAA (arch/17 S10, гейт тика): нет дельты с последнего обработанного
    // состояния → тик БЕЗ изменений (processes не зовутся, клэймов нет), но
    // heartbeat/loops-alive отмечаются всегда (watchdog жив, arch/24 §6).
    [Fact]
    public async Task Tick_NoDeltaSinceProcessed_SkipsWork_HeartbeatAlways()
    {
        // Arrange — стартовый слепок; отметки HealthState — на управляемых часах
        SeedCluster("backups");
        var processes = new FakeProcesses();
        var clock = new MutableClock();
        var health = new HealthState(clock);
        var sources = new FakeSources().Reload(_etcd);
        var loop = CreateLoop(processes, sources, health);

        // Act 1 — первый тик обрабатывает стартовый слепок (baseline)
        var tick1 = await loop.TickAsync(TestContext.Current.CancellationToken);

        // Assert 1 — процесс зван
        tick1.IsSuccess.Should().BeTrue();
        processes.Ticks.Should().HaveCount(1);
        var afterTick1 = health.Snapshot();

        // Act 2 — дельты нет (опрос не принёс изменений): тик без изменений
        clock.Now = clock.Now.AddMinutes(1);
        var tick2 = await loop.TickAsync(TestContext.Current.CancellationToken);

        // Assert 2 — работы нет; heartbeat/loops-alive обновлены (watchdog жив),
        // счётчик клэймов не тронут (клэймы держит keepalive-цикл, не тик)
        tick2.IsSuccess.Should().BeTrue();
        processes.Ticks.Should().HaveCount(1);
        var afterTick2 = health.Snapshot();
        afterTick2.LastReconcileTick.Should().BeAfter(afterTick1.LastReconcileTick!.Value);
        afterTick2.LastReconcileActivity.Should().BeAfter(afterTick1.LastReconcileActivity!.Value);
        afterTick2.ClaimsHeld.Should().Be(afterTick1.ClaimsHeld);
    }

    // AAA (arch/17 S10): дельта наблюдаемого префикса (изменение ключа домена,
    // опросный кэш поднял Changed) → тик исполняется по новому состоянию.
    [Fact]
    public async Task Tick_DeltaInWatchedPrefix_ProcessesNewState()
    {
        // Arrange — первый тик обрабатывает стартовый слепок
        SeedCluster("backups");
        var processes = new FakeProcesses();
        var sources = new FakeSources().Reload(_etcd);
        var loop = CreateLoop(processes, sources: sources);
        var tick1 = await loop.TickAsync(TestContext.Current.CancellationToken);
        tick1.IsSuccess.Should().BeTrue();
        processes.Ticks.Should().HaveCount(1);

        // Act — дельта: панель заявила TO_REMOVE; опрос кэша принёс её (Version++)
        _etcd.Seed($"/s3/clusters/backups/config",
            """{"platform":"owns3","nodes":1,"created_unix":1756500000,"state":"TO_REMOVE"}""");
        sources.PollChanged(_etcd);
        var tick2 = await loop.TickAsync(TestContext.Current.CancellationToken);

        // Assert — тик по дельте исполнился: кластер обработан снова
        tick2.IsSuccess.Should().BeTrue();
        processes.Ticks.Should().HaveCount(2);
    }

    // AAA (arch/14 §3 + S10, удаление = различие): смерть чужого инстанса гасит
    // его клэйм-lease — ключ исчезает из /s3worker/claims/; опросный кэш видит
    // удаление → дельта → тик исполняется — takeover доезжает (≤ TTL 15 с + тик).
    [Fact]
    public async Task Tick_ClaimKeyVanishes_Delta_Processes()
    {
        // Arrange — клэйм кластера у чужого инстанса
        SeedCluster("backups");
        _etcd.Seed("/s3worker/claims/backups", """{"instance":"someoneelse","since_unix":1}""");
        var processes = new FakeProcesses();
        var sources = new FakeSources().Reload(_etcd);
        var loop = CreateLoop(processes, sources: sources);

        // Act 1 — стартовый слепок (клэйм чужой — наблюдаемый префикс всё равно
        // даёт baseline тика)
        var tick1 = await loop.TickAsync(TestContext.Current.CancellationToken);
        tick1.IsSuccess.Should().BeTrue();
        processes.Ticks.Should().HaveCount(1);

        // Act 2 — чужой инстанс умер: ключ клэйма исчез; дельта удаления дошла до тика
        _etcd.Store.Remove("/s3worker/claims/backups");
        sources.PollChanged(_etcd);
        var tick2 = await loop.TickAsync(TestContext.Current.CancellationToken);

        // Assert — тик по дельте удаления исполнился
        tick2.IsSuccess.Should().BeTrue();
        processes.Ticks.Should().HaveCount(2);
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
    // фейках, arch/17 S10): слепок домена и Version правит тест — «опрос принёс
    // изменение» эмулируется перечитыванием Store: Version растёт ТОЛЬКО если
    // наблюдаемое множество реально изменилось (реальный кэш поднимает Changed
    // на факте различия); «сбой опроса» — ничего не меняет.
    private sealed class FakeSources : IPrefixPollSources
    {
        // Наблюдаемые префиксы — как у прод-реализации (arch/24 §5); слепок
        // домена — как у прод (ClustersSnapshot = только /s3/).
        private static readonly string[] Prefixes =
        [
            "/s3/",
            "/s3worker/claims/",
            "/s3worker/locks/",
            "/s3worker/rotations/",
            "/s3worker/probes/",
        ];

        public bool Started { get; private set; }

        public long Version { get; private set; }

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

        // Успешный опрос: слепки перечитаны из Store; дельта (Version++) — только
        // если наблюдаемое множество изменилось (добавление/изменение/удаление).
        public FakeSources PollChanged(Fakes.FakeEtcd etcd)
        {
            var observed = etcd.Store
                .Where(p => Prefixes.Any(pref => p.Key.StartsWith(pref, StringComparison.Ordinal)))
                .Select(p => $"{p.Key}={p.Value.Value}@{p.Value.ModRevision}")
                .OrderBy(s => s, StringComparer.Ordinal)
                .ToList();
            var signature = string.Join("\n", observed);
            if (signature == _lastObserved)
                return this;
            _lastObserved = signature;
            Reload(etcd);
            Version++;
            return this;
        }

        private string? _lastObserved;

        public FakeSources Reload(Fakes.FakeEtcd etcd)
        {
            Clusters.Clear();
            foreach (var pair in etcd.Store)
                if (pair.Key.StartsWith("/s3/", StringComparison.Ordinal))
                    Clusters[pair.Key] = (pair.Value.Value, pair.Value.ModRevision);
            return this;
        }
    }

    // Управляемые часы для HealthState: различимость отметок тик/без изменений.
    private sealed class MutableClock : TimeProvider
    {
        public DateTimeOffset Now { get; set; } = DateTimeOffset.UtcNow;

        public override DateTimeOffset GetUtcNow() => Now;
    }
}
