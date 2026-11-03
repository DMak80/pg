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

// ReconcileLoop на моках с гейтом тика по дельте (arch/17 S10): тик применяется
// только при факте изменения наблюдаемых префиксов; тик без изменений не
// работает (processes не зовутся), но heartbeat/loops-alive отмечаются всегда
// (watchdog жив, arch/16 §6).
public class ReconcileLoopTests
{
    private readonly Fakes.FakeEtcd _etcd = new();

    private readonly FixedOptionsMonitor _options = new(new KafkaWorkerOptions
    {
        Etcd = new EtcdOptions { Endpoints = ["http://etcd:2379"] },
        Loops = new LoopsOptions { ScanIntervalSec = 5, ErrorDelayMs = 10 },
        Parallelism = new ParallelismOptions { MaxClusters = 4 },
    });

    private ReconcileLoop CreateLoop(FakeProcesses processes, FakeSources? sources = null, HealthState? health = null)
    {
        sources ??= new FakeSources().Reload(_etcd);
        return new ReconcileLoop(
            _options, sources,
            new ClaimStore("/kafkaworker", _options.CurrentValue.Etcd.Endpoints, _etcd, TimeProvider.System),
            processes,
            new WorkJournal("/kafkaworker", _etcd, _options.CurrentValue.Etcd.Endpoints),
            NullLogger<ReconcileLoop>.Instance, health ?? new HealthState(TimeProvider.System),
            new Shared.Metrics.Worker.WorkerMetricsInstrumentation(
                new System.Diagnostics.Metrics.Meter("TestReconcile"), TimeProvider.System),
            new KafkaClusterBackoff(TimeProvider.System));
    }

    // Active-кластер (state отсутствует = Active); при state != null — заявка.
    private void SeedCluster(string name, string? state)
    {
        var config = state is null
            ? """{"brokers":1,"replication_factor":1,"min_insync_replicas":1,"default_partitions":1,"default_retention_ms":1}"""
            : $$"""{"brokers":1,"replication_factor":1,"min_insync_replicas":1,"default_partitions":1,"default_retention_ms":1,"state":"{{state}}"}""";
        _etcd.Seed($"/kafka/clusters/{name}/config", config);
        _etcd.Seed($"/kafka/clusters/{name}/brokers/broker1/state", "RUNNING");
    }

    // AAA (arch/17 S10, гейт тика): нет дельты с последнего обработанного
    // состояния → тик БЕЗ изменений (processes не зовутся, клэймов нет), но
    // heartbeat/loops-alive отмечаются всегда (watchdog жив, arch/16 §6).
    [Fact]
    public async Task Tick_NoDeltaSinceProcessed_SkipsWork_HeartbeatAlways()
    {
        // Arrange — стартовый слепок; отметки HealthState — на управляемых часах
        SeedCluster("events", null);
        var processes = new FakeProcesses();
        var clock = new MutableClock();
        var health = new HealthState(clock);
        var sources = new FakeSources().Reload(_etcd);
        var loop = CreateLoop(processes, sources, health);

        // Act 1 — первый тик обрабатывает стартовый слепок (baseline)
        var tick1 = await loop.TickAsync(TestContext.Current.CancellationToken);

        // Assert 1 — процесс зван
        tick1.IsSuccess.Should().BeTrue();
        processes.Active.Should().Equal("events");
        var afterTick1 = health.Snapshot();

        // Act 2 — дельты нет (опрос не принёс изменений): тик без изменений
        clock.Now = clock.Now.AddMinutes(1);
        var tick2 = await loop.TickAsync(TestContext.Current.CancellationToken);

        // Assert 2 — работы нет; heartbeat/loops-alive обновлены (watchdog жив),
        // счётчик клэймов не тронут (клэймы держит keepalive-цикл, не тик)
        tick2.IsSuccess.Should().BeTrue();
        processes.Active.Should().HaveCount(1);
        var afterTick2 = health.Snapshot();
        afterTick2.LastReconcileTick.Should().BeAfter(afterTick1.LastReconcileTick!.Value);
        afterTick2.LastReconcileActivity.Should().BeAfter(afterTick1.LastReconcileActivity!.Value);
        afterTick2.ClaimsHeld.Should().Be(afterTick1.ClaimsHeld);
    }

    // AAA (arch/17 S10): дельта наблюдаемого префикса (изменение ключа домена,
    // опросный кэш поднял Changed) → тик исполняется и обрабатывает НОВОЕ
    // состояние (классификация по нему — заявка provisioning вместо Active).
    [Fact]
    public async Task Tick_DeltaInWatchedPrefix_ProcessesNewState()
    {
        // Arrange — первый тик обрабатывает Active-кластер
        SeedCluster("events", null);
        var processes = new FakeProcesses();
        var sources = new FakeSources().Reload(_etcd);
        var loop = CreateLoop(processes, sources: sources);
        var tick1 = await loop.TickAsync(TestContext.Current.CancellationToken);
        tick1.IsSuccess.Should().BeTrue();
        processes.Active.Should().Equal("events");

        // Act — дельта: панель заявила NOT_INITIALIZED; опрос кэша принёс её (Version++)
        SeedCluster("events", "NOT_INITIALIZED");
        sources.PollChanged(_etcd);
        var tick2 = await loop.TickAsync(TestContext.Current.CancellationToken);

        // Assert — тик по дельте исполнился по новому состоянию: provisioning
        tick2.IsSuccess.Should().BeTrue();
        processes.Active.Should().HaveCount(1);
        processes.Provisioned.Should().Equal("events");
    }

    // AAA (arch/14 §3 + S10, удаление = различие): смерть чужого инстанса гасит
    // его клэйм-lease — ключ исчезает из /kafkaworker/claims/; опросный кэш
    // видит удаление → дельта → тик исполняется и клэйм берём — takeover
    // доезжает (≤ TTL 15 с + тик).
    [Fact]
    public async Task Tick_ClaimKeyVanishes_Delta_TakeoverProcesses()
    {
        // Arrange — клэйм кластера у чужого инстанса
        SeedCluster("events", "NOT_INITIALIZED");
        _etcd.Seed("/kafkaworker/claims/events", """{"instance":"someoneelse","since_unix":1}""");
        var processes = new FakeProcesses();
        var sources = new FakeSources().Reload(_etcd);
        var loop = CreateLoop(processes, sources: sources);

        // Act 1 — чужой клэйм жив: кластер не обрабатываем (exclusivity, Д2)
        var tick1 = await loop.TickAsync(TestContext.Current.CancellationToken);
        tick1.IsSuccess.Should().BeTrue();
        processes.Provisioned.Should().BeEmpty();

        // Act 2 — чужой инстанс умер: ключ клэйма исчез; дельта удаления дошла до тика
        _etcd.Store.Remove("/kafkaworker/claims/events");
        sources.PollChanged(_etcd);
        var tick2 = await loop.TickAsync(TestContext.Current.CancellationToken);

        // Assert — клэйм взят, процесс доведён: takeover доехал
        tick2.IsSuccess.Should().BeTrue();
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
    // фейках, arch/17 S10): слепок домена и Version правит тест — «опрос принёс
    // изменение» эмулируется перечитыванием Store: Version растёт ТОЛЬКО если
    // наблюдаемое множество реально изменилось (реальный кэш поднимает Changed
    // на факте различия); «сбой опроса» — ничего не меняет.
    private sealed class FakeSources : IPrefixPollSources
    {
        // Наблюдаемые префиксы — как у прод-реализации (arch/16 §5); слепок
        // домена — как у прод (ClustersSnapshot = только /kafka/clusters/).
        private static readonly string[] Prefixes =
        [
            "/kafka/clusters/",
            "/kafkaworker/claims/",
            "/kafkaworker/locks/",
            "/kafkaworker/rotations/",
            "/kafkaworker/admin_rotations/",
            "/kafkaworker/ca_rotations/",
            "/kafkaworker/rebalances/",
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
                if (pair.Key.StartsWith("/kafka/clusters/", StringComparison.Ordinal))
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
