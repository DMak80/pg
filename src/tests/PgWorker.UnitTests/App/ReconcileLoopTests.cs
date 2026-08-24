using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using PgWorker.App;
using PgWorker.App.Loops;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Etcd.Coordination;
using PgWorker.Provisioning.Processes;
using PgWorker.UnitTests.Provisioning;

namespace PgWorker.UnitTests.App;

// ReconcileLoop на моках (задача 23; spec §6.2): тик читает /clusters/ + /service/,
// классифицирует, клэймит и вызывает нужный процесс; DeadShards надзора →
// BucketEvacuator; параллелизм кластеров ограничен SemaphoreSlim.
public class ReconcileLoopTests
{
    private readonly Fakes.FakeEtcd _etcd = new();

    private readonly FixedOptionsMonitor _options = new(new PgWorkerOptions
    {
        Etcd = new EtcdOptions { Endpoints = ["http://etcd:2379"] },
        Loops = new LoopsOptions { ScanIntervalSec = 5, ErrorDelayMs = 10 },
        Parallelism = new ParallelismOptions { MaxClusters = 4 },
    });

    private ReconcileLoop CreateLoop(FakeProcesses processes, ClaimStore? claims = null)
    {
        claims ??= new ClaimStore(_options.CurrentValue.Etcd.Endpoints, _etcd, TimeProvider.System);
        return new ReconcileLoop(
            _options, _etcd, claims, processes,
            new WorkJournal(_etcd, _options.CurrentValue.Etcd.Endpoints),
            NullLogger<ReconcileLoop>.Instance, new HealthState(TimeProvider.System));
    }

    private void SeedCluster(string name, string? state)
    {
        var config = state is null
            ? $$"""{"buckets":6,"dbname":"{{name}}"}"""
            : $$"""{"buckets":6,"dbname":"{{name}}","state":"{{state}}"}""";
        _etcd.Seed($"/clusters/{name}/config", config);
        _etcd.Seed($"/clusters/{name}/shards/shard1/replicas", "2");
        _etcd.Seed($"/clusters/{name}/shards/shard1/nodes/shard1a/state", "RUNNING");
        _etcd.Seed($"/clusters/{name}/shards/shard1/nodes/shard1b/state", "RUNNING");
        _etcd.Seed($"/clusters/{name}/shards/shard2/replicas", "2");
        _etcd.Seed($"/clusters/{name}/shards/shard2/nodes/shard2a/state", "RUNNING");
        _etcd.Seed($"/clusters/{name}/shards/shard2/nodes/shard2b/state", "RUNNING");
        for (var i = 0; i < 6; i++)
            _etcd.Seed($"/clusters/{name}/buckets/routing/bucket_{i}", $"shard{i % 2 + 1}");
    }

    [Fact]
    public async Task Tick_NotInitializedCluster_CallsProvisioningProcess()
    {
        // Arrange — кластер заявлен панелью (NOT_INITIALIZED)
        SeedCluster("shop", "NOT_INITIALIZED");
        var processes = new FakeProcesses();
        var loop = CreateLoop(processes);

        // Act
        var tick = await loop.TickAsync(TestContext.Current.CancellationToken);

        // Assert — тик успешен и доволен ровно нужный процесс; заявки переездов
        // в NOT_INITIALIZED не обрабатываются (spec §5.3 — только Active)
        tick.IsSuccess.Should().BeTrue();
        processes.Provisioned.Should().Equal("shop");
        processes.Deprovisioned.Should().BeEmpty();
        processes.Supervised.Should().BeEmpty();
        processes.Moved.Should().BeEmpty();
        processes.Scaled.Should().BeEmpty();
    }

    [Fact]
    public async Task Tick_ToRemoveCluster_CallsDeprovisioningProcess()
    {
        // Arrange — кластер переведён панелью в TO_REMOVE
        SeedCluster("shop", "TO_REMOVE");
        var processes = new FakeProcesses();
        var loop = CreateLoop(processes);

        // Act
        var tick = await loop.TickAsync(TestContext.Current.CancellationToken);

        // Assert
        tick.IsSuccess.Should().BeTrue();
        processes.Deprovisioned.Should().Equal("shop");
        processes.Provisioned.Should().BeEmpty();
        processes.Supervised.Should().BeEmpty();
        processes.Moved.Should().BeEmpty("TO_REMOVE не обрабатывает заявки переездов (spec §5.3)");
        processes.Scaled.Should().BeEmpty();
    }

    [Fact]
    public async Task Tick_ActiveCluster_CallsSupervisor()
    {
        // Arrange — инициализированный кластер (state отсутствует, Д1)
        SeedCluster("shop", null);
        var processes = new FakeProcesses();
        var loop = CreateLoop(processes);

        // Act
        var tick = await loop.TickAsync(TestContext.Current.CancellationToken);

        // Assert
        tick.IsSuccess.Should().BeTrue();
        processes.Supervised.Should().Equal("shop");
        processes.Provisioned.Should().BeEmpty();
    }

    // AAA: кластер Active после надзора обрабатывает заявки переездов (t01, spec §5.3)
    [Fact]
    public async Task Tick_ActiveCluster_CallsProcessMoves()
    {
        // Arrange — Active-кластер (state отсутствует, Д1): надзор → эвакуации → moves
        SeedCluster("shop", null);
        var processes = new FakeProcesses();
        var loop = CreateLoop(processes);

        // Act
        var tick = await loop.TickAsync(TestContext.Current.CancellationToken);

        // Assert — ProcessMovesAsync вызван после SuperviseAsync (порядок — Calls);
        // мёртвых шардов нет — moves идёт сразу за supervise
        tick.IsSuccess.Should().BeTrue();
        processes.Moved.Should().Equal("shop");
        processes.Calls.Should().ContainInOrder("supervise/shop", "moves/shop");
    }

    // AAA: scale-проход Active-ветки — после надзора, до эвакуаций/moves (t06 §5.1)
    [Fact]
    public async Task Tick_ActiveCluster_ScaleShardsAfterSuperviseBeforeMoves()
    {
        // Arrange — Active-кластер; надзор → scale-проход → moves (порядок §5.1)
        SeedCluster("shop", null);
        var processes = new FakeProcesses();
        var loop = CreateLoop(processes);

        // Act
        var tick = await loop.TickAsync(TestContext.Current.CancellationToken);

        // Assert — scale-проход вызван ровно один раз; порядок — Calls-трейс
        // (FakeProcesses записывает порядок: supervise → scale-shards → moves)
        tick.IsSuccess.Should().BeTrue();
        processes.Scaled.Should().Equal("shop");
        processes.Calls.Should().ContainInOrder("supervise/shop", "scale-shards/shop", "moves/shop");
    }

    [Fact]
    public async Task Tick_ClusterClaimedByOtherInstance_SkipsProcessing()
    {
        // Arrange — клэйм кластера уже у «другого инстанса» (ключ занят)
        SeedCluster("shop", "NOT_INITIALIZED");
        _etcd.Seed("/pgworker/claims/shop", """{"instance":"someoneelse","since_unix":1}""");
        var processes = new FakeProcesses();
        var loop = CreateLoop(processes);

        // Act
        var tick = await loop.TickAsync(TestContext.Current.CancellationToken);

        // Assert — тик не ошибка, но процессы не звались (exclusivity, Д2)
        tick.IsSuccess.Should().BeTrue();
        processes.Provisioned.Should().BeEmpty();
    }

    [Fact]
    public async Task Tick_SupervisorReportsDeadShard_EvacuatorInvoked()
    {
        // Arrange — надзор нашёл полностью мёртвый шард
        SeedCluster("shop", null);
        var processes = new FakeProcesses
        {
            SuperviseResult = _ => new SuperviseOutcome(ProcessOutcome.Done, ["shard2"]),
        };
        var loop = CreateLoop(processes);

        // Act
        var tick = await loop.TickAsync(TestContext.Current.CancellationToken);

        // Assert — эвакуатор получил событие DeadShards
        tick.IsSuccess.Should().BeTrue();
        processes.Evacuated.Should().Equal("shop/shard2");
    }

    [Fact]
    public async Task Tick_TwoClustersDeadShardOnlyInOne_EvacuatesOnlyOwnShard()
    {
        // Arrange — шаблонно одноимённые шарды «shard1» в двух кластерах,
        // DeadShards — только у shopA (rework №1: мёртвые шарды — значение
        // тика своего кластера, эвакуация чужого живого шарда исключена)
        SeedCluster("shopA", null);
        SeedCluster("shopB", null);
        var processes = new FakeProcesses
        {
            SuperviseResult = cluster => cluster == "shopA"
                ? new SuperviseOutcome(ProcessOutcome.Done, ["shard1"])
                : new SuperviseOutcome(ProcessOutcome.Done, []),
        };
        var loop = CreateLoop(processes);

        // Act
        var tick = await loop.TickAsync(TestContext.Current.CancellationToken);

        // Assert — эвакуирован ТОЛЬКО шард кластера, сообщившего о смерти
        tick.IsSuccess.Should().BeTrue();
        processes.Evacuated.Should().BeEquivalentTo(["shopA/shard1"]);
        processes.Evacuated.Should().NotContain(e => e.StartsWith("shopB/", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Tick_TwoClusters_ParallelismCappedBySemaphore()
    {
        // Arrange — два кластера, лимит параллелизма 1: второй ждёт первого
        SeedCluster("shopA", null);
        SeedCluster("shopB", null);
        _options.CurrentValue.Parallelism.MaxClusters = 1;
        var processes = new FakeProcesses();
        var loop = CreateLoop(processes);

        // Act
        var tick = await loop.TickAsync(TestContext.Current.CancellationToken);

        // Assert — оба обработаны, но одновременно не больше лимита
        tick.IsSuccess.Should().BeTrue();
        processes.Supervised.Should().HaveCount(2);
        processes.MaxConcurrent.Should().Be(1);
    }

    [Fact]
    public async Task Tick_EtcdUnreachable_TickFails()
    {
        // Arrange — все endpoints недоступны (gateway всегда падает)
        var deadEtcd = new DeadEtcd();
        var claims = new ClaimStore(["http://dead:2379"], deadEtcd, TimeProvider.System);
        var processes = new FakeProcesses();
        var loop = new ReconcileLoop(
            _options, deadEtcd, claims, processes,
            new WorkJournal(deadEtcd, ["http://dead:2379"]),
            NullLogger<ReconcileLoop>.Instance, new HealthState(TimeProvider.System));

        // Act
        var tick = await loop.TickAsync(TestContext.Current.CancellationToken);

        // Assert — тик не прошёл (цикл залогирует и повторит с ErrorDelayMs)
        tick.IsSuccess.Should().BeFalse();
        processes.Supervised.Should().BeEmpty();
    }

    [Fact]
    public async Task Tick_ProcessThrowsException_TickSurvives_JournalHasLastError_NextTickContinues()
    {
        // Arrange — процесс бросает НЕОБРАБОТАННОЕ исключение (баг процесса),
        // а не Result.Failed: раньше оно уходило в Task.WhenAll → StopHost
        SeedCluster("shop", "NOT_INITIALIZED");
        var processes = new FakeProcesses { ThrowProvisions = 1 };
        var loop = CreateLoop(processes);

        // Act — первый тик: исключение процесса проглочено (catch-all, rework №3)
        var tick1 = await loop.TickAsync(TestContext.Current.CancellationToken);

        // Assert — тик успешен (сервис жив), след краха — в journal.last_error
        tick1.IsSuccess.Should().BeTrue();
        var work = _etcd.Store.Should().ContainKey("/pgworker/work/shop").WhoseValue.Value;
        work.Should().Contain("\"op\":\"provision\"");
        work.Should().Contain("\"phase\":\"crashed\"");
        work.Should().Contain("process bug");

        // Act — второй тик: процесс уже не бросает — цикл продолжил работу
        var tick2 = await loop.TickAsync(TestContext.Current.CancellationToken);

        // Assert — упавший вызов не зарегистрирован, второй тик дошёл до процесса
        tick2.IsSuccess.Should().BeTrue();
        processes.Provisioned.Should().HaveCount(1);
    }

    [Fact]
    public async Task TickSafelyAsync_TickThrows_ReturnsFailed_NextTickWorks()
    {
        // Arrange — первый Range /clusters/ бросает исключение (широкий сбой
        // шлюза): защита тела ExecuteAsync (rework №3) превращает его в ошибку тика
        SeedCluster("shop", null);
        var faulted = true;
        _etcd.RangeFault = prefix =>
        {
            if (!faulted || prefix != "/clusters/")
                return null;
            faulted = false;
            return new HttpRequestException("gateway exploded");
        };
        var processes = new FakeProcesses();
        var loop = CreateLoop(processes);

        // Act
        var first = await loop.TickSafelyAsync(TestContext.Current.CancellationToken);
        var second = await loop.TickSafelyAsync(TestContext.Current.CancellationToken);

        // Assert — исключение тика не покинуло цикл: ошибка тика (лог +
        // ErrorDelayMs в ExecuteAsync), следующий тик полностью жив
        first.IsSuccess.Should().BeFalse();
        first.Error.Should().BeAssignableTo<HttpRequestException>();
        second.IsSuccess.Should().BeTrue();
        processes.Supervised.Should().Equal("shop");
    }

    // Фиксированный IOptionsMonitor и DeadEtcd — общие даблы TestSupport.cs.

    // Мок агрегатора процессов: фиксирует вызовы (включая порядок), меряет параллелизм.
    private sealed class FakeProcesses : IClusterProcesses
    {
        private readonly object _sync = new();
        private int _concurrent;

        public List<string> Provisioned { get; } = [];

        public List<string> Deprovisioned { get; } = [];

        public List<string> Supervised { get; } = [];

        public List<string> Evacuated { get; } = [];

        public List<string> Moved { get; } = [];

        public List<string> Scaled { get; } = [];

        // Порядок вызовов процессов кластера ("supervise/shop", "moves/shop", …).
        public List<string> Calls { get; } = [];

        public int MaxConcurrent { get; private set; }

        public Func<string, SuperviseOutcome>? SuperviseResult { get; set; }

        // Сколько следующих вызовов ProvisionAsync бросают исключение (баг
        // процесса вместо Result.Failed — catch-all цикла, rework №3).
        public int ThrowProvisions { get; set; }

        public Task<Result<ProcessOutcome>> ProvisionAsync(ClusterSnapshot snap, CancellationToken ct)
        {
            if (ThrowProvisions > 0)
            {
                ThrowProvisions--;
                throw new InvalidOperationException("process bug");
            }

            using var _ = Track(snap.Config.Cluster, Provisioned);
            return Task.FromResult(Result<ProcessOutcome>.Success(ProcessOutcome.Done));
        }

        public Task<Result<ProcessOutcome>> DeprovisionAsync(ClusterSnapshot snap, CancellationToken ct)
        {
            using var _ = Track(snap.Config.Cluster, Deprovisioned);
            return Task.FromResult(Result<ProcessOutcome>.Success(ProcessOutcome.Done));
        }

        public async Task<Result<SuperviseOutcome>> SuperviseAsync(ClusterSnapshot snap, CancellationToken ct)
        {
            using var _ = Track(snap.Config.Cluster, Supervised, callName: "supervise");
            await Task.Yield(); // расшиваем параллелизм: оба кластера стартуют
            return Result<SuperviseOutcome>.Success(
                SuperviseResult?.Invoke(snap.Config.Cluster) ?? new SuperviseOutcome(ProcessOutcome.Done, []));
        }

        public Task<Result<ProcessOutcome>> EvacuateAsync(ClusterSnapshot snap, string deadShard, CancellationToken ct)
        {
            using var _ = Track(snap.Config.Cluster, Evacuated, deadShard);
            return Task.FromResult(Result<ProcessOutcome>.Success(ProcessOutcome.Done));
        }

        public Task<Result<ProcessOutcome>> ProcessMovesAsync(ClusterSnapshot snap, CancellationToken ct)
        {
            using var _ = Track(snap.Config.Cluster, Moved, callName: "moves");
            return Task.FromResult(Result<ProcessOutcome>.Success(ProcessOutcome.Done));
        }

        public Task<Result<ProcessOutcome>> ScaleShardsAsync(ClusterSnapshot snap, CancellationToken ct)
        {
            using var _ = Track(snap.Config.Cluster, Scaled, callName: "scale-shards");
            return Task.FromResult(Result<ProcessOutcome>.Success(ProcessOutcome.Done));
        }

        private TrackHandle Track(string cluster, List<string> sink, string? suffix = null, string? callName = null)
        {
            lock (_sync)
            {
                _concurrent++;
                MaxConcurrent = Math.Max(MaxConcurrent, _concurrent);
                if (callName is not null)
                    Calls.Add($"{callName}/{cluster}");
            }

            sink.Add(suffix is null ? cluster : $"{cluster}/{suffix}");
            return new TrackHandle(() =>
            {
                lock (_sync)
                {
                    _concurrent--;
                }
            });
        }

        private readonly struct TrackHandle(Action dispose) : IDisposable
        {
            public void Dispose() => dispose();
        }
    }
}
