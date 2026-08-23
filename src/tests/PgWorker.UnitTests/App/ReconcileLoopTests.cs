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

        // Assert — тик успешен и доволен ровно нужный процесс
        tick.IsSuccess.Should().BeTrue();
        processes.Provisioned.Should().Equal("shop");
        processes.Deprovisioned.Should().BeEmpty();
        processes.Supervised.Should().BeEmpty();
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
            NullLogger<ReconcileLoop>.Instance, new HealthState(TimeProvider.System));

        // Act
        var tick = await loop.TickAsync(TestContext.Current.CancellationToken);

        // Assert — тик не прошёл (цикл залогирует и повторит с ErrorDelayMs)
        tick.IsSuccess.Should().BeFalse();
        processes.Supervised.Should().BeEmpty();
    }

    // Фиксированный IOptionsMonitor (значение не меняется в тесте).
    private sealed class FixedOptionsMonitor(PgWorkerOptions value) : IOptionsMonitor<PgWorkerOptions>
    {
        public PgWorkerOptions CurrentValue => value;

        public IDisposable? OnChange(Action<PgWorkerOptions, string?> listener) => null;

        public PgWorkerOptions Get(string? name) => value;
    }

    // Мок агрегатора процессов: фиксирует вызовы, меряет параллелизм.
    private sealed class FakeProcesses : IClusterProcesses
    {
        private readonly object _sync = new();
        private int _concurrent;

        public List<string> Provisioned { get; } = [];

        public List<string> Deprovisioned { get; } = [];

        public List<string> Supervised { get; } = [];

        public List<string> Evacuated { get; } = [];

        public int MaxConcurrent { get; private set; }

        public Func<string, SuperviseOutcome>? SuperviseResult { get; set; }

        public Task<Result<ProcessOutcome>> ProvisionAsync(ClusterSnapshot snap, CancellationToken ct)
        {
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
            using var _ = Track(snap.Config.Cluster, Supervised);
            await Task.Yield(); // расшиваем параллелизм: оба кластера стартуют
            return Result<SuperviseOutcome>.Success(
                SuperviseResult?.Invoke(snap.Config.Cluster) ?? new SuperviseOutcome(ProcessOutcome.Done, []));
        }

        public Task<Result<ProcessOutcome>> EvacuateAsync(ClusterSnapshot snap, string deadShard, CancellationToken ct)
        {
            using var _ = Track(snap.Config.Cluster, Evacuated, deadShard);
            return Task.FromResult(Result<ProcessOutcome>.Success(ProcessOutcome.Done));
        }

        private TrackHandle Track(string cluster, List<string> sink, string? suffix = null)
        {
            lock (_sync)
            {
                _concurrent++;
                MaxConcurrent = Math.Max(MaxConcurrent, _concurrent);
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

    // Полностью недоступный etcd: любой вызов — ошибка сети.
    private sealed class DeadEtcd : PgWorker.Etcd.Client.IEtcdGateway
    {
        private static Result<T> Fail<T>()
            => Result<T>.Failed(new HttpRequestException("etcd недоступен"));

        public Task<Result<IReadOnlyList<PgWorker.Etcd.Client.Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
            => Task.FromResult(Fail<IReadOnlyList<PgWorker.Etcd.Client.Kv>>());

        public Task<Result<PgWorker.Etcd.Client.Kv?>> GetAsync(string endpoint, string key, CancellationToken ct)
            => Task.FromResult(Fail<PgWorker.Etcd.Client.Kv?>());

        public Task<Result> PutAsync(string endpoint, string key, string value, long? lease, CancellationToken ct)
            => Task.FromResult(Result.Failed(new HttpRequestException("etcd недоступен")));

        public Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
            => Task.FromResult(Result.Failed(new HttpRequestException("etcd недоступен")));

        public Task<Result<PgWorker.Etcd.Client.TxnResult>> TxnAsync(
            string endpoint, PgWorker.Etcd.Client.TxnRequest req, CancellationToken ct)
            => Task.FromResult(Fail<PgWorker.Etcd.Client.TxnResult>());

        public Task<Result<long>> LeaseGrantAsync(string endpoint, int ttlSec, CancellationToken ct)
            => Task.FromResult(Fail<long>());

        public Task<Result> LeaseRevokeAsync(string endpoint, long lease, CancellationToken ct)
            => Task.FromResult(Result.Failed(new HttpRequestException("etcd недоступен")));

        public Task<Result> LeaseKeepaliveAsync(string endpoint, long lease, CancellationToken ct)
            => Task.FromResult(Result.Failed(new HttpRequestException("etcd недоступен")));

        public Task<Result<byte[]>> SnapshotSaveAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Fail<byte[]>());
    }
}
