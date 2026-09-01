using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Client;
using AdminPanel.Infrastructure;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.UnitTests;

// Общая тест-обвязка refresher'а: FakeEtcdGateway + конструктор с любыми endpoints.
// Используется и EtcdHealthCheckTests (Task 10) — internal на сборку.
internal static class RefresherTestHarness
{
    // Старая сигнатура (обёртка) — существующие вызовы не меняются.
    public static SnapshotRefresher New(FakeEtcdGateway gateway, ISnapshotStore store, params string[] endpoints)
        => New(gateway, store, null, endpoints);

    // Расширенная: с стором проб (spec §10.8 — конструктор refresher'а t06).
    public static SnapshotRefresher New(
        FakeEtcdGateway gateway,
        ISnapshotStore store,
        SettableProbeStateStore? probes,
        params string[] endpoints)
        => new(
            gateway,
            new AlertEngine(AlertTestRules.All()),
            store,
            probes ?? new SettableProbeStateStore(),
            Options.Create(new EtcdOptions { Endpoints = endpoints }),
            new FixedTimeProvider(),
            NullLogger<SnapshotRefresher>.Instance);
}

// Управляемый стор состояния проб (unit-аналог TestSnapshotStore; spec §10.8).
internal sealed class SettableProbeStateStore : IProbeStateStore
{
    public ProbeState? Current { get; set; }

    public void Replace(ProbeState state) => Current = state;
}

// Управляемый gateway: данные/отказы по endpoints, счётчики вызовов.
internal sealed class FakeEtcdGateway : IEtcdGateway
{
    public List<string> StatusFailEndpoints { get; } = [];

    public List<string> RangeFailEndpoints { get; } = [];

    public List<string> RangeFailPrefixes { get; } = [];

    public IReadOnlyList<Kv> ClustersKv { get; init; } = [];

    public IReadOnlyList<Kv> ServiceKv { get; init; } = [];

    public IReadOnlyList<Kv> NodesKv { get; init; } = [];

    public IReadOnlyList<Kv> MovesKv { get; set; } = [];

    public IReadOnlyList<Kv> PgApiKv { get; set; } = [];

    public IReadOnlyList<Kv> WorkKv { get; set; } = [];

    public IReadOnlyList<EtcdMember> Members { get; init; } = [];

    public IReadOnlyList<EtcdAlarm> Alarms { get; init; } = [];

    public int RangeCalls { get; private set; }

    public int StatusCalls { get; private set; }

    public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
    {
        RangeCalls++;
        return Task.FromResult(RangeFailEndpoints.Contains(endpoint) || RangeFailPrefixes.Contains(prefix)
            ? Result<IReadOnlyList<Kv>>.Failed(new EtcdUnreachableException(endpoint))
            : Result<IReadOnlyList<Kv>>.Success(prefix switch
            {
                "/clusters/" => ClustersKv,
                "/service/" => ServiceKv,
                "/pgworker/moves/" => MovesKv,
                "/pgworker/api/" => PgApiKv,
                "/pgworker/work/" => WorkKv,
                _ => NodesKv,
            }));
    }

    public Task<Result<EtcdStatusPayload>> StatusAsync(string endpoint, CancellationToken ct)
    {
        StatusCalls++;
        return Task.FromResult(StatusFailEndpoints.Contains(endpoint)
            ? Result<EtcdStatusPayload>.Failed(new EtcdUnreachableException(endpoint))
            : Result<EtcdStatusPayload>.Success(new EtcdStatusPayload("3.5.21", 20480, 42, 17, 3)));
    }

    public Task<Result<IReadOnlyList<EtcdMember>>> MemberListAsync(string endpoint, CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyList<EtcdMember>>.Success(Members));

    public Task<Result<IReadOnlyList<EtcdAlarm>>> AlarmAsync(string endpoint, CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyList<EtcdAlarm>>.Success(Alarms));

    // Write-методы (t12): refresher не пишет — заглушки ради интерфейса.
    public Task<Result<TxnResult>> TxnAsync(
        string endpoint, IReadOnlyList<TxnCompare> compares, IReadOnlyList<KvPut> puts, CancellationToken ct)
        => Task.FromResult(Result<TxnResult>.Failed(new EtcdUnreachableException(endpoint)));

    public Task<Result> PutAsync(string endpoint, string key, string value, CancellationToken ct)
        => Task.FromResult(Result.Failed(new EtcdUnreachableException(endpoint)));

    public Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
        => Task.FromResult(Result.Failed(new EtcdUnreachableException(endpoint)));
}

// Refresher: живые/мёртвые endpoints, sticky-failover, отказ с сохранением данных (spec §10.9).
public class SnapshotRefresherTests
{
    private static FakeEtcdGateway DemoGateway() => new()
    {
        ClustersKv = EtcdFixtures.LoadKv("clusters-full.json"),
        ServiceKv = EtcdFixtures.LoadKv("service-full.json"),
        NodesKv = EtcdFixtures.LoadKv("stand-nodes.json"),
        Members = [new EtcdMember(42, "test", ["http://p"], ["http://c"])],
    };

    [Fact]
    public async Task Refresh_AllAlive_BuildsAndStoresSnapshot()
    {
        // Arrange
        var gateway = DemoGateway();
        var store = new SnapshotStore();
        var refresher = RefresherTestHarness.New(gateway, store, "http://e1", "http://e2");

        // Act
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        store.Current.Should().NotBeNull();
        var snapshot = store.Current!;
        snapshot.Etcd.Reachable.Should().BeTrue();
        snapshot.Etcd.Endpoints.Should().HaveCount(2);
        snapshot.Etcd.ActiveEndpoint.Should().Be("http://e1"); // sticky: первый по списку
        snapshot.Etcd.ConsecutiveFailures.Should().Be(0);
        snapshot.Clusters.Should().ContainSingle(c => c.Name == "demo");
        snapshot.Etcd.Members.Should().ContainSingle(m => m.Name == "test");
        gateway.StatusCalls.Should().Be(2); // персонально по всем endpoints (arch/02 §2.4)
        refresher.Working.Should().BeTrue();
        refresher.Inited.Should().BeTrue();
    }

    [Fact]
    public async Task Refresh_AllDead_PreservesDataAndCountsFailure()
    {
        // Arrange
        var gateway = DemoGateway();
        var store = new SnapshotStore();
        var refresher = RefresherTestHarness.New(gateway, store, "http://e1", "http://e2");
        await refresher.RefreshOnceAsync(CancellationToken.None);
        var builtAt = store.Current!.BuiltAtUtc;
        var clusters = store.Current.Clusters;
        gateway.StatusFailEndpoints.AddRange(["http://e1", "http://e2"]);

        // Act
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);
        await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        store.Current!.BuiltAtUtc.Should().Be(builtAt);       // возраст данных растёт (spec §3.9)
        store.Current.Clusters.Should().BeSameAs(clusters);   // данные прежние
        store.Current.Etcd.Reachable.Should().BeFalse();
        store.Current.Etcd.ConsecutiveFailures.Should().Be(2);
        store.Current.Etcd.Endpoints.Should().OnlyContain(e => !e.Reachable);
        refresher.Working.Should().BeFalse();
        refresher.StatusError.IsSuccess.Should().BeFalse();
    }

    [Fact]
    public async Task Refresh_Recovery_ResetsFailures()
    {
        // Arrange
        var gateway = DemoGateway();
        var store = new SnapshotStore();
        var refresher = RefresherTestHarness.New(gateway, store, "http://e1");
        await refresher.RefreshOnceAsync(CancellationToken.None);
        gateway.StatusFailEndpoints.Add("http://e1");
        await refresher.RefreshOnceAsync(CancellationToken.None);
        store.Current!.Etcd.ConsecutiveFailures.Should().Be(1);

        // Act — endpoint ожил
        gateway.StatusFailEndpoints.Clear();
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        store.Current.Etcd.ConsecutiveFailures.Should().Be(0);
        store.Current.Etcd.Reachable.Should().BeTrue();
        refresher.Working.Should().BeTrue();
    }

    [Fact]
    public async Task Refresh_StickyFails_OverToNextAlive()
    {
        // Arrange
        var gateway = DemoGateway();
        var store = new SnapshotStore();
        var refresher = RefresherTestHarness.New(gateway, store, "http://e1", "http://e2");
        await refresher.RefreshOnceAsync(CancellationToken.None);
        gateway.StatusFailEndpoints.Add("http://e1"); // активный умер между тиками

        // Act
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        store.Current!.Etcd.ActiveEndpoint.Should().Be("http://e2");
        store.Current.Etcd.Endpoints.Single(e => e.Url == "http://e1").Reachable.Should().BeFalse();
    }

    [Fact]
    public async Task Refresh_MidTickFailure_FailsOverWithoutLosingTick()
    {
        // Arrange — статус жив, но KV-чтения на активном падают: failover внутри тика (spec §3.10)
        var gateway = DemoGateway();
        gateway.RangeFailEndpoints.Add("http://e1");
        var store = new SnapshotStore();
        var refresher = RefresherTestHarness.New(gateway, store, "http://e1", "http://e2");

        // Act
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        store.Current!.Clusters.Should().ContainSingle(c => c.Name == "demo");
    }

    [Fact]
    public async Task Refresh_EmptyEndpoints_FailedTickWithEmptySnapshot()
    {
        // Arrange
        var gateway = DemoGateway();
        var store = new SnapshotStore();
        var refresher = RefresherTestHarness.New(gateway, store);

        // Act
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        store.Current.Should().NotBeNull(); // пустой снапшот с Reachable=false (spec §3.12)
        store.Current!.Etcd.Reachable.Should().BeFalse();
        store.Current.Etcd.ConsecutiveFailures.Should().Be(1);
        store.Current.Clusters.Should().BeEmpty();
        refresher.Inited.Should().BeTrue();
        refresher.Working.Should().BeFalse();
    }

    [Fact]
    public async Task Refresh_AlertsStoredOnSuccessTick()
    {
        // Arrange: полный demo-сид + один битый статус-ключ → key-malformed (spec §10.2).
        var store = new SnapshotStore();
        var gateway = new FakeEtcdGateway
        {
            ClustersKv =
            [
                .. EtcdFixtures.LoadKv("clusters-full.json"),
                new Kv("/clusters/demo/buckets/status/bucket_9", "not json", 99),
            ],
            ServiceKv = EtcdFixtures.LoadKv("service-full.json"),
        };
        var refresher = RefresherTestHarness.New(gateway, store, "http://e1");

        // Act
        await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert: key-malformed от битого ключа + 5 move-алертов сида demo (spec §3.15, §10.4)
        // + worker-api-unreachable (живых ключей /pgworker/api/ в фикстуре нет, arch/03 §4.1).
        var alerts = store.Current!.Alerts;
        alerts.Should().HaveCount(7);
        alerts.Should().Contain(a => a.Id == "worker-api-unreachable:pgworker");
        alerts.Should().Contain(a => a.Id == "key-malformed:/clusters/demo/buckets/status/bucket_9");
        alerts.Should().Contain(a => a.Id == "move-stale:demo/bucket_3");
        alerts.Should().Contain(a => a.Id == "move-stale:demo/bucket_7");
        alerts.Should().Contain(a => a.Id == "move-stale:demo/bucket_11");
        alerts.Should().Contain(a => a.Id == "move-frozen-long:demo/bucket_11");
        alerts.Should().Contain(a => a.Id == "move-aborting:demo/bucket_7");
    }

    [Fact]
    public async Task Refresh_AlertsComputedOnFailTick()
    {
        // Arrange: первый тик собирает снапшот с incomplete-кластером; затем endpoints умирают.
        var store = new SnapshotStore();
        var gateway = new FakeEtcdGateway
        {
            ClustersKv = [new Kv("/clusters/ghost/shards/g1/dsn", "host=g1 port=5432", 1)],
        };
        var refresher = RefresherTestHarness.New(gateway, store, "http://e1");
        await refresher.RefreshOnceAsync(CancellationToken.None);
        gateway.StatusFailEndpoints.Add("http://e1");

        // Act: два отказных тика — порог etcd-unreachable = 2 (spec §4.2).
        await refresher.RefreshOnceAsync(CancellationToken.None);
        await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert: unreachable вспыхнул; data-алерт из прежних данных сохранён,
        // sinceUnix не рвётся (перенос null с первого тика — §3.4).
        var alerts = store.Current!.Alerts;
        alerts.Should().Contain(a => a.Id == "etcd-unreachable:etcd"
            && a.Severity == AlertSeverity.Critical);
        var incomplete = alerts.Single(a => a.Kind == "cluster-incomplete");
        incomplete.Target.Should().Be("ghost");
        incomplete.SinceUnix.Should().BeNull();
    }

    // Минимальный gateway с /service/ demo-s1 и шардем demo/s1 (spec §10.8).
    private static FakeEtcdGateway HaGateway() => new()
    {
        ClustersKv =
        [
            new Kv("/clusters/demo/config", "{\"buckets\":16,\"dbname\":\"demo\",\"created_unix\":1755800000}", 1),
            new Kv("/clusters/demo/shards/s1/dsn", "host=s1a port=5432 dbname=demo user=postgres", 2),
        ],
        ServiceKv =
        [
            new Kv("/service/demo-s1/leader", "{\"name\":\"s1a\"}", 3),
            new Kv("/service/demo-s1/members/s1a", "{\"name\":\"s1a\",\"conn_url\":\"s1a:5432\",\"role\":\"master\",\"state\":\"running\"}", 4),
        ],
    };

    private static readonly DateTimeOffset ProbesAt = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task Refresh_EnrichesFromProbeState()
    {
        // Arrange: стор проб с member-обогащением и runtime шарда.
        var store = new SnapshotStore();
        var probes = new SettableProbeStateStore
        {
            Current = new ProbeState(
                ProbesAt,
                [],
                new Dictionary<string, HaMemberProbe>
                {
                    ["demo-s1/s1a"] = new("master", "running", 2L, 123L, ProbesAt, null),
                },
                new Dictionary<string, ShardRuntime>
                {
                    ["demo/s1"] = new("s1", [], [], [], ["bucket_0"], false, null),
                }),
        };
        var refresher = RefresherTestHarness.New(HaGateway(), store, probes, "http://etcd:2379");

        // Act
        await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert: член обогащён, runtime проставлен (spec §4.2 через refresher).
        var member = store.Current!.HaScopes.Single(s => s.Scope == "demo-s1").Members.Single(m => m.Name == "s1a");
        member.Timeline.Should().Be(2L);
        member.LagBytes.Should().Be(123L);
        member.ProbeAtUtc.Should().Be(ProbesAt);
        var shard = store.Current.Clusters.Single().Shards.Single();
        shard.Runtime.Should().NotBeNull();
        shard.Runtime!.BucketSchemas.Should().ContainSingle().Which.Should().Be("bucket_0");
    }

    [Fact]
    public async Task Refresh_FailTick_PreservesProbes()
    {
        // Arrange: снапшот с живым проб-результатом; все endpoints мертвы.
        var probe = new ProbeResult("demo-s1/s1a", "patroni", true, 5.0, null, ProbesAt);
        var store = new SnapshotStore();
        store.Replace(TestSnapshots.Healthy(ProbesAt) with { Probes = [probe] });
        var gateway = new FakeEtcdGateway();
        gateway.StatusFailEndpoints.Add("http://etcd:2379"); // свойство get-only — наполняется (CS8852 на object initializer)
        var refresher = RefresherTestHarness.New(gateway, store, (SettableProbeStateStore?)null, "http://etcd:2379");

        // Act
        await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert: отказ etcd не теряет снапшотные пробы (spec §4.3).
        store.Current!.Probes.Should().ContainSingle().Which.Should().BeSameAs(probe);
    }

    [Fact]
    public async Task Refresh_WithMovesQueue_StoresTickets()
    {
        // Arrange: тик с непустой очередью заявок (арх/02 §2.3.1)
        var gateway = DemoGateway();
        gateway.MovesKv = EtcdFixtures.LoadKv("moves-queue.json");
        var store = new SnapshotStore();

        // Act
        await RefresherTestHarness.New(gateway, store, "http://etcd1:2379")
            .RefreshOnceAsync(CancellationToken.None);

        // Assert: валидные заявки — в MoveTickets, битые — в ParseErrors (Д11)
        var snapshot = store.Current!;
        snapshot.MoveTickets.Should().HaveCount(6);
        snapshot.MoveTickets.Should().Contain(t =>
            t.Cluster == "demo" && t.Bucket == "bucket_3" && t.Op == "move" && t.To == "shard2");
        snapshot.ParseErrors.Should().Contain(e => e.Key == "/pgworker/moves/demo/bucket_13");
    }

    [Fact]
    public async Task Refresh_WithApiKeys_StoresWorkerEndpoints()
    {
        // Arrange: живой ключ доступа воркера + битый (arch/02 §2.3.1)
        var gateway = DemoGateway();
        gateway.PgApiKv =
        [
            new Kv("/pgworker/api/inst1", """{"url":"http://pgw:8080","instance":"inst1","since_unix":1756000000}""", 7),
            new Kv("/pgworker/api/bad", "{oops", 8),
        ];
        var store = new SnapshotStore();

        // Act
        await RefresherTestHarness.New(gateway, store, "http://etcd1:2379")
            .RefreshOnceAsync(CancellationToken.None);

        // Assert: валидный ключ — в PgWorkerEndpoints, битый — parseError (тик жив)
        var snapshot = store.Current!;
        snapshot.PgWorkerEndpoints.Should().ContainSingle().Which
            .Should().Be(new WorkerEndpoint("inst1", "http://pgw:8080", 1756000000));
        snapshot.ParseErrors.Should().Contain(e => e.Key == "/pgworker/api/bad");
    }

    [Fact]
    public async Task Refresh_MovesRangeFails_FailsTickKeepsPrevious()
    {
        // Arrange: точечный отказ чтения очереди — неполный снапшот хуже прежнего (Д10)
        var gateway = DemoGateway();
        gateway.MovesKv = EtcdFixtures.LoadKv("moves-queue.json"); // первый тик — с очередью
        var store = new SnapshotStore();
        var refresher = RefresherTestHarness.New(gateway, store, "http://etcd1:2379");
        await refresher.RefreshOnceAsync(CancellationToken.None); // успешный тик ДО поломки
        var before = store.Current;
        gateway.RangeFailPrefixes.Add("/pgworker/moves/");        // ломаем только новый range

        // Act
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert: отказ тика (FailTick строит новый снапшот с прежними данными — образец
        // Refresh_AllDead); BuiltAtUtc и очередь заявок — прежние (Д10)
        result.IsSuccess.Should().BeFalse();
        store.Current!.BuiltAtUtc.Should().Be(before!.BuiltAtUtc);
        store.Current.MoveTickets.Should().BeSameAs(before.MoveTickets);
    }

    // AAA: R4 — транспортный провал range /pgworker/work/ роняет тик, но прежние
    // work-записи переживают отказ (FailTick переносит, неполный снапшот хуже прежнего)
    [Fact]
    public async Task Refresh_WorkRangeTransportFails_PreviousPgWorkerWorkKept()
    {
        // Arrange: прежний снапшот несёт PgWorkerWork; gateway валит transport
        // range /pgworker/work/ (FailTick: неполный снапшот хуже прежнего — spec R4).
        var gateway = new FakeEtcdGateway
        {
            ClustersKv = EtcdFixtures.LoadKv("clusters-full.json"),
            ServiceKv = EtcdFixtures.LoadKv("service-full.json"),
            NodesKv = EtcdFixtures.LoadKv("stand-nodes.json"),
        };
        gateway.RangeFailPrefixes.Add("/pgworker/work/");
        var store = new SnapshotStore();
        var previous = TestSnapshots.Healthy(new DateTimeOffset(2026, 9, 1, 12, 0, 0, TimeSpan.Zero)) with
        {
            PgWorkerWork = [new WorkJournalInfo("demo", "provision", "planned", "w-1", 1756000000, null, null, null, null)],
        };
        store.Replace(previous);
        var refresher = RefresherTestHarness.New(gateway, store, "http://e1");

        // Act
        var tick = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert: тик отказной (FailTick: Reachable=false), прежние work-записи пережили отказ.
        tick.IsSuccess.Should().BeFalse();
        store.Current!.Etcd.Reachable.Should().BeFalse();
        store.Current.PgWorkerWork.Should().ContainSingle().Which.Cluster.Should().Be("demo");
    }
}
