using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using AdminPanel.Etcd;
using Shared.Etcd.Client;
using AdminPanel.Etcd.Workers;
using AdminPanel.Probes.S3;
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

    // Расширенная: с стором проб (spec §10.8 — конструктор refresher'а).
    public static SnapshotRefresher New(
        FakeEtcdGateway gateway,
        ISnapshotStore store,
        SettableProbeStateStore? probes,
        params string[] endpoints)
        => New(gateway, store, probes, null, endpoints);

    // С стором S3-инвентаря (— конструктор refresher'а): null → пустой стор.
    public static SnapshotRefresher New(
        FakeEtcdGateway gateway,
        ISnapshotStore store,
        SettableProbeStateStore? probes,
        S3InventoryStore? s3,
        params string[] endpoints)
        => New(gateway, store, probes, s3, new AlertEngine(AlertTestRules.All()), new FixedTimeProvider(), endpoints);

    // Кейсы diff-skip (arch/02 §5): подмены алерт-движка (счёт вызовов
    // Evaluate) и времени (движение штампов BuiltAtUtc между тиками).
    // Отдельный оверлоад без optional-параметров — не конфликтует по params.
    public static SnapshotRefresher New(
        FakeEtcdGateway gateway,
        ISnapshotStore store,
        SettableProbeStateStore? probes,
        S3InventoryStore? s3,
        IAlertEngine alertEngine,
        TimeProvider time,
        params string[] endpoints)
        => new(
            gateway,
            alertEngine,
            store,
            probes ?? new SettableProbeStateStore(),
            new WorkerHealthStore(),
            s3 ?? new S3InventoryStore(),
            new EtcdWorkerHealthStore(),
            new SettableS3SnapshotReader(),
            Options.Create(new EtcdOptions { Endpoints = endpoints }),
            time,
            NullLogger<SnapshotRefresher>.Instance);
}

// Счётный алертный движок: тик без изменений (S10) Evaluate не вызывает —
// счётчик отражает фактические пересборки с алертным пересчётом.
internal sealed class CountingAlertEngine : IAlertEngine
{
    private readonly AlertEngine _inner = new(AlertTestRules.All());

    public int EvaluateCalls { get; private set; }

    public IReadOnlyList<Alert> Evaluate(
        EtcdSnapshot snapshot,
        EtcdSnapshot? previous,
        DateTimeOffset nowUtc,
        double refreshIntervalSeconds)
    {
        EvaluateCalls++;
        return _inner.Evaluate(snapshot, previous, nowUtc, refreshIntervalSeconds);
    }
}

// Управляемый читатель S3-домена (конструктор refresher'а): null — домен
// не читался (алерт etcd-backup-cluster-missing молчит).
internal sealed class SettableS3SnapshotReader : IS3SnapshotReader
{
    public AdminPanel.Core.S3.S3Snapshot? Current { get; set; }
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

    // u10–u14: данные новых префиксов pg-тика (backups-disabled/evacuations/
    // rotations/leader/claims/instances) — точные данные теста вместо fallback.
    public Dictionary<string, IReadOnlyList<Kv>> ExtraKv { get; } = [];

    public IReadOnlyList<EtcdMember> Members { get; set; } = [];

    public IReadOnlyList<EtcdAlarm> Alarms { get; set; } = [];

    // Управляемый payload /v3/maintenance/status: кейсы diff-skip по фактам
    // статуса (raftTerm/version) против шума (latency/dbSize/raftIndex).
    public EtcdStatusPayload StatusPayload { get; set; } = new("3.5.21", 20480, 42, 17, 3, null);

    public int RangeCalls { get; private set; }

    public int StatusCalls { get; private set; }

    public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
    {
        RangeCalls++;
        return Task.FromResult(RangeFailEndpoints.Contains(endpoint) || RangeFailPrefixes.Contains(prefix)
            ? Result<IReadOnlyList<Kv>>.Failed(new EtcdUnreachableException(endpoint))
            : Result<IReadOnlyList<Kv>>.Success(ExtraKv.TryGetValue(prefix, out var extra)
                ? extra
                : prefix switch
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
            : Result<EtcdStatusPayload>.Success(StatusPayload));
    }

    public Task<Result<IReadOnlyList<EtcdMember>>> MemberListAsync(string endpoint, CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyList<EtcdMember>>.Success(Members));

    // Member-API этими тестами не используется — заглушки интерфейса.
    public Task<Result<EtcdMember>> MemberAddAsync(string endpoint, IReadOnlyList<string> peerUrls, bool isLearner, CancellationToken ct)
        => throw new NotSupportedException("member-API не поддерживается этим фейком");

    public Task<Result> MemberRemoveAsync(string endpoint, ulong memberId, CancellationToken ct)
        => throw new NotSupportedException("member-API не поддерживается этим фейком");

    public Task<Result> MemberPromoteAsync(string endpoint, ulong memberId, CancellationToken ct)
        => throw new NotSupportedException("member-API не поддерживается этим фейком");

    public Task<Result<IReadOnlyList<EtcdAlarm>>> AlarmAsync(string endpoint, CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyList<EtcdAlarm>>.Success(Alarms));

    // Write-методы : refresher не пишет — заглушки ради интерфейса.
    public Task<Result<TxnResult>> TxnAsync(string endpoint, TxnRequest req, CancellationToken ct)
        => Task.FromResult(Result<TxnResult>.Failed(new EtcdUnreachableException(endpoint)));

    public Task<Result> PutAsync(string endpoint, string key, string value, long? lease, CancellationToken ct)
        => Task.FromResult(Result.Failed(new EtcdUnreachableException(endpoint)));

    public Task<Result<Kv?>> GetAsync(string endpoint, string key, CancellationToken ct)
        => Task.FromResult(Result<Kv?>.Failed(new EtcdUnreachableException(endpoint)));

    public Task<Result<long>> LeaseGrantAsync(string endpoint, int ttlSec, CancellationToken ct)
        => Task.FromResult(Result<long>.Failed(new EtcdUnreachableException(endpoint)));

    public Task<Result> LeaseRevokeAsync(string endpoint, long lease, CancellationToken ct)
        => Task.FromResult(Result.Failed(new EtcdUnreachableException(endpoint)));

    public Task<Result> LeaseKeepaliveAsync(string endpoint, long lease, CancellationToken ct)
        => Task.FromResult(Result.Failed(new EtcdUnreachableException(endpoint)));

    public Task<Result<byte[]>> SnapshotSaveAsync(string endpoint, CancellationToken ct)
        => Task.FromResult(Result<byte[]>.Failed(new EtcdUnreachableException(endpoint)));

    public Task<Result> CompactAsync(string endpoint, long revision, CancellationToken ct)
        => Task.FromResult(Result.Failed(new EtcdUnreachableException(endpoint)));

    public Task<Result> DefragmentAsync(string endpoint, CancellationToken ct)
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
        // + worker-api-unreachable ×2: pgworker и etcdworker (живых ключей /pgworker/api/
        // и /etcdworker/api/ в фикстуре нет, arch/03 §4.1).
        var alerts = store.Current!.Alerts;
        alerts.Should().HaveCount(8);
        alerts.Should().Contain(a => a.Id == "worker-api-unreachable:pgworker");
        alerts.Should().Contain(a => a.Id == "worker-api-unreachable:etcdworker");
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

    // AAA : успешный RefreshOnceAsync переносит store.Current в snapshot.S3Storage
    // (по образцу WorkerHealth — готовым состоянием из стора).
    [Fact]
    public async Task Refresh_Success_CarriesS3InventoryIntoSnapshot()
    {
        // Arrange — стор ownS3-инвентаря с готовым инвентарём (кластер demo, 152 байта)
        var s3 = new S3InventoryStore();
        s3.Replace(new S3StorageInfo(
            Configured: true, ["http://s3:9000"], "pgworker-backups", Health: null,
            Buckets: ["pgworker-backups"], UsedBytes: 152, ObjectCount: 5,
            Clusters: [], ForeignPrefixes: [], UpdatedAtUnix: 42,
            ConsecutiveFailures: 0, LastError: null));
        var store = new SnapshotStore();
        var refresher = RefresherTestHarness.New(DemoGateway(), store, null, s3, "http://e1");

        // Act
        await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert — инвентарь в снапшоте
        store.Current!.S3Storage.Should().NotBeNull();
        store.Current.S3Storage!.UsedBytes.Should().Be(152);
        store.Current.S3Storage.UpdatedAtUnix.Should().Be(42);
    }

    // AAA : FailTick сохраняет прежний S3Storage (инвентарь переживает отказ etcd).
    [Fact]
    public async Task Refresh_FailTick_PreservesS3Inventory()
    {
        // Arrange — успешный тик с инвентарём, затем endpoints умирают
        var s3 = new S3InventoryStore();
        s3.Replace(new S3StorageInfo(
            Configured: true, ["http://s3:9000"], "pgworker-backups", Health: null,
            Buckets: ["pgworker-backups"], UsedBytes: 152, ObjectCount: 5,
            Clusters: [], ForeignPrefixes: [], UpdatedAtUnix: 42,
            ConsecutiveFailures: 0, LastError: null));
        var store = new SnapshotStore();
        var gateway = DemoGateway();
        var refresher = RefresherTestHarness.New(gateway, store, null, s3, "http://e1");
        await refresher.RefreshOnceAsync(CancellationToken.None);
        gateway.StatusFailEndpoints.Add("http://e1");

        // Act — отказный тик
        await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert — инвентарь жив в снапшоте (Reachable=false)
        store.Current!.Etcd.Reachable.Should().BeFalse();
        store.Current.S3Storage.Should().NotBeNull();
        store.Current.S3Storage!.UsedBytes.Should().Be(152);
    }

    // u10–u14: новые ключи в ответах gateway → новые поля снапшота (один кейс
    // на набор: стоп бэкапов, приёмники WAL, эвакуации, заявки ротаций, координация).
    [Fact]
    public async Task Refresh_NewObservabilityKeys_SnapshotCarriesFields()
    {
        // Arrange — gateway с новыми ключами (FakeEtcdGateway.ExtraKv)
        var gateway = DemoGateway();
        gateway.ExtraKv["/pgworker/backups-disabled"] =
            [new Kv("/pgworker/backups-disabled", """{"updated_unix":1761900000,"updated_by":"ops"}""", 1)];
        gateway.ExtraKv["/pgworker/backups/"] =
        [
            new Kv("/pgworker/backups/demo/s1/agent/s1b",
                """{"node":"s1b","role":"sync","state":"streaming","pool_exhausted":false,"updated_unix":1761900010}""", 2),
        ];
        gateway.ExtraKv["/pgworker/evacuations/"] =
        [
            new Kv("/pgworker/evacuations/demo/s2",
                """{"evacuated_unix":1761900000,"reason":"dead","buckets":{"bucket_3":"shard1"},"state":"DONE"}""", 3),
        ];
        gateway.ExtraKv["/pgworker/rotations/"] =
        [
            new Kv("/pgworker/rotations/demo", """{"requested_unix":1761900100,"requested_by":"ops"}""", 4),
        ];
        gateway.ExtraKv["/pgworker/leader"] =
            [new Kv("/pgworker/leader", """{"instance":"pgw-1","since_unix":1761900000}""", 5)];
        gateway.ExtraKv["/pgworker/claims/"] =
            [new Kv("/pgworker/claims/demo", """{"instance":"pgw-1","since_unix":1761900100}""", 6)];
        gateway.ExtraKv["/pgworker/instances/"] =
            [new Kv("/pgworker/instances/pgw-1", """{"since_unix":1761900000}""", 7)];
        var store = new SnapshotStore();
        var refresher = RefresherTestHarness.New(gateway, store, "http://etcd1:2379");

        // Act
        var result = await refresher.RefreshOnceAsync(TestContext.Current.CancellationToken);

        // Assert — все новые поля в снапшоте
        result.IsSuccess.Should().BeTrue();
        var snapshot = store.Current!;
        snapshot.BackupsDisabled!.UpdatedUnix.Should().Be(1761900000);
        snapshot.BackupsDisabled.UpdatedBy.Should().Be("ops");
        snapshot.WalAgents.Should().ContainSingle()
            .Which.Node.Should().Be("s1b");
        snapshot.Evacuations.Should().ContainSingle()
            .Which.State.Should().Be("DONE");
        snapshot.PgRotations.Should().ContainSingle()
            .Which.Cluster.Should().Be("demo");
        snapshot.Coordination!.LeaderInstance.Should().Be("pgw-1");
        snapshot.Coordination.Claims.Should().ContainSingle()
            .Which.Cluster.Should().Be("demo");
        snapshot.Coordination.Instances.Should().ContainSingle()
            .Which.InstanceId.Should().Be("pgw-1");
    }

    // u10: битый JSON /pgworker/backups-disabled — parseError-запись в снапшоте
    // (key-malformed), BackupsDisabled == null: стоп бэкапов с битым значением
    // НЕ невидим (алерт key-malformed собирается по ParseErrors снапшота).
    [Fact]
    public async Task Refresh_BrokenBackupsDisabled_KeyMalformedRecorded()
    {
        // Arrange — точечный ключ с повреждённым значением
        var gateway = DemoGateway();
        gateway.ExtraKv["/pgworker/backups-disabled"] =
            [new Kv("/pgworker/backups-disabled", "not-a-json{", 1)];
        var store = new SnapshotStore();
        var refresher = RefresherTestHarness.New(gateway, store, "http://etcd1:2379");

        // Act
        var result = await refresher.RefreshOnceAsync(TestContext.Current.CancellationToken);

        // Assert — тик успешен (толерантный парсер), ошибка ключа — в ParseErrors
        result.IsSuccess.Should().BeTrue();
        var snapshot = store.Current!;
        snapshot.BackupsDisabled.Should().BeNull("битое значение не даёт факта стопа");
        snapshot.ParseErrors.Should().Contain(e =>
            e.Key == "/pgworker/backups-disabled"
            && e.Reason.Contains("broken backups-disabled JSON", StringComparison.Ordinal));
    }

    // u10–u14: транспортный провал НОВОГО range → отказ тика, данные прежние
    // (инвариант «неполный снапшот хуже прежнего», adminpanel/02 §4).
    [Fact]
    public async Task Refresh_NewRangeTransportFail_TickFailsDataPreserved()
    {
        // Arrange — первый тик успешен (данных нет, поля дефолтны)
        var gateway = DemoGateway();
        var store = new SnapshotStore();
        var refresher = RefresherTestHarness.New(gateway, store, "http://etcd1:2379");
        (await refresher.RefreshOnceAsync(TestContext.Current.CancellationToken)).IsSuccess.Should().BeTrue();
        var first = store.Current!;
        first.Etcd.Reachable.Should().BeTrue();

        // Act — второй тик: новый range /pgworker/leader отказал
        gateway.RangeFailPrefixes.Add("/pgworker/leader");
        var second = await refresher.RefreshOnceAsync(TestContext.Current.CancellationToken);

        // Assert — тик отказан, прежние данные/BuiltAtUtc сохранены
        second.IsSuccess.Should().BeFalse();
        store.Current!.Etcd.Reachable.Should().BeFalse();
        store.Current.BuiltAtUtc.Should().Be(first.BuiltAtUtc);
        store.Current.Clusters.Should().Equal(first.Clusters);
    }

    // S10 (arch/02 §5): тик без изменений — чтение совпало с кэшем → пересборки нет,
    // AlertEngine не вызывается; штампы свежести обновляются, данные — те же
    // ссылки; poll-цикл продолжается (чтения идут каждым тиком).
    [Fact]
    public async Task Refresh_SecondIdenticalTick_SkipsRebuildAndAlerts()
    {
        // Arrange — первый тик собирает снапшот, время двигается вперёд
        var gateway = DemoGateway();
        var store = new SnapshotStore();
        var engine = new CountingAlertEngine();
        var time = new FixedTimeProvider();
        var refresher = RefresherTestHarness.New(gateway, store, null, null, engine, time, "http://e1");
        await refresher.RefreshOnceAsync(CancellationToken.None);
        var first = store.Current!;
        time.Utc = time.Utc.AddSeconds(5);

        // Act — второй тик по неизменному etcd (латентность статуса при этом
        // шумит естественно — из сравнения она исключена)
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert — снапшот не пересобран, алерты не пересчитаны, штампы свежие
        result.IsSuccess.Should().BeTrue();
        engine.EvaluateCalls.Should().Be(1, "тик без изменений не запускает алертный пересчёт");
        store.Current!.Clusters.Should().BeSameAs(first.Clusters, "снапшот не пересобирается");
        store.Current.MoveTickets.Should().BeSameAs(first.MoveTickets);
        store.Current.Alerts.Should().BeSameAs(first.Alerts);
        store.Current.BuiltAtUtc.Should().Be(time.Utc, "штамп свежести обновляется");
        store.Current.Etcd.LastRefreshUtc.Should().Be(time.Utc);
        gateway.StatusCalls.Should().Be(2, "poll-цикл продолжается");
        refresher.Working.Should().BeTrue();
    }

    // S10: изменение KV (новая заявка в очереди moves) → пересборка и
    // алертный пересчёт.
    [Fact]
    public async Task Refresh_KvChange_RebuildsAndRecomputesAlerts()
    {
        // Arrange — первый тик с очередью, затем новая заявка в KV
        var gateway = DemoGateway();
        gateway.MovesKv = EtcdFixtures.LoadKv("moves-queue.json");
        var store = new SnapshotStore();
        var engine = new CountingAlertEngine();
        var time = new FixedTimeProvider();
        var refresher = RefresherTestHarness.New(gateway, store, null, null, engine, time, "http://e1");
        await refresher.RefreshOnceAsync(CancellationToken.None);
        var first = store.Current!;
        gateway.MovesKv = [.. gateway.MovesKv, new Kv("/pgworker/moves/demo/bucket_15", """{"op":"move","to":"shard2","requested_unix":1757000000}""", 99)];

        // Act
        await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert — пересборка: новые ссылки модели и новый пересчёт алертов
        store.Current!.MoveTickets.Should().HaveCount(7);
        store.Current.MoveTickets.Should().Contain(t => t.Bucket == "bucket_15");
        store.Current.MoveTickets.Should().NotBeSameAs(first.MoveTickets);
        engine.EvaluateCalls.Should().Be(2);
    }

    // S10: исчезновение ключа видно в полном range-чтении (ключа нет против
    // кэша) → пересборка.
    [Fact]
    public async Task Refresh_KeyGone_Rebuilds()
    {
        // Arrange — первый тик с очередью, затем заявка bucket_3 снята (ключ удалён)
        var gateway = DemoGateway();
        gateway.MovesKv = EtcdFixtures.LoadKv("moves-queue.json");
        var store = new SnapshotStore();
        var engine = new CountingAlertEngine();
        var time = new FixedTimeProvider();
        var refresher = RefresherTestHarness.New(gateway, store, null, null, engine, time, "http://e1");
        await refresher.RefreshOnceAsync(CancellationToken.None);
        gateway.MovesKv = gateway.MovesKv.Skip(1).ToList();

        // Act
        await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert — очередь пересобрана без снятой заявки, пересчёт выполнен
        store.Current!.MoveTickets.Should().HaveCount(5);
        store.Current.MoveTickets.Should().NotContain(t => t.Bucket == "bucket_3");
        engine.EvaluateCalls.Should().Be(2);
    }

    // S10: изменился member list → пересборка (списки URL в члене сравниваются
    // проекцией — reference-equality списков не даёт ложных «изменений»).
    [Fact]
    public async Task Refresh_MemberChange_Rebuilds()
    {
        // Arrange — первый тик с одним членом, затем их два
        var gateway = DemoGateway();
        var store = new SnapshotStore();
        var engine = new CountingAlertEngine();
        var time = new FixedTimeProvider();
        var refresher = RefresherTestHarness.New(gateway, store, null, null, engine, time, "http://e1");
        await refresher.RefreshOnceAsync(CancellationToken.None);
        store.Current!.Etcd.Members.Should().ContainSingle();
        gateway.Members = [new EtcdMember(42, "test", ["http://p"], ["http://c"]), new EtcdMember(43, "test2", ["http://p2"], ["http://c2"])];

        // Act
        await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert — member list обновлён, пересчёт алертов выполнен
        store.Current!.Etcd.Members.Should().HaveCount(2);
        engine.EvaluateCalls.Should().Be(2);
    }

    // S10: тождественный member list (новые списки-инстансы, те же значения)
    // НЕ считается изменением — тик без изменений.
    [Fact]
    public async Task Refresh_MemberListNewInstances_SameValues_Skips()
    {
        // Arrange — первый тик, затем member list пересоздан теми же значениями
        var gateway = DemoGateway();
        var store = new SnapshotStore();
        var engine = new CountingAlertEngine();
        var time = new FixedTimeProvider();
        var refresher = RefresherTestHarness.New(gateway, store, null, null, engine, time, "http://e1");
        await refresher.RefreshOnceAsync(CancellationToken.None);
        gateway.Members = [new EtcdMember(42, "test", ["http://p"], ["http://c"])];

        // Act
        await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert — пересборки нет
        engine.EvaluateCalls.Should().Be(1);
    }

    // S10: изменился alarm кластера → пересборка.
    [Fact]
    public async Task Refresh_AlarmChange_Rebuilds()
    {
        // Arrange — первый тик без алармов, затем NOSPACE-тревога
        var gateway = DemoGateway();
        var store = new SnapshotStore();
        var engine = new CountingAlertEngine();
        var time = new FixedTimeProvider();
        var refresher = RefresherTestHarness.New(gateway, store, null, null, engine, time, "http://e1");
        await refresher.RefreshOnceAsync(CancellationToken.None);
        store.Current!.Etcd.Alarms.Should().BeEmpty();
        gateway.Alarms = [new EtcdAlarm(42, EtcdAlarmType.NoSpace)];

        // Act
        await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert — тревога в снапшоте, пересчёт выполнен
        store.Current!.Etcd.Alarms.Should().ContainSingle(a => a.Type == EtcdAlarmType.NoSpace);
        engine.EvaluateCalls.Should().Be(2);
    }

    // S10: факты статуса изменились (raftTerm) → пересборка; шумовые поля
    // (latency/dbSize/raftIndex) сравнением не охватываются.
    [Fact]
    public async Task Refresh_StatusFactChange_Rebuilds()
    {
        // Arrange — первый тик, затем сменился raftTerm (выборы)
        var gateway = DemoGateway();
        var store = new SnapshotStore();
        var engine = new CountingAlertEngine();
        var time = new FixedTimeProvider();
        var refresher = RefresherTestHarness.New(gateway, store, null, null, engine, time, "http://e1");
        await refresher.RefreshOnceAsync(CancellationToken.None);
        gateway.StatusPayload = new EtcdStatusPayload("3.5.21", 20480, 42, 17, 4, null);

        // Act
        await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert — пересборка по изменению факта статуса
        engine.EvaluateCalls.Should().Be(2);
        store.Current!.Etcd.Endpoints.Should().OnlyContain(e => e.RaftTerm == 4);
    }

    // S10: сбойный опрос ничего не меняет — данные снапшота и BuiltAtUtc не
    // тронуты (применения нет), обновляется только Etcd-часть отказа.
    [Fact]
    public async Task Refresh_FailTick_DoesNotTouchSnapshotData()
    {
        // Arrange — успешный тик, затем endpoints умирают
        var gateway = DemoGateway();
        var store = new SnapshotStore();
        var engine = new CountingAlertEngine();
        var time = new FixedTimeProvider();
        var refresher = RefresherTestHarness.New(gateway, store, null, null, engine, time, "http://e1");
        await refresher.RefreshOnceAsync(CancellationToken.None);
        var first = store.Current!;
        gateway.StatusFailEndpoints.Add("http://e1");

        // Act — сбойный опрос
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert — тик отказан; данные и штамп не тронуты
        result.IsSuccess.Should().BeFalse();
        store.Current!.Clusters.Should().BeSameAs(first.Clusters);
        store.Current.MoveTickets.Should().BeSameAs(first.MoveTickets);
        store.Current.BuiltAtUtc.Should().Be(first.BuiltAtUtc);
        store.Current.Etcd.Reachable.Should().BeFalse();
    }

    // S10: после сбоя данные те же, что в кэше → тик без изменений (пересборки/алертного
    // пересчёта нет): сравнение всегда с кэшем последнего успешно прочитанного
    // состояния, исходы тиков в ветвлении не участвуют. Etcd-часть при тихом
    // тике свежая — восстановление доступности видно без пересборки.
    [Fact]
    public async Task Refresh_AfterFailSameData_IsNoChangeTick()
    {
        // Arrange — успешный тик (кэш=X), сбой статусов, восстановление при тех же данных
        var gateway = DemoGateway();
        var store = new SnapshotStore();
        var engine = new CountingAlertEngine();
        var time = new FixedTimeProvider();
        var refresher = RefresherTestHarness.New(gateway, store, null, null, engine, time, "http://e1");
        await refresher.RefreshOnceAsync(CancellationToken.None);
        var first = store.Current!;
        gateway.StatusFailEndpoints.Add("http://e1");
        await refresher.RefreshOnceAsync(CancellationToken.None);
        store.Current!.Etcd.Reachable.Should().BeFalse();
        gateway.StatusFailEndpoints.Clear();
        time.Utc = time.Utc.AddSeconds(5);

        // Act — первый опрос после сбоя по тем же данным etcd
        await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert — тик без изменений: данные те же ссылки, алерты не пересчитаны;
        // Etcd-часть свежая (reachable восстановлен), штамп обновлён.
        // Счёт 2: пересборка т1 + пересчёт отказного т2 (spec §3.5).
        engine.EvaluateCalls.Should().Be(2);
        store.Current!.Clusters.Should().BeSameAs(first.Clusters);
        store.Current.Etcd.Reachable.Should().BeTrue();
        store.Current.Etcd.ConsecutiveFailures.Should().Be(0);
        store.Current.BuiltAtUtc.Should().Be(time.Utc);
    }
}
