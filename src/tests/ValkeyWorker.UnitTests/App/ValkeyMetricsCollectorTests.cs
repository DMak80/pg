using System.Diagnostics.Metrics;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Shared.Core;
using ValkeyWorker.App;
using ValkeyWorker.Core.Model;
using ValkeyWorker.Core.Valkey;
using Xunit;

namespace ValkeyWorker.UnitTests.App;

// Юнит-тесты ValkeyMetricsCollector (t05, arch/18 §4.2) на inline-фейке
// IValkeyConnection: успешный сбор обновляет стейт, ошибка ноды — тик жив и
// LastSuccess не двигается, пустой домен — успех, пропуски не-Active/без кредов/
// без portalloc, advertised-приоритет хоста, консервативные null-поля, дефолт <=0.
public sealed class ValkeyMetricsCollectorTests
{
    // Фейк RESP-клиента: INFO-ответы по (host, port) + журнал вызовов.
    private sealed class FakeValkey : IValkeyConnection
    {
        public Dictionary<(string Host, int Port), IReadOnlyDictionary<string, string>> Info = [];
        public Exception? FailAll; // сетевой отказ всех проб (нода лежит)
        public List<ValkeyEndpoint> Calls = [];

        public Task<Result> PingAsync(ValkeyEndpoint ep, CancellationToken ct) => Task.FromResult(Result.Success());
        public Task<Result<IReadOnlyDictionary<string, string>>> ConfigGetAsync(ValkeyEndpoint ep, string parameter, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyDictionary<string, string>>.Success(
                new Dictionary<string, string>()));
        public Task<Result> ConfigSetAsync(ValkeyEndpoint ep, string parameter, string value, CancellationToken ct)
            => Task.FromResult(Result.Success());
        public Task<Result<IReadOnlyList<string>>> AclListAsync(ValkeyEndpoint ep, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<string>>.Success([]));
        public Task<Result> AclSetUserAsync(ValkeyEndpoint ep, IReadOnlyList<string> args, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result<IReadOnlyDictionary<string, string>>> InfoAllAsync(ValkeyEndpoint ep, CancellationToken ct)
        {
            Calls.Add(ep);
            if (FailAll is not null)
                return Task.FromResult(Result<IReadOnlyDictionary<string, string>>.Failed(
                    new ApplicationException(FailAll.Message)));
            return Task.FromResult(Result<IReadOnlyDictionary<string, string>>.Success(
                Info.GetValueOrDefault((ep.Host, ep.Port),
                    new Dictionary<string, string>())));
        }
    }

    // Снапшот Active-кластера c1/node1 (RUNNING) с admin-кредами.
    private static ValkeyClusterSnapshot Snap(string cluster = "c1", string? state = null,
        string? adminUser = "admin", string? adminPassword = "secret")
        => new(cluster,
            new ValkeyClusterConfig(1, 536870912, "allkeys-lru", 1756500000, state),
            new Dictionary<string, ValkeyNodeSnapshot> { ["node1"] = new("node1", "RUNNING", null) },
            Endpoints: "dockhost:17001", AppUser: "app", AppPassword: "app-secret",
            AdminUser: adminUser, AdminPassword: adminPassword,
            CaPem: null, CaKey: null, UnknownKeys: [], ParseErrors: []);

    private static IReadOnlyDictionary<string, NodeAddress> Alloc(params (string Node, int Port)[] nodes)
        => nodes.ToDictionary(n => n.Node, n => new NodeAddress("dockhost", n.Port));

    private static Result<IReadOnlyDictionary<string, IReadOnlyDictionary<string, NodeAddress>>> AllocsOf(
        string cluster, IReadOnlyDictionary<string, NodeAddress> nodes)
        => Result<IReadOnlyDictionary<string, IReadOnlyDictionary<string, NodeAddress>>>.Success(
            new Dictionary<string, IReadOnlyDictionary<string, NodeAddress>> { [cluster] = nodes });

    private static readonly IReadOnlyDictionary<string, string> FullInfo =
        new Dictionary<string, string>
        {
            ["used_memory"] = "1048576",
            ["maxmemory"] = "536870912",
            ["connected_clients"] = "3",
            ["blocked_clients"] = "0",
            ["evicted_keys"] = "0",
            ["expired_keys"] = "12",
            ["keyspace_hits"] = "5",
            ["keyspace_misses"] = "2",
            ["instantaneous_ops_per_sec"] = "7",
            ["total_connections_received"] = "10",
            ["rejected_connections"] = "0",
            ["total_commands_processed"] = "42",
            ["role"] = "master",
            ["connected_slaves"] = "0",
        };

    private static ValkeyMetricsCollector Collector(FakeValkey valkey, ValkeyMetricsState state,
        Func<CancellationToken, Task<Result<IReadOnlyList<ValkeyClusterSnapshot>>>>? clusters = null,
        Func<CancellationToken, Task<Result<IReadOnlyDictionary<string, IReadOnlyDictionary<string, NodeAddress>>>>>? allocs = null,
        string? advertised = "localhost", TimeProvider? clock = null)
        => new(30,
            clusters ?? (ct => Task.FromResult(Result<IReadOnlyList<ValkeyClusterSnapshot>>.Success([Snap()]))),
            allocs ?? (ct => Task.FromResult(AllocsOf("c1", Alloc(("node1", 17001))))),
            valkey, advertised, state, clock ?? TimeProvider.System,
            NullLogger<ValkeyMetricsCollector>.Instance);

    // AAA: успешный сбор — стейт обновлён, проба по advertised-хосту, LastSuccess двигается.
    [Fact]
    public async Task Collect_InfoOk_СерииИLastSuccess()
    {
        // Arrange: INFO-ответ на (localhost, 17001) — advertised приоритетнее portalloc.host.
        var valkey = new FakeValkey { Info = { [("localhost", 17001)] = FullInfo } };
        var clock = new FakeClock(DateTimeOffset.UnixEpoch.AddHours(5));
        var state = new ValkeyMetricsState(new Meter("TestValkeyCollector"));
        var collector = Collector(valkey, state, clock: clock);

        // Act
        await collector.CollectOnceAsync(TestContext.Current.CancellationToken);

        // Assert
        var node = state.DebugSnapshot().Nodes[("c1", "node1")];
        node.UsedMemoryBytes.Should().Be(1048576);
        node.MaxMemoryBytes.Should().Be(536870912);
        node.Role.Should().Be("master");
        node.ConnectedSlaves.Should().Be(0);
        valkey.Calls.Should().ContainSingle().Which.Host.Should().Be("localhost");
        state.DebugSnapshot().LastSuccess.Should().Be(DateTimeOffset.UnixEpoch.AddHours(5));
    }

    // AAA: advertised == null → хост пробы из portalloc-записи (правило §2 arch/21).
    [Fact]
    public async Task Collect_БезAdvertised_ХостИзPortalloc()
    {
        // Arrange
        var valkey = new FakeValkey { Info = { [("dockhost", 17001)] = FullInfo } };
        var state = new ValkeyMetricsState(new Meter("TestValkeyCollector"));
        var collector = Collector(valkey, state, advertised: null);

        // Act
        await collector.CollectOnceAsync(TestContext.Current.CancellationToken);

        // Assert
        valkey.Calls.Should().ContainSingle().Which.Host.Should().Be("dockhost");
        state.DebugSnapshot().LastSuccess.Should().NotBeNull();
    }

    // AAA: ошибка ноды — тик жив (не бросает), LastSuccess НЕ обновляется (зеркало Kafka).
    [Fact]
    public async Task Collect_ОшибкаНоды_ТикЖив_LastSuccessМёртв()
    {
        // Arrange: все INFO-провалы (нода лежит).
        var valkey = new FakeValkey { FailAll = new ApplicationException("connection refused") };
        var state = new ValkeyMetricsState(new Meter("TestValkeyCollector"));
        var collector = Collector(valkey, state);

        // Act
        var act = () => collector.CollectOnceAsync(TestContext.Current.CancellationToken);

        // Assert
        await act.Should().NotThrowAsync();
        state.DebugSnapshot().Nodes.Should().BeEmpty();
        state.DebugSnapshot().LastSuccess.Should().BeNull();
    }

    // AAA: пустой домен — консервативный успех (LastSuccess двигается; алерт не горит).
    [Fact]
    public async Task Collect_ПустойДомен_Успех()
    {
        // Arrange: кластеров нет; portalloc пуст.
        var valkey = new FakeValkey();
        var state = new ValkeyMetricsState(new Meter("TestValkeyCollector"));
        var collector = Collector(valkey, state,
            clusters: ct => Task.FromResult(Result<IReadOnlyList<ValkeyClusterSnapshot>>.Success([])),
            allocs: ct => Task.FromResult(AllocsOf("c1", Alloc())));

        // Act
        await collector.CollectOnceAsync(TestContext.Current.CancellationToken);

        // Assert
        valkey.Calls.Should().BeEmpty();
        state.DebugSnapshot().LastSuccess.Should().NotBeNull();
    }

    // AAA: не-Active (Config.State задан) — проб нет; тик успешен (skip ≠ fail).
    [Fact]
    public async Task Collect_НеActive_ПропускБезПробы()
    {
        // Arrange: заявка PROVISIONING.
        var valkey = new FakeValkey();
        var state = new ValkeyMetricsState(new Meter("TestValkeyCollector"));
        var collector = Collector(valkey, state,
            clusters: ct => Task.FromResult(Result<IReadOnlyList<ValkeyClusterSnapshot>>.Success(
                [Snap(state: "PROVISIONING")])));

        // Act
        await collector.CollectOnceAsync(TestContext.Current.CancellationToken);

        // Assert
        valkey.Calls.Should().BeEmpty();
        state.DebugSnapshot().LastSuccess.Should().NotBeNull();
    }

    // AAA: неполные дискавери-креды (AdminUser/AdminPassword null) — пропуск без пробы.
    [Fact]
    public async Task Collect_БезAdminКредов_ПропускБезПробы()
    {
        // Arrange
        var valkey = new FakeValkey();
        var state = new ValkeyMetricsState(new Meter("TestValkeyCollector"));
        var collector = Collector(valkey, state,
            clusters: ct => Task.FromResult(Result<IReadOnlyList<ValkeyClusterSnapshot>>.Success(
                [Snap(adminUser: null, adminPassword: null)])));

        // Act
        await collector.CollectOnceAsync(TestContext.Current.CancellationToken);

        // Assert
        valkey.Calls.Should().BeEmpty();
        state.DebugSnapshot().LastSuccess.Should().NotBeNull();
    }

    // AAA: нода без portalloc-записи — пропуск (E9 — забота надзора C), тик успешен.
    [Fact]
    public async Task Collect_НодаБезPortalloc_Пропуск()
    {
        // Arrange: portalloc-ключ кластера пуст.
        var valkey = new FakeValkey();
        var state = new ValkeyMetricsState(new Meter("TestValkeyCollector"));
        var collector = Collector(valkey, state,
            allocs: ct => Task.FromResult(AllocsOf("c1", Alloc())));

        // Act
        await collector.CollectOnceAsync(TestContext.Current.CancellationToken);

        // Assert
        valkey.Calls.Should().BeEmpty();
        state.DebugSnapshot().Nodes.Should().BeEmpty();
        state.DebugSnapshot().LastSuccess.Should().NotBeNull();
    }

    // AAA: ошибка чтения снапшота кластеров — тик пропущен, LastSuccess не двигается.
    [Fact]
    public async Task Collect_СнапшотНедоступен_ТикПропущен()
    {
        // Arrange: etcd-отказ (обёртка Failed).
        var valkey = new FakeValkey();
        var state = new ValkeyMetricsState(new Meter("TestValkeyCollector"));
        var collector = Collector(valkey, state,
            clusters: ct => Task.FromResult(Result<IReadOnlyList<ValkeyClusterSnapshot>>.Failed(
                new ApplicationException("etcd down"))));

        // Act
        await collector.CollectOnceAsync(TestContext.Current.CancellationToken);

        // Assert
        valkey.Calls.Should().BeEmpty();
        state.DebugSnapshot().LastSuccess.Should().BeNull();
    }

    // AAA: отсутствующие/нечисловые поля INFO → null-поля сэмпла (серии не эмитятся).
    [Fact]
    public void ToSample_ОтсутствующиеПоля_КонсервативныйNull()
    {
        // Arrange: только used_memory и role.
        IReadOnlyDictionary<string, string> partial = new Dictionary<string, string>
        {
            ["used_memory"] = "1",
            ["role"] = "master",
            ["instantaneous_ops_per_sec"] = "not-a-number",
        };

        // Act
        var sample = ValkeyMetricsCollector.ToSample("node1", partial);

        // Assert
        sample.UsedMemoryBytes.Should().Be(1);
        sample.Role.Should().Be("master");
        sample.MaxMemoryBytes.Should().BeNull();
        sample.InstantaneousOpsPerSec.Should().BeNull("нечисловое поле — серия не эмитится");
        sample.ConnectedSlaves.Should().BeNull();
    }

    // AAA: <=0-интервал → дефолт 30 (зеркало Kafka-паттерна SnapshotRefresher).
    [Theory]
    [InlineData(0, 30)]
    [InlineData(-5, 30)]
    [InlineData(45, 45)]
    public void EffectiveIntervalSec_НеПоложительный_Дефолт30(int configured, int expected)
    {
        // Arrange/Act/Assert
        ValkeyMetricsCollector.EffectiveIntervalSec(configured).Should().Be(expected);
    }

    private sealed class FakeClock(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }
}
