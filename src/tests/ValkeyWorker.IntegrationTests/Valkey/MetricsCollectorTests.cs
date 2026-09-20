using System.Diagnostics;
using System.Diagnostics.Metrics;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Shared.Core;
using Shared.Etcd.Client;
using ValkeyWorker.App;
using ValkeyWorker.Core.Valkey;
using ValkeyWorker.Etcd.Parsing;
using ValkeyWorker.Provisioning.Processes;
using Xunit;

namespace ValkeyWorker.IntegrationTests.Valkey;

// Живой коллектор против docker-ноды (t05, spec §9): provisioning кластера →
// CollectOnceAsync → серии словаря со значениями (роль master, maxmemory из
// декларации); docker stop ноды → тик не падает, LastSuccess стоит, стейт не
// перезаписан. Отдельный от live-WAF (задача 5) тест: прямой контроль тиков.
[Collection(ValkeyClusterCollection.Name)]
public sealed class MetricsCollectorTests(ValkeyClusterFixture fx)
{
    // Settable-часы: между тиками двигаем время — «LastSuccess стоит» проверяемо.
    private sealed class SettableClock(DateTimeOffset utc) : TimeProvider
    {
        private DateTimeOffset _utc = utc;
        public override DateTimeOffset GetUtcNow() => _utc;
        public void Advance(TimeSpan span) => _utc += span;
    }

    private static async Task<Result<IReadOnlyList<ValkeyWorker.Core.Model.ValkeyClusterSnapshot>>> ClustersAsync(
        ValkeyClusterFixture fx, CancellationToken ct)
    {
        var range = await fx.Gateway.RangeAsync(fx.Endpoint, "/valkey/clusters/", ct);
        if (!range.IsSuccess)
            return Result<IReadOnlyList<ValkeyWorker.Core.Model.ValkeyClusterSnapshot>>.Failed(range.Error!);
        var parsed = ValkeySnapshotParser.Parse(range.Value);
        return parsed.IsSuccess
            ? Result<IReadOnlyList<ValkeyWorker.Core.Model.ValkeyClusterSnapshot>>.Success(parsed.Value.Clusters)
            : Result<IReadOnlyList<ValkeyWorker.Core.Model.ValkeyClusterSnapshot>>.Failed(parsed.Error!);
    }

    private static async Task<Result<IReadOnlyDictionary<string, IReadOnlyDictionary<string, ValkeyWorker.Core.Model.NodeAddress>>>> AllocsAsync(
        ValkeyClusterFixture fx, CancellationToken ct)
    {
        var range = await fx.Gateway.RangeAsync(fx.Endpoint, "/valkeyworker/portalloc/", ct);
        if (!range.IsSuccess)
            return Result<IReadOnlyDictionary<string, IReadOnlyDictionary<string, ValkeyWorker.Core.Model.NodeAddress>>>.Failed(range.Error!);
        var allocs = new Dictionary<string, IReadOnlyDictionary<string, ValkeyWorker.Core.Model.NodeAddress>>();
        foreach (var kv in range.Value)
            allocs[kv.Key.Split('/')[^1]] = ProcessCommon.ParsePortAlloc(kv.Value);
        return Result<IReadOnlyDictionary<string, IReadOnlyDictionary<string, ValkeyWorker.Core.Model.NodeAddress>>>.Success(allocs);
    }

    private static void DockerStop(string container)
    {
        var psi = new ProcessStartInfo("docker", $"stop {container}")
        {
            RedirectStandardOutput = true,
        };
        using var proc = Process.Start(psi)!;
        proc.WaitForExit();
    }

    // AAA: живая нода — серии словаря со значениями; остановленная — тик жив,
    // LastSuccess стоит (консервативно), стейт не перезаписан.
    [Fact]
    public async Task Collect_ЖиваяНодаИОстановленная_СерииИLastSuccess()
    {
        // Arrange: provisioning кластера (паттерн ProvisioningTests).
        var ct = TestContext.Current.CancellationToken;
        var cluster = fx.Cluster("mtr");
        await fx.SeedClusterAsync(cluster);
        var claims = fx.NewClaimStore();
        (await claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        var process = fx.NewProvisioning(claims, fx.NewJournal(),
            fx.NewPortAllocLock(claims.InstanceId), fx.NewPortAllocIndex(),
            fx.NewSecretEnsurer());
        var snap = await fx.RequireSnapshotAsync(cluster);
        (await process.TickAsync(snap, ct)).IsSuccess.Should().BeTrue("нода поднята provisioning-тиком");

        var state = new ValkeyMetricsState(new Meter("TestValkeyLiveCollector"));
        var clock = new SettableClock(DateTimeOffset.UnixEpoch.AddHours(9));
        var collector = new ValkeyMetricsCollector(30,
            ct2 => ClustersAsync(fx, ct2),
            ct2 => AllocsAsync(fx, ct2),
            new ValkeyConnection(TimeSpan.FromSeconds(2)),
            ValkeyClusterFixture.AdvertisedClientHost,
            state, clock, NullLogger<ValkeyMetricsCollector>.Instance);

        // Act 1: сбор живой ноды.
        await collector.CollectOnceAsync(ct);

        // Assert 1: поля INFO с фактическими значениями; LastSuccess = T0.
        var node = state.DebugSnapshot().Nodes[(cluster, "node1")];
        node.UsedMemoryBytes.Should().BeGreaterThan(0);
        node.MaxMemoryBytes.Should().Be(536870912, "maxmemory задан декларацией сида");
        node.Role.Should().Be("master");
        node.ConnectedSlaves.Should().Be(0);
        node.TotalCommandsProcessed.Should().BeGreaterThanOrEqualTo(0);
        state.DebugSnapshot().LastSuccess.Should().Be(DateTimeOffset.UnixEpoch.AddHours(9));

        // Arrange 2: останавливаем ноду (порт закрылся; portalloc-ключ жив).
        DockerStop($"vwk-{cluster}-node1");
        clock.Advance(TimeSpan.FromSeconds(60));
        var before = state.DebugSnapshot().LastSuccess;

        // Act 2: тик на лежачей ноде.
        var act = () => collector.CollectOnceAsync(ct);

        // Assert 2: тик не бросает; LastSuccess НЕ двигался; стейт прежний.
        await act.Should().NotThrowAsync();
        state.DebugSnapshot().LastSuccess.Should().Be(before, "LastSuccess стоит при неуспехе тика");
        state.DebugSnapshot().Nodes[(cluster, "node1")].UsedMemoryBytes
            .Should().Be(node.UsedMemoryBytes, "стейт не перезаписан провальным тиком");
    }
}
