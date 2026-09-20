using AdminPanel.Core;
using AdminPanel.Core.Valkey;
using AdminPanel.Etcd;
using AdminPanel.Probes;
using AdminPanel.Probes.Valkey;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.UnitTests.ProbesValkey;

// ValkeyProbeLoop (spec §4.6): PING по Active-кластерам с endpoints и полным
// набором admin-кредов; без кредов/endpoints — кластер без пробы; ошибка —
// Live=false+Error (креды в результат не попадают).
public class ValkeyProbeLoopTests
{
    private static readonly DateTimeOffset Now = new(2026, 9, 17, 12, 0, 0, TimeSpan.Zero);

    // Фейк клиента (spec §4.6): in-memory флаг «отвечает», счётчик AUTH-кредов.
    private sealed class FakeProbeClient(bool ok = true, string? error = null) : IValkeyProbeClient
    {
        public int Calls { get; private set; }

        public ValkeyProbeTarget? LastTarget { get; private set; }

        public Task<Shared.Core.Result> PingAsync(ValkeyProbeTarget target, TimeSpan timeout, CancellationToken ct)
        {
            Calls++;
            LastTarget = target;
            return Task.FromResult(ok
                ? Shared.Core.Result.Success()
                : Shared.Core.Result.Failed(new ApplicationException(error ?? "probe failed")));
        }
    }

    private sealed class StubSnapshotReader(ValkeySnapshot? snapshot) : IValkeySnapshotReader
    {
        public ValkeySnapshot? Current { get; } = snapshot;
    }

    private sealed class StubSecretsStore(Dictionary<string, ValkeyClusterSecrets> secrets)
        : IValkeySecretsStore
    {
        public IReadOnlyDictionary<string, ValkeyClusterSecrets> Current { get; } = secrets;

        public void Replace(IReadOnlyDictionary<string, ValkeyClusterSecrets> s) => throw new NotSupportedException();
    }

    private static ValkeySnapshot SnapshotWith(params ValkeyClusterInfo[] clusters) => new(
        Now, EtcdReachable: true, ConsecutiveFailures: 0,
        [.. clusters], [], [], [], [], [], [], 0);

    private static ValkeyClusterInfo ActiveCluster(
        string name = "live", string? endpoints = "127.0.0.1:17001")
        => new(name, ValkeyClusterState.Active, 1, 1, "noeviction", 1, endpoints,
            [new ValkeyNodeInfo("node1", "RUNNING", 1, 1, 10)]);

    private static ValkeyProbeLoop NewLoop(
        ValkeySnapshot? snapshot,
        Dictionary<string, ValkeyClusterSecrets>? secrets,
        IValkeyProbeClient client,
        ValkeyProbeStore store)
        => new(
            new StubSnapshotReader(snapshot),
            new StubSecretsStore(secrets ?? []),
            client,
            store,
            Options.Create(new ProbesOptions()),
            new FixedTimeProvider { Utc = Now },
            NullLogger<ValkeyProbeLoop>.Instance);

    // Arrange: снапшот: Active-кластер live с endpoints + полный набор кредов в сторе.
    // Act: RunOnceAsync. Assert: в IValkeyProbeStore запись Live=true, Node="node1".
    [Fact]
    public async Task Loop_ActiveClusterWithCreds_Probes()
    {
        var store = new ValkeyProbeStore();
        var client = new FakeProbeClient(ok: true);
        var loop = NewLoop(
            SnapshotWith(ActiveCluster()),
            new() { ["live"] = new ValkeyClusterSecrets("live", "admin", "secret0123456789", null) },
            client, store);

        // Act
        await loop.RunOnceAsync(CancellationToken.None);

        // Assert
        var probe = store.Current.Should().ContainSingle().Subject;
        probe.Cluster.Should().Be("live");
        probe.Node.Should().Be("node1");
        probe.Live.Should().BeTrue();
        probe.Error.Should().BeNull();
        probe.CheckedUnix.Should().Be(Now.ToUnixTimeSeconds());
        // Креды дошли до клиента (AUTH), но в результат не попали.
        client.LastTarget!.AdminUser.Should().Be("admin");
    }

    // Arrange: Active-кластер без кредов (частичный набор — refresher его в стор
    // не кладёт) и без endpoints. Act: RunOnceAsync.
    // Assert: кластер НЕ пробится — записи с его именем нет.
    [Fact]
    public async Task Loop_NoCredsOrNoEndpoints_Skips()
    {
        var store = new ValkeyProbeStore();
        var client = new FakeProbeClient(ok: true);
        var loop = NewLoop(
            SnapshotWith(ActiveCluster("nocreds"), ActiveCluster("noep", endpoints: null)),
            [], // частичный набор не попадает в стор (ReadSecrets refresher'а)
            client, store);

        // Act
        await loop.RunOnceAsync(CancellationToken.None);

        // Assert
        client.Calls.Should().Be(0);
        store.Current.Should().BeEmpty();
    }

    // Arrange: креды есть, фейк отвечает ошибкой. Act: RunOnceAsync.
    // Assert: запись Live=false, Error заполнен; креды НЕ попадают в Error.
    [Fact]
    public async Task Loop_ProbeError_LiveFalseWithError()
    {
        var store = new ValkeyProbeStore();
        var client = new FakeProbeClient(ok: false, error: "WRONGPASS denied");
        var loop = NewLoop(
            SnapshotWith(ActiveCluster()),
            new() { ["live"] = new ValkeyClusterSecrets("live", "admin", "topsecret42", null) },
            client, store);

        // Act
        await loop.RunOnceAsync(CancellationToken.None);

        // Assert
        var probe = store.Current.Should().ContainSingle().Subject;
        probe.Live.Should().BeFalse();
        probe.Error.Should().Contain("WRONGPASS");
        probe.Error.Should().NotContain("topsecret42");
    }

    // Arrange: NOT_INITIALIZED-кластер. Assert: не пробится.
    [Fact]
    public async Task Loop_NotInitialized_Skips()
    {
        var store = new ValkeyProbeStore();
        var client = new FakeProbeClient(ok: true);
        var pending = new ValkeyClusterInfo("cache", ValkeyClusterState.NotInitialized, 1, 1,
            "noeviction", 1, "127.0.0.1:17002", [new ValkeyNodeInfo("node1", "NOT_INITIALIZED", 1, 1, 10)]);
        var loop = NewLoop(
            SnapshotWith(pending),
            new() { ["cache"] = new ValkeyClusterSecrets("cache", "admin", "secret0123456789", null) },
            client, store);

        // Act
        await loop.RunOnceAsync(CancellationToken.None);

        // Assert
        client.Calls.Should().Be(0);
        store.Current.Should().BeEmpty();
    }
}
