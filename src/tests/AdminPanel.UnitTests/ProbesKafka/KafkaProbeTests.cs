using AdminPanel.Core;
using AdminPanel.Core.Kafka;
using AdminPanel.Etcd;
using AdminPanel.Infrastructure;
using AdminPanel.Probes;
using AdminPanel.Probes.Kafka;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.UnitTests.ProbesKafka;

// KafkaProbeLoop (план B6): HostMap-резолюция endpoints, SASL из internal-стора
// кредов, ошибка пробы → ProbeResult.Error (etcd-часть жива), пароль в
// результаты не попадает.
public class KafkaProbeTests
{
    private sealed class FakeProbeClient : IKafkaProbeClient
    {
        public KafkaProbeView? View;
        public Exception? Error;
        public List<(string Bootstrap, string User, string Password)> Calls = [];

        public Task<Result<KafkaProbeView>> DescribeClusterAsync(
            string bootstrap, string user, string password, TimeSpan timeout, CancellationToken ct)
        {
            Calls.Add((bootstrap, user, password));
            return Task.FromResult(Error is not null
                ? Result<KafkaProbeView>.Failed(Error)
                : Result<KafkaProbeView>.Success(View!));
        }
    }

    private static KafkaSnapshot Snapshot(params KafkaClusterInfo[] clusters) => new(
        new DateTimeOffset(2026, 8, 30, 12, 0, 0, TimeSpan.Zero),
        EtcdReachable: true, ConsecutiveFailures: 0,
        [.. clusters], Rotations: [], Rebalances: [], Reassignments: [],
        Probes: [], Alerts: [], ParseErrors: [], UnknownKeyCount: 0);

    private static KafkaClusterInfo ActiveCluster(
        string name = "events", string endpoints = "host.docker.internal:16001")
        => new(
            name, KafkaClusterState.Active, 3, 3, 2, 12, 604800000, 1756500000,
            endpoints,
            [new KafkaBrokerInfo("broker1", "RUNNING", "controller", 2m, 4, 40)],
            []);

    private sealed record Rig(
        FakeProbeClient Client,
        KafkaSnapshotStore SnapshotStore,
        KafkaSecretsStore Secrets,
        KafkaProbeStore ProbeStore,
        KafkaProbeLoop Loop);

    private static Rig NewRig(
        Dictionary<string, string>? hostMap = null,
        params KafkaClusterInfo[] clusters)
    {
        var client = new FakeProbeClient
        {
            View = new KafkaProbeView(
                [new KafkaProbeBroker(1, "broker1"), new KafkaProbeBroker(2, "broker2")],
                ControllerId: 1),
        };
        var snapshotStore = new KafkaSnapshotStore();
        snapshotStore.Replace(Snapshot(clusters));
        var secrets = new KafkaSecretsStore();
        secretsStoreReplace(secrets);
        var probeStore = new KafkaProbeStore();
        var loop = new KafkaProbeLoop(
            snapshotStore, secrets, client, probeStore,
            Options.Create(new KafkaProbeOptions()),
            Options.Create(new ProbesOptions { HostMap = hostMap ?? [] }),
            TimeProvider.System,
            NullLogger<KafkaProbeLoop>.Instance);
        return new Rig(client, snapshotStore, secrets, probeStore, loop);

        static void secretsStoreReplace(KafkaSecretsStore store)
            => store.Replace(new Dictionary<string, KafkaClusterSecrets>
            {
                ["events"] = new("events", "app", "SecretPassword0123456789"),
            });
    }

    [Fact]
    public async Task RunOnce_ActiveCluster_ProbesWithSaslAndStoresLive()
    {
        // Arrange: Active-кластер с endpoints.
        var rig = NewRig(clusters: ActiveCluster());

        // Act
        await rig.Loop.RunOnceAsync(CancellationToken.None);

        // Assert: DescribeCluster вызван с bootstrap из endpoints и кредами стора;
        // live-данные (brokers + controller) в сторе проб; ProbeResult ok.
        rig.Client.Calls.Should().ContainSingle().Which
            .Bootstrap.Should().Be("host.docker.internal:16001");
        rig.Client.Calls.Single().User.Should().Be("app");
        var state = rig.ProbeStore.Current!;
        state.Results.Should().ContainSingle().Which.Ok.Should().BeTrue();
        var live = state.Clusters["events"];
        live.Brokers.Should().HaveCount(2);
        live.Brokers.Single(b => b.Id == 1).Controller.Should().BeTrue();
        live.Brokers.Single(b => b.Id == 2).Controller.Should().BeFalse();
    }

    [Fact]
    public async Task RunOnce_HostMap_ResolvesAdvertisedAddresses()
    {
        // Arrange: стенд-маппинг host.docker.internal:16001 → localhost:16001
        // (симметрия advertised-паттерна A2/A13).
        var rig = NewRig(
            hostMap: new Dictionary<string, string> { ["host.docker.internal:16001"] = "localhost:16001" },
            clusters: ActiveCluster());

        // Act
        await rig.Loop.RunOnceAsync(CancellationToken.None);

        // Assert: проба подключается по маппированному адресу.
        rig.Client.Calls.Single().Bootstrap.Should().Be("localhost:16001");
    }

    [Fact]
    public async Task RunOnce_ProbeFails_ResultCarriesErrorEtcdPartAlive()
    {
        // Arrange: кластер не отвечает.
        var rig = NewRig(clusters: ActiveCluster());
        rig.Client.Error = new InvalidOperationException("connection refused");

        // Act
        await rig.Loop.RunOnceAsync(CancellationToken.None);

        // Assert: ProbeResult с ошибкой; live-данных нет; снапшот (etcd-часть) жив.
        var result = rig.ProbeStore.Current!.Results.Single();
        result.Ok.Should().BeFalse();
        result.Error.Should().Contain("connection refused");
        result.Kind.Should().Be("kafka");
        rig.ProbeStore.Current!.Clusters.Should().BeEmpty();
        rig.SnapshotStore.Current.Should().NotBeNull();
    }

    [Fact]
    public async Task RunOnce_PasswordNeverInResults()
    {
        // Arrange: проба падает — текст ошибки формируется из bootstrap и исключения.
        var rig = NewRig(clusters: ActiveCluster());
        rig.Client.Error = new InvalidOperationException("auth failed");

        // Act
        await rig.Loop.RunOnceAsync(CancellationToken.None);

        // Assert: ни один артефакт состояния пробы не содержит пароль.
        var state = rig.ProbeStore.Current!;
        var material = string.Join(" ",
            state.Results.Select(r => $"{r.Target} {r.Kind} {r.Error}"),
            string.Join(" ", state.Clusters.Values.SelectMany(c => c.Brokers.Select(b => b.Host))));
        material.Should().NotContain("SecretPassword0123456789");
    }

    [Fact]
    public async Task RunOnce_NoSecrets_ResultWithErrorNoCall()
    {
        // Arrange: кредов в сторе нет (воркер ensure не выполнил).
        var client = new FakeProbeClient { View = new KafkaProbeView([], null) };
        var snapshotStore = new KafkaSnapshotStore();
        snapshotStore.Replace(Snapshot(ActiveCluster()));
        var probeStore = new KafkaProbeStore();
        var loop = new KafkaProbeLoop(
            snapshotStore, new KafkaSecretsStore(), client, probeStore,
            Options.Create(new KafkaProbeOptions()),
            Options.Create(new ProbesOptions()),
            TimeProvider.System,
            NullLogger<KafkaProbeLoop>.Instance);

        // Act
        await loop.RunOnceAsync(CancellationToken.None);

        // Assert: клиент не дёргался; результат с пояснением.
        client.Calls.Should().BeEmpty();
        probeStore.Current!.Results.Should().ContainSingle()
            .Which.Error.Should().Contain("app-кредов");
    }

    [Fact]
    public async Task RunOnce_NotInitializedCluster_Skipped()
    {
        // Arrange: NOT_INITIALIZED-кластер без endpoints — не цель пробы.
        var rig = NewRig(clusters: new KafkaClusterInfo(
            "pending", KafkaClusterState.NotInitialized, 3, 3, 2, 12, 604800000,
            1756500000, null, [], []));

        // Act
        await rig.Loop.RunOnceAsync(CancellationToken.None);

        // Assert
        rig.Client.Calls.Should().BeEmpty();
        rig.ProbeStore.Current!.Results.Should().BeEmpty();
    }
}
