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
        WorkerEndpoints: [], WorkerHealth: [], Probes: [], Alerts: [], ParseErrors: [], UnknownKeyCount: 0);

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
        TimeProvider? time = null,
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
            time ?? TimeProvider.System,
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

    // Backoff недоступного кластера (t11): окно повтора 15 c → 60 c → 300 c —
    // мёртвые endpoints не штурмуются каждый тик (churn-инцидент 2026-09-02).
    [Fact]
    public async Task RunOnce_FailingCluster_BackoffSkipsTicksAndGrowsWindow()
    {
        // Arrange: кластер мёртв (клиент падает), время управляемое.
        var clock = new FixedTimeProvider();
        var rig = NewRig(time: clock, clusters: ActiveCluster());
        rig.Client.Error = new InvalidOperationException("connection refused");

        // Act/Assert: тик 1 (t0) — первая неудача, окно = обычный интервал 15 c.
        await rig.Loop.RunOnceAsync(CancellationToken.None);
        rig.Client.Calls.Should().HaveCount(1);

        // Тик 2 (t0+15) — вторая неудача, окно растёт до 60 c (след. t0+75).
        clock.Utc = clock.Utc.AddSeconds(15);
        await rig.Loop.RunOnceAsync(CancellationToken.None);
        rig.Client.Calls.Should().HaveCount(2);

        // Тик 3 (t0+30) — внутри окна: проба пропущена, состояние несёт ошибку
        // с пометкой backoff (кластер не мерцает, клиент не дёргается).
        clock.Utc = clock.Utc.AddSeconds(15);
        await rig.Loop.RunOnceAsync(CancellationToken.None);
        rig.Client.Calls.Should().HaveCount(2);
        var skipped = rig.ProbeStore.Current!.Results.Single();
        skipped.Ok.Should().BeFalse();
        skipped.Error.Should().Contain("connection refused").And.Contain("backoff");
        rig.ProbeStore.Current!.Clusters.Should().BeEmpty();

        // Тик 4 (t0+60) — всё ещё внутри 60-секундного окна.
        clock.Utc = clock.Utc.AddSeconds(30);
        await rig.Loop.RunOnceAsync(CancellationToken.None);
        rig.Client.Calls.Should().HaveCount(2);

        // Тик 5 (t0+75) — окно истекло, третья неудача растит его до 300 c.
        clock.Utc = clock.Utc.AddSeconds(15);
        await rig.Loop.RunOnceAsync(CancellationToken.None);
        rig.Client.Calls.Should().HaveCount(3);

        // Тик 6 (t0+200) — внутри 300-секундного окна: снова пропуск.
        clock.Utc = clock.Utc.AddSeconds(125);
        await rig.Loop.RunOnceAsync(CancellationToken.None);
        rig.Client.Calls.Should().HaveCount(3);
    }

    [Fact]
    public async Task RunOnce_BackoffResetOnSuccess()
    {
        // Arrange: две неудачи (окно 60 c), затем брокеры поднялись.
        var clock = new FixedTimeProvider();
        var rig = NewRig(time: clock, clusters: ActiveCluster());
        rig.Client.Error = new InvalidOperationException("connection refused");
        await rig.Loop.RunOnceAsync(CancellationToken.None);
        clock.Utc = clock.Utc.AddSeconds(15);
        await rig.Loop.RunOnceAsync(CancellationToken.None);
        rig.Client.Calls.Should().HaveCount(2);
        rig.Client.Error = null;

        // Act: окно истекло — проба успешна, backoff сброшен.
        clock.Utc = clock.Utc.AddSeconds(60);
        await rig.Loop.RunOnceAsync(CancellationToken.None);

        // Assert: кластер жив; следующий тик через обычные 15 c (не через 300 c).
        rig.Client.Calls.Should().HaveCount(3);
        rig.ProbeStore.Current!.Results.Single().Ok.Should().BeTrue();
        clock.Utc = clock.Utc.AddSeconds(15);
        await rig.Loop.RunOnceAsync(CancellationToken.None);
        rig.Client.Calls.Should().HaveCount(4);
        rig.ProbeStore.Current!.Clusters.Should().ContainKey("events");
    }

    [Fact]
    public async Task RunOnce_BackoffDroppedWhenClusterLeavesEtcd()
    {
        // Arrange: неудачная проба завела кластер в backoff.
        var clock = new FixedTimeProvider();
        var rig = NewRig(time: clock, clusters: ActiveCluster());
        rig.Client.Error = new InvalidOperationException("connection refused");
        await rig.Loop.RunOnceAsync(CancellationToken.None);
        clock.Utc = clock.Utc.AddSeconds(15);
        await rig.Loop.RunOnceAsync(CancellationToken.None);

        // Act: кластер удалён из etcd (снапшот пустеет), затем вернулся.
        rig.SnapshotStore.Replace(Snapshot());
        await rig.Loop.RunOnceAsync(CancellationToken.None);
        rig.SnapshotStore.Replace(Snapshot(ActiveCluster()));
        clock.Utc = clock.Utc.AddSeconds(5); // раньше любого backoff-окна
        await rig.Loop.RunOnceAsync(CancellationToken.None);

        // Assert: вернувшийся кластер пробуется сразу, без хвостового backoff.
        rig.Client.Calls.Should().HaveCount(3);
    }
}
