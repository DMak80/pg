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

// Runtime-уровень пробы (план C3): DescribeTopics (USR по ISR) + группы с
// totalLag (end − committed; чистый расчёт — KafkaGroupLag), сортировка групп
// по лагу; ошибка runtime не роняет live-брокерскую часть.
public class KafkaProbeTopicsTests
{
    private sealed class FakeProbeClient : IKafkaProbeClient
    {
        public Task<Result<KafkaProbeView>> DescribeClusterAsync(
            string bootstrap, string user, string password, string? caPem, TimeSpan timeout, CancellationToken ct)
            => Task.FromResult(Result<KafkaProbeView>.Success(new KafkaProbeView(
                [new KafkaProbeBroker(1, "broker1")], ControllerId: 1)));
    }

    private sealed class FakeRuntimeClient : IKafkaProbeRuntimeClient
    {
        public IReadOnlyList<KafkaProbeTopic>? Topics;
        public Exception? TopicsError;
        public IReadOnlyList<KafkaProbeGroupDetail>? GroupDetails;
        public IReadOnlyList<string> Groups = [];
        public IReadOnlyDictionary<(string Topic, int Partition), long> End = new Dictionary<(string Topic, int Partition), long>();
        public IReadOnlyDictionary<string, IReadOnlyDictionary<(string Topic, int Partition), long>> CommittedByGroup = new Dictionary<string, IReadOnlyDictionary<(string Topic, int Partition), long>>();

        public Task<Result<IReadOnlyList<KafkaProbeTopic>>> DescribeTopicsAsync(
            string bootstrap, string user, string password, string? caPem, TimeSpan timeout, CancellationToken ct)
            => Task.FromResult(TopicsError is not null
                ? Result<IReadOnlyList<KafkaProbeTopic>>.Failed(TopicsError)
                : Result<IReadOnlyList<KafkaProbeTopic>>.Success(Topics ?? []));

        public Task<Result<IReadOnlyList<string>>> ListGroupsAsync(
            string bootstrap, string user, string password, string? caPem, TimeSpan timeout, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<string>>.Success(Groups));

        public Task<Result<IReadOnlyList<KafkaProbeGroupDetail>>> DescribeGroupsAsync(
            string bootstrap, string user, string password, string? caPem, IReadOnlyList<string> groups,
            TimeSpan timeout, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<KafkaProbeGroupDetail>>.Success(GroupDetails ?? []));

        public Task<Result<IReadOnlyDictionary<(string Topic, int Partition), long>>> EndOffsetsAsync(
            string bootstrap, string user, string password, string? caPem,
            IReadOnlyList<(string Topic, int Partition)> partitions, TimeSpan timeout, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyDictionary<(string Topic, int Partition), long>>.Success(
                End.Where(p => partitions.Contains(p.Key)).ToDictionary(p => p.Key, p => p.Value)));

        public Task<Result<IReadOnlyDictionary<(string Topic, int Partition), long>>> CommittedAsync(
            string bootstrap, string user, string password, string? caPem, string group,
            IReadOnlyList<(string Topic, int Partition)> partitions, TimeSpan timeout, CancellationToken ct)
        {
            // Пустой набор партиций = все закоммиченные группы (Burrow-семантика).
            var all = CommittedByGroup.GetValueOrDefault(group, new Dictionary<(string Topic, int Partition), long>());
            return Task.FromResult(Result<IReadOnlyDictionary<(string Topic, int Partition), long>>.Success(
                partitions.Count == 0
                    ? all
                    : all.Where(p => partitions.Contains(p.Key)).ToDictionary(p => p.Key, p => p.Value)));
        }
    }

    private static (KafkaProbeLoop Loop, KafkaProbeStore Store, FakeRuntimeClient Runtime) NewRig(
        FakeRuntimeClient runtime)
    {
        var snapshotStore = new KafkaSnapshotStore();
        snapshotStore.Replace(new KafkaSnapshot(
            new DateTimeOffset(2026, 8, 30, 12, 0, 0, TimeSpan.Zero),
            EtcdReachable: true, ConsecutiveFailures: 0,
            [new KafkaClusterInfo(
                "events", KafkaClusterState.Active, 1, 1, 1, 12, 604800000, 1756500000,
                "host.docker.internal:16001",
                [new KafkaBrokerInfo("broker1", "RUNNING", "controller", 2m, 4, 40)],
                [])],
            Rotations: [], Rebalances: [], Reassignments: [], Regens: [],
            WorkerEndpoints: [], WorkerHealth: [], Probes: [], Alerts: [], ParseErrors: [], UnknownKeyCount: 0));
        var secrets = new KafkaSecretsStore();
        secrets.Replace(new Dictionary<string, KafkaClusterSecrets>
        {
            ["events"] = new("events", "admin", "SecretPassword0123456789", "CAPEM"),
        });
        var probeStore = new KafkaProbeStore();
        var loop = new KafkaProbeLoop(
            snapshotStore, secrets, new FakeProbeClient(), probeStore,
            Options.Create(new KafkaProbeOptions()),
            Options.Create(new ProbesOptions()),
            TimeProvider.System,
            NullLogger<KafkaProbeLoop>.Instance,
            runtime);
        return (loop, probeStore, runtime);
    }

    [Fact]
    public async Task RunOnce_TopicsAndGroups_WithLagsInLive()
    {
        // Arrange: топик с USR=1; группа lag-test (2 партиции, лаг 5+3),
        // группа idle (нет назначения — лаг 0).
        var runtime = new FakeRuntimeClient
        {
            Topics =
            [
                new KafkaProbeTopic("orders", 3, 1, UnderReplicatedPartitions: 1),
                new KafkaProbeTopic("payments", 6, 1, UnderReplicatedPartitions: 0),
            ],
            Groups = ["idle", "lag-test"],
            GroupDetails =
            [
                new KafkaProbeGroupDetail("idle", "Empty", 0, []),
                new KafkaProbeGroupDetail("lag-test", "Stable", 1,
                    [("orders", 0), ("orders", 1)]),
            ],
            End = new Dictionary<(string Topic, int Partition), long>
            {
                [("orders", 0)] = 100,
                [("orders", 1)] = 50,
            },
            CommittedByGroup = new Dictionary<string, IReadOnlyDictionary<(string Topic, int Partition), long>>
            {
                ["lag-test"] = new Dictionary<(string Topic, int Partition), long>
                {
                    [("orders", 0)] = 95,
                    [("orders", 1)] = 47,
                },
            },
        };
        var (loop, store, _) = NewRig(runtime);

        // Act
        await loop.RunOnceAsync(CancellationToken.None);

        // Assert: live кластер несёт топики (USR) и группы с totalLag
        // (сортировка по лагу: lag-test впереди idle).
        var live = store.Current!.Clusters["events"];
        live.Topics.Should().HaveCount(2);
        live.Topics!.Single(t => t.Topic == "orders").UnderReplicatedPartitions.Should().Be(1);
        live.Groups.Should().HaveCount(2);
        live.Groups![0].Group.Should().Be("lag-test");
        live.Groups[0].TotalLag.Should().Be(8);
        live.Groups[0].Members.Should().Be(1);
        live.Groups[1].Group.Should().Be("idle");
        live.Groups[1].TotalLag.Should().Be(0);
    }

    [Fact]
    public async Task RunOnce_GroupWithoutCommit_ShownWithZeroLag()
    {
        // Arrange: группа ни разу не коммитилась — committed-оффсетов нет,
        // лаг по committed-семантике неопределим: группа показана с 0.
        var runtime = new FakeRuntimeClient
        {
            Groups = ["fresh"],
            GroupDetails = [new KafkaProbeGroupDetail("fresh", "Stable", 1, [("orders", 0)])],
            End = new Dictionary<(string Topic, int Partition), long> { [("orders", 0)] = 42 },
        };
        var (loop, store, _) = NewRig(runtime);

        // Act
        await loop.RunOnceAsync(CancellationToken.None);

        // Assert
        var group = store.Current!.Clusters["events"].Groups!.Single();
        group.Group.Should().Be("fresh");
        group.TotalLag.Should().Be(0, "нет committed — лаг по committed-семантике не определён");
    }

    [Fact]
    public async Task RunOnce_DeadGroupWithCommitted_LagSurvives()
    {
        // Arrange: консьюмер умер (Empty, 0 участников), но committed остались:
        // отставание продолжает светиться — ровно то, для чего мониторинг.
        var runtime = new FakeRuntimeClient
        {
            Groups = ["dead"],
            GroupDetails = [new KafkaProbeGroupDetail("dead", "Empty", 0, [])],
            End = new Dictionary<(string Topic, int Partition), long> { [("orders", 0)] = 100 },
            CommittedByGroup = new Dictionary<string, IReadOnlyDictionary<(string Topic, int Partition), long>>
            {
                ["dead"] = new Dictionary<(string Topic, int Partition), long> { [("orders", 0)] = 90 },
            },
        };
        var (loop, store, _) = NewRig(runtime);

        // Act
        await loop.RunOnceAsync(CancellationToken.None);

        // Assert: лаг 10 жив и без участников группы.
        store.Current!.Clusters["events"].Groups!.Single().TotalLag.Should().Be(10);
    }

    [Fact]
    public async Task RunOnce_RuntimeTopicsFail_BrokersStillLive()
    {
        // Arrange: DescribeTopics падает — брокерская часть пробы жива,
        // runtime-поля просто отсутствуют.
        var runtime = new FakeRuntimeClient
        {
            TopicsError = new InvalidOperationException("boom"),
            Groups = [],
        };
        var (loop, store, _) = NewRig(runtime);

        // Act
        await loop.RunOnceAsync(CancellationToken.None);

        // Assert
        var state = store.Current!;
        state.Results.Should().ContainSingle().Which.Ok.Should().BeTrue();
        state.Clusters["events"].Brokers.Should().HaveCount(1);
        state.Clusters["events"].Topics.Should().BeNull();
        state.Clusters["events"].Groups.Should().BeNull();
    }

    [Fact]
    public async Task RunOnce_PasswordNeverLeaksIntoResults()
    {
        // Arrange
        var runtime = new FakeRuntimeClient { Topics = [], Groups = [] };
        var (loop, store, _) = NewRig(runtime);

        // Act
        await loop.RunOnceAsync(CancellationToken.None);

        // Assert: пароль не появляется ни в ProbeResult, ни в live-данных.
        var raw = System.Text.Json.JsonSerializer.Serialize(store.Current);
        raw.Should().NotContain("SecretPassword");
    }
}
