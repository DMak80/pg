using System.Diagnostics.Metrics;
using FluentAssertions;
using KafkaWorker.App;
using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using KafkaWorker.Provisioning.Kafka;
using Microsoft.Extensions.Logging.Abstractions;

namespace KafkaWorker.UnitTests.App;

// Юнит-тесты KafkaMetricsCollector (t04, arch/18 §4) на фейках seam: лаг =
// watermark − committed (clamped), USR по ISR, устойчивость к ошибке кластера,
// самонаблюдение LastSuccess, пропуск неподнятых и не-Active кластеров.
public sealed class KafkaMetricsCollectorTests
{
    // Настраиваемый фейк IKafkaAdminClient: ответы по группе, ошибки по флагу.
    private sealed class FakeAdmin : IKafkaAdminClient
    {
        public List<KafkaGroupView> Groups = [];
        public Dictionary<string, List<KafkaTopicPartitionOffset>> Committed = [];
        public List<KafkaTopicView> Topics = [];
        public Exception? FailAll;

        public Task<Result<KafkaClusterView>> DescribeClusterAsync(CancellationToken ct)
            => Fail<KafkaClusterView>();

        public Task<Result<IReadOnlyList<KafkaTopicView>>> DescribeTopicsAsync(bool includeInternal, CancellationToken ct)
        {
            if (FailAll is not null)
                return Task.FromResult(Result<IReadOnlyList<KafkaTopicView>>.Failed(FailAll));
            return Task.FromResult(Result<IReadOnlyList<KafkaTopicView>>.Success(Topics));
        }

        public Task<Result<IReadOnlyDictionary<string, string>>> DescribeBrokerConfigsAsync(int brokerId, CancellationToken ct)
            => FailDict();

        public Task<Result> AlterBrokerConfigsAsync(int brokerId, IReadOnlyDictionary<string, string> configs, CancellationToken ct)
            => Task.FromResult(Result.Failed(new ApplicationException("n/a")));

        public Task<Result<IReadOnlyDictionary<string, string>>> DescribeTopicConfigsAsync(string topic, CancellationToken ct)
            => FailDict();

        public Task<Result> AlterTopicConfigsAsync(string topic, IReadOnlyDictionary<string, string> configs, CancellationToken ct)
            => Task.FromResult(Result.Failed(new ApplicationException("n/a")));

        public Task<Result> CreatePartitionsAsync(string topic, int totalPartitions, CancellationToken ct)
            => Task.FromResult(Result.Failed(new ApplicationException("n/a")));

        public Task<Result<TopicCreateOutcome>> CreateTopicAsync(string topic, int partitions, short rf,
            IReadOnlyDictionary<string, string>? configs, CancellationToken ct)
            => Task.FromResult(Result<TopicCreateOutcome>.Failed(new ApplicationException("n/a")));

        public Task<Result<TopicDeleteOutcome>> DeleteTopicAsync(string topic, CancellationToken ct)
            => Task.FromResult(Result<TopicDeleteOutcome>.Failed(new ApplicationException("n/a")));

        public Task<Result<IReadOnlyList<KafkaGroupView>>> ListGroupsAsync(CancellationToken ct)
        {
            if (FailAll is not null)
                return Task.FromResult(Result<IReadOnlyList<KafkaGroupView>>.Failed(FailAll));
            return Task.FromResult(Result<IReadOnlyList<KafkaGroupView>>.Success(Groups));
        }

        public Task<Result<IReadOnlyList<KafkaTopicPartitionOffset>>> ListConsumerGroupOffsetsAsync(
            string group, CancellationToken ct)
        {
            if (FailAll is not null)
                return Task.FromResult(Result<IReadOnlyList<KafkaTopicPartitionOffset>>.Failed(FailAll));
            return Task.FromResult(Result<IReadOnlyList<KafkaTopicPartitionOffset>>.Success(
                Committed.GetValueOrDefault(group, [])));
        }

        public Task<Result<IReadOnlyList<KafkaTopicPartitionOffset>>> ListOffsetsAsync(
            IReadOnlyList<KafkaTopicPartition> partitions, CancellationToken ct)
        {
            if (FailAll is not null)
                return Task.FromResult(Result<IReadOnlyList<KafkaTopicPartitionOffset>>.Failed(FailAll));
            // Фейковые watermarks: фиксированные значения для расчёта лага.
            var watermarks = new Dictionary<(string, int), long>
            {
                [("t1", 0)] = 20,
                [("t1", 1)] = 10,
            };
            return Task.FromResult(Result<IReadOnlyList<KafkaTopicPartitionOffset>>.Success(
                partitions.Select(p => new KafkaTopicPartitionOffset(p.Topic, p.Partition,
                    watermarks.GetValueOrDefault((p.Topic, p.Partition), 0))).ToList()));
        }

        // ACL (t03): коллектору не нужны — заглушки-успехи без действия.
        public Task<Result<IReadOnlyList<KafkaAclBinding>>> DescribeAclsAsync(CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<KafkaAclBinding>>.Success([]));

        public Task<Result> CreateAclsAsync(IReadOnlyList<KafkaAclBinding> acls, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result> DeleteAclsAsync(IReadOnlyList<KafkaAclBinding> acls, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        private static Task<Result<T>> Fail<T>() where T : class
            => Task.FromResult(Result<T>.Failed(new ApplicationException("n/a")));

        private static Task<Result<IReadOnlyDictionary<string, string>>> FailDict()
            => Task.FromResult(Result<IReadOnlyDictionary<string, string>>.Failed(
                new ApplicationException("n/a")));
    }

    // Фейк фабрики: Next — admin по умолчанию, ByBootstrap — точечная подмена.
    private sealed class FakeFactory : IKafkaAdminClientFactory
    {
        public FakeAdmin? Next;
        public Dictionary<string, FakeAdmin> ByBootstrap = [];
        public List<FakeAdmin> Created = [];
        public List<(string Bootstrap, string User, string Password, string? CaPem)> CreateArgs = [];

        public IKafkaAdminClient Create(string bootstrap, string user, string password, string? caPem)
        {
            CreateArgs.Add((bootstrap, user, password, caPem));
            var admin = ByBootstrap.GetValueOrDefault(bootstrap) ?? Next ?? new FakeAdmin();
            Created.Add(admin);
            return admin;
        }
    }

    private static KafkaClusterSnapshot Snapshot(
        string cluster, string? state = null, string? endpoints = "kafka:9092",
        string? user = "app", string? password = "secret")
        => new(
            cluster,
            new KafkaClusterConfig(3, 3, 2, 6, 604800000, null, state),
            [new KafkaBrokerDecl("k1", "running", "controller", null)],
            [],
            [], 0,
            Endpoints: endpoints, AppUser: user, AppPassword: password);

    [Fact]
    public async Task Collect_LagComputed_WatermarkMinusCommitted()
    {
        // Arrange: кластер c1: группа g1 committed {t1p0=5, t1p1=10}; watermarks {t1p0=20, t1p1=10}
        var admin = new FakeAdmin
        {
            Groups = [new KafkaGroupView("g1", "Stable")],
            Committed =
            {
                ["g1"] =
                [
                    new KafkaTopicPartitionOffset("t1", 0, 5),
                    new KafkaTopicPartitionOffset("t1", 1, 10),
                ],
            },
        };
        var factory = new FakeFactory { Next = admin };
        var clock = TestTime;
        var state = new KafkaMetricsState(new Meter("TestKafkaWorker"));
        var collector = new KafkaMetricsCollector(30,
            ct => Task.FromResult(Result<IReadOnlyList<KafkaClusterSnapshot>>.Success([Snapshot("c1")])),
            factory, state, clock, NullLogger<KafkaMetricsCollector>.Instance);

        // Act
        await collector.CollectOnceAsync(TestContext.Current.CancellationToken);

        // Assert: лаг по (c1, g1, t1) = (20-5) + max(0, 10-10) = 15
        state.DebugSnapshot().Lag[("c1", "g1", "t1")].Should().Be(15);
    }

    [Fact]
    public async Task Collect_UnderReplicated_IssrSubsetOfReplicas()
    {
        // Arrange: топик t1 — 2 партиции: p0 ISR(2)<replicas(3) → USR; p1 ISR=3 → нет
        var admin = new FakeAdmin
        {
            Topics =
            [
                new KafkaTopicView("t1", 2,
                    [[1, 2, 3], [1, 2, 3]],
                    [[1, 2], [1, 2, 3]]),
            ],
        };
        var factory = new FakeFactory { Next = admin };
        var state = new KafkaMetricsState(new Meter("TestKafkaWorker"));
        var collector = new KafkaMetricsCollector(30,
            ct => Task.FromResult(Result<IReadOnlyList<KafkaClusterSnapshot>>.Success([Snapshot("c1")])),
            factory, state, TestTime, NullLogger<KafkaMetricsCollector>.Instance);

        // Act
        await collector.CollectOnceAsync(TestContext.Current.CancellationToken);

        // Assert
        state.DebugSnapshot().Usr[("c1", "t1")].Should().Be(1);
    }

    [Fact]
    public async Task Collect_ClusterFails_LagsNotUpdated_TickSurvives()
    {
        // Arrange: два кластера; второй падает по всем вызовам
        var okAdmin = new FakeAdmin
        {
            Groups = [new KafkaGroupView("g1", "Stable")],
            Committed = { ["g1"] = [new KafkaTopicPartitionOffset("t1", 0, 5)] },
        };
        var factory = new FakeFactory
        {
            Next = okAdmin,
            ByBootstrap = { ["c2:9092"] = new FakeAdmin { FailAll = new ApplicationException("down") } },
        };
        var state = new KafkaMetricsState(new Meter("TestKafkaWorker"));
        var clock = new FakeClock(DateTimeOffset.UnixEpoch.AddHours(5));
        var collector = new KafkaMetricsCollector(30,
            ct => Task.FromResult(Result<IReadOnlyList<KafkaClusterSnapshot>>.Success(
                [Snapshot("c1"), Snapshot("c2", endpoints: "c2:9092")])),
            factory, state, clock, NullLogger<KafkaMetricsCollector>.Instance);

        // Act
        var act = async () => await collector.CollectOnceAsync(TestContext.Current.CancellationToken);

        // Assert: тик не бросает; лаги c1 собраны; LastSuccess НЕ обновлён
        // (полный успех всех кластеров — консервативно, алерт §3.7)
        await act.Should().NotThrowAsync();
        state.DebugSnapshot().Lag.Should().ContainKey(("c1", "g1", "t1"));
        state.DebugSnapshot().LastSuccess.Should().BeNull();
    }

    [Fact]
    public async Task Collect_AllOk_UpdatesLastSuccess()
    {
        // Arrange: единственный здоровый кластер
        var admin = new FakeAdmin();
        var factory = new FakeFactory { Next = admin };
        var state = new KafkaMetricsState(new Meter("TestKafkaWorker"));
        var clock = new FakeClock(DateTimeOffset.UnixEpoch.AddHours(7));
        var collector = new KafkaMetricsCollector(30,
            ct => Task.FromResult(Result<IReadOnlyList<KafkaClusterSnapshot>>.Success([Snapshot("c1")])),
            factory, state, clock, NullLogger<KafkaMetricsCollector>.Instance);

        // Act
        await collector.CollectOnceAsync(TestContext.Current.CancellationToken);

        // Assert: LastSuccess = fake-время тика (TimeProvider инжектится)
        state.DebugSnapshot().LastSuccess.Should().Be(DateTimeOffset.UnixEpoch.AddHours(7));
    }

    [Fact]
    public async Task Collect_SkipsClustersWithoutBootstrap()
    {
        // Arrange: снапшот с Endpoints/AppUser/AppPassword == null (кластер не поднят)
        var factory = new FakeFactory();
        var state = new KafkaMetricsState(new Meter("TestKafkaWorker"));
        var collector = new KafkaMetricsCollector(30,
            ct => Task.FromResult(Result<IReadOnlyList<KafkaClusterSnapshot>>.Success(
                [Snapshot("c1", endpoints: null, user: null, password: null)])),
            factory, state, TestTime, NullLogger<KafkaMetricsCollector>.Instance);

        // Act
        await collector.CollectOnceAsync(TestContext.Current.CancellationToken);

        // Assert: к фабрике не обращались, ошибка не фиксируется (проба невозможна —
        // паттерн NodeSupervisor); LastSuccess обновляется (сборка «успешна»: нечего собирать)
        factory.Created.Should().BeEmpty();
        state.DebugSnapshot().LastSuccess.Should().NotBeNull();
    }

    [Fact]
    public async Task Collect_OnlyActiveClusters_StateNullMeansActive()
    {
        // Arrange (ревью Ф4-6): Active (Config.State == null) и невыполненная
        // заявка (Config.State == "PROVISIONING"; KafkaDomain.cs:11-18, arch/15 §2.1)
        var factory = new FakeFactory { Next = new FakeAdmin() };
        var state = new KafkaMetricsState(new Meter("TestKafkaWorker"));
        var collector = new KafkaMetricsCollector(30,
            ct => Task.FromResult(Result<IReadOnlyList<KafkaClusterSnapshot>>.Success(
                [Snapshot("active"), Snapshot("pending", state: "PROVISIONING")])),
            factory, state, TestTime, NullLogger<KafkaMetricsCollector>.Instance);

        // Act
        await collector.CollectOnceAsync(TestContext.Current.CancellationToken);

        // Assert: AdminClient создавался только для Active; bootstrap = Endpoints
        factory.CreateArgs.Should().ContainSingle(a => a.Bootstrap == "kafka:9092");
        factory.Created.Should().ContainSingle();
    }

    private static readonly TimeProvider TestTime = new FakeClock(DateTimeOffset.UnixEpoch);

    // FakeTimeProvider (новый пакет НЕ тащим, CPM чистый).
    private sealed class FakeClock(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }
}
