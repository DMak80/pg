using FluentAssertions;
using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using KafkaWorker.Etcd.Coordination;
using KafkaWorker.Etcd.Parsing;
using KafkaWorker.Provisioning.Kafka;
using KafkaWorker.Provisioning.Processes;

namespace KafkaWorker.UnitTests.Provisioning;

// ProvisioningProcess K0–K6 (arch/16 §5 A): полный прогон на fake'ах,
// снапшоты «до/после», неготовность кластера, идемпотентность re-run,
// TO_REMOVE посреди работы (R6).

public class ProvisioningProcessTests
{
    private const string Ep = "http://etcd:2379";

    private sealed record Rig(
        Fakes.FakeEtcd Etcd,
        Fakes.FakeKafkaDriver Driver,
        FakeKafkaAdminClient Admin,
        ClaimStore Claims,
        WorkJournal Journal,
        ProvisioningProcess Process,
        FakeConverger Converger,
        List<string> SnapshotPoints);

    // Заглушка converger'а (реализация — задача A11; здесь важен сам вызов).
    private sealed class FakeConverger : IClusterConfigConverger
    {
        public int Calls;

        public Task<Result> ApplyAsync(
            string cluster, string bootstrap, string user, string password, KafkaClusterConfig config, CancellationToken ct)
        {
            Calls++;
            return Task.FromResult(Result.Success());
        }
    }

    private static void SeedCluster(Fakes.FakeEtcd etcd, int brokers = 3)
    {
        etcd.Seed("/kafka/clusters/events/config",
            $$"""{"brokers":{{brokers}},"replication_factor":3,"min_insync_replicas":2,"default_partitions":12,"default_retention_ms":604800000,"created_unix":1756500000,"state":"NOT_INITIALIZED"}""");
        for (var k = 1; k <= brokers; k++)
        {
            etcd.Seed($"/kafka/clusters/events/brokers/broker{k}/state", "NOT_INITIALIZED");
            etcd.Seed($"/kafka/clusters/events/brokers/broker{k}/resources",
                """{"cpu":"2","mem":"4Gi","disk":"40Gi"}""");
        }
    }

    // Снапшот кластера из имитации etcd (как это сделает ReconcileLoop A12).
    private static async Task<KafkaClusterSnapshot> Snapshot(Fakes.FakeEtcd etcd)
    {
        var range = await etcd.RangeAsync(Ep, "/kafka/clusters/", CancellationToken.None);
        return KafkaSnapshotParser.Parse(range.Value).Value.Single(c => c.Cluster == "events");
    }

    private static async Task<Rig> NewRig(
        int brokers = 3,
        int brokerBootSec = 600,
        Action<Fakes.FakeEtcd, Fakes.FakeKafkaDriver, FakeKafkaAdminClient>? setup = null)
    {
        var etcd = new Fakes.FakeEtcd();
        SeedCluster(etcd, brokers);
        var claims = new ClaimStore([Ep], etcd, TimeProvider.System);
        await claims.TryClaimClusterAsync("events", CancellationToken.None);
        var journal = new WorkJournal(etcd, [Ep]);
        var driver = new Fakes.FakeKafkaDriver();
        var admin = new FakeKafkaAdminClient();
        var converger = new FakeConverger();
        var snapshotPoints = new List<string>();
        var process = new ProvisioningProcess(
            etcd, [Ep], driver, claims, journal,
            new AppSecretEnsurer(etcd, [Ep]),
            new FakeAdminFactory(admin),
            converger,
            new ProvisioningOptions(16000, 16999, brokerBootSec, 90, null, "apache/kafka:4.0.0"),
            snapshot: ct =>
            {
                snapshotPoints.Add($"n{snapshotPoints.Count}");
                return ValueTask.FromResult(Result.Success()).AsTask();
            });
        setup?.Invoke(etcd, driver, admin);
        return new Rig(etcd, driver, admin, claims, journal, process, converger, snapshotPoints);
    }

    private sealed class FakeAdminFactory(FakeKafkaAdminClient client) : IKafkaAdminClientFactory
    {
        public IKafkaAdminClient Create(string bootstrap, string user, string password) => client;
    }

    private void ReadyCluster(FakeKafkaAdminClient admin, int brokers)
        => admin.ClusterView = new KafkaClusterView(
            Enumerable.Range(1, brokers).Select(i => new KafkaBrokerView(i, $"broker{i}")).ToList(),
            ControllerId: 1);

    [Fact]
    public async Task Run_FullPass_ContainersEndpointsRunningConfigCommitted()
    {
        // Arrange: заявка 3-брокерного кластера; AdminClient сразу видит готовый кластер.
        var rig = await NewRig();
        ReadyCluster(rig.Admin, 3);

        // Act: полный прогон.
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: контейнеры созданы (env от NodeEnvBuilder), endpoints по
        // advertised-правилу (AdvertisedClientHost=null → docker-хост h1),
        // states PROVISIONING→RUNNING, config перезаписан без state, роли
        // зафиксированы (broker1..3 — controller, m=min(3,3)).
        result.IsSuccess.Should().BeTrue();
        rig.Driver.Ensured.Should().HaveCount(3);
        var broker1 = rig.Driver.Ensured.Single(s => s.NodeName == "broker1");
        broker1.Env["KAFKA_NODE_ID"].Should().Be("1");
        broker1.Env["KAFKA_PROCESS_ROLES"].Should().Be("broker,controller");
        broker1.Env["KAFKA_CONTROLLER_QUORUM_VOTERS"].Should().Be("1@broker1:9093,2@broker2:9093,3@broker3:9093");
        broker1.Env["KAFKA_ADVERTISED_LISTENERS"].Should().Contain("CLIENT://h1:16000");
        broker1.ClientHostPort.Should().Be(16000);

        rig.Etcd.Store["/kafka/clusters/events/endpoints"].Value
            .Should().Be("h1:16000,h1:16001,h1:16002");
        rig.Etcd.Store["/kafka/clusters/events/brokers/broker1/state"].Value.Should().Be("RUNNING");
        rig.Etcd.Store["/kafka/clusters/events/brokers/broker3/state"].Value.Should().Be("RUNNING");
        rig.Etcd.Store["/kafka/clusters/events/config"].Value
            .Should().NotContain("state")
            .And.Contain("\"brokers\":3");
        rig.Etcd.Store["/kafka/clusters/events/brokers/broker1/role"].Value.Should().Be("controller");
        rig.Etcd.Store["/kafka/clusters/events/app_password"].Value.Should().HaveLength(32);
        rig.Etcd.Store["/kafkaworker/portalloc/events"].Value
            .Should().Contain("\"client\":16000").And.Contain("\"broker3\"");
        // Секреты переданы в env (JAAS; пароли в кавычках — валидны для
        // Java-парсера при любом первом символе).
        broker1.Env["KAFKA_LISTENER_NAME_CLIENT_PLAIN_SASL_JAAS_CONFIG"]
            .Should().Contain($"user_app=\"{rig.Etcd.Store["/kafka/clusters/events/app_password"].Value}\"");
    }

    [Fact]
    public async Task Run_SnapshotDelegate_CalledBeforeAndAfter()
    {
        // Arrange: полный прогон с snapshot-делегатом (порт P12).
        var rig = await NewRig();
        ReadyCluster(rig.Admin, 3);

        // Act: прогон.
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: делегат вызван ровно дважды — «до» (после claim) и «после»
        // (перед journal done).
        result.IsSuccess.Should().BeTrue();
        rig.SnapshotPoints.Should().HaveCount(2);
    }

    [Fact]
    public async Task Run_ClusterNotReady_JournalErrorNoConfigRewrite()
    {
        // Arrange: DescribeCluster всегда падает; бюджет 0 с — мгновенный отказ.
        var rig = await NewRig(brokerBootSec: 0);
        rig.Admin.ClusterError = new ApplicationException("broker not up yet");

        // Act: прогон.
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: Failed; journal last_error записан; config не переписан
        // (state=NOT_INITIALIZED на месте); endpoints не записан.
        result.IsSuccess.Should().BeFalse();
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.LastError.Should().Contain("не собрался");
        rig.Etcd.Store["/kafka/clusters/events/config"].Value.Should().Contain("NOT_INITIALIZED");
        rig.Etcd.Store.Should().NotContainKey("/kafka/clusters/events/endpoints");
        // Но контейнеры уже созданы (K3 до ожидания готовности).
        rig.Driver.Ensured.Should().HaveCount(3);
    }

    [Fact]
    public async Task Run_RerunWithExistingContainers_IdempotentSkip()
    {
        // Arrange: первый прогон прошёл полностью.
        var rig = await NewRig();
        ReadyCluster(rig.Admin, 3);
        (await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None))
            .IsSuccess.Should().BeTrue();

        // Act: повторный прогон (контейнеры существуют, states RUNNING).
        var second = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: успех; новые контейнеры не создаются (сверка по имени),
        // config стабилен, endpoints на месте.
        second.IsSuccess.Should().BeTrue();
        rig.Driver.Ensured.Should().HaveCount(3);
        rig.Etcd.Store["/kafka/clusters/events/config"].Value.Should().NotContain("state");
        rig.Etcd.Store["/kafka/clusters/events/endpoints"].Value
            .Should().Be("h1:16000,h1:16001,h1:16002");
    }

    [Fact]
    public async Task Run_ToRemoveMidProvisioning_AbortsBeforeConfigPhase()
    {
        // Arrange: панель переводит кластер в TO_REMOVE, как только первый
        // брокер ушёл в PROVISIONING (R6: перечитывание config перед фазами).
        var rig = await NewRig();
        ReadyCluster(rig.Admin, 3);
        rig.Etcd.OnPut = key =>
        {
            if (key == "/kafka/clusters/events/brokers/broker1/state")
                rig.Etcd.Seed("/kafka/clusters/events/config", """{"brokers":3,"replication_factor":3,"min_insync_replicas":2,"default_partitions":12,"default_retention_ms":604800000,"created_unix":1756500000,"state":"TO_REMOVE"}""");
        };

        // Act: прогон.
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: процесс успешно прекратился до config-фазы: config не
        // переписан (TO_REMOVE на месте), endpoints не записан; демонтаж —
        // deprovisioning следующим тиком.
        result.IsSuccess.Should().BeTrue();
        rig.Etcd.Store["/kafka/clusters/events/config"].Value.Should().Contain("TO_REMOVE");
        rig.Etcd.Store.Should().NotContainKey("/kafka/clusters/events/endpoints");
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.Phase.Should().Be("aborted");
    }

    [Fact]
    public async Task Run_NotClaimed_RefusesMutations()
    {
        // Arrange: кластер заявлен, но клэйм не захвачен этим инстансом.
        var etcd = new Fakes.FakeEtcd();
        SeedCluster(etcd);
        var claims = new ClaimStore([Ep], etcd, TimeProvider.System);
        var process = new ProvisioningProcess(
            etcd, [Ep], new Fakes.FakeKafkaDriver(), claims, new WorkJournal(etcd, [Ep]),
            new AppSecretEnsurer(etcd, [Ep]), new FakeAdminFactory(new FakeKafkaAdminClient()),
            new FakeConverger(), ProvisioningOptions.Default, snapshot: null);

        // Act: прогон без клэйма.
        var result = await process.RunAsync(await Snapshot(etcd), CancellationToken.None);

        // Assert: отказ до любых мутаций.
        result.IsSuccess.Should().BeFalse();
        result.Error!.Message.Should().Contain("клэйм не наш");
        etcd.Store.Should().NotContainKey("/kafka/clusters/events/endpoints");
    }

    [Fact]
    public async Task Run_FourthBroker_IsBrokerOnlyRole()
    {
        // Arrange: 4-брокерный кластер (m=min(3,4): broker4 — broker-only).
        var rig = await NewRig(brokers: 4);
        ReadyCluster(rig.Admin, 4);

        // Act: полный прогон.
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: broker4 — роль broker; в кворум не входит; контейнер создан.
        result.IsSuccess.Should().BeTrue();
        rig.Etcd.Store["/kafka/clusters/events/brokers/broker4/role"].Value.Should().Be("broker");
        var broker4 = rig.Driver.Ensured.Single(s => s.NodeName == "broker4");
        broker4.Env["KAFKA_PROCESS_ROLES"].Should().Be("broker");
        broker4.Env["KAFKA_CONTROLLER_QUORUM_VOTERS"].Should().NotContain("4@");
        broker4.Env["KAFKA_LISTENERS"].Should().NotContain("CONTROLLER");
    }
}
