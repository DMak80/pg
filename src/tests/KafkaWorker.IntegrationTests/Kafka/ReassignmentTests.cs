using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using FluentAssertions;
using KafkaWorker.Core;
using KafkaWorker.Etcd.Coordination;
using KafkaWorker.Provisioning.Kafka;
using KafkaWorker.Provisioning.Processes;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace KafkaWorker.IntegrationTests.Kafka;

// Reassignment против реального Kafka (t02; Docker required, spec §11.2/§11.3/
// §11.5/§11.7): drain непустого broker-only со снижением RF 4→3 (данные целы,
// internal-топики уехали), идемпотентность повторной подачи того же assignment.

[Collection(KafkaCollection.Name)]
public class ReassignmentTests(KafkaClusterFixture fixture)
{
    // Риг процессов кластера (как ProvisioningTests): процессы напрямую,
    // тики цикла — тестом.
    private sealed record Rig(
        ClaimStore Claims,
        WorkJournal Journal,
        ProvisioningProcess Provision,
        DeprovisioningProcess Deprovision,
        PartitionReassignerProcess Reassigner,
        RemoveBrokerProcess Remove,
        AddBrokerProcess Add,
        TopicSyncProcess Sync);

    private async Task<Rig> NewRigAsync(string cluster, int brokers)
    {
        var ct = TestContext.Current.CancellationToken;
        await fixture.SeedClusterAsync(cluster, brokers);
        var claims = new ClaimStore([fixture.Endpoint], fixture.Gateway, TimeProvider.System);
        await claims.TryClaimClusterAsync(cluster, ct);
        var journal = new WorkJournal(fixture.Gateway, [fixture.Endpoint]);
        return new Rig(
            claims,
            journal,
            new ProvisioningProcess(
                fixture.Gateway, [fixture.Endpoint], fixture.Driver, claims, journal,
                new PortAllocLock([fixture.Endpoint], fixture.Gateway, TimeProvider.System, claims.InstanceId),
                new PortAllocIndex(fixture.Gateway, [fixture.Endpoint], NullLogger<PortAllocIndex>.Instance),
                new ClusterSecretEnsurer(fixture.Gateway, [fixture.Endpoint]),
                fixture.AdminFactory, new ClusterConfigConverger(fixture.AdminFactory),
                fixture.Options, fixture.Certificates,
            snapshot: null),
            new DeprovisioningProcess(
                fixture.Gateway, [fixture.Endpoint], fixture.Driver, claims, journal, snapshot: null),
            new PartitionReassignerProcess(
                fixture.Gateway, [fixture.Endpoint], fixture.Driver, claims, journal,
                fixture.AdminFactory, new ReassignOptions(IntervalSec: 0, BatchPartitions: 10, ExecSec: 180, RetrySubmitSec: 120),
                TimeProvider.System),
            new RemoveBrokerProcess(
                fixture.Gateway, [fixture.Endpoint], fixture.Driver, claims, journal,
                fixture.AdminFactory, fixture.Options),
            new AddBrokerProcess(
                fixture.Gateway, [fixture.Endpoint], fixture.Driver, claims, journal,
                new PortAllocLock([fixture.Endpoint], fixture.Gateway, TimeProvider.System, claims.InstanceId),
                new PortAllocIndex(fixture.Gateway, [fixture.Endpoint], NullLogger<PortAllocIndex>.Instance),
                fixture.AdminFactory, fixture.Options, fixture.Certificates),
            new TopicSyncProcess(
                fixture.Gateway, [fixture.Endpoint], claims, journal,
                fixture.AdminFactory, TimeProvider.System, intervalSec: 0));
    }

    // Дискавери-креды (endpoints + app_* + ca_pem) из etcd — bootstrap хост-процесса.
    private sealed record Creds(string Bootstrap, string User, string Password, string CaPem);

    private async Task<Creds> CredsAsync(string cluster)
    {
        var endpoints = await fixture.GetAsync($"/kafka/clusters/{cluster}/endpoints");
        var user = await fixture.GetAsync($"/kafka/clusters/{cluster}/app_user");
        var password = await fixture.GetAsync($"/kafka/clusters/{cluster}/app_password");
        var caPem = await fixture.GetAsync($"/kafka/clusters/{cluster}/ca_pem");
        return new Creds(endpoints!.Replace("host.docker.internal", "localhost", StringComparison.Ordinal),
            user!, password!, caPem!);
    }

    // Provisioning-цикл до готовности (config без state).
    private async Task UpAsync(Rig rig, string cluster, int budgetSec)
    {
        var ct = TestContext.Current.CancellationToken;
        var deadline = DateTimeOffset.UtcNow.AddSeconds(budgetSec);
        while (DateTimeOffset.UtcNow < deadline)
        {
            var snap = await fixture.SnapshotAsync(cluster);
            if (snap!.Config.State is null)
                return;

            var tick = await rig.Provision.RunAsync(snap, ct);
            tick.IsSuccess.Should().BeTrue(
                $"тик provisioning не должен падать (waiting-brokers — успех): {tick.Error?.Message}");
            await Task.Delay(3000, ct);
        }

        throw new TimeoutException($"кластер {cluster} не поднялся за {budgetSec} с");
    }

    // Финал теста: демонтаж кластера (контейнеры kfw-* не оставляем коллегам).
    private async Task TeardownAsync(Rig rig, string cluster)
    {
        var ct = TestContext.Current.CancellationToken;
        var raw = await fixture.GetAsync($"/kafka/clusters/{cluster}/config");
        if (raw is not null)
            await fixture.Gateway.PutAsync(fixture.Endpoint, $"/kafka/clusters/{cluster}/config",
                raw.Replace("}", ",\"state\":\"TO_REMOVE\"}", StringComparison.Ordinal), lease: null, ct);
        var dying = await fixture.SnapshotAsync(cluster);
        if (dying is not null)
            await rig.Deprovision.RunAsync(cluster, dying.Brokers.Select(b => b.Name).ToList(), ct);
        await rig.Claims.DisposeAsync();
    }

    // describe-all через адаптер воркера (включая __-топики, t02).
    private async Task<IReadOnlyList<KafkaTopicView>> DescribeAllAsync(Creds creds)
    {
        await using var admin = fixture.AdminFactory.Create(
            creds.Bootstrap, creds.User, creds.Password, creds.CaPem);
        var described = await admin.DescribeTopicsAsync(
            includeInternal: true, TestContext.Current.CancellationToken);
        described.IsSuccess.Should().BeTrue($"describe-all должен работать: {described.Error?.Message}");
        return described.Value;
    }

    // Produce коротких сообщений (ключи → разные партиции). Канон клиента
    // t03: SASL_SSL + доверие per-cluster CA (ca_pem из etcd, arch/15 §5).
    private static async Task ProduceAsync(Creds creds, string topic, int count)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = creds.Bootstrap,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.Plain,
            SaslUsername = creds.User,
            SaslPassword = creds.Password,
        };
        config.Set("ssl.ca.pem", creds.CaPem);
        using var producer = new ProducerBuilder<string, string>(config).Build();
        for (var i = 0; i < count; i++)
        {
            await producer.ProduceAsync(topic, new Message<string, string>
            {
                Key = $"k{i % 6}",
                Value = $"msg-{i}",
            });
        }
    }

    // Consume всех сообщений топика с earliest (свежая группа; коммит оффсетов
    // создаёт __consumer_offsets при первом прогоне — до drain). SASL_SSL + ca_pem.
    private static async Task<int> ConsumeAsync(Creds creds, string topic, int expected, int budgetSec)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = creds.Bootstrap,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.Plain,
            SaslUsername = creds.User,
            SaslPassword = creds.Password,
            GroupId = $"it-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true,
        };
        config.Set("ssl.ca.pem", creds.CaPem);
        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(topic);

        var deadline = DateTimeOffset.UtcNow.AddSeconds(budgetSec);
        var read = 0;
        while (read < expected && DateTimeOffset.UtcNow < deadline)
        {
            var result = consumer.Consume(TimeSpan.FromSeconds(3));
            if (result is not null)
                read++;
        }

        consumer.Close();
        return await Task.FromResult(read);
    }

    [Fact]
    public async Task Drain_RemovesNonEmptyBroker_Снижает_RF()
    {
        var cluster = fixture.Cluster("re1");
        const string topic = "reorders";
        var ct = TestContext.Current.CancellationToken;
        var rig = await NewRigAsync(cluster, brokers: 4);
        try
        {
            // Arrange: 4-брокерный кластер (3 controller + broker4 broker-only),
            // юзер-топик RF=4/6 партиций с данными; группа создала __consumer_offsets.
            // Бюджет 200 с — потолок с запасом над воркерным BrokerBootSec=100
            // (AGENTS.md: тестовый BrokerBootSec <= 100): зависание фейлится быстро.
            await UpAsync(rig, cluster, budgetSec: 200);
            var creds = await CredsAsync(cluster);
            var builder = await fixture.DiscoveryAdminBuilderAsync(cluster, "admin");
            using (var admin = builder.Build())
            {
                await admin.CreateTopicsAsync([new TopicSpecification
                {
                    Name = topic,
                    NumPartitions = 6,
                    ReplicationFactor = 4,
                }], new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(30) });
            }

            await ProduceAsync(creds, topic, 12);
            (await ConsumeAsync(creds, topic, 12, budgetSec: 180)).Should().Be(12,
                "до drain все сообщения читаются");

            // Act: broker4 TO_REMOVE → тики reassigner+remove+topicsync до демонтажа.
            // Транзиентные отказы тика (exec CLI, коннекты) — не провал: реальный
            // ReconcileLoop ретраит следующим тиком; здесь проверяется сходимость.
            await fixture.Gateway.PutAsync(fixture.Endpoint, $"/kafka/clusters/{cluster}/brokers/broker4/state",
                "TO_REMOVE", lease: null, ct);
            Exception? lastTickError = null;
            var deadline = DateTimeOffset.UtcNow.AddSeconds(300);
            while (DateTimeOffset.UtcNow < deadline)
            {
                var snap = await fixture.SnapshotAsync(cluster);
                var reassignTick = await rig.Reassigner.RunAsync(snap!, ct);
                var removeTick = reassignTick.IsSuccess
                    ? await rig.Remove.RunAsync(snap!, ct)
                    : reassignTick;
                var syncTick = removeTick.IsSuccess
                    ? await rig.Sync.RunAsync(snap!, ct)
                    : removeTick;
                if (!syncTick.IsSuccess)
                    lastTickError = syncTick.Error!;

                // Выход — по демонтажу И очистке прогресс-ключа: демонтаж может
                // выиграть гонку у done-тика reassigner'а (fresh describe G уже
                // не видит реплик) — прогресс-ключ добьёт следующий тик (cancelled).
                if (await fixture.GetAsync($"/kafka/clusters/{cluster}/brokers/broker4/state") is null
                    && await fixture.GetAsync($"/kafkaworker/reassignments/{cluster}") is null)
                    break;

                await Task.Delay(3000, ct);
            }

            (await fixture.GetAsync($"/kafka/clusters/{cluster}/brokers/broker4/state")).Should().BeNull(
                $"broker4 должен демонтироваться за 300 c (последняя ошибка тика: {lastTickError?.Message})");

            // Автосинк подтягивает replication_factor=3 (бюджет 120 с — с запасом
            // под параллельный dev-стенд).
            var syncDeadline = DateTimeOffset.UtcNow.AddSeconds(120);
            while (DateTimeOffset.UtcNow < syncDeadline)
            {
                var snap = await fixture.SnapshotAsync(cluster);
                await rig.Sync.RunAsync(snap!, ct);
                var raw = await fixture.GetAsync($"/kafka/clusters/{cluster}/topics/{topic}");
                if (raw is not null && raw.Contains("\"replication_factor\":3", StringComparison.Ordinal))
                    break;

                await Task.Delay(3000, ct);
            }

            // Assert: демонтаж завершён — ключей/контейнера нет, endpoints сократился.
            (await fixture.GetAsync($"/kafka/clusters/{cluster}/brokers/broker4/state")).Should().BeNull(
                "broker4 демонтирован после drain");
            var objects = await fixture.Driver.ListNodeObjectsAsync(cluster, ct);
            objects.Value.Should().NotContain($"kfw-{cluster}-broker4", "контейнер broker4 удалён");
            var endpoints = await fixture.GetAsync($"/kafka/clusters/{cluster}/endpoints");
            endpoints!.Split(',').Should().HaveCount(3, "endpoints без адреса broker4");
            (await fixture.GetAsync($"/kafkaworker/reassignments/{cluster}")).Should().BeNull(
                "прогресс-ключ удалён по завершении drain");

            // Describe-all (включая __): nodeId=4 нигде; RF юзер-топика == 3.
            var described = await DescribeAllAsync(creds);
            described.SelectMany(t => t.ReplicasPerPartition).SelectMany(r => r)
                .Should().NotContain(4, "ни одна партиция (включая __-топики) не держит реплику на 4");
            var orders = described.Single(t => t.Topic == topic);
            orders.ReplicasPerPartition.Should().HaveCount(6);
            orders.ReplicasPerPartition.Should().OnlyContain(p => p.Count == 3, "RF снижен 4→3");

            var registry = await fixture.GetAsync($"/kafka/clusters/{cluster}/topics/{topic}");
            registry.Should().Contain("\"replication_factor\":3", "автосинк обновил факт RF");

            // Данные целы: все сообщения читаются (приёмка §11.2).
            (await ConsumeAsync(creds, topic, 12, budgetSec: 120)).Should().Be(12,
                "после drain все сообщения читаются");
        }
        finally
        {
            await TeardownAsync(rig, cluster);
        }
    }

    [Fact]
    public async Task Reassign_Повторная_Подача_Безопасна()
    {
        var cluster = fixture.Cluster("re3");
        const string topic = "rere";
        var ct = TestContext.Current.CancellationToken;
        var rig = await NewRigAsync(cluster, brokers: 3);
        try
        {
            // Arrange: стабильный кластер с топиком RF=3 (drain завершён/не нужен —
            // проверяем идемпотентность повторной подачи НА СТАБИЛЬНОМ факте).
            await UpAsync(rig, cluster, budgetSec: 200);
            var creds = await CredsAsync(cluster);
            var builder = await fixture.DiscoveryAdminBuilderAsync(cluster, "admin");
            using (var admin = builder.Build())
            {
                await admin.CreateTopicsAsync([new TopicSpecification
                {
                    Name = topic,
                    NumPartitions = 3,
                    ReplicationFactor = 3,
                }], new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(30) });
            }

            await ProduceAsync(creds, topic, 6);

            // Act 1: повторный RunAsync на стабильном факте — no-op без подач.
            var snap = await fixture.SnapshotAsync(cluster);
            (await rig.Reassigner.RunAsync(snap!, ct)).IsSuccess.Should().BeTrue(
                "повторный тик reassigner на сошедшемся факте — успех");

            // Act 2: повторная CLI-подача того же assignment (факт == план) —
            // Kafka обязана принять идемпотентно (KIP-455).
            var before = await DescribeAllAsync(creds);
            var moves = before
                .SelectMany(t => t.ReplicasPerPartition.Select((_, p) => new ReassignMove(t.Topic, p, [.. t.ReplicasPerPartition[p]])))
                .ToList();
            var bootstrap = ReassignCli.Bootstrap(["broker1", "broker2", "broker3"]);
            // CLI-канон t03: SASL_SSL command-config с admin-кредами и per-cluster
            // CA из etcd (литералы "adminpw"/"CAPEM" — легаси PLAINTEXT-эпохи).
            var adminUser = await fixture.GetAsync($"/kafka/clusters/{cluster}/admin_user");
            var adminPassword = await fixture.GetAsync($"/kafka/clusters/{cluster}/admin_password");
            var cmd = ReassignCli.BuildExecCommand(moves, bootstrap, adminUser!, adminPassword!, creds.CaPem);
            for (var i = 0; i < 2; i++)
            {
                var exec = await fixture.Driver.ExecNodeAsync(cluster, "broker1", cmd, ct);
                exec.IsSuccess.Should().BeTrue($"повторная подача №{i + 1} не падает: {exec.Error?.Message}");
            }

            // Assert: describe до/после идентичен по assignment всех партиций.
            var after = await DescribeAllAsync(creds);
            var assignment = (IReadOnlyList<KafkaTopicView> list) => list
                .OrderBy(t => t.Topic, StringComparer.Ordinal)
                .SelectMany(t => t.ReplicasPerPartition.Select((r, p) => $"{t.Topic}:{p}=[{string.Join(",", r)}]"))
                .ToList();
            assignment(after).Should().BeEquivalentTo(assignment(before),
                "повторная подача того же assignment не меняет размещение");
        }
        finally
        {
            await TeardownAsync(rig, cluster);
        }
    }

    [Fact]
    public async Task Balance_Восстанавливает_RF_После_Повторного_Add()
    {
        var cluster = fixture.Cluster("re2");
        const string topic = "reorders2";
        var ct = TestContext.Current.CancellationToken;
        var rig = await NewRigAsync(cluster, brokers: 4);
        try
        {
            // Arrange: 4-брокерный кластер, юзер-топик RF=4/6 партиций с данными.
            await UpAsync(rig, cluster, budgetSec: 200);
            var creds = await CredsAsync(cluster);
            var builder = await fixture.DiscoveryAdminBuilderAsync(cluster, "admin");
            using (var admin = builder.Build())
            {
                await admin.CreateTopicsAsync([new TopicSpecification
                {
                    Name = topic,
                    NumPartitions = 6,
                    ReplicationFactor = 4,
                }], new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(30) });
            }

            await ProduceAsync(creds, topic, 6);

            // Act 1: drain broker4 со снижением RF 4→3 и демонтажем (как T7.1).
            await fixture.Gateway.PutAsync(fixture.Endpoint, $"/kafka/clusters/{cluster}/brokers/broker4/state",
                "TO_REMOVE", lease: null, ct);
            var drainDeadline = DateTimeOffset.UtcNow.AddSeconds(300);
            while (DateTimeOffset.UtcNow < drainDeadline)
            {
                var snap = await fixture.SnapshotAsync(cluster);
                await rig.Reassigner.RunAsync(snap!, ct);
                await rig.Remove.RunAsync(snap!, ct);
                await rig.Sync.RunAsync(snap!, ct);
                if (await fixture.GetAsync($"/kafka/clusters/{cluster}/brokers/broker4/state") is null
                    && await fixture.GetAsync($"/kafkaworker/reassignments/{cluster}") is null)
                    break;

                await Task.Delay(3000, ct);
            }

            var afterDrain = await DescribeAllAsync(creds);
            var ordersDrained = afterDrain.Single(t => t.Topic == topic);
            ordersDrained.ReplicasPerPartition.Should().OnlyContain(p => p.Count == 3, "RF снижен 4→3");
            ordersDrained.ReplicasPerPartition.Should().OnlyContain(p => !p.Contains(4));
            (await fixture.GetAsync($"/kafka/clusters/{cluster}/brokers/broker4/state")).Should().BeNull(
                "broker4 демонтирован после drain");

            // Act 2: повторный add broker4 (без него восстановление RF=4
            // недостижимо — targets=3 и заявка снялась бы без движения).
            await fixture.Gateway.PutAsync(fixture.Endpoint, $"/kafka/clusters/{cluster}/brokers/broker4/state",
                "NOT_INITIALIZED", lease: null, ct);
            await fixture.Gateway.PutAsync(fixture.Endpoint, $"/kafka/clusters/{cluster}/brokers/broker4/resources",
                """{"cpu":"1","mem":"1Gi","disk":"10Gi"}""", lease: null, ct);
            // Бюджет add-фазы 300 с — с запасом под параллельный dev-стенд.
            var addDeadline = DateTimeOffset.UtcNow.AddSeconds(300);
            while (DateTimeOffset.UtcNow < addDeadline)
            {
                var snap = await fixture.SnapshotAsync(cluster);
                (await rig.Add.RunAsync(snap!, ct)).IsSuccess.Should().BeTrue("тик add не падает");
                if (await fixture.GetAsync($"/kafka/clusters/{cluster}/brokers/broker4/state") == "RUNNING")
                    break;

                await Task.Delay(3000, ct);
            }

            (await fixture.GetAsync($"/kafka/clusters/{cluster}/brokers/broker4/state")).Should().Be("RUNNING",
                "broker4 повторно поднят за 180 c (NodeId=4 детерминирован именем)");
            var endpointsAfterAdd = await fixture.GetAsync($"/kafka/clusters/{cluster}/endpoints");
            endpointsAfterAdd!.Split(',').Should().HaveCount(4, "endpoints содержит адрес broker4");
            await using (var clusterAdmin = fixture.AdminFactory.Create(
                creds.Bootstrap, creds.User, creds.Password, creds.CaPem))
            {
                var view = await clusterAdmin.DescribeClusterAsync(ct);
                view.IsSuccess.Should().BeTrue();
                view.Value.Brokers.Should().HaveCount(4, "кластер видит всех 4 брокеров");
            }

            // Факт до баланса: лидеры (первые реплики) для сверки неизменности.
            var beforeBalance = await DescribeAllAsync(creds);
            var leadersBefore = beforeBalance.Single(t => t.Topic == topic)
                .ReplicasPerPartition.Select(p => p[0]).ToList();

            // Act 3: заявка ребалансировки при config RF=4 → converge к RF=4.
            var config = await fixture.GetAsync($"/kafka/clusters/{cluster}/config");
            await fixture.Gateway.PutAsync(fixture.Endpoint, $"/kafka/clusters/{cluster}/config",
                config!.Replace("\"replication_factor\":3", "\"replication_factor\":4", StringComparison.Ordinal),
                lease: null, ct);
            await fixture.Gateway.PutAsync(fixture.Endpoint, $"/kafkaworker/rebalances/{cluster}",
                $$"""{"requested_unix":{{DateTimeOffset.UtcNow.ToUnixTimeSeconds()}},"requested_by":"it"}""",
                lease: null, ct);

            var balanceDeadline = DateTimeOffset.UtcNow.AddSeconds(300);
            while (DateTimeOffset.UtcNow < balanceDeadline)
            {
                var snap = await fixture.SnapshotAsync(cluster);
                await rig.Reassigner.RunAsync(snap!, ct);
                await rig.Sync.RunAsync(snap!, ct);
                if (await fixture.GetAsync($"/kafkaworker/rebalances/{cluster}") is null)
                    break;

                await Task.Delay(3000, ct);
            }

            // Assert: заявка исполнена и снята, прогресс удалён.
            (await fixture.GetAsync($"/kafkaworker/rebalances/{cluster}")).Should().BeNull(
                "заявка ребалансировки снята по сходимости");
            (await fixture.GetAsync($"/kafkaworker/reassignments/{cluster}")).Should().BeNull(
                "прогресс-ключ удалён");

            // Каждая партиция юзер-топика: 4 реплики, среди них nodeId=4,
            // лидер (первая реплика) не изменился.
            var afterBalance = await DescribeAllAsync(creds);
            var ordersBalanced = afterBalance.Single(t => t.Topic == topic);
            ordersBalanced.ReplicasPerPartition.Should().HaveCount(6);
            ordersBalanced.ReplicasPerPartition.Should().OnlyContain(p => p.Count == 4, "RF восстановлен до 4");
            ordersBalanced.ReplicasPerPartition.Should().OnlyContain(p => p.Contains(4), "nodeId=4 в репликах");
            ordersBalanced.ReplicasPerPartition.Select(p => p[0]).Should().BeEquivalentTo(leadersBefore,
                o => o.WithStrictOrdering(), "лидер (первая реплика) сохранён");

            var registry = await fixture.GetAsync($"/kafka/clusters/{cluster}/topics/{topic}");
            registry.Should().Contain("\"replication_factor\":4", "автосинк обновил факт RF=4");
        }
        finally
        {
            await TeardownAsync(rig, cluster);
        }
    }
}
