using Confluent.Kafka;
using Confluent.Kafka.Admin;
using FluentAssertions;
using FluentAssertions.Execution;
using KafkaWorker.Core;
using KafkaWorker.Etcd.Coordination;
using KafkaWorker.Provisioning.Processes;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace KafkaWorker.IntegrationTests.Kafka;

// Интеграционный тест ротации admin-пароля (t03 Ф4, spec §8.2): TLS-кластер →
// заявка /kafkaworker/admin_rotations → тики PasswordRotator до завершения.
// Assert: admin_password изменён, заявка удалена; app-кред работает НЕПРЕРЫВНО
// (Produce+Consume после каждого тика — точки на всём окне фаз A→C);
// admin-дискавери с НОВЫМ паролем успешен, со старым — SASL-отказ.
[Collection(KafkaCollection.Name)]
public class AdminRotationTests(KafkaClusterFixture fixture)
{
    [Fact]
    public async Task AdminRotation_PhasesABC_AppContinuity_AdminOldPasswordRejected()
    {
        var cluster = fixture.Cluster("rotadm");
        var ct = TestContext.Current.CancellationToken;

        // Arrange: 1-брокерный TLS-кластер (как TlsClusterTests).
        await fixture.SeedClusterAsync(cluster, brokers: 1);
        var claims = new ClaimStore([fixture.Endpoint], fixture.Gateway, TimeProvider.System);
        await claims.TryClaimClusterAsync(cluster, ct);
        var journal = new WorkJournal(fixture.Gateway, [fixture.Endpoint]);
        var provision = new ProvisioningProcess(
            fixture.Gateway, [fixture.Endpoint], fixture.Driver, claims, journal,
            new PortAllocLock([fixture.Endpoint], fixture.Gateway, TimeProvider.System, claims.InstanceId),
            new PortAllocIndex(fixture.Gateway, [fixture.Endpoint], NullLogger<PortAllocIndex>.Instance),
            new ClusterSecretEnsurer(fixture.Gateway, [fixture.Endpoint]),
            fixture.AdminFactory, new ClusterConfigConverger(fixture.AdminFactory),
            fixture.Options, fixture.Certificates,
            snapshot: null);
        var deadline = DateTimeOffset.UtcNow.AddSeconds(200);
        while (DateTimeOffset.UtcNow < deadline)
        {
            var snap = await fixture.SnapshotAsync(cluster);
            if (snap!.Config.State is null)
                break; // provisioning завершён
            (await provision.RunAsync(snap, ct)).IsSuccess.Should().BeTrue();
            await Task.Delay(3000, ct);
        }

        var oldAdminPassword = await fixture.GetAsync($"/kafka/clusters/{cluster}/admin_password");
        var appPassword = await fixture.GetAsync($"/kafka/clusters/{cluster}/app_password");
        oldAdminPassword.Should().NotBeNull();
        appPassword.Should().NotBeNull();

        // Заявка ротации admin-пароля (клэйм-txn панели — здесь напрямую).
        await fixture.Gateway.PutAsync(fixture.Endpoint, $"/kafkaworker/admin_rotations/{cluster}",
            """{"requested_unix":1756500900,"requested_by":"test"}""", null, ct);

        var rotator = new PasswordRotator(
            fixture.Gateway, [fixture.Endpoint], fixture.Driver, claims, journal,
            fixture.AdminFactory, fixture.Options, fixture.Certificates, snapshot: null);

        var topic = $"cont-{fixture.RunTag}";
        using (var admin = (await fixture.DiscoveryAdminBuilderAsync(cluster, "admin")).Build())
            await admin.CreateTopicsAsync(
                [new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }],
                new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(15) });

        // Act: тики ротатора до done (поллинг ≤ 200 с); между тиками — точка
        // непрерывности app-креда (Produce+Consume).
        var rotateDeadline = DateTimeOffset.UtcNow.AddSeconds(200);
        var ticks = 0;
        while (DateTimeOffset.UtcNow < rotateDeadline)
        {
            var snap = await fixture.SnapshotAsync(cluster);
            var result = await rotator.RunAsync(snap!, ct);
            result.IsSuccess.Should().BeTrue($"тик ротации не должен падать: {result.Error?.Message}");

            ticks++;
            await AssertAppProduceConsumeAsync(cluster, topic, appPassword!, ct);

            var newAdmin = await fixture.GetAsync($"/kafka/clusters/{cluster}/admin_password");
            if (newAdmin != oldAdminPassword
                && await fixture.GetAsync($"/kafkaworker/admin_rotations/{cluster}") is null)
                break; // B закоммитила, заявка снята, rolling завершён
            await Task.Delay(3000, ct);
        }

        // Assert 1: admin_password изменён; заявка удалена; app-пароль не тронут.
        var adminPassword = await fixture.GetAsync($"/kafka/clusters/{cluster}/admin_password");
        adminPassword.Should().NotBe(oldAdminPassword, "фаза B обязана заменить admin_password");
        adminPassword.Should().HaveLength(32).And.MatchRegex("^[A-Za-z0-9]{32}$");
        (await fixture.GetAsync($"/kafkaworker/admin_rotations/{cluster}")).Should().BeNull();
        (await fixture.GetAsync($"/kafka/clusters/{cluster}/app_password")).Should().Be(appPassword);
        ticks.Should().BeGreaterThanOrEqualTo(1);

        // Финальная точка непрерывности app-креда (после фазы C).
        await AssertAppProduceConsumeAsync(cluster, topic, appPassword!, ct);

        // Assert 2: admin-дискавери с НОВЫМ паролем — DescribeCluster успешен.
        using (var admin = (await fixture.DiscoveryAdminBuilderAsync(cluster, "admin")).Build())
            admin.GetMetadata(TimeSpan.FromSeconds(15)).Brokers.Should().HaveCount(1);

        // Assert 3: со СТАРЫМ паролем — SASL-отказ аутентификации.
        var endpoints = await fixture.GetAsync($"/kafka/clusters/{cluster}/endpoints");
        var caPem = await fixture.GetAsync($"/kafka/clusters/{cluster}/ca_pem");
        var oldConfig = new AdminClientConfig
        {
            BootstrapServers = endpoints!.Replace("host.docker.internal", "localhost", StringComparison.Ordinal),
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.Plain,
            SaslUsername = "admin",
            SaslPassword = oldAdminPassword!,
        };
        oldConfig.Set("ssl.ca.pem", caPem!);
        using (var staleAdmin = new AdminClientBuilder(oldConfig).Build())
        {
            // GetMetadata синхронный: SASL-отказ ожидаем как KafkaException.
            var act = () => staleAdmin.GetMetadata(TimeSpan.FromSeconds(15));
            act.Should().Throw<KafkaException>("старый admin-кред отвергнут после фазы C")
                .Which.Error.IsError.Should().BeTrue();
        }

        await claims.DisposeAsync();
    }

    // Непрерывность app-креда: Produce+Consume с ретраями (брокер в rolling
    // может быть недоступен мгновение — точка непрерывности обязана пройти).
    private async Task AssertAppProduceConsumeAsync(
        string cluster, string topic, string appPassword, CancellationToken ct)
    {
        var (bootstrap, caPem, appUser, _) = await fixture.DiscoveryPartsAsync(cluster);
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = bootstrap,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.Plain,
            SaslUsername = appUser,
            SaslPassword = appPassword,
        };
        producerConfig.Set("ssl.ca.pem", caPem);

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrap,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.Plain,
            SaslUsername = appUser,
            SaslPassword = appPassword,
            GroupId = $"cont-{fixture.RunTag}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
        };
        consumerConfig.Set("ssl.ca.pem", caPem);

        // Один клиент-набор на точку (как реальное приложение): новый консьюмер
        // на каждую попытку гонял бы бесконечный ребаланс группы (member failed →
        // rejoin). Consume сам переживает rolling-рестарт брокера переподключением.
        var clientErrors = new List<string>();
        using var producer = new ProducerBuilder<Null, string>(producerConfig).Build();
        using var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig)
            .SetErrorHandler((_, e) => clientErrors.Add($"{e.Code}:{e.Reason}"))
            .Build();
        consumer.Subscribe(topic);

        var deadline = DateTimeOffset.UtcNow.AddSeconds(90);
        while (DateTimeOffset.UtcNow < deadline)
        {
            var message = $"cont-{Guid.NewGuid():N}";
            (await producer.ProduceAsync(topic, new Message<Null, string> { Value = message }, ct)).Status
                .Should().Be(PersistenceStatus.Persisted);

            // Consume возвращает null только по окну — цикл повторит produce+consume.
            // Получено может быть и сообщение прошлой итерации (backlog) —
            // непрерывность доказывает ЛЮБОЕ сообщение, произведённое этим тестом.
            var consumed = consumer.Consume(TimeSpan.FromSeconds(30));
            if (consumed is null)
                continue;
            consumed.Message.Value.Should().StartWith("cont-");
            return; // точка непрерывности пройдена
        }

        throw new ApplicationException(
            $"точка непрерывности не прошла за бюджет; errors={string.Join("|", clientErrors)}");
    }
}
