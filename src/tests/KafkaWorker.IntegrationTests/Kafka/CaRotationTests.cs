using System.IO;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using FluentAssertions;
using KafkaWorker.Provisioning.Processes;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace KafkaWorker.IntegrationTests.Kafka;

// Интеграция ротации CA (t07, arch/16 §2.3/§5 K): заявка → CaRotator доводит
// окно двойного доверия на живом docker-кластере; после коммита ca_pem = NEW,
// staging удалён, клиенты с NEW работают, клиенты со старым CA-кэшем отвергаются.
[Collection(KafkaCollection.Name)]
public class CaRotationTests(KafkaClusterFixture fixture)
{
    [Fact]
    public async Task CaRotate_RollingWindow_FinalsOnNewCa()
    {
        var cluster = fixture.Cluster("carot");
        var ct = TestContext.Current.CancellationToken;

        // Arrange: канонический TLS-кластер (provisioning — образец TlsClusterTests);
        // клэйм ОДИН на тест — provisioning и CaRotator один держатель
        await fixture.SeedClusterAsync(cluster, brokers: 1);
        var claims = new ClaimStore("/kafkaworker", [fixture.Endpoint], fixture.Gateway, TimeProvider.System);
        await claims.TryClaimClusterAsync(cluster, ct);
        var provision = new ProvisioningProcess(
            fixture.Gateway, [fixture.Endpoint], fixture.Driver, claims,
            new WorkJournal("/kafkaworker", fixture.Gateway, [fixture.Endpoint]),
            new PortAllocLock("/kafkaworker", [fixture.Endpoint], fixture.Gateway, TimeProvider.System, claims.InstanceId),
            new PortAllocIndex(fixture.Gateway, [fixture.Endpoint], NullLogger<PortAllocIndex>.Instance),
            new ClusterSecretEnsurer(fixture.Gateway, [fixture.Endpoint]),
            fixture.AdminFactory, new ClusterConfigConverger(fixture.AdminFactory),
            fixture.Options, fixture.Certificates, snapshot: null);
        var deadline = DateTimeOffset.UtcNow.AddSeconds(200);
        while (DateTimeOffset.UtcNow < deadline)
        {
            var snap = await fixture.SnapshotAsync(cluster);
            if (snap!.Config.State is null)
                break; // provisioning завершён (config без state)
            var tick = await provision.RunAsync(snap, ct);
            tick.IsSuccess.Should().BeTrue(
                $"тик provisioning не должен падать: {tick.Error?.Message}");
            await Task.Delay(3000, ct);
        }

        var oldCaPem = (await fixture.GetAsync($"/kafka/clusters/{cluster}/ca_pem"))!;
        var (bootstrap, _, appUser, appPassword) = await fixture.DiscoveryPartsAsync(cluster);
        var topic = $"carot-{fixture.RunTag}";
        using (var admin = (await fixture.DiscoveryAdminBuilderAsync(cluster, "admin")).Build())
            await admin.CreateTopicsAsync(
                [new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }],
                new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(15) });

        // Baseline: приложение со старым CA производит (TLS-контур жив).
        using (var producer = Producer(bootstrap, appUser, appPassword, oldCaPem))
            (await producer.ProduceAsync(topic, new Message<Null, string> { Value = "before" }, ct)).Status
                .Should().Be(PersistenceStatus.Persisted);

        // Act: заявка ротации (формат §9.8) + CaRotator tick-цикл до del заявки.
        await fixture.PutAsync($"/kafkaworker/ca_rotations/{cluster}",
            $$"""{"requested_unix":{{DateTimeOffset.UtcNow.ToUnixTimeSeconds()}},"requested_by":"it"}""");
        var caRotator = new CaRotator(
            fixture.Gateway, [fixture.Endpoint], fixture.Driver, claims,
            new WorkJournal("/kafkaworker", fixture.Gateway, [fixture.Endpoint]),
            fixture.AdminFactory, fixture.Options, fixture.Certificates, snapshot: null);
        deadline = DateTimeOffset.UtcNow.AddSeconds(360);
        var ticketGone = false;
        while (DateTimeOffset.UtcNow < deadline)
        {
            var tick = await caRotator.RunAsync((await fixture.SnapshotAsync(cluster))!, ct);
            tick.IsSuccess.Should().BeTrue($"тик ротации не должен падать: {tick.Error?.Message}");
            if (await fixture.GetAsync($"/kafkaworker/ca_rotations/{cluster}") is null)
            {
                ticketGone = true;
                break;
            }

            await Task.Delay(3000, ct);
        }

        var snapDiag = await fixture.SnapshotAsync(cluster);
        string adminDiag;
        try
        {
            await using var probe = fixture.AdminFactory.Create(
                bootstrap, "admin",
                (await fixture.GetAsync($"/kafka/clusters/{cluster}/admin_password"))!,
                await fixture.GetAsync($"/kafka/clusters/{cluster}/ca_pem"));
            var view = await probe.DescribeClusterAsync(ct);
            adminDiag = view.IsSuccess ? $"ok:{view.Value.Brokers.Count}" : $"fail:{view.Error!.Message}";
        }
        catch (Exception e)
        {
            adminDiag = $"throw:{e.Message}";
        }
        var brokerUp = (await fixture.Driver.NodeResourcesAsync(cluster, "broker1", ct)).Value is not null;
        var diag = $"journal={await fixture.GetAsync($"/kafkaworker/work/{cluster}")}, " +
            $"next_key={(await fixture.GetAsync($"/kafka/clusters/{cluster}/ca_next_key")) is not null}, " +
            $"broker_up={brokerUp}, ep={snapDiag!.Endpoints is not null}, app={snapDiag.AppPassword is not null}, " +
            $"admin={snapDiag.AdminPassword is not null}, ca={snapDiag.CaPem is not null}, cakey={snapDiag.CaKey is not null}, " +
            $"parse_errors={string.Join(";", snapDiag.ParseErrors)}, admin={adminDiag}, {await EnvDiagAsync(fixture, cluster)}, " +
            "logs=[" + BrokerLogTail(cluster) + "]";
        ticketGone.Should().BeTrue($"ротация исполнена: заявка удалена [{diag}]");

        // Assert 1: ca_pem = NEW (только новый), staging удалён, ключ CA сменён.
        var newCaPem = (await fixture.GetAsync($"/kafka/clusters/{cluster}/ca_pem"))!;
        newCaPem.Should().NotBe(oldCaPem, "канон переключён на NEW CA");
        newCaPem.Should().NotContain(oldCaPem, "bundle свёрнут после коммита");
        (await fixture.GetAsync($"/kafka/clusters/{cluster}/ca_next_key")).Should().BeNull();
        (await fixture.GetAsync($"/kafka/clusters/{cluster}/ca_next_pem")).Should().BeNull();

        // Assert 2: приложение, перечитавшее ca_pem из etcd (NEW), работает.
        using (var producer = Producer(bootstrap, appUser, appPassword, newCaPem))
            (await producer.ProduceAsync(topic, new Message<Null, string> { Value = "after" }, ct)).Status
                .Should().Be(PersistenceStatus.Persisted);

        // Assert 3: клиент со старым CA-кэшем отвергается TLS-хендшейком
        // (сертификат брокера подписан NEW, OLD доверия больше нет).
        var rejected = false;
        try
        {
            using var stale = Producer(bootstrap, appUser, appPassword, oldCaPem);
            var report = await stale.ProduceAsync(
                topic, new Message<Null, string> { Value = "stale" },
                CancellationToken.None);
            rejected = report.Status != PersistenceStatus.Persisted;
        }
        catch (ProduceException<Null, string>)
        {
            rejected = true; // TLS-хендшейк/доставка с недоверенным CA — ожидаемо
        }

        rejected.Should().BeTrue("клиент со старым CA-кэшем не работает после коммита");
    }

    // Env живого контейнера брокера: число сертов в truststore/keystore
    // (вхождения BEGIN, не строки — PEM в env экранирован \n и лежит одной
    // строкой) + сверка truststore с ca_pem из etcd (раз-экранирование).
    private static async Task<string> EnvDiagAsync(KafkaClusterFixture fixture, string cluster)
    {
        var envRes = await fixture.Driver.NodeEnvAsync(cluster, "broker1", TestContext.Current.CancellationToken);
        if (!envRes.IsSuccess || envRes.Value is not { } env)
            return "env=none";
        var ts = env.TryGetValue("KAFKA_SSL_TRUSTSTORE_CERTIFICATES", out var tsVal) ? tsVal : null;
        var ks = env.TryGetValue("KAFKA_SSL_KEYSTORE_CERTIFICATE_CHAIN", out var ksVal) ? ksVal : null;
        var caEtcd = await fixture.GetAsync($"/kafka/clusters/{cluster}/ca_pem");
        var tsUnescaped = ts?.Replace("\\n", "\n");
        return $"ts_certs={Certs(ts)}, ks_certs={Certs(ks)}, ts_eq_etcd={tsUnescaped == caEtcd}";
    }

    private static string Certs(string? pem)
        => pem is null ? "-" : (pem.Split("-----BEGIN CERTIFICATE-----").Length - 1).ToString();

    // Хвост логов брокера в diagnose-сообщение (root-cause застрявшей ротации).
    private static string BrokerLogTail(string cluster)
    {
        try
        {
            var psi = new System.Diagnostics.ProcessStartInfo(
                "/bin/sh", $"-c \"docker logs --tail 25 kfw-{cluster}-broker1 2>&1\"")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            using var proc = System.Diagnostics.Process.Start(psi)!;
            var output = proc.StandardOutput.ReadToEnd();
            proc.WaitForExit(5000);
            return output.Length > 2600 ? output[^2600..] : output;
        }
        catch (Exception e)
        {
            return $"log-fail:{e.Message}";
        }
    }

    private static IProducer<Null, string> Producer(
        string bootstrap, string user, string password, string caPem)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = bootstrap,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.Plain,
            SaslUsername = user,
            SaslPassword = password,
            MessageTimeoutMs = 20000,
            SocketTimeoutMs = 15000,
        };
        // Файловый truststore (t07): inline ssl.ca.pem у librdkafka — один серт,
        // bundle окна ротации требует файла (ssl.ca.location).
        var caPath = Path.Combine(Path.GetTempPath(),
            $"carot-ca-{Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(
                System.Text.Encoding.UTF8.GetBytes(caPem))[..16]).ToLowerInvariant()}.pem");
        if (!File.Exists(caPath))
            File.WriteAllText(caPath, caPem);
        config.Set("ssl.ca.location", caPath);
        return new ProducerBuilder<Null, string>(config).Build();
    }
}
