using System.Net;
using Amazon;
using Amazon.S3;
using FluentAssertions;
using S3Worker.Core.Model;
using S3Worker.IntegrationTests.Etcd;
using S3Worker.Provisioning.Processes;
using Xunit;

namespace S3Worker.IntegrationTests.S3;

// Надзор реального контура (spec §11 сценарий 2): docker rm -f → тик надзора
// → пересоздан на том же placed_on, volume сохранён (объект bucket жив),
// usage-ключ обновился.
[Collection(S3ClusterCollection.Name)]
public class SupervisionTests(S3ClusterFixture fx)
{
    [Fact]
    public async Task ContainerRemoval_RecreateOnSameHost_VolumeAlive()
    {
        // Arrange: поднятый кластер N=1 + объект-метка в bucket.
        DockerTrait.SkipIfUnavailable();
        var cluster = fx.Cluster("sup");
        await fx.SeedClusterAsync(cluster);
        var claims = fx.NewClaimStore();
        (await claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken)).Value.Should().BeTrue();
        var journal = fx.NewJournal();
        var portLock = fx.NewPortAllocLock(claims.InstanceId);
        var portIndex = fx.NewPortAllocIndex();
        var provision = fx.NewProvisioning(claims, journal, portLock, portIndex);
        (await ProvisioningTests.RunToActiveAsync(fx, provision, cluster))
            .IsSuccess.Should().BeTrue();
        var address = S3ProcessCommon.ParsePortAlloc(
            (await fx.GetAsync($"/s3worker/portalloc/{cluster}"))!)["node1"];
        // Пробы теста — по advertised-хосту (нода публикуется на localhost).
        const string probeHost = S3ClusterFixture.AdvertisedHost;

        // Метка-объект в bucket (данные на volume).
        var s3 = NewS3Client(address.S3Port);
        var bucketExists = await fx.NodeClient.BucketExistsAsync(
            S3ClusterFixture.AdvertisedHost, address.S3Port, fx.NodeSecrets, "pgworker-backups",
            TestContext.Current.CancellationToken);
        bucketExists.Value.Should().BeTrue(
            $"bucket after provisioning: exists={bucketExists.Value} err={bucketExists.Error?.Message}");
        // Бакет создан provisioning'ом — ждём оживления ноды перед PUT.
        await S3ClusterFixture.WaitAsync(async () =>
            (await fx.NodeClient.HealthAsync(probeHost, address.S3Port, TestContext.Current.CancellationToken)).Value,
            TimeSpan.FromSeconds(fx.Options.NodeBootSec), "node came alive before PUT of marker");
        (await s3.PutObjectAsync(new Amazon.S3.Model.PutObjectRequest
        {
            BucketName = "pgworker-backups",
            Key = $"supervision/{cluster}/marker.txt",
            ContentBody = "volume-preserved",
        }, TestContext.Current.CancellationToken)).HttpStatusCode.Should().Be(HttpStatusCode.OK);

        // docker rm -f контейнера ноды (положительное свидетельство сноса).
        var containerName = $"s3w-{cluster}-node1";
        var engine = new DockerEngineFactory().Create("unix:///var/run/docker.sock", hostAlias: "local");
        (await engine.RemoveContainerAsync(containerName, force: true, TestContext.Current.CancellationToken))
            .IsSuccess.Should().BeTrue();

        // Act: тик надзора (пересоздание по S7-свидетельству).
        var healer = fx.NewHealer(claims, journal, portLock, portIndex);
        var supervisor = fx.NewSupervisor(claims, journal, healer);
        var result = await supervisor.TickAsync(await fx.RequireSnapshotAsync(cluster), TestContext.Current.CancellationToken);

        // Assert: Success; нода пересоздана на том же placed_on; state=PROVISIONING
        // (в RUNNING переведёт следующий тик по health).
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        (await fx.GetAsync($"/s3/clusters/{cluster}/nodes/node1/placed_on")).Should().Be("local");
        (await fx.GetAsync($"/s3/clusters/{cluster}/nodes/node1/state")).Should().Be("PROVISIONING");

        // Volume сохранён: объект-метка читается (после оживления ноды).
        await S3ClusterFixture.WaitAsync(async () =>
            (await fx.NodeClient.HealthAsync("localhost", address.S3Port, TestContext.Current.CancellationToken)).Value,
            TimeSpan.FromSeconds(fx.Options.NodeBootSec), "node came alive after re-creation");
        var marker = await s3.GetObjectAsync("pgworker-backups",
            $"supervision/{cluster}/marker.txt", TestContext.Current.CancellationToken);
        new StreamReader(marker.ResponseStream).ReadToEnd().Should().Be("volume-preserved");

        // Доводка: тики надзора до RUNNING (задержка — бюджет ≤ 30 c; arch/24 §5 C
        // «в RUNNING переводит следующий тик по health»).
        var deadline = DateTimeOffset.UtcNow.AddSeconds(30);
        while (DateTimeOffset.UtcNow < deadline)
        {
            (await supervisor.TickAsync(await fx.RequireSnapshotAsync(cluster), TestContext.Current.CancellationToken))
                .IsSuccess.Should().BeTrue();
            if (await fx.GetAsync($"/s3/clusters/{cluster}/nodes/node1/state") == "RUNNING")
                break;
            await Task.Delay(1000, TestContext.Current.CancellationToken);
        }

        (await fx.GetAsync($"/s3/clusters/{cluster}/nodes/node1/state")).Should().Be("RUNNING");

        // usage-ключ обновился (updated_unix свежий; при отказе — warning
        // в supervision-записи work/<C> — выводим для диагностики).
        var usage = await fx.GetAsync($"/s3/clusters/{cluster}/nodes/node1/usage");
        var supervision = await fx.GetAsync($"/s3worker/work/{cluster}");
        usage.Should().NotBeNull($"usage not written; supervision={supervision}").And.Contain("\"updated_unix\":");
    }

    private static AmazonS3Client NewS3Client(int port)
        => new(
            new Amazon.Runtime.BasicAWSCredentials("it-root", "it-root-password"),
            new AmazonS3Config
            {
                ServiceURL = $"http://localhost:{port}",
                ForcePathStyle = true,
                Timeout = TimeSpan.FromSeconds(7.5),
                MaxErrorRetry = 0,
            });
}
