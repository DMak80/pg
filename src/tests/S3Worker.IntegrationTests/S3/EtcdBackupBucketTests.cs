using FluentAssertions;
using S3Worker.IntegrationTests.Etcd;
using S3Worker.Provisioning;
using S3Worker.Provisioning.Processes;
using Xunit;

namespace S3Worker.IntegrationTests.S3;

// Фиксированный etcd-слепковый bucket (arch/23 §5, t32 §3.4-правка): кластер с
// признаком etcd_backup=true — на КАЖДОМ инстансе оба bucket'а (per-install +
// etcd-backups), идемпотентно на повторном тике; новый инстанс (AddNode)
// получает оба. Креды/спец-ключ — per-cluster secrets + env-пара рига.
[Collection(S3ClusterCollection.Name)]
public class EtcdBackupBucketTests(S3ClusterFixture fx)
{
    // AAA: etcd_backup-кластер — оба bucket'а на каждом инстансе, повторный
    // тик no-op, добавленный инстанс получает оба
    [Fact]
    public async Task EtcdBackupCluster_BothBucketsOnEachInstance()
    {
        // Arrange: сид + признак etcd_backup в config.
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        var cluster = fx.Cluster("etbkbkt");
        await fx.SeedClusterAsync(cluster, "node1");
        await fx.Gateway.PutAsync(fx.Endpoint, $"/s3/clusters/{cluster}/config",
            $$"""{"platform":"owns3","nodes":1,"created_unix":1756500000,"etcd_backup":true,"state":"NOT_INITIALIZED"}""",
            lease: null, ct);
        var claims = fx.NewClaimStore();
        (await claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        var journal = fx.NewJournal();
        var portLock = fx.NewPortAllocLock(claims.InstanceId);
        var portIndex = fx.NewPortAllocIndex();
        var process = fx.NewProvisioning(claims, journal, portLock, portIndex);

        // Act: тики до Active (P3 создаёт bucket'ы).
        (await ProvisioningTests.RunToActiveAsync(fx, process, cluster))
            .IsSuccess.Should().BeTrue();

        // Assert: node1 — ОБА bucket'а (per-install + фиксированный слепковый).
        await AssertBothBucketsAsync(cluster, "node1");

        // Act-2: повторный тик (уже Active) — no-op, bucket'ы на месте.
        var again = await process.RunAsync(await fx.RequireSnapshotAsync(cluster), ct);
        again.IsSuccess.Should().BeTrue(again.Error?.Message);
        await AssertBothBucketsAsync(cluster, "node1");

        // Act-3: добавление второго инстанса (сид node2 → тики AddNodeProcess
        // до RUNNING — одну docker-мутацию за тик, arch/24 §5 D).
        await fx.Gateway.PutAsync(fx.Endpoint, $"/s3/clusters/{cluster}/nodes/node2/state",
            "NOT_INITIALIZED", lease: null, ct);
        await fx.Gateway.PutAsync(fx.Endpoint, $"/s3/clusters/{cluster}/nodes/node2/placement",
            $$"""{"docker_host":"{{S3ClusterFixture.DockerHost}}"}""", lease: null, ct);
        await fx.Gateway.PutAsync(fx.Endpoint, $"/s3/clusters/{cluster}/nodes/node2/resources",
            """{"cpu":"1","mem":"1Gi","disk":"5Gi"}""", lease: null, ct);
        var addNode = new AddNodeProcess(
            fx.Gateway, [fx.Endpoint], fx.Driver, claims, journal, portLock, portIndex,
            fx.NewSecrets(), fx.NewPki(), new InstanceEnvComposer(), fx.NodeClient, fx.Options);
        var deadline = DateTimeOffset.UtcNow.AddSeconds(fx.Options.NodeBootSec);
        while (DateTimeOffset.UtcNow < deadline)
        {
            var tick = await addNode.TickAsync(await fx.RequireSnapshotAsync(cluster), ct);
            tick.IsSuccess.Should().BeTrue(tick.Error?.Message);
            if (await fx.GetAsync($"/s3/clusters/{cluster}/nodes/node2/state") == "RUNNING")
                break;
            await Task.Delay(1000, ct);
        }

        (await fx.GetAsync($"/s3/clusters/{cluster}/nodes/node2/state"))
            .Should().Be("RUNNING", "node2 must reach RUNNING within NodeBootSec");
        await AssertBothBucketsAsync(cluster, "node2");
    }

    private async Task AssertBothBucketsAsync(string cluster, string node)
    {
        var ct = TestContext.Current.CancellationToken;
        var address = S3ProcessCommon.ParsePortAlloc(
            (await fx.GetAsync($"/s3worker/portalloc/{cluster}"))!)[node];
        foreach (var bucket in new[] { "pgworker-backups", Shared.Core.EtcdSnapshotBucket.Name })
        {
            var exists = await fx.NodeClient.BucketExistsAsync(
                S3ClusterFixture.AdvertisedHost, address.S3Port, fx.NodeSecrets, bucket, ct);
            exists.IsSuccess.Should().BeTrue(exists.Error?.Message);
            exists.Value.Should().BeTrue($"{bucket} must exist on {node} of etcd-backup cluster {cluster}");
        }
    }
}
