using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;
using PgWorker.Etcd.Parsing;
using PgWorker.Moves;
using PgWorker.Provisioning.Processes;
using Xunit;

namespace PgWorker.IntegrationTests.Etcd;

// Контракт репарации на реальном etcd (adopt-repair spec §6): брошенные статусы
// без заявок → синтетические заявки put-if-absent; операторская заявка не
// затирается; свежий статус не диспатчится. ClaimStore — реальный с клэймом.
[Collection(EtcdCollection.Name)]
public class RepairContractTests(EtcdFixture fixture)
{
    private EtcdGateway Gateway => fixture.Gateway;

    private string Endpoint => fixture.Endpoint;

    // Сид Active-кластера + брошенный статус (updated_unix задаётся тестом).
    private async Task SeedAsync(string cluster, int bucket, string state, string phase, long updatedUnix)
    {
        var ct = TestContext.Current.CancellationToken;
        await Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/config",
            $$"""{"buckets":12,"dbname":"{{cluster}}","created_unix":1755900000}""", null, ct);
        await Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/buckets/routing/bucket_{bucket}", "s1", null, ct);
        await Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/buckets/status/bucket_{bucket}",
            $$"""{"bucket":"bucket_{{bucket}}","state":"{{state}}","owner":"s1","target":"s2","started_unix":{{updatedUnix}},"updated_unix":{{updatedUnix}},"phase":"{{phase}}"}""",
            null, ct);
    }

    private async Task<ClusterSnapshot> SnapshotAsync(string cluster)
    {
        var range = await Gateway.RangeAsync(Endpoint, "/clusters/", TestContext.Current.CancellationToken);
        var parsed = ClusterSnapshotParser.ParseClusters(range.Value, out _);
        return parsed.Value.Single(c => c.Config.Cluster == cluster);
    }

    private async Task<MoveRepairProcess> NewRepairAsync(string cluster)
    {
        var claims = new ClaimStore([Endpoint], Gateway, TimeProvider.System);
        (await claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken)).IsSuccess
            .Should().BeTrue();
        return new MoveRepairProcess(
            Gateway, [Endpoint], claims, new WorkJournal(Gateway, [Endpoint]),
            new MovesRuntimeOptions(), TimeProvider.System);
    }

    [Fact]
    public async Task Repair_StaleStatusesWithoutRequests_SyntheticRequestsAppear()
    {
        // Arrange: сид трёх брошенных статусов (updated_unix = now-3600):
        // bucket_3 SYNCING/copy owner=s1; bucket_7 ABORTING/cleanup;
        // bucket_11 FROZEN/cutover-wait owner=s1. Заявок нет.
        const string cluster = "repairc1";
        var past = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 3600;
        await SeedAsync(cluster, 3, "SYNCING", "copy", past);
        await SeedAsync(cluster, 7, "ABORTING", "cleanup", past);
        await SeedAsync(cluster, 11, "FROZEN", "cutover-wait", past);
        var repair = await NewRepairAsync(cluster);

        // Act: первый тик repair; второй тик (идемпотентный no-op — заявки уже стоят).
        var first = await repair.TickAsync(await SnapshotAsync(cluster), TestContext.Current.CancellationToken);
        var second = await repair.TickAsync(await SnapshotAsync(cluster), TestContext.Current.CancellationToken);

        // Assert: синтетические заявки op abort (force false), requested_by=pgworker-repair.
        first.IsSuccess.Should().BeTrue(first.Error?.ToString());
        second.IsSuccess.Should().BeTrue(second.Error?.ToString());
        foreach (var bucket in new[] { "bucket_3", "bucket_7", "bucket_11" })
        {
            var kv = await Gateway.GetAsync(Endpoint, MoveNames.MoveKey(cluster, bucket),
                TestContext.Current.CancellationToken);
            kv.Value!.Value.Should().Contain("\"op\":\"abort\"", $"{bucket} — синтетическая abort-заявка");
            kv.Value.Value.Should().NotContain("\"force\":true", $"{bucket} — routing==owner, force не нужен");
            kv.Value.Value.Should().Contain("\"requested_by\":\"pgworker-repair\"");
        }
    }

    [Fact]
    public async Task Repair_OperatorRequestPresent_NotReplaced()
    {
        // Arrange: статус bucket_3 SYNCING протухший + операторская заявка move.
        const string cluster = "repairc2";
        var past = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 3600;
        await SeedAsync(cluster, 3, "SYNCING", "copy", past);
        const string operatorRequest =
            """{"op":"move","to":"s2","requested_unix":100,"requested_by":"operator"}""";
        await Gateway.PutAsync(Endpoint, MoveNames.MoveKey(cluster, "bucket_3"), operatorRequest, null,
            TestContext.Current.CancellationToken);
        var repair = await NewRepairAsync(cluster);

        // Act
        var tick = await repair.TickAsync(await SnapshotAsync(cluster), TestContext.Current.CancellationToken);

        // Assert: заявка move не перезаписана (txn проигран), значение ключа неизменно.
        tick.IsSuccess.Should().BeTrue(tick.Error?.ToString());
        var kv = await Gateway.GetAsync(Endpoint, MoveNames.MoveKey(cluster, "bucket_3"),
            TestContext.Current.CancellationToken);
        kv.Value!.Value.Should().Be(operatorRequest, "операторская заявка жива — репарация не лезет");
    }

    [Fact]
    public async Task Repair_FreshStatus_NoRequest()
    {
        // Arrange: bucket_3 SYNCING updated_unix = now-30 (< 600).
        const string cluster = "repairc3";
        var fresh = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 30;
        await SeedAsync(cluster, 3, "SYNCING", "copy", fresh);
        var repair = await NewRepairAsync(cluster);

        // Act
        var tick = await repair.TickAsync(await SnapshotAsync(cluster), TestContext.Current.CancellationToken);

        // Assert: заявок не появилось.
        tick.IsSuccess.Should().BeTrue(tick.Error?.ToString());
        var range = await Gateway.RangeAsync(Endpoint, MoveNames.MovesPrefix(cluster),
            TestContext.Current.CancellationToken);
        range.Value.Should().BeEmpty("свежий статус — живой владелец, домен MoveProcess");
    }
}
