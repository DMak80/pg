using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Shared.Etcd.Client;
using ValkeyWorker.Core.Model;
using ValkeyWorker.Etcd.Parsing;
using ValkeyWorker.UnitTests.Provisioning;
using Xunit;

namespace ValkeyWorker.UnitTests.Provisioning;

// DeprovisioningProcess X0–X3 (arch/21 §5 B): порядок «сначала docker, потом
// etcd», 404 = ок, ошибка docker оставляет декларацию нетронутой.
public class DeprovisioningProcessTests
{
    private sealed class Rig
    {
        public Fakes.FakeEtcd Etcd = new();
        public Fakes.FakeDriver Driver = new();
        public ClaimStore Claims = null!;
        public List<string> Log = [];
        public List<string> Snapshots = [];
        public ValkeyWorker.Provisioning.Processes.DeprovisioningProcess Process = null!;

        public static Rig Create()
        {
            var rig = new Rig();
            rig.Claims = new ClaimStore("/valkeyworker", ["http://etcd:2379"], rig.Etcd, TimeProvider.System);
            rig.Process = new ValkeyWorker.Provisioning.Processes.DeprovisioningProcess(
                rig.Etcd, ["http://etcd:2379"], rig.Driver, rig.Claims,
                new WorkJournal("/valkeyworker", rig.Etcd, ["http://etcd:2379"]),
                async _ =>
                {
                    rig.Snapshots.Add("shot");
                    return Result.Success();
                });
            // Общий журнал вызовов — проверка порядка «docker → etcd».
            rig.Driver.OnRemove = name => rig.Log.Add($"docker:{name}");
            rig.Etcd.OnDelete = key => rig.Log.Add($"etcd:{key}");
            return rig;
        }

        public void SeedCluster(string cluster)
        {
            Etcd.Seed($"/valkey/clusters/{cluster}/config",
                """{"nodes":1,"maxmemory_bytes":536870912,"maxmemory_policy":"allkeys-lru","created_unix":1756500000,"state":"TO_REMOVE"}""");
            Etcd.Seed($"/valkey/clusters/{cluster}/endpoints", "localhost:17001");
            Etcd.Seed($"/valkey/clusters/{cluster}/nodes/node1/state", "RUNNING");
            Etcd.Seed($"/valkey/clusters/{cluster}/app_password", "AppPassword0123456789abcdef12345");
            Etcd.Seed($"/valkeyworker/portalloc/{cluster}", """{"node1":{"host":"h1","client":17001}}""");
            Etcd.Seed($"/valkeyworker/work/{cluster}", """{"op":"provision","phase":"done"}""");
            // Стейт доигрывания ротации — вложенный ключ чистки X2.
            Etcd.Seed($"/valkeyworker/work/{cluster}/rotation",
                """{"phase":"e2-committed","role":"app","old":"o","new":"n","requested_by":"api"}""");
            Etcd.Seed($"/valkeyworker/rotations/{cluster}", """{"role":"app"}""");
            Driver.Containers[$"vwk-{cluster}-node1"] =
                new Fakes.FakeDriver.ContainerFact("h1", 17001, null, null, ["valkey-server"], "valkey/valkey:9.1.2", "id1");
        }

        public ValkeyClusterSnapshot Snapshot(string cluster)
        {
            var range = Etcd.RangeAsync("", "/valkey/clusters/", TestContext.Current.CancellationToken)
                .GetAwaiter().GetResult();
            return ValkeySnapshotParser.Parse(range.Value).Value.Clusters.First(c => c.Cluster == cluster);
        }
    }

    [Fact]
    public async Task ПолныйПрогон_DockerДоОтcd_ВсёЧисто()
    {
        // Arrange: кластер TO_REMOVE с контейнером и координацией.
        const string cluster = "dep";
        var rig = Rig.Create();
        rig.SeedCluster(cluster);
        await rig.Claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken);

        // Act
        var result = await rig.Process.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: контейнер удалён ДО чистки etcd; все префиксы пусты; клэйм снят.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        var dockerIndex = rig.Log.FindIndex(l => l.StartsWith("docker:"));
        var etcdIndex = rig.Log.FindIndex(l => l.StartsWith("etcd:"));
        dockerIndex.Should().BeGreaterThanOrEqualTo(0);
        etcdIndex.Should().BeGreaterThan(dockerIndex, "порядок arch/21 §5 B: сначала docker, потом etcd");

        rig.Driver.Containers.Should().NotContainKey($"vwk-{cluster}-node1");
        rig.Etcd.Store.Keys.Where(k => k.StartsWith($"/valkey/clusters/{cluster}/")).Should().BeEmpty();
        rig.Etcd.Store.Keys.Where(k => k.Contains($"/valkeyworker/portalloc/{cluster}")).Should().BeEmpty();
        rig.Etcd.Store.Keys.Where(k => k.Contains($"/valkeyworker/rotations/{cluster}")).Should().BeEmpty();
        // Координация <C> пуста ЦЕЛИКОМ: journal-записи после чистки нет
        // (запись done воскресила бы удалённый work/<C> — arch/21 §5 B),
        // стейт доигрывания ротации тоже удалён.
        rig.Etcd.Store.Keys.Where(k => k.Contains($"/valkeyworker/work/{cluster}")).Should().BeEmpty();
        (await rig.Claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken)).Value
            .Should().BeTrue("клэйм снят явно — второй инстанс захватывает");
        rig.Snapshots.Should().HaveCount(2);
    }

    [Fact]
    public async Task DockerПуст_404КакОк_EtcdЧистится()
    {
        // Arrange: TO_REMOVE без контейнеров (повторный демонтаж).
        const string cluster = "empty";
        var rig = Rig.Create();
        rig.SeedCluster(cluster);
        rig.Driver.Containers.Clear();
        await rig.Claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken);

        // Act
        var result = await rig.Process.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: успех, etcd-чистка выполнена.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Etcd.Store.Keys.Where(k => k.StartsWith($"/valkey/clusters/{cluster}/")).Should().BeEmpty();
        rig.Etcd.Store.Keys.Where(k => k.Contains($"/valkeyworker/portalloc/{cluster}")).Should().BeEmpty();
    }

    [Fact]
    public async Task ОшибкаDocker_ДекларацияНетронута()
    {
        // Arrange: docker-хост отказывает на удалении (ListNodeObjects ок, но
        // RemoveNodeAsync падает — инъекция через Fault-обёртку).
        const string cluster = "fail";
        var rig = Rig.Create();
        rig.SeedCluster(cluster);
        await rig.Claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken);
        rig.Driver.EndpointFault = true; // «хост молчит» — X1 не успевает пройти? нет:
        // EndpointFault ломает inspect-ы; для отказа X1 валим список объектов.
        rig.Driver.OnRemove = name => throw new ApplicationException("docker host refuses");

        // Act
        var result = await rig.Process.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: Failed, etcd-префикс домена ЦЕЛ (порядок!), координация не тронута.
        result.IsSuccess.Should().BeFalse();
        result.Error!.Message.Should().Contain("docker host refuses");
        rig.Etcd.Store.Keys.Where(k => k.StartsWith($"/valkey/clusters/{cluster}/")).Should().NotBeEmpty();
        rig.Etcd.Store.Should().ContainKey($"/valkeyworker/portalloc/{cluster}");
    }

    [Fact]
    public async Task Deprovision_WithLiveCaRotationTicket_CleansCoordination()
    {
        // Arrange — TO_REMOVE-кластер с ЖИВОЙ заявкой ca_rotations и staging
        // (t07, X2: координация ротации CA сносится вместе с прочей)
        const string cluster = "x2ca";
        var rig = Rig.Create();
        rig.SeedCluster(cluster);
        rig.Etcd.Seed($"/valkeyworker/ca_rotations/{cluster}",
            """{"requested_unix":1756500000,"requested_by":"it"}""");
        rig.Etcd.Seed($"/valkey/clusters/{cluster}/ca_next_key", "stg");
        rig.Etcd.Seed($"/valkey/clusters/{cluster}/ca_next_pem", "stg");
        await rig.Claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken);

        // Act — тик демонтажа доводит до конца
        var result = await rig.Process.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert — ни заявки, ни staging, ни клэйма
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Etcd.Store.Should().NotContainKey($"/valkeyworker/ca_rotations/{cluster}");
        rig.Etcd.Store.Keys.Where(k => k.StartsWith($"/valkey/clusters/{cluster}/"))
            .Should().BeEmpty("префиксный del домена забирает staging ca_next_*");
        rig.Etcd.Store.Should().NotContainKey($"/valkeyworker/claims/{cluster}");
    }
}
