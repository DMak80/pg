using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Planning;
using PgWorker.Core.Templates;
using PgWorker.Docker.Drivers;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Probes;
using PgWorker.Provisioning.Processes;
using PgWorker.Provisioning.Sql;
using Xunit;

namespace PgWorker.IntegrationTests.Etcd;

// Контракт усыновления на реальном etcd (adopt-repair spec §6): сид «внешнего»
// кластера (dsn-шарды, HA-members) + стаб-драйвер с контейнерами → portalloc
// с object, nodes-ключи RUNNING, идемпотентность второго тика; частичная
// находка журналируется. SQL/секреты — на стабах (etcd-контракт здесь).
[Collection(EtcdCollection.Name)]
public class AdoptionContractTests(EtcdFixture fixture)
{
    private EtcdGateway Gateway => fixture.Gateway;

    private string Endpoint => fixture.Endpoint;

    // Сид «внешнего» кластера: config Active (без state), dsn-шард s1 с
    // master-ключом byName-формата и живыми HA-members; portalloc нет.
    private async Task SeedExternalClusterAsync(string cluster)
    {
        var ct = TestContext.Current.CancellationToken;
        await Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/config",
            $$"""{"buckets":12,"dbname":"{{cluster}}","created_unix":1755900000}""", null, ct);
        await Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/shards/s1/replicas", "2", null, ct);
        // dsn консистентен факту находок (host=local, pg 5433/5434; креды P2.5):
        // Д2-инвариант dsn = portalloc не должен репарировать консистентный сид.
        await Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/shards/s1/dsn",
            $"host=local,local port=5433,5434 dbname={cluster} user=bucket_admin password=adm-pw", null, ct);
        await Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/shards/s1/master", "s1a:5433", null, ct);
        await Gateway.PutAsync(Endpoint, $"/service/{cluster}-s1/members/s1a",
            """{"role":"replica","state":"running"}""", null, ct);
        await Gateway.PutAsync(Endpoint, $"/service/{cluster}-s1/members/s1b",
            """{"role":"replica","state":"running"}""", null, ct);
    }

    private async Task<ClusterSnapshot> SnapshotAsync(string cluster)
    {
        var range = await Gateway.RangeAsync(Endpoint, "/clusters/", TestContext.Current.CancellationToken);
        var parsed = ClusterSnapshotParser.ParseClusters(range.Value, out _);
        return parsed.Value.Single(c => c.Config.Cluster == cluster);
    }

    private AdoptionProcess NewAdoption(StubScaleDriver driver, ClaimStore claims)
        => new(
            Gateway, [Endpoint], driver,
            new ShardEndpoints(Gateway, [Endpoint], new ShardProbe(new HttpClient())),
            new StubSql(), new StubAppSecret(),
            new AppParamsEnsurer(Gateway, [Endpoint], "sslmode=require"),
            new InstallSecrets("su-pw", "sb-pw", "adm-pw", "mov-pw"),
            claims, new WorkJournal(Gateway, [Endpoint]),
            new PortAllocIndex(Gateway, [Endpoint], NullLogger<PortAllocIndex>.Instance),
            new PlacementOptions(15000, 15100, PatroniBootSec: 600));

    // Запись журнала /pgworker/work/<C> (последняя фаза тика).
    private async Task<(string Op, string Phase, string Message)> ReadJournalAsync(string cluster)
    {
        var kv = await Gateway.GetAsync(Endpoint, $"/pgworker/work/{cluster}", TestContext.Current.CancellationToken);
        using var doc = JsonDocument.Parse(kv.Value!.Value);
        var root = doc.RootElement;
        return (root.GetProperty("op").GetString() ?? "",
            root.GetProperty("phase").GetString() ?? "",
            root.TryGetProperty("last_error", out var err) ? err.GetString() ?? "" : "");
    }

    [Fact]
    public async Task Adopt_ExternalCluster_WritesPortallocAndNodeStates()
    {
        // Arrange: живой etcd; сид внешнего кластера; стаб-драйвер нашёл обе ноды.
        const string cluster = "adoptc1";
        await SeedExternalClusterAsync(cluster);
        var driver = new StubScaleDriver
        {
            InspectResult = new Dictionary<string, DiscoveredNode>
            {
                ["s1a"] = new("s1a", "local", "as-s1a", 5433, 8011, 0),
                ["s1b"] = new("s1b", "local", "as-s1b", 5434, 8012, 0),
            },
        };
        var claims = new ClaimStore([Endpoint], Gateway, TimeProvider.System);
        (await claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken)).IsSuccess
            .Should().BeTrue();
        var adoption = NewAdoption(driver, claims);

        // Act: два тика (идемпотентность: второй — no-op).
        var first = await adoption.TickAsync(await SnapshotAsync(cluster), TestContext.Current.CancellationToken);
        var second = await adoption.TickAsync(await SnapshotAsync(cluster), TestContext.Current.CancellationToken);

        // Assert: portalloc содержит object-записи обеих нод; nodes-ключи RUNNING;
        // journal /pgworker/work/<C> — op adopt phase done.
        first.IsSuccess.Should().BeTrue(first.Error?.ToString());
        second.IsSuccess.Should().BeTrue(second.Error?.ToString());
        var kv = await Gateway.GetAsync(Endpoint, $"/pgworker/portalloc/{cluster}", TestContext.Current.CancellationToken);
        kv.Value!.Value.Should().Contain("\"object\":\"as-s1a\"").And.Contain("\"object\":\"as-s1b\"");
        var s1a = await Gateway.GetAsync(Endpoint, $"/clusters/{cluster}/shards/s1/nodes/s1a/state",
            TestContext.Current.CancellationToken);
        s1a.Value!.Value.Should().Be("RUNNING");
        var s1b = await Gateway.GetAsync(Endpoint, $"/clusters/{cluster}/shards/s1/nodes/s1b/state",
            TestContext.Current.CancellationToken);
        s1b.Value!.Value.Should().Be("RUNNING");
        var entry = await ReadJournalAsync(cluster);
        entry.Op.Should().Be("adopt");
        entry.Phase.Should().Be("done");
    }

    [Fact]
    public async Task Adopt_PartialDiscovery_JournalContainsSkipped()
    {
        // Arrange: members s1a+s1b, InspectResult — только s1a (s1b не опознана).
        const string cluster = "adoptc2";
        await SeedExternalClusterAsync(cluster);
        var driver = new StubScaleDriver
        {
            InspectResult = new Dictionary<string, DiscoveredNode>
            {
                ["s1a"] = new("s1a", "local", "as-s1a", 5433, 8011, 0),
            },
        };
        var claims = new ClaimStore([Endpoint], Gateway, TimeProvider.System);
        (await claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken)).IsSuccess
            .Should().BeTrue();
        var adoption = NewAdoption(driver, claims);

        // Act: тик.
        var outcome = await adoption.TickAsync(await SnapshotAsync(cluster), TestContext.Current.CancellationToken);

        // Assert: s1a в portalloc; журнальная запись усыновления несёт факт
        // пропуска (фаза skipped с именем s1b покрывается unit-тестом —
        // WorkJournal держит одну запись на кластер, здесь виден итог).
        outcome.IsSuccess.Should().BeTrue(outcome.Error?.ToString());
        var kv = await Gateway.GetAsync(Endpoint, $"/pgworker/portalloc/{cluster}", TestContext.Current.CancellationToken);
        kv.Value!.Value.Should().Contain("\"object\":\"as-s1a\"");
        kv.Value.Value.Should().NotContain("as-s1b");
        var entry = await ReadJournalAsync(cluster);
        entry.Op.Should().Be("adopt");
        entry.Phase.Should().Be("done");
        entry.Message.Should().Contain("пропущено: 1");
    }

    // SQL-стаб: контракт усыновления — про etcd, SQL-механика покрыта unit P2.3.
    private sealed class StubSql : ISqlExecutor
    {
        public Task<Result> ExecuteAsync(string dsn, string sql, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result<object?>> ExecuteScalarAsync(string dsn, string sql, CancellationToken ct)
            => Task.FromResult(Result<object?>.Success(null));

        public Task<Result> EnsureDatabaseAsync(string dsn, string dbname, CancellationToken ct)
            => Task.FromResult(Result.Success());
    }

    // Секрет-стаб: пер-кластерный app-секрет «уже есть».
    private sealed class StubAppSecret : IAppSecretEnsurer
    {
        public Task<Result<AppCredentials>> EnsureAsync(string cluster, CancellationToken ct)
            => Task.FromResult(Result<AppCredentials>.Success(new AppCredentials("app", "pw")));
    }
}
