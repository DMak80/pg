using System.Net;
using System.Text;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Processes;
using PgWorker.Provisioning.Probes;
using PgWorker.Provisioning.Snapshots;

namespace PgWorker.UnitTests.Provisioning;

// BucketEvacuator E0–E4 + SnapshotJob (задача 22; arch/14 §5 D/E, P12):
// journal-before-manipulations, txn-flip routing, карантин вернувшегося
// шарда (stop без rm — данные целы), снапшеты до/после, ретеншн файлов.
public class BucketEvacuatorTests
{
    private const string Ep = "http://etcd:2379";
    private static readonly InstallSecrets Secrets = new("su-pw", "sb-pw", "app-pw", "adm-pw", "mov-pw");

    private static ShardProbe Probe(Func<int, HttpResponseMessage> respondByPort)
        => new(new HttpClient(new FakeHandler(r => respondByPort(r.RequestUri!.Port))));

    private sealed class FakeHandler(Func<HttpRequestMessage, HttpResponseMessage> responder) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
            => Task.FromResult(responder(request));
    }

    private static HttpResponseMessage PatroniOk() => new(HttpStatusCode.OK)
    {
        Content = new StringContent("""{"members":[]}""", Encoding.UTF8, "application/json"),
    };

    private static HttpResponseMessage Down() => new(HttpStatusCode.ServiceUnavailable);

    private static void SeedCluster(Fakes.FakeEtcd etcd)
    {
        etcd.Seed("/clusters/shop/config", """{"buckets":4,"dbname":"shop","created_unix":1755900000}""");
        etcd.Seed("/clusters/shop/shards/shard1/replicas", "2");
        etcd.Seed("/clusters/shop/shards/shard2/replicas", "2");
        etcd.Seed("/clusters/shop/shards/shard1/nodes/shard1a/state", "RUNNING");
        etcd.Seed("/clusters/shop/shards/shard1/nodes/shard1b/state", "RUNNING");
        etcd.Seed("/clusters/shop/shards/shard2/nodes/shard2a/state", "RUNNING");
        etcd.Seed("/clusters/shop/shards/shard2/nodes/shard2b/state", "RUNNING");
        // shard1 мёртв целиком: его бакеты 0,2 → эвакуировать на shard2 (бакеты 1,3)
        etcd.Seed("/clusters/shop/buckets/routing/bucket_0", "shard1");
        etcd.Seed("/clusters/shop/buckets/routing/bucket_1", "shard2");
        etcd.Seed("/clusters/shop/buckets/routing/bucket_2", "shard1");
        etcd.Seed("/clusters/shop/buckets/routing/bucket_3", "shard2");
        // portalloc: patroni-порты 18000/18001 (shard1), 18002/18003 (shard2)
        etcd.Seed("/pgworker/portalloc/shop", PgWorker.Core.Model.Portalloc.Serialize(
            new Dictionary<string, NodeAddress>
            {
                ["shard1/shard1a"] = new("h1", new NodePorts(15000, 18000, 16500)),
                ["shard1/shard1b"] = new("h2", new NodePorts(15001, 18001, 16501)),
                ["shard2/shard2a"] = new("h1", new NodePorts(15002, 18002, 16502)),
                ["shard2/shard2b"] = new("h2", new NodePorts(15003, 18003, 16503)),
            }));
        // мастер целевого шарда — ключ (host:doormanPort), SQL через него
        etcd.Seed("/clusters/shop/shards/shard2/master", "h1:16502");
    }

    private static async Task<ClusterSnapshot> Snapshot(Fakes.FakeEtcd etcd)
    {
        var range = await etcd.RangeAsync(Ep, "/clusters/", CancellationToken.None);
        var parsed = ClusterSnapshotParser.ParseClusters(range.Value, out _);
        return parsed.Value.Single(c => c.Config.Cluster == "shop");
    }

    private sealed record Rig(Fakes.FakeEtcd Etcd, Fakes.FakeDriver Driver, Fakes.FakeSql Sql,
        ClaimStore Claims, WorkJournal Journal, BucketEvacuator Evacuator, List<string> Events, List<int> Snapshots);

    private static async Task<Rig> NewRig(Func<int, HttpResponseMessage> respond)
    {
        var etcd = new Fakes.FakeEtcd();
        SeedCluster(etcd);
        var events = new List<string>();
        etcd.OnPut = key => events.Add($"etcd:{key}");
        var claims = new ClaimStore([Ep], etcd, TimeProvider.System);
        await claims.TryClaimClusterAsync("shop", CancellationToken.None);
        var journal = new WorkJournal(etcd, [Ep]);
        var driver = new Fakes.FakeDriver();
        var sql = new Fakes.FakeSql
        {
            OnExecute = dsn => events.Add($"sql:{dsn}"),
        };
        var snapshots = new List<int>();
        var probe = Probe(respond);
        var evacuator = new BucketEvacuator(
            etcd, [Ep], driver, sql, probe, new ShardEndpoints(etcd, [Ep], probe), claims, journal, Secrets,
            snapshot: ct =>
            {
                snapshots.Add(1);
                return Task.FromResult(Result.Success());
            });
        return new Rig(etcd, driver, sql, claims, journal, evacuator, events, snapshots);
    }

    [Fact]
    public async Task Tick_DeadShard_JournalBeforeSql_FlipsRouting_SchemasOnTarget()
    {
        // Arrange — shard1 мёртв (пробы 500), shard2 жив; бакеты 0,2 эвакуируются
        var rig = await NewRig(port => port >= 18002 ? PatroniOk() : Down());

        // Act
        var outcome = await rig.Evacuator.TickAsync(await Snapshot(rig.Etcd), "shard1", CancellationToken.None);

        // Assert: Done; журнал эвакуации записан ДО первого SQL (P7)
        outcome.IsSuccess.Should().BeTrue();
        outcome.Value.Should().Be(ProcessOutcome.Done);
        var journalIdx = rig.Events.IndexOf("etcd:/pgworker/evacuations/shop/shard1");
        var firstSql = rig.Events.FindIndex(e => e.StartsWith("sql:", StringComparison.Ordinal));
        journalIdx.Should().BeGreaterThanOrEqualTo(0);
        journalIdx.Should().BeLessThan(firstSql);

        // routing переведён txn-ом с compare по старому значению
        rig.Etcd.Store["/clusters/shop/buckets/routing/bucket_0"].Value.Should().Be("shard2");
        rig.Etcd.Store["/clusters/shop/buckets/routing/bucket_2"].Value.Should().Be("shard2");
        rig.Etcd.Txns.Should().Contain(t => t.Compare.Any(c =>
            c.Key == "/clusters/shop/buckets/routing/bucket_0"
            && c.Target == TxnTarget.Value && c.Arg == "shard1"));

        // схемы эвакуированных бакетов созданы на цели (мастер shard2 = h1:15002)
        rig.Sql.Executed.Should().Contain(e =>
            e.Dsn.Contains("Host=h1;Port=15002") && e.Sql.Contains("CREATE SCHEMA IF NOT EXISTS bucket_0"));
        rig.Sql.Executed.Should().Contain(e => e.Sql.Contains("CREATE SCHEMA IF NOT EXISTS bucket_2"));

        // ноды мёртвого шарда — QUARANTINED (контейнеры не тронуты), журнал DONE,
        // снапшоты «до» и «после» сняты (P12)
        rig.Etcd.Store["/clusters/shop/shards/shard1/nodes/shard1a/state"].Value.Should().Be("QUARANTINED");
        rig.Driver.RemovedNodes.Should().BeEmpty();
        rig.Driver.StoppedNodes.Should().BeEmpty();
        var evacuation = await rig.Journal.ReadEvacuationAsync("shop", "shard1", CancellationToken.None);
        evacuation.Value!.State.Should().Be("DONE");
        evacuation.Value.Buckets.Should().BeEquivalentTo(new Dictionary<int, string> { [0] = "shard2", [2] = "shard2" });
        rig.Snapshots.Should().HaveCount(2);
    }

    [Fact]
    public async Task Tick_CompetingRoutingFlip_TxnRejected_ConflictInJournal()
    {
        // Arrange — чужой процесс меняет routing МЕЖДУ нашим journal (E0) и flip (E2):
        // txn compare не сходится, перечитанное значение — не old и не new → конфликт
        var rig = await NewRig(port => port >= 18002 ? PatroniOk() : Down());
        var routingKey = "/clusters/shop/buckets/routing/bucket_0";
        var substituted = false;
        var baseOnPut = rig.Etcd.OnPut;
        rig.Etcd.OnPut = key =>
        {
            baseOnPut?.Invoke(key);
            if (key == "/pgworker/evacuations/shop/shard1" && !substituted)
            {
                substituted = true; // конкурент перевёл bucket_0 на shard3 сразу после E0
                rig.Etcd.Store[routingKey] = rig.Etcd.Store[routingKey] with { Value = "shard3" };
            }
        };

        // Act
        var outcome = await rig.Evacuator.TickAsync(await Snapshot(rig.Etcd), "shard1", CancellationToken.None);

        // Assert: эвакуация остановлена, конфликт зафиксирован в журнале
        outcome.IsSuccess.Should().BeFalse();
        var evacuation = await rig.Journal.ReadEvacuationAsync("shop", "shard1", CancellationToken.None);
        evacuation.Value!.State.Should().Be("CONFLICT");
        rig.Etcd.Store[routingKey].Value.Should().Be("shard3"); // чужой flip не затёрт
    }

    [Fact]
    public async Task Tick_MovingBucketInProgress_EvacuationBlocked()
    {
        // Arrange — у кластера бакет в статусе SYNCING (незавершённый переезд):
        // guard блокирует эвакуацию (arch/14 §5 D), alert — в work-журнале
        var rig = await NewRig(port => port >= 18002 ? PatroniOk() : Down());
        rig.Etcd.Seed("/clusters/shop/buckets/status/bucket_1", """{"state":"SYNCING"}""");

        // Act
        var outcome = await rig.Evacuator.TickAsync(await Snapshot(rig.Etcd), "shard1", CancellationToken.None);

        // Assert: блокировка до разбора оператором — ничего не изменено
        outcome.IsSuccess.Should().BeFalse();
        rig.Etcd.Store.Should().NotContainKey("/pgworker/evacuations/shop/shard1");
        rig.Etcd.Store["/clusters/shop/buckets/routing/bucket_0"].Value.Should().Be("shard1");
        (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.Phase.Should().Be("blocked-moving");
    }

    [Fact]
    public async Task Tick_ShardReturnedAfterDone_NodesStoppedNotRemoved()
    {
        // Arrange — эвакуация завершена ранее (journal DONE), шард «ожил» (REST 200)
        var rig = await NewRig(_ => PatroniOk()); // все пробы живы — включая вернувшийся shard1
        await rig.Journal.WriteEvacuationAsync("shop", "shard1", new EvacuationJournal(
            new Dictionary<int, string> { [0] = "shard2", [2] = "shard2" },
            "shard-dead", 1755900000, "DONE", null), CancellationToken.None);
        rig.Etcd.Seed("/clusters/shop/shards/shard1/nodes/shard1a/state", "RUNNING");

        // Act
        var outcome = await rig.Evacuator.TickAsync(await Snapshot(rig.Etcd), "shard1", CancellationToken.None);

        // Assert: карантин — docker stop БЕЗ удаления (данные на месте), state
        // QUARANTINED, journal фиксирует возврат (P1-логика «призраков»)
        outcome.Value.Should().Be(ProcessOutcome.Done);
        rig.Driver.StoppedNodes.Should().BeEquivalentTo(["shard1/shard1a", "shard1/shard1b"]);
        rig.Driver.RemovedNodes.Should().BeEmpty();
        rig.Etcd.Store["/clusters/shop/shards/shard1/nodes/shard1a/state"].Value.Should().Be("QUARANTINED");
        var evacuation = await rig.Journal.ReadEvacuationAsync("shop", "shard1", CancellationToken.None);
        evacuation.Value!.State.Should().Be("QUARANTINED");
        evacuation.Value.ReturnedUnix.Should().NotBeNull();
    }
}

// SnapshotJob (P12): /v3/snapshot/save → файл с таймштампом, ретеншн N файлов.
public class SnapshotJobTests
{
    [Fact]
    public async Task TakeAsync_WritesFileAndAppliesRetention()
    {
        // Arrange — tmp-каталог с 12 старыми снапшотами (ретеншн 10)
        var dir = Path.Combine(Path.GetTempPath(), $"pgw-snapshots-{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);
        for (var i = 0; i < 12; i++)
            await File.WriteAllTextAsync(Path.Combine(dir, $"snapshot-20200101-0000{i:00}.db"), "old", TestContext.Current.CancellationToken);

        var etcd = new Fakes.FakeEtcd();
        var job = new SnapshotJob(etcd, ["http://etcd:2379"], dir, retentionFiles: 10);

        // Act
        var result = await job.TakeAsync(TestContext.Current.CancellationToken);

        // Assert: файл создан с содержимым snapshot-байтов; старейшие сверх
        // ретеншна удалены — осталось ровно 10 файлов (новый включён)
        result.IsSuccess.Should().BeTrue();
        File.Exists(result.Value).Should().BeTrue();
        (await File.ReadAllBytesAsync(result.Value, TestContext.Current.CancellationToken)).Should().BeEquivalentTo([1, 2, 3]);
        Directory.GetFiles(dir, "snapshot-*.db").Should().HaveCount(10);
        Directory.Delete(dir, recursive: true);
    }

    // MaintainAsync: первый запуск — compact с ревизией из status + defrag каждой ноды.
    [Fact]
    public async Task MaintainAsync_FirstRun_CompactsAndDefragsEachNode()
    {
        // Arrange — два endpoint, static-состояние сброшено
        SnapshotJobTests.ResetTestState();
        var etcd = new Fakes.FakeEtcd { StatusRevision = 77 };
        var job = new SnapshotJob(
            etcd, ["http://etcd1:2379", "http://etcd2:2379"], "/snapshots", retentionFiles: 10, maintenanceIntervalMin: 60);

        // Act
        var result = await job.MaintainAsync(TestContext.Current.CancellationToken);

        // Assert — compact вызван с ревизией из status; defrag на каждой ноде последовательно
        result.IsSuccess.Should().BeTrue();
        etcd.StatusCalls.Should().ContainSingle().Which.Should().Be("http://etcd1:2379");
        etcd.CompactCalls.Should().ContainSingle().Which.Should().Be(("http://etcd1:2379", 77L));
        etcd.DefragmentCalls.Should().Equal(["http://etcd1:2379", "http://etcd2:2379"]);
    }

    // MaintainAsync: повторный вызов в пределах интервала — пропускает процедуру.
    [Fact]
    public async Task MaintainAsync_WithinInterval_SkipsProcedure()
    {
        // Arrange — static-состояние сброшено, затем первый запуск
        SnapshotJobTests.ResetTestState();
        var etcd = new Fakes.FakeEtcd { StatusRevision = 1 };
        var job = new SnapshotJob(etcd, ["http://etcd:2379"], "/snapshots", 10, maintenanceIntervalMin: 60);

        await job.MaintainAsync(TestContext.Current.CancellationToken);
        etcd.StatusCalls.Clear();
        etcd.CompactCalls.Clear();
        etcd.DefragmentCalls.Clear();

        // Act — повторный вызов сразу после первого
        var result = await job.MaintainAsync(TestContext.Current.CancellationToken);

        // Assert — процедура пропущена, вызовов нет
        result.IsSuccess.Should().BeTrue();
        etcd.StatusCalls.Should().BeEmpty();
        etcd.CompactCalls.Should().BeEmpty();
        etcd.DefragmentCalls.Should().BeEmpty();
    }

    // MaintainAsync: дефрагментация строго последовательно — порядок вызовов = порядок endpoints.
    [Fact]
    public async Task MaintainAsync_DefragIsStrictlySequential()
    {
        // Arrange — три endpoint, static-состояние сброшено
        SnapshotJobTests.ResetTestState();
        var etcd = new Fakes.FakeEtcd { StatusRevision = 10 };
        var endpoints = new[] { "http://e1:2379", "http://e2:2379", "http://e3:2379" };
        var job = new SnapshotJob(etcd, endpoints, "/snapshots", 10, maintenanceIntervalMin: 60);

        // Act
        await job.MaintainAsync(TestContext.Current.CancellationToken);

        // Assert — defrag вызван в порядке endpoints (не параллельно)
        etcd.DefragmentCalls.Should().HaveCount(3);
        etcd.DefragmentCalls.Should().BeInAscendingOrder();
        etcd.DefragmentCalls.Should().Equal(endpoints);
    }

    internal static void ResetTestState() => SnapshotJob.ResetMaintenanceState();
}
