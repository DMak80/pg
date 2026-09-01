using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using PgWorker.Core.Model;
using PgWorker.Core.Planning;
using PgWorker.Core.Templates;
using PgWorker.Docker.Drivers;
using PgWorker.Etcd.Coordination;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Probes;
using PgWorker.Provisioning.Processes;
using PgWorker.UnitTests.Provisioning;
using Xunit;

namespace PgWorker.UnitTests.Provisioning;

// AdoptionProcess AD0–AD4 (adopt-repair spec §3.2): усыновление Active-кластера
// с dsn-шардами без portalloc — адреса из HA-контура + docker-инспекции.
public class AdoptionProcessTests
{
    private const string Ep = "http://etcd:2379";
    private static readonly InstallSecrets Secrets = new("su-pw", "sb-pw", "adm-pw", "mov-pw");

    // Сид Active-кластера demo: конфиг + dsn-шарды (master-ключи byName-формата).
    private static async Task<ClusterSnapshot> SnapshotActive(
        Fakes.FakeEtcd etcd, string[] shards, string[] membersShards)
    {
        etcd.Seed("/clusters/demo/config",
            """{"buckets":12,"dbname":"demo","created_unix":1755900000}""");
        foreach (var shard in shards)
        {
            etcd.Seed($"/clusters/demo/shards/{shard}/replicas", "2");
            etcd.Seed($"/clusters/demo/shards/{shard}/dsn",
                $"host=local port=5433 dbname=demo user=bucket_admin");
            etcd.Seed($"/clusters/demo/shards/{shard}/master", $"{shard}a:5433");
            if (membersShards.Contains(shard))
            {
                etcd.Seed($"/service/demo-{shard}/members/{shard}a", """{"role":"replica","state":"running"}""");
                etcd.Seed($"/service/demo-{shard}/members/{shard}b", """{"role":"replica","state":"running"}""");
            }
        }

        var range = await etcd.RangeAsync(Ep, "/clusters/", CancellationToken.None);
        var parsed = ClusterSnapshotParser.ParseClusters(range.Value, out _);
        return parsed.Value.Single(c => c.Config.Cluster == "demo");
    }

    // Значение ключа (string?) — хелпер поверх FakeEtcd.GetAsync.
    private static async Task<string?> GetValueAsync(Fakes.FakeEtcd etcd, string key)
    {
        var result = await etcd.GetAsync(Ep, key, CancellationToken.None);
        return result.Value?.Value;
    }

    // Стенд процесса: фейковый etcd + фейковый драйвер с InspectResult + FakeSql;
    // ensurer'ы реальные (put-if-absent поверх FakeEtcd), Patroni-проба молчит.
    private static async Task<(AdoptionProcess Process, Fakes.FakeSql Sql, Fakes.FakeDriver Driver)> NewAdoption(
        Fakes.FakeEtcd etcd,
        IReadOnlyDictionary<string, DiscoveredNode> inspect)
    {
        var claims = new ClaimStore([Ep], etcd, TimeProvider.System);
        await claims.TryClaimClusterAsync("demo", CancellationToken.None);
        var driver = new Fakes.FakeDriver { InspectResult = inspect };
        var sql = new Fakes.FakeSql();
        var process = new AdoptionProcess(
            etcd, [Ep], driver,
            new ShardEndpoints(etcd, [Ep], new ShardProbe(new HttpClient())),
            sql,
            new AppSecretEnsurer(etcd, [Ep]),
            new AppParamsEnsurer(etcd, [Ep], "sslmode=require"),
            Secrets, claims, new WorkJournal(etcd, [Ep]),
            new PortAllocIndex(etcd, [Ep], NullLogger<PortAllocIndex>.Instance),
            new PlacementOptions(15000, 15100, PatroniBootSec: 600),
            new EtcdEndpoints([Ep]),
            snapshot: null);
        return (process, sql, driver);
    }

    // Журнал-записыватель: хук OnPut FakeEtcd разбирает WorkState /pgworker/work/*.
    // (WorkJournal пишет в etcd, отдельного интерфейса нет — слушаем puts.)
    private sealed class RecordingJournal
    {
        public readonly List<(string Op, string Phase, string Message)> Entries = [];

        public void Attach(Fakes.FakeEtcd etcd) => etcd.OnPut = key =>
        {
            if (!key.StartsWith("/pgworker/work/", StringComparison.Ordinal))
                return;
            using var doc = JsonDocument.Parse(etcd.Store[key].Value);
            var root = doc.RootElement;
            Entries.Add((
                root.GetProperty("op").GetString() ?? "",
                root.GetProperty("phase").GetString() ?? "",
                root.TryGetProperty("last_error", out var err) ? err.GetString() ?? "" : ""));
        };
    }

    [Fact]
    public async Task TickAsync_FullPortalloc_NoOpButRolesEnsured()
    {
        // Arrange: кластер Active, у обоих шардов dsn, portalloc полон — усыновлять
        // нечего; НО инвариант «воркер — хозяин» всё равно прогоняет ensure БД/ролей
        // (падение ensure после записи portalloc больше не теряется навсегда).
        var etcd = new Fakes.FakeEtcd();
        var snap = await SnapshotActive(etcd, ["s1", "s2"], []);
        etcd.Seed("/pgworker/portalloc/demo",
            """{"s1/s1a":{"host":"local","pg":15432,"patroni":18008,"doorman":16432}}""");
        var (adoption, sql, _) = await NewAdoption(etcd, new Dictionary<string, DiscoveredNode>());

        // Act
        var outcome = await adoption.TickAsync(snap, CancellationToken.None);

        // Assert: Done, portalloc не перезаписан, nodes-ключи не тронуты — но
        // гварды ролей (app/bucket_admin/bucket_mover) и ensure БД исполнены.
        Assert.True(outcome.IsSuccess);
        Assert.Equal(ProcessOutcome.Done, outcome.Value);
        Assert.Null(await GetValueAsync(etcd, "/clusters/demo/shards/s1/nodes/s1a/state"));
        Assert.Contains(sql.EnsuredDatabases, e => e.DbName == "demo");
        Assert.Contains(sql.Scalars, s =>
            s.Sql.Contains("pg_roles", StringComparison.Ordinal) && s.Sql.Contains("rolname", StringComparison.Ordinal));
    }

    [Fact]
    public async Task TickAsync_ExternalShard_MergesPortallocWithObject()
    {
        // Arrange: members /service/demo-s1/members/{s1a,s1b} живы, portalloc пуст,
        // драйвер нашёл as-контейнеры (5433/8011 и 5434/8012).
        var etcd = new Fakes.FakeEtcd();
        var snap = await SnapshotActive(etcd, ["s1"], ["s1"]);
        var (adoption, _, _) = await NewAdoption(etcd, new Dictionary<string, DiscoveredNode>
        {
            ["s1a"] = new("s1a", "local", "as-s1a", 5433, 8011, 0),
            ["s1b"] = new("s1b", "local", "as-s1b", 5434, 8012, 0),
        });

        // Act
        var outcome = await adoption.TickAsync(snap, CancellationToken.None);

        // Assert: portalloc дополнен записями с object; nodes-ключи = RUNNING.
        Assert.True(outcome.IsSuccess);
        Assert.Equal(ProcessOutcome.Done, outcome.Value);
        var raw = await GetValueAsync(etcd, "/pgworker/portalloc/demo");
        Assert.NotNull(raw);
        Assert.Contains("\"object\":\"as-s1a\"", raw);
        Assert.Contains("\"object\":\"as-s1b\"", raw);
        Assert.Equal("RUNNING", await GetValueAsync(etcd, "/clusters/demo/shards/s1/nodes/s1a/state"));
        Assert.Equal("RUNNING", await GetValueAsync(etcd, "/clusters/demo/shards/s1/nodes/s1b/state"));
    }

    [Fact]
    public async Task TickAsync_NoContainersFound_SilentSkip()
    {
        // Arrange: members есть, docker находок 0 — не наш docker-домен (spec §2.5).
        var etcd = new Fakes.FakeEtcd();
        var snap = await SnapshotActive(etcd, ["s1"], ["s1"]);
        var (adoption, _, _) = await NewAdoption(etcd, new Dictionary<string, DiscoveredNode>());

        // Act
        var outcome = await adoption.TickAsync(snap, CancellationToken.None);

        // Assert: Done (тихий skip) — portalloc/nodes не тронуты, журнала adopt нет.
        Assert.True(outcome.IsSuccess);
        Assert.Equal(ProcessOutcome.Done, outcome.Value);
        Assert.Null(await GetValueAsync(etcd, "/pgworker/portalloc/demo"));
        Assert.Null(await GetValueAsync(etcd, "/clusters/demo/shards/s1/nodes/s1a/state"));
    }

    [Fact]
    public async Task TickAsync_PartialDiscovery_JournalsSkippedNodes()
    {
        // Arrange: members s1a+s1b, инспекция опознала только s1a (s1b —
        // неоднозначный матчинг двух контейнеров, безопасный пропуск spec §3.1).
        var etcd = new Fakes.FakeEtcd();
        var snap = await SnapshotActive(etcd, ["s1"], ["s1"]);
        var journal = new RecordingJournal();
        journal.Attach(etcd);
        var (adoption, _, _) = await NewAdoption(etcd, new Dictionary<string, DiscoveredNode>
        {
            ["s1a"] = new("s1a", "local", "as-s1a", 5433, 8011, 0),
        });

        // Act
        var outcome = await adoption.TickAsync(snap, CancellationToken.None);

        // Assert: s1a усыновлена; в журнале adopt/skipped с именем s1b (оператор
        // видит, какая нода не усыновлена и почему — spec §3.1 «journal-запись»).
        Assert.True(outcome.IsSuccess);
        Assert.Equal(ProcessOutcome.Done, outcome.Value);
        var skipped = journal.Entries.Single(e => e.Phase == "skipped");
        Assert.Equal("adopt", skipped.Op);
        Assert.Contains("s1b", skipped.Message);
    }

    // AAA: Д2 — фальш-Active (portalloc/dsn на чужие порты — наследие коллизии):
    // первый тик с живой docker-картиной репарирует адреса фактом и пересобирает dsn
    [Fact]
    public async Task TickAsync_DivergedPortalloc_FactRepairsAddressesAndDsn()
    {
        // Arrange: Active demo, шард s1 с dsn; HA-members /service/demo-s1/members/{s1a,s1b}
        // сеются SnapshotActive ДО парсинга — кандидаты репарации непусты БЕЗ nodes-ключей
        // (Nodes строятся только из nodes/<n>/state, сид AFTER-парсинга парсер бы не увидел —
        // ревью). Запись s1/s1a РАСХОДИТСЯ с фактом (15014 — наследие коллизии).
        var etcd = new Fakes.FakeEtcd();
        var snap = await SnapshotActive(etcd, ["s1"], ["s1"]);
        etcd.Seed("/pgworker/portalloc/demo",
            """
            {"s1/s1a":{"host":"h1","pg":15014,"patroni":18014,"doorman":16514},
            "s1/s1b":{"host":"h2","pg":15005,"patroni":18005,"doorman":16505}}
            """);
        var journal = new RecordingJournal();
        journal.Attach(etcd);
        var (process, _, _) = await NewAdoption(etcd, new Dictionary<string, DiscoveredNode>
        {
            ["s1a"] = new("s1a", "h1", "pgw-demo-s1-s1a", 15004, 18004, 16504),
            ["s1b"] = new("s1b", "h2", "pgw-demo-s1-s1b", 15005, 18005, 16505),
        });

        // Act
        var outcome = await process.TickAsync(snap, CancellationToken.None);

        // Assert: запись перезаписана фактом; dsn пересобран из фактического portalloc
        // (по кандидатам nodes ∪ members); обе репарации в журнале (Д2, AD2').
        outcome.IsSuccess.Should().BeTrue();
        (await GetValueAsync(etcd, "/pgworker/portalloc/demo")).Should().Contain("\"pg\":15004");
        (await GetValueAsync(etcd, "/pgworker/portalloc/demo")).Should().NotContain("\"pg\":15014");
        (await GetValueAsync(etcd, "/clusters/demo/shards/s1/dsn"))
            .Should().Contain("port=15004,15005");
        journal.Entries.Should().Contain(e => e.Phase == "repaired-portalloc");
        journal.Entries.Should().Contain(e => e.Phase == "repaired-dsn");
    }

    // AAA: Д2 — сходящийся кластер: адреса/dsn соответствуют факту — мутаций нет
    [Fact]
    public async Task TickAsync_AddressesMatchFact_NoRepairMutations()
    {
        // Arrange: portalloc == факт контейнеров; dsn уже равен пересобранному из
        // факта по кандидатам members (креды P2.5: bucket_admin + глобальный password).
        var etcd = new Fakes.FakeEtcd();
        var snap = await SnapshotActive(etcd, ["s1"], ["s1"]);
        etcd.Seed("/clusters/demo/shards/s1/dsn",
            "host=h1,h2 port=15004,15005 dbname=demo user=bucket_admin password=adm-pw");
        // перечитываем снапшот: сид dsn обязан попасть в ClusterSnapshot.Dsn до тика
        // (репарация сравнивает dsn снапшота с пересобранным из факта).
        var range = await etcd.RangeAsync(Ep, "/clusters/", CancellationToken.None);
        snap = ClusterSnapshotParser.ParseClusters(range.Value, out _).Value.Single(c => c.Config.Cluster == "demo");
        etcd.Seed("/pgworker/portalloc/demo",
            """
            {"s1/s1a":{"host":"h1","pg":15004,"patroni":18004,"doorman":16504},
            "s1/s1b":{"host":"h2","pg":15005,"patroni":18005,"doorman":16505}}
            """);
        var journal = new RecordingJournal();
        journal.Attach(etcd);
        var (process, _, _) = await NewAdoption(etcd, new Dictionary<string, DiscoveredNode>
        {
            ["s1a"] = new("s1a", "h1", "pgw-demo-s1-s1a", 15004, 18004, 16504),
            ["s1b"] = new("s1b", "h2", "pgw-demo-s1-s1b", 15005, 18005, 16505),
        });

        // Act
        var outcome = await process.TickAsync(snap, CancellationToken.None);

        // Assert: никаких repaired-фаз, version portalloc-ключа не выросла (идемпотентность).
        outcome.IsSuccess.Should().BeTrue();
        journal.Entries.Should().NotContain(e => e.Phase.StartsWith("repaired"));
        etcd.Store["/pgworker/portalloc/demo"].Version.Should().Be(1);
    }

    // AAA: живой-Ф7/Д2 — Running-нода с Created-черепком (create по битому плану
    // прошёл, start упал): записи без ЖИВОГО контейнера чинятся EnsureNode НАПРЯМУЮ
    // (мимо процессного скипа RUNNING и running-only инспекции) — recreated-node
    [Fact]
    public async Task TickAsync_RunningNodeWithDeadContainer_EnsureNodeRecreates()
    {
        // Arrange: portalloc полный; живой контейнер ТОЛЬКО s1b; s1a — Created-черепок
        // (running-инспекцией невидим), node state=RUNNING (процессные пути скипают).
        var etcd = new Fakes.FakeEtcd();
        var snap = await SnapshotActive(etcd, ["s1"], ["s1"]);
        etcd.Seed("/clusters/demo/shards/s1/nodes/s1a/state", "RUNNING");
        etcd.Seed("/pgworker/portalloc/demo",
            """
            {"s1/s1a":{"host":"h1","pg":15004,"patroni":18004,"doorman":16504},
            "s1/s1b":{"host":"h2","pg":15005,"patroni":18005,"doorman":16505}}
            """);
        var journal = new RecordingJournal();
        journal.Attach(etcd);
        var (process, _, driver) = await NewAdoption(etcd, new Dictionary<string, DiscoveredNode>
        {
            ["s1b"] = new("s1b", "h2", "pgw-demo-s1-s1b", 15005, 18005, 16505),
        });

        // Act
        var outcome = await process.TickAsync(snap, CancellationToken.None);

        // Assert: EnsureNode вызван для s1a (сверка портов → пересоздание черепка),
        // фаза recreated-node; живая s1b не тронута.
        outcome.IsSuccess.Should().BeTrue();
        driver.EnsuredNodes.Should().Contain("s1/s1a");
        driver.EnsuredNodes.Should().NotContain("s1/s1b");
        journal.Entries.Should().Contain(e => e.Phase == "recreated-node");
    }

    // AAA: Д2 — transport-провал инспекции = transient: тик не ронывается, мутаций адресов нет
    [Fact]
    public async Task TickAsync_InspectTransportFails_TickSurvives()
    {
        // Arrange: portalloc полный по обоим HA-members → missing пуст (основной AD1-путь
        // инспекцию НЕ зовёт) — docker-сбой ловит ТОЛЬКО AD2'-инспекция по кандидатам.
        var etcd = new Fakes.FakeEtcd();
        var snap = await SnapshotActive(etcd, ["s1"], ["s1"]);
        etcd.Seed("/pgworker/portalloc/demo",
            """
            {"s1/s1a":{"host":"h1","pg":15004,"patroni":18004,"doorman":16504},
            "s1/s1b":{"host":"h2","pg":15005,"patroni":18005,"doorman":16505}}
            """);
        var journal = new RecordingJournal();
        journal.Attach(etcd);
        var (process, _, driver) = await NewAdoption(etcd, new Dictionary<string, DiscoveredNode>());
        driver.InspectFault = new ApplicationException("docker: connection refused");

        // Act
        var outcome = await process.TickAsync(snap, CancellationToken.None);

        // Assert: тик жив (Done по инварианту ролей), адреса не тронуты (version та же),
        // repaired-фаз нет — следующий тик повторит сверку.
        outcome.IsSuccess.Should().BeTrue();
        journal.Entries.Should().NotContain(e => e.Phase.StartsWith("repaired"));
        etcd.Store["/pgworker/portalloc/demo"].Version.Should().Be(1);
    }
}
