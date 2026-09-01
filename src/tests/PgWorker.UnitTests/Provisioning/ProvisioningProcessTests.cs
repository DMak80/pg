using System.Net;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Docker.Drivers;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Processes;
using PgWorker.Provisioning.Probes;

namespace PgWorker.UnitTests.Provisioning;

// ProvisioningProcess P0–P5 (задача 19; arch/14 §5 A): guard входа, план
// placement+portalloc, EnsureNode×ноды, ожидание Patroni, БД/роли/схемы, dsn,
// снятие status-ключей, config без state, снапшот, journal-фазы.
public class ProvisioningProcessTests
{
    private static readonly InstallSecrets Secrets = new("su-pw", "sb-pw", "adm-pw", "mov-pw");
    private static readonly EtcdEndpoints EtcdEndp = new(["http://etcd:2379"]);
    private static readonly PlacementOptions Opts = new(15000, 15100, PatroniBootSec: 600);
    private const string Ep = "http://etcd:2379";

    private static void SeedCluster(Fakes.FakeEtcd etcd)
    {
        etcd.Seed("/clusters/shop/config",
            """{"buckets":4,"dbname":"shop","created_unix":1755900000,"state":"NOT_INITIALIZED"}""");
        etcd.Seed("/clusters/shop/shards/shard1/replicas", "2");
        etcd.Seed("/clusters/shop/shards/shard2/replicas", "2");
        etcd.Seed("/clusters/shop/buckets/routing/bucket_0", "shard1");
        etcd.Seed("/clusters/shop/buckets/routing/bucket_1", "shard2");
        etcd.Seed("/clusters/shop/buckets/routing/bucket_2", "shard1");
        etcd.Seed("/clusters/shop/buckets/routing/bucket_3", "shard2");
        etcd.Seed("/clusters/shop/buckets/status/bucket_0", """{"state":"NOT_INITIALIZED"}""");
        etcd.Seed("/clusters/shop/buckets/status/bucket_1", """{"state":"NOT_INITIALIZED"}""");
        etcd.Seed("/clusters/shop/buckets/status/bucket_2", """{"state":"NOT_INITIALIZED"}""");
        etcd.Seed("/clusters/shop/buckets/status/bucket_3", """{"state":"NOT_INITIALIZED"}""");
        etcd.Seed("/clusters/shop/shards/shard1/nodes/shard1a/state", "NOT_INITIALIZED");
        etcd.Seed("/clusters/shop/shards/shard1/nodes/shard1b/state", "NOT_INITIALIZED");
        etcd.Seed("/clusters/shop/shards/shard2/nodes/shard2a/state", "NOT_INITIALIZED");
        etcd.Seed("/clusters/shop/shards/shard2/nodes/shard2b/state", "NOT_INITIALIZED");
        // Patroni DCS поднявшихся шардов (P2.2: initialize + leader)
        etcd.Seed("/service/shop-shard1/initialize", "7403705125687833961");
        etcd.Seed("/service/shop-shard1/leader", """{"name":"shard1a","poll_queued_commands":0}""");
        etcd.Seed("/service/shop-shard2/initialize", "7403705125687833962");
        etcd.Seed("/service/shop-shard2/leader", """{"name":"shard2a","poll_queued_commands":0}""");
    }

    // Снапшот кластера из имитации etcd (как это сделает цикл задачи 23).
    private static async Task<ClusterSnapshot> Snapshot(Fakes.FakeEtcd etcd)
    {
        var range = await etcd.RangeAsync(Ep, "/clusters/", CancellationToken.None);
        var parsed = ClusterSnapshotParser.ParseClusters(range.Value, out _);
        return parsed.Value.Single(c => c.Config.Cluster == "shop");
    }

    // Patroni-проба с управляемым ответом (живой шард или глухой); ответ по порту ноды.
    private static ShardProbe Probe(Func<int, HttpResponseMessage> respondByPort, List<int>? trace = null)
        => new(new HttpClient(new FakeHandler(r =>
        {
            var port = r.RequestUri!.Port;
            lock (trace ?? new object())
            {
                trace?.Add(port);
            }

            return respondByPort(port);
        })));

    private sealed class FakeHandler(Func<HttpRequestMessage, HttpResponseMessage> responder) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
            => Task.FromResult(responder(request));
    }

    private static HttpResponseMessage Patroni(string masterName) => new()
    {
        StatusCode = HttpStatusCode.OK,
        Content = new StringContent(
            $$"""{"members":[{"name":"{{masterName}}","role":"master","state":"running"},{"name":"other","role":"replica","state":"streaming"}]}""",
            Encoding.UTF8,
            "application/json"),
    };

    private static HttpResponseMessage DeadPatroni() => new(HttpStatusCode.InternalServerError);

    // Находка инспекции: имя узла игнорируется (матчится по карте драйвера).
    private static DiscoveredNode Node(string host, string obj, int pg, int patroni, int doorman = 16500)
        => new("ignored", host, obj, pg, patroni, doorman);

    private sealed record Rig(Fakes.FakeEtcd Etcd, Fakes.FakeDriver Driver, Fakes.FakeSql Sql,
        ClaimStore Claims, WorkJournal Journal, ProvisioningProcess Process);

    private static async Task<Rig> NewRig(
        Func<int, HttpResponseMessage> patroniResponse, List<int>? trace = null, PlacementOptions? opts = null)
    {
        var etcd = new Fakes.FakeEtcd();
        SeedCluster(etcd);
        var claims = new ClaimStore([Ep], etcd, TimeProvider.System);
        await claims.TryClaimClusterAsync("shop", CancellationToken.None);
        var journal = new WorkJournal(etcd, [Ep]);
        var driver = new Fakes.FakeDriver();
        var sql = new Fakes.FakeSql();
        var appSecret = new AppSecretEnsurer(etcd, [Ep]);
        var portAlloc = new PortAllocIndex(etcd, [Ep], NullLogger<PortAllocIndex>.Instance);
        var process = new ProvisioningProcess(
            etcd, [Ep], driver, sql, Probe(patroniResponse, trace), claims, journal, opts ?? Opts, Secrets,
            appSecret, new AppParamsEnsurer(etcd, [Ep], "sslmode=require"), EtcdEndp, portAlloc, snapshot: null);
        return new Rig(etcd, driver, sql, claims, journal, process);
    }

    [Fact]
    public async Task Tick_FreshCluster_EnsureNodesThenInProgressWaitingPatroni()
    {
        // Arrange — чистый NOT_INITIALIZED; Patroni молчит (500 на все пробы)
        var rig = await NewRig(_ => DeadPatroni());

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: тик дошёл до P2.2 и ждёт Patroni; все ноды созданы и PROVISIONING
        outcome.IsSuccess.Should().BeTrue();
        outcome.Value.Should().Be(ProcessOutcome.InProgress);
        rig.Driver.EnsuredNodes.Should().BeEquivalentTo(
        [
            "shard1/shard1a", "shard1/shard1b", "shard2/shard2a", "shard2/shard2b",
        ]);
        rig.Etcd.Store["/clusters/shop/shards/shard1/nodes/shard1a/state"].Value.Should().Be("PROVISIONING");
        rig.Etcd.Store["/clusters/shop/shards/shard2/nodes/shard2b/state"].Value.Should().Be("PROVISIONING");
        var work = await rig.Journal.ReadAsync("shop", CancellationToken.None);
        work.Value!.Phase.Should().Be("waiting-patroni");
        work.Value.Op.Should().Be("provision");
    }

    [Fact]
    public async Task Tick_PatroniAlive_DoesEverythingToDone()
    {
        // Arrange — Patroni жив: master шарда1 = shard1a, шарда2 = shard2a
        var rig = await NewRig(port => Patroni(port == 18000 ? "shard1a" : "shard2a"));
        await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);
        rig.Driver.EnsuredNodes.Should().HaveCount(4); // первый тик создал ноды

        // Act — тик при живом Patroni (master по имени ноды через probe)
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);


        // Assert: DONE — dsn записан, БД/роли/схемы исполнены, статус-ключи
        // сняты, config перезаписан без state (txn compare mod_revision)
        outcome.IsSuccess.Should().BeTrue();
        outcome.Value.Should().Be(ProcessOutcome.Done);

        // placement: анти-аффинити → ноды шарда на h1,h2; на каждом хосте свой base
        // (шард1: 15000, шард2: 15001 — тройки портов не пересекаются на хосте)
        rig.Etcd.Store["/clusters/shop/shards/shard1/dsn"].Value.Should()
            .Be("host=h1,h2 port=15000,15000 dbname=shop user=bucket_admin password=adm-pw");
        rig.Etcd.Store["/clusters/shop/shards/shard2/dsn"].Value.Should()
            .Be("host=h1,h2 port=15001,15001 dbname=shop user=bucket_admin password=adm-pw");

        // portalloc закреплён (ключ создан txn-ом NotExists)
        rig.Etcd.Txns.Should().Contain(t => t.Compare.Any(c =>
            c.Key == "/pgworker/portalloc/shop" && c.Target == TxnTarget.Version && c.Num == 0));

        // БД создаётся на мастере каждого шарда (shard1a → h1:15000, shard2a → h1:15001);
        // оба тика повторяют вызовы — SQL идемпотентен (повтор безопасен, §7)
        rig.Sql.EnsuredDatabases.Should().Contain(
            ("Host=h1;Port=15000;Database=postgres;Username=postgres;Password=su-pw;SSL Mode=Require;Trust Server Certificate=true", "shop"));
        rig.Sql.EnsuredDatabases.Should().Contain(
            ("Host=h1;Port=15001;Database=postgres;Username=postgres;Password=su-pw;SSL Mode=Require;Trust Server Certificate=true", "shop"));
        rig.Sql.Executed.Should().Contain(e => e.Sql.Contains("CREATE SCHEMA IF NOT EXISTS bucket_0"));
        rig.Sql.Executed.Should().Contain(e => e.Sql.Contains("CREATE SCHEMA IF NOT EXISTS bucket_1"));

        // ноды RUNNING, статус-ключи сняты
        rig.Etcd.Store["/clusters/shop/shards/shard1/nodes/shard1a/state"].Value.Should().Be("RUNNING");
        rig.Etcd.Store.Keys.Should().NotContain(k => k.Contains("/status/bucket_"));

        // config перезаписан без state через txn compare mod_revision
        var configTxn = rig.Etcd.Txns.Should().Contain(t =>
            t.Success.OfType<TxnOp.Put>().Any(put => put.Key == "/clusters/shop/config")).Subject;
        configTxn.Compare.Should().ContainSingle(c =>
            c.Target == TxnTarget.ModRevision && c.Key == "/clusters/shop/config");
        var config = rig.Etcd.Store["/clusters/shop/config"].Value;
        config.Should().NotContain("state");
        JsonDocument.Parse(config).RootElement.GetProperty("buckets").GetInt32().Should().Be(4);

        (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.Phase.Should().Be("done");
    }

    [Fact]
    public async Task Tick_AfterDone_NoNewEnsureNodes()
    {
        // Arrange — кластер доведён до DONE, снапшот перечитан (ноды RUNNING)
        var rig = await NewRig(port => Patroni(port == 18000 ? "shard1a" : "shard2a"));
        await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);
        await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);
        var ensuredTotal = rig.Driver.EnsuredNodes.Count;
        (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.Phase.Should().Be("done");

        // Act — повторный тик по свежему снапшоту
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: идемпотентность — новых EnsureNode нет, всё ещё DONE
        outcome.IsSuccess.Should().BeTrue();
        outcome.Value.Should().Be(ProcessOutcome.Done);
        rig.Driver.EnsuredNodes.Should().HaveCount(ensuredTotal);
    }

    [Fact]
    public async Task Tick_RequestResources_PassedToEnsureNodePerShard()
    {
        // Arrange — панель заявила ресурсы только у shard1 (rework №5):
        // request_cpu=2 (ядра), request_mem=8Gi → лимиты нод shard1; ноды
        // shard2 без заявки — без лимита
        var rig = await NewRig(_ => DeadPatroni());
        rig.Etcd.Seed("/service/shop-shard1/request_cpu", "2");
        rig.Etcd.Seed("/service/shop-shard1/request_mem", "8Gi");

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert — заявка дошла до драйвера только для своего шарда
        outcome.IsSuccess.Should().BeTrue();
        rig.Driver.EnsuredDetails.Should().Contain(d =>
            d.Node == "shard1a" && d.Resources == new NodeResources(2, 8L << 30));
        rig.Driver.EnsuredDetails.Should().Contain(d =>
            d.Node == "shard1b" && d.Resources == new NodeResources(2, 8L << 30));
        rig.Driver.EnsuredDetails.Should().Contain(d => d.Node == "shard2a" && d.Resources == null);
    }

    [Fact]
    public async Task Tick_NoRoutingKeys_WaitingKeys_NoDocker()
    {
        // Arrange — полуфабрикат панели: config есть, routing-ключей нет
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/clusters/shop/config",
            """{"buckets":4,"dbname":"shop","created_unix":1755900000,"state":"NOT_INITIALIZED"}""");
        etcd.Seed("/clusters/shop/shards/shard1/replicas", "2");
        etcd.Seed("/clusters/shop/shards/shard1/nodes/shard1a/state", "NOT_INITIALIZED");
        etcd.Seed("/clusters/shop/shards/shard1/nodes/shard1b/state", "NOT_INITIALIZED");
        var claims = new ClaimStore([Ep], etcd, TimeProvider.System);
        await claims.TryClaimClusterAsync("shop", CancellationToken.None);
        var journal = new WorkJournal(etcd, [Ep]);
        var driver = new Fakes.FakeDriver();
        var process = new ProvisioningProcess(
            etcd, [Ep], driver, new Fakes.FakeSql(), Probe(_ => Patroni("shard1a")),
            claims, journal, Opts, Secrets, new AppSecretEnsurer(etcd, [Ep]),
            new AppParamsEnsurer(etcd, [Ep], "sslmode=require"), EtcdEndp,
            new PortAllocIndex(etcd, [Ep], NullLogger<PortAllocIndex>.Instance), snapshot: null);

        // Act
        var outcome = await process.TickAsync(await Snapshot(etcd), CancellationToken.None);

        // Assert: guard входа — docker не трогаем, ждём доустойчивости ключей
        outcome.Value.Should().Be(ProcessOutcome.InProgress);
        driver.EnsuredNodes.Should().BeEmpty();
        etcd.Store.Should().NotContainKey("/pgworker/portalloc/shop");
        (await journal.ReadAsync("shop", CancellationToken.None)).Value!.Phase.Should().Be("waiting-keys");
    }

    [Fact]
    public async Task Tick_ConfigSwitchedToRemove_SafelyAborts()
    {
        // Arrange — R6: панель перевела кластер в TO_REMOVE до тика
        var rig = await NewRig(port => Patroni(port == 18000 ? "shard1a" : "shard2a"));
        rig.Etcd.Seed("/clusters/shop/config",
            """{"buckets":4,"dbname":"shop","created_unix":1755900000,"state":"TO_REMOVE"}""");

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: provisioning прекращается без мутаций — кластер подхватит deprovisioning
        outcome.Value.Should().Be(ProcessOutcome.InProgress);
        rig.Driver.EnsuredNodes.Should().BeEmpty();
        (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.Phase.Should().Be("aborted");
    }

    [Fact]
    public async Task Tick_CreatesAppSecretKeysAndAlignsRole()
    {
        // Arrange — Patroni жив (проход до SQL-фазы)
        var rig = await NewRig(_ => Patroni("shard1a"));

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert — P1.5: оба ключа созданы и валидны (spec §7.1)
        rig.Etcd.Store["/clusters/shop/app_user"].Value.Should().Be("app");
        rig.Etcd.Store["/clusters/shop/app_password"].Value.Should().MatchRegex("^[A-Za-z0-9]{32}$");
        // Роль app создаётся из кредов + выравнивается ALTER (SQL через фейк)
        var password = rig.Etcd.Store["/clusters/shop/app_password"].Value;
        rig.Sql.Executed.Should().Contain(s => s.Sql.Contains("ALTER ROLE \"app\" PASSWORD"));
        rig.Sql.Scalars.Should().Contain(s =>
            s.Sql.Contains($"CREATE ROLE \"app\" LOGIN PASSWORD ''{password}'''"));
    }

    [Fact]
    public async Task Tick_SqlFailure_ErrorAndJournalHaveNoAppPassword()
    {
        // Arrange — Patroni жив; SQL-исполнение падает на ALTER/схемах
        var rig = await NewRig(_ => Patroni("shard1a"));
        rig.Sql.ExecuteResult = () => Result.Failed(new ApplicationException("connection refused"));

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert — сбой не выносит app-пароль ни в текст ошибки процесса,
        // ни в last_error журнала /pgworker/work/<C> (SQL-тексты с паролем
        // в сообщения исключений не включаются — spec §4.1)
        outcome.IsSuccess.Should().BeFalse();
        var password = rig.Etcd.Store["/clusters/shop/app_password"].Value;
        outcome.Error!.ToString().Should().NotContain(password);
        var work = await rig.Journal.ReadAsync("shop", CancellationToken.None);
        work.Value!.LastError.Should().NotContain(password);
    }

    // AAA: P2.5' — после SQL-фазы шарда у КАЖДОЙ ноды есть app_params дефолта (spec §4.2)
    [Fact]
    public async Task Tick_SqlPhase_WritesNodeAppParamsForAllShardNodes()
    {
        // Arrange — Patroni жив, мастера shard1a/shard2a; первый тик создаёт ноды,
        // SQL-фаза идёт вторым тиком по свежему снапшоту (образец DoesEverythingToDone)
        var rig = await NewRig(port => Patroni(port == 18000 ? "shard1a" : "shard2a"));
        await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert — все 4 ноды двух шардов получили ключ-дефолт
        outcome.Value.Should().Be(ProcessOutcome.Done);
        foreach (var (shard, node) in new[]
                 { ("shard1", "shard1a"), ("shard1", "shard1b"), ("shard2", "shard2a"), ("shard2", "shard2b") })
            rig.Etcd.Store[$"/clusters/shop/shards/{shard}/nodes/{node}/app_params"].Value
                .Should().Be("sslmode=require");
    }

    // AAA: E2 — бюджет-фейл пишет серию ретраев; тики до retry_not_before скипаются
    [Fact]
    public async Task Tick_PatroniBudgetFail_WritesRetrySeriesAndBacksOff()
    {
        // Arrange: Patroni мёртв на все пробы, бюджет PatroniBootSec=-1 — первый же тик фейлит ожидание.
        var rig = await NewRig(_ => DeadPatroni(),
            opts: new PlacementOptions(15000, 15100, PatroniBootSec: -1, ProvisionRetryBaseSec: 5, ProvisionRetryMaxSec: 60));

        // Act: первый тик.
        var first = await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: фейл с серией fail_count=1 и retry_not_before=now+5; ноды созданы (P2.1 успел).
        first.IsSuccess.Should().BeFalse();
        var work = await rig.Journal.ReadAsync("shop", CancellationToken.None);
        work.Value!.LastError.Should().Contain("не поднялся");
        work.Value.FailCount.Should().Be(1);
        work.Value.RetryNotBeforeUnix.Should().BeGreaterThan(work.Value.UpdatedUnix - 1);
        (work.Value.RetryNotBeforeUnix!.Value - work.Value.UpdatedUnix).Should().Be(5);

        // Act: второй тик до истечения retry_not_before — skip (без новых EnsureNode и без перезаписи журнала).
        var driverCalls = rig.Driver.EnsuredNodes.Count;
        var second = await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: InProgress без мутаций; журнал не тронут (фаза/updated_unix прежние).
        second.IsSuccess.Should().BeTrue();
        second.Value.Should().Be(ProcessOutcome.InProgress);
        rig.Driver.EnsuredNodes.Count.Should().Be(driverCalls);
        var after = await rig.Journal.ReadAsync("shop", CancellationToken.None);
        after.Value!.UpdatedUnix.Should().Be(work.Value.UpdatedUnix);
    }

    // AAA: E2 — после истечения retry_not_before тик снова работает, серия нарастает
    [Fact]
    public async Task Tick_AfterRetryDeadline_FailsAgainWithIncrementedSeries()
    {
        // Arrange: серия из одного фейла; retry_not_before уже в прошлом (подделано в etcd).
        var rig = await NewRig(_ => DeadPatroni(),
            opts: new PlacementOptions(15000, 15100, PatroniBootSec: -1, ProvisionRetryBaseSec: 5, ProvisionRetryMaxSec: 60));
        await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);
        var work = await rig.Journal.ReadAsync("shop", CancellationToken.None);
        var prior = work.Value!;
        rig.Etcd.Store["/pgworker/work/shop"] = new Fakes.FakeEtcd.Entry(
            $$"""{"op":"provision","phase":"planning","instance":"{{prior.Instance}}","updated_unix":{{prior.UpdatedUnix - 100}},"last_error":"boom","fail_count":1,"fail_first_unix":{{prior.FailFirstUnix}},"retry_not_before_unix":{{prior.UpdatedUnix - 50}}}""",
            prior.UpdatedUnix - 100, 2);

        // Act: тик после дедлайна ретрая.
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: снова фейл, серия наросла (fail_count=2, delay=base·2).
        outcome.IsSuccess.Should().BeFalse();
        var state = await rig.Journal.ReadAsync("shop", CancellationToken.None);
        state.Value!.FailCount.Should().Be(2);
        (state.Value.RetryNotBeforeUnix!.Value - state.Value.UpdatedUnix).Should().Be(10);
    }

    // AAA: E2/E1 — фазы прогресса тика переносят серию до следующего фейла (миганий нет)
    [Fact]
    public async Task Tick_InProgressPhasesAfterFail_CarrySeriesUntilNextFail()
    {
        // Arrange: серия fail_count=1 с истёкшим retry; Patroni мёртв (тик дойдёт до фейла P2.2).
        var rig = await NewRig(_ => DeadPatroni(),
            opts: new PlacementOptions(15000, 15100, PatroniBootSec: -1, ProvisionRetryBaseSec: 5, ProvisionRetryMaxSec: 60));
        await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);
        var failed = await rig.Journal.ReadAsync("shop", CancellationToken.None);
        var f = failed.Value!;
        rig.Etcd.Store["/pgworker/work/shop"] = new Fakes.FakeEtcd.Entry(
            $$"""{"op":"provision","phase":"planning","instance":"{{f.Instance}}","updated_unix":{{f.UpdatedUnix - 100}},"last_error":"boom","fail_count":1,"fail_first_unix":{{f.FailFirstUnix}},"retry_not_before_unix":{{f.UpdatedUnix - 50}}}""",
            f.UpdatedUnix - 100, 2);
        // Сбор КАЖДОЙ записи work-ключа в тике (FakeEtcd.OnPut): фазы тика
        // обязаны нести серию — включая P0 «started» (ревью Ф4-2: без `, series`
        // optional-параметр молча стирает поля — provision-stuck мигает).
        var workWrites = new List<string>();
        rig.Etcd.OnPut = key =>
        {
            if (key == "/pgworker/work/shop")
                lock (workWrites)
                {
                    workWrites.Add(rig.Etcd.Store[key].Value);
                }
        };

        // Act: тик после дедлайна (FailAsync снова фейлит P2.2).
        await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);
        rig.Etcd.OnPut = null;

        // Assert: fail_first_unix ПЕРЕЖИЛ промежуточные фазы (started/planned) — серия та же, счётчик 2.
        var state = await rig.Journal.ReadAsync("shop", CancellationToken.None);
        state.Value!.FailFirstUnix.Should().Be(f.FailFirstUnix);
        state.Value.FailCount.Should().Be(2);
        // И каждая промежуточная запись тика несла поля серии (started не стирал).
        workWrites.Should().NotBeEmpty();
        workWrites.Should().OnlyContain(v => v.Contains("\"fail_count\":"));
    }

    // AAA: E3 — бюджет-фейл сбрасывает трекер ожидания: новая попытка (после
    // бэкоффа) получает полный бюджет заново, а не мгновенный фейл от протухшего
    // «первого наблюдения» (234 фейла/10 мин на живом стенде)
    [Fact]
    public async Task Tick_PatroniBudgetFail_ResetsWaitTrackerForNextAttempt()
    {
        // Arrange: мгновенный бюджет (-1) — первый же тик фейлит ожидание Patroni.
        var rig = await NewRig(_ => DeadPatroni(), opts: new PlacementOptions(15000, 15100, PatroniBootSec: -1));

        // Act
        await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: трекер бюджета очищен — следующая попытка получит полный бюджет,
        // а не мгновенный фейл от протухшего «первого наблюдения».
        var field = typeof(ProvisioningProcess).GetField("_patroniWaitSince",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
        var tracker = (System.Collections.Concurrent.ConcurrentDictionary<string, long>)field.GetValue(rig.Process)!;
        tracker.Should().BeEmpty();
    }

    // AAA: A — portalloc ПОТЕРЯН, но живые канонические контейнеры есть: каноном
    // становится фактические порты контейнеров (сценарий стенда canon10)
    [Fact]
    public async Task Tick_LostPortalloc_AdoptsActualContainerPorts()
    {
        // Arrange: portalloc ПОТЕРЯН (ключа нет), но живые контейнеры есть — инспекция
        // возвращает фактические порты канонических объектов.
        var rig = await NewRig(_ => Patroni("shard1a"));
        rig.Driver.InspectResult = new Dictionary<string, DiscoveredNode>
        {
            ["shard1a"] = Node("h1", "pgw-shop-shard1-shard1a", 15004, 18004),
            ["shard1b"] = Node("h2", "pgw-shop-shard1-shard1b", 15005, 18005),
            ["shard2a"] = Node("h1", "pgw-shop-shard2-shard2a", 15006, 18006),
            ["shard2b"] = Node("h2", "pgw-shop-shard2-shard2b", 15007, 18007),
        };

        // Act
        await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: portalloc записан ФАКТОМ контейнеров (не свежей аллокацией 15000+), без object.
        var raw = rig.Etcd.Store["/pgworker/portalloc/shop"].Value;
        raw.Should().Contain("\"shard1/shard1a\":{\"host\":\"h1\",\"pg\":15004,\"patroni\":18004,\"doorman\":16500}");
        raw.Should().NotContain("\"object\"");
    }

    // AAA: A — ПОЛНЫЙ расходящийся portalloc перезаписывается фактом контейнеров
    // (merge «только отсутствующие» дефект не чинит — spec §8.1; ревью Ф4-1:
    // расхождение полно(portalloc)↔факт — ровно состояние стенда canon10/smoke)
    [Fact]
    public async Task Tick_DivergedPortalloc_RewritesRecordWithActualFact()
    {
        // Arrange: записи ЕСТЬ, но битые (15014+ — свежая аллокация после потери);
        // факт контейнеров — 15004..15007. Расхождение → каноном становится факт.
        var rig = await NewRig(_ => Patroni("shard1a"));
        rig.Etcd.Seed("/pgworker/portalloc/shop",
            """
            {"shard1/shard1a":{"host":"h1","pg":15014,"patroni":18014,"doorman":16514},
            "shard1/shard1b":{"host":"h2","pg":15015,"patroni":18015,"doorman":16515},
            "shard2/shard2a":{"host":"h1","pg":15016,"patroni":18016,"doorman":16516},
            "shard2/shard2b":{"host":"h2","pg":15017,"patroni":18017,"doorman":16517}}
            """);
        rig.Driver.InspectResult = new Dictionary<string, DiscoveredNode>
        {
            ["shard1a"] = Node("h1", "pgw-shop-shard1-shard1a", 15004, 18004),
            ["shard1b"] = Node("h2", "pgw-shop-shard1-shard1b", 15005, 18005),
            ["shard2a"] = Node("h1", "pgw-shop-shard2-shard2a", 15006, 18006),
            ["shard2b"] = Node("h2", "pgw-shop-shard2-shard2b", 15007, 18007),
        };

        // Act
        await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: запись перезаписана фактом.
        var raw = rig.Etcd.Store["/pgworker/portalloc/shop"].Value;
        raw.Should().Contain("\"pg\":15004");
        raw.Should().NotContain("\"pg\":15014");
    }

    // AAA: A — ЧАСТИЧНЫЙ portalloc: расходящаяся запись перезаписывается фактом,
    // object-запись не тронута, недобор добран; запись put'ом (не txn)
    [Fact]
    public async Task Tick_PartialPortalloc_MergesFactKeepsObjectRecord_WritesByPut()
    {
        // Arrange: ЧАСТИЧНЫЙ portalloc (2 записи из 4): shard1a расходится (без
        // object), shard1b — object-запись (усыновлённая ранее); инспекция видит
        // все 4 канонических контейнера. app-секрет уже обеспечен (P1.5 без txn);
        // Patroni мёртв — тик останавливается на waiting-patroni (P3/P4- txn не
        // достигаются — иначе дельта txn-ассерта ниже была бы не нулевой).
        var rig = await NewRig(_ => DeadPatroni());
        rig.Etcd.Seed("/clusters/shop/app_user", "app");
        rig.Etcd.Seed("/clusters/shop/app_password", "pw");
        rig.Etcd.Seed("/pgworker/portalloc/shop",
            """
            {"shard1/shard1a":{"host":"h1","pg":15014,"patroni":18014,"doorman":16514},
            "shard1/shard1b":{"host":"h2","pg":15015,"patroni":18015,"doorman":16515,"object":"external-1"}}
            """);
        rig.Driver.InspectResult = new Dictionary<string, DiscoveredNode>
        {
            ["shard1a"] = Node("h1", "pgw-shop-shard1-shard1a", 15004, 18004),
            ["shard1b"] = Node("h2", "pgw-shop-shard1-shard1b", 15005, 18005),
            ["shard2a"] = Node("h1", "pgw-shop-shard2-shard2a", 15006, 18006),
            ["shard2b"] = Node("h2", "pgw-shop-shard2-shard2b", 15007, 18007),
        };
        var txnsBefore = rig.Etcd.Txns.Count; // клэйм NewRig уже записал свои txn

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: merge — расходящаяся запись перезаписана фактом; object-запись
        // НЕ тронута (порты и object на месте); недобор добран из факта (spec §6).
        outcome.IsSuccess.Should().BeTrue();
        var raw = rig.Etcd.Store["/pgworker/portalloc/shop"].Value;
        raw.Should().Contain("\"shard1/shard1a\":{\"host\":\"h1\",\"pg\":15004");
        raw.Should().Contain("\"shard1/shard1b\":{\"host\":\"h2\",\"pg\":15015");
        raw.Should().Contain("\"object\":\"external-1\"");
        raw.Should().Contain("\"shard2/shard2a\":{\"host\":\"h1\",\"pg\":15006");
        raw.Should().Contain("\"shard2/shard2b\":{\"host\":\"h2\",\"pg\":15007");
        // Ключ существовал → запись put'ом, НЕ txn (ревью Ф4-1): новых txn в тике нет.
        rig.Etcd.Txns.Count.Should().Be(txnsBefore);
    }

    // AAA: A/R1 — контейнер с НЕканоническим именем объекта не усыновляется:
    // обычная аллокация (skip с journal-заметкой adopt-skipped)
    [Fact]
    public async Task Tick_ForeignObjectMatch_SkippedAndAllocatedNormally()
    {
        // Arrange: инспекция нашла контейнер с НЕканоническим именем объекта — не наша находка.
        var rig = await NewRig(_ => DeadPatroni());
        rig.Driver.InspectResult = new Dictionary<string, DiscoveredNode>
        {
            ["shard1a"] = Node("h1", "weird-container", 15004, 18004),
        };

        // Act
        await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: shard1a НЕ усыновлён — обычная аллокация (первая свободная база 15000).
        var raw = rig.Etcd.Store["/pgworker/portalloc/shop"].Value;
        raw.Should().Contain("\"shard1/shard1a\":{\"host\":\"h1\",\"pg\":15000");
    }

    // AAA: A — сходящийся кластер: полный portalloc, совпадающий с фактом, НЕ
    // перезаписывается (инспекция выполняется всегда — ревью Ф4-1; идемпотентность)
    [Fact]
    public async Task Tick_FullPortallocMatchingFact_NotRewritten()
    {
        // Arrange: полный portalloc, СОВПАДАЮЩИЙ с фактом контейнеров.
        var rig = await NewRig(_ => Patroni("shard1a"));
        rig.Etcd.Seed("/pgworker/portalloc/shop",
            """
            {"shard1/shard1a":{"host":"h1","pg":15000,"patroni":18000,"doorman":16500},
            "shard1/shard1b":{"host":"h2","pg":15001,"patroni":18001,"doorman":16501},
            "shard2/shard2a":{"host":"h1","pg":15002,"patroni":18002,"doorman":16502},
            "shard2/shard2b":{"host":"h2","pg":15003,"patroni":18003,"doorman":16503}}
            """);
        rig.Driver.InspectResult = new Dictionary<string, DiscoveredNode>
        {
            ["shard1a"] = Node("h1", "pgw-shop-shard1-shard1a", 15000, 18000, 16500),
            ["shard1b"] = Node("h2", "pgw-shop-shard1-shard1b", 15001, 18001, 16501),
            ["shard2a"] = Node("h1", "pgw-shop-shard2-shard2a", 15002, 18002, 16502),
            ["shard2b"] = Node("h2", "pgw-shop-shard2-shard2b", 15003, 18003, 16503),
        };

        // Act
        await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: запись НЕ перезаписана — version ключа не выросла (ранний выход
        // шага 4 spec §3.1: полный portalloc + merge ничего не изменил).
        var entry = rig.Etcd.Store["/pgworker/portalloc/shop"];
        entry.Version.Should().Be(1);
        entry.Value.Should().Contain("\"pg\":15000");
    }

    // AAA: C — свежий provision видит закрепления соседей: база уходит от
    // чужой тройки (кросс-кластерная коллизия smoke/canon10 закрыта)
    [Fact]
    public async Task Tick_FreshCluster_AvoidsForeignPortallocRecords()
    {
        // Arrange: сосед закрепил 15000-тройку (все 3 порта); наш кластер без контейнеров.
        var rig = await NewRig(_ => Patroni("shard1a"));
        rig.Etcd.Seed("/pgworker/portalloc/canon10",
            """{"s1/n1":{"host":"h1","pg":15000,"patroni":18000,"doorman":16500}}""");

        // Act
        await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: первая нода h1 ушла от занятой тройки соседа: база 15001 даёт
        // 15001/18001/16501 — свободно.
        var raw = rig.Etcd.Store["/pgworker/portalloc/shop"].Value;
        raw.Should().Contain("\"shard1/shard1a\":{\"host\":\"h1\",\"pg\":15001");
    }

    // AAA: Д1 — порт плана занят чужим docker-фактом: нода перепланирована и
    // контейнер создаётся В ТОТ ЖЕ тик; остальные записи не тронуты
    [Fact]
    public async Task Tick_ForeignDockerBusyPort_ReplansNodeInSameTick()
    {
        // Arrange: ПОЛНЫЙ portalloc, контейнеров нет (InspectResult пуст — наследие
        // инцидента); docker-факт: порт (h1, 15000) — нода shard1a — занял чужой.
        var rig = await NewRig(_ => DeadPatroni());
        rig.Etcd.Seed("/pgworker/portalloc/shop",
            """
            {"shard1/shard1a":{"host":"h1","pg":15000,"patroni":18000,"doorman":16500},
            "shard1/shard1b":{"host":"h2","pg":15001,"patroni":18001,"doorman":16501},
            "shard2/shard2a":{"host":"h1","pg":15002,"patroni":18002,"doorman":16502},
            "shard2/shard2b":{"host":"h2","pg":15003,"patroni":18003,"doorman":16503}}
            """);
        rig.Driver.BusyPorts = new HashSet<(string, int)> { ("h1", 15000) };

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: ТОЛЬКО shard1a перепланирована (свободная тройка h1 = 15001/18001/16501),
        // остальные записи переиспользованы; EnsureNode выполнен в том же тике
        // (контейнер создаётся сразу — Д1 «сам починил» без оператора).
        outcome.IsSuccess.Should().BeTrue();
        var raw = rig.Etcd.Store["/pgworker/portalloc/shop"].Value;
        raw.Should().Contain("\"shard1/shard1a\":{\"host\":\"h1\",\"pg\":15001");
        raw.Should().Contain("\"shard1/shard1b\":{\"host\":\"h2\",\"pg\":15001");
        raw.Should().Contain("\"shard2/shard2a\":{\"host\":\"h1\",\"pg\":15002");
        rig.Driver.EnsuredNodes.Should().Contain("shard1/shard1a");
        // Ключ существовал → перезапись put-ом (не txn), version вырос.
        rig.Etcd.Store["/pgworker/portalloc/shop"].Version.Should().Be(2);
    }

    // AAA: Д1 — docker-busy СВОИХ живых контейнеров не «занимает» их же закрепления:
    // записи подтверждены фактом (adoption-находки) → ничего не перепланируется
    [Fact]
    public async Task Tick_SelfContainerDockerBusy_NotReplanned()
    {
        // Arrange: полный portalloc == факт контейнеров (все 4 ноды с явными doorman!);
        // docker-busy содержит ИХ ЖЕ порты (живые публикации своих контейнеров).
        var rig = await NewRig(_ => DeadPatroni());
        rig.Etcd.Seed("/pgworker/portalloc/shop",
            """
            {"shard1/shard1a":{"host":"h1","pg":15000,"patroni":18000,"doorman":16500},
            "shard1/shard1b":{"host":"h2","pg":15001,"patroni":18001,"doorman":16501},
            "shard2/shard2a":{"host":"h1","pg":15002,"patroni":18002,"doorman":16502},
            "shard2/shard2b":{"host":"h2","pg":15003,"patroni":18003,"doorman":16503}}
            """);
        rig.Driver.InspectResult = new Dictionary<string, DiscoveredNode>
        {
            ["shard1a"] = Node("h1", "pgw-shop-shard1-shard1a", 15000, 18000, 16500),
            ["shard1b"] = Node("h2", "pgw-shop-shard1-shard1b", 15001, 18001, 16501),
            ["shard2a"] = Node("h1", "pgw-shop-shard2-shard2a", 15002, 18002, 16502),
            ["shard2b"] = Node("h2", "pgw-shop-shard2-shard2b", 15003, 18003, 16503),
        };
        rig.Driver.BusyPorts = new HashSet<(string, int)> { ("h1", 15000), ("h1", 18000), ("h2", 15001) };

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: свои живые публикации — НЕ чужая занятость: записи не тронуты
        // (version ключа не выросла — ранний выход без записи), перепланирования нет.
        outcome.IsSuccess.Should().BeTrue();
        rig.Etcd.Store["/pgworker/portalloc/shop"].Version.Should().Be(1);
    }
}
