using System.Net;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Processes;
using PgWorker.Provisioning.Probes;
using Xunit;

namespace PgWorker.UnitTests.Provisioning;

// Ротация app-пароля по заявке /pgworker/rotations/<C> (spec §4.3, arch/14 §5 I):
// ALTER на мастерах всех шардов с dsn → атомарный txn-коммит put+del; transient-отказы.
public class AppPasswordRotatorTests
{
    private const string Ep = "http://etcd:2379";
    private static readonly InstallSecrets Secrets = new("su-pw", "sb-pw", "adm-pw", "mov-pw");

    private sealed class DeadHandler : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken ct)
            => Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable));
    }

    // Сид: Active-кластер, 2 шарда с dsn + master-ключи (мастера по host из portalloc),
    // app-секрет, portalloc (мастера — первые ноды, host h1/pg 15000 и h2/pg 15001).
    private static void SeedCluster(Fakes.FakeEtcd etcd, string cluster = "shop")
    {
        etcd.Seed($"/clusters/{cluster}/config",
            $$"""{"buckets":2,"dbname":"{{cluster}}","created_unix":1755900000}""");
        etcd.Seed("/clusters/shop/app_user", "app");
        etcd.Seed("/clusters/shop/app_password", "OldPassword000000000000000000A");
        foreach (var (shard, host, pg) in new[] { ("shard1", "h1", 15000), ("shard2", "h2", 15001) })
        {
            etcd.Seed($"/clusters/shop/shards/{shard}/replicas", "2");
            etcd.Seed($"/clusters/shop/shards/{shard}/nodes/{shard}a/state", "RUNNING");
            etcd.Seed($"/clusters/shop/shards/{shard}/nodes/{shard}b/state", "RUNNING");
            etcd.Seed($"/clusters/shop/shards/{shard}/dsn",
                $"host={host} port={pg} dbname=shop user=bucket_admin password=x");
            etcd.Seed($"/clusters/shop/shards/{shard}/master", $"{host}:16500");
            etcd.Seed($"/clusters/shop/shards/{shard}/nodes/{shard}a/app_params", "sslmode=require");
            etcd.Seed($"/clusters/shop/shards/{shard}/nodes/{shard}b/app_params", "sslmode=require");
        }

        var alloc = new Dictionary<string, NodeAddress>
        {
            ["shard1/shard1a"] = new("h1", new NodePorts(15000, 18000, 16500)),
            ["shard1/shard1b"] = new("h2", new NodePorts(15002, 18002, 16502)),
            ["shard2/shard2a"] = new("h2", new NodePorts(15001, 18001, 16501)),
            ["shard2/shard2b"] = new("h1", new NodePorts(15003, 18003, 16503)),
        };
        etcd.Seed("/pgworker/portalloc/shop", Portalloc.Serialize(alloc));
        etcd.Seed("/clusters/shop/buckets/routing/bucket_0", "shard1");
        etcd.Seed("/clusters/shop/buckets/routing/bucket_1", "shard2");
    }

    private static async Task<ClusterSnapshot> Snapshot(Fakes.FakeEtcd etcd)
    {
        var range = await etcd.RangeAsync(Ep, "/clusters/", CancellationToken.None);
        var parsed = ClusterSnapshotParser.ParseClusters(range.Value, out _);
        return parsed.Value.Single(c => c.Config.Cluster == "shop");
    }

    private sealed record Rig(Fakes.FakeEtcd Etcd, Fakes.FakeSql Sql, ClaimStore Claims,
        WorkJournal Journal, AppPasswordRotator Rotator);

    private static async Task<Rig> NewRig(Fakes.FakeEtcd? etcd = null, Fakes.FakeSql? sql = null)
    {
        var store = etcd ?? new Fakes.FakeEtcd();
        if (etcd is null)
            SeedCluster(store);
        var usedSql = sql ?? new Fakes.FakeSql();
        var claims = new ClaimStore([Ep], store, TimeProvider.System);
        await claims.TryClaimClusterAsync("shop", CancellationToken.None);
        store.Txns.Clear(); // отсечь claim-txn: ассерты — только про txn ротации
        var journal = new WorkJournal(store, [Ep]);
        var probe = new ShardProbe(new HttpClient(new DeadHandler()));
        var rotator = new AppPasswordRotator(
            store, [Ep], usedSql, probe, claims, journal, Secrets,
            new AppSecretEnsurer(store, [Ep]), snapshot: null);
        return new Rig(store, usedSql, claims, journal, rotator);
    }

    private static void SeedTicket(Fakes.FakeEtcd etcd, string raw =
        """{"requested_unix":1755900100,"requested_by":"admin"}""")
        => etcd.Seed("/pgworker/rotations/shop", raw);

    [Fact]
    public async Task Tick_NoTicket_NoOp()
    {
        // Arrange — заявки нет
        var rig = await NewRig();

        // Act
        var outcome = await rig.Rotator.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert — no-op: пароль нетронут, SQL/txn не было
        outcome.IsSuccess.Should().BeTrue();
        outcome.Value.Should().Be(ProcessOutcome.Done);
        rig.Etcd.Store["/clusters/shop/app_password"].Value
            .Should().Be("OldPassword000000000000000000A");
        rig.Sql.Executed.Should().BeEmpty();
        rig.Etcd.Txns.Should().BeEmpty();
    }

    [Fact]
    public async Task Tick_Ticket_AltersAllShardsAndCommitsAtomically()
    {
        // Arrange — заявка стоит; оба шарда с dsn, мастера известны из master-ключей
        var rig = await NewRig();
        SeedTicket(rig.Etcd);

        // Act
        var outcome = await rig.Rotator.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert — ALTER на мастерах ОБЕИХ шардов; одна txn: compare OLD + put NEW + del заявки
        outcome.IsSuccess.Should().BeTrue();
        var sqlTexts = rig.Sql.Executed.Select(e => e.Sql).ToList();
        sqlTexts.Should().HaveCount(2).And.OnlyContain(s => s.Contains("ALTER ROLE \"app\" PASSWORD"));
        rig.Etcd.Store["/clusters/shop/app_password"].Value
            .Should().MatchRegex("^[A-Za-z0-9]{32}$").And.NotBe("OldPassword000000000000000000A");
        rig.Etcd.Store.Should().NotContainKey("/pgworker/rotations/shop");
        var commit = rig.Etcd.Txns.Single(t => t.Success.Any(op => op is TxnOp.Put));
        commit.Compare.Should().Contain(c =>
            c.Key == "/clusters/shop/app_password" && c.Arg == "OldPassword000000000000000000A");
        commit.Success.OfType<TxnOp.Delete>()
            .Should().ContainSingle(d => d.Key == "/pgworker/rotations/shop");
        (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.Op
            .Should().Be("rotate-app-password");
    }

    [Fact]
    public async Task Tick_TicketShardWithoutMaster_PasswordUntouchedTicketAlive()
    {
        // Arrange — у shard2 нет master-ключа и Patroni мёртв (transient, spec §4.3 R2)
        var rig = await NewRig();
        rig.Etcd.Store.Remove("/clusters/shop/shards/shard2/master");
        SeedTicket(rig.Etcd);

        // Act
        var outcome = await rig.Rotator.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert — Failed: app_password прежний, заявка жива (ретрай тиком с начала)
        outcome.IsSuccess.Should().BeFalse();
        rig.Etcd.Store["/clusters/shop/app_password"].Value
            .Should().Be("OldPassword000000000000000000A");
        rig.Etcd.Store.Should().ContainKey("/pgworker/rotations/shop");
    }

    [Fact]
    public async Task Tick_TicketExternalPasswordChange_CompareLostRetriable()
    {
        // Arrange — внешний etcdctl меняет app_password между чтением и коммитом:
        // инъекция — перезапись ключа при ВТОРОМ ALTER (spec §4.3 R3)
        var rig = await NewRig();
        SeedTicket(rig.Etcd);
        var alters = 0;
        rig.Sql.OnExecute = _ =>
        {
            if (++alters == 2)
                rig.Etcd.Seed("/clusters/shop/app_password", "External0000000000000000000000X");
        };

        // Act
        var outcome = await rig.Rotator.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert — compare проигран: заявка жива, значение = внешнее (ретрай тиком)
        outcome.IsSuccess.Should().BeFalse();
        rig.Etcd.Store["/clusters/shop/app_password"].Value
            .Should().Be("External0000000000000000000000X");
        rig.Etcd.Store.Should().ContainKey("/pgworker/rotations/shop");
    }

    [Fact]
    public async Task Tick_MalformedTicket_RemovedAsGarbage()
    {
        // Arrange — битая заявка-мусор (не-JSON, spec §4.3 R0/arch §5 I)
        var rig = await NewRig();
        SeedTicket(rig.Etcd, "not-json");

        // Act
        var outcome = await rig.Rotator.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert — удалена с journal-записью; пароль/SQL не тронуты
        outcome.IsSuccess.Should().BeTrue();
        rig.Etcd.Store.Should().NotContainKey("/pgworker/rotations/shop");
        rig.Sql.Executed.Should().BeEmpty();
        (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.Phase
            .Should().Be("malformed-ticket-removed");
    }

    [Fact]
    public async Task Tick_ClaimNotMine_MutationsForbidden()
    {
        // Arrange — заявка есть, клэйм не взят (инвариант мутаций /clusters/)
        var etcd = new Fakes.FakeEtcd();
        SeedCluster(etcd);
        SeedTicket(etcd);
        var journal = new WorkJournal(etcd, [Ep]);
        var probe = new ShardProbe(new HttpClient(new DeadHandler()));
        var rotator = new AppPasswordRotator(
            etcd, [Ep], new Fakes.FakeSql(), probe,
            new ClaimStore([Ep], etcd, TimeProvider.System), journal, Secrets,
            new AppSecretEnsurer(etcd, [Ep]), snapshot: null);

        // Act
        var outcome = await rotator.TickAsync(await Snapshot(etcd), CancellationToken.None);

        // Assert — отказ до любых мутаций
        outcome.IsSuccess.Should().BeFalse();
        etcd.Txns.Should().BeEmpty();
        etcd.Store["/clusters/shop/app_password"].Value
            .Should().Be("OldPassword000000000000000000A");
    }
}
