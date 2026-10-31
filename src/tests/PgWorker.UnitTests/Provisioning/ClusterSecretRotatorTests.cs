using System.Net;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Core.Tuning;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Processes;
using PgWorker.Provisioning.Probes;
using PgWorker.UnitTests.Templates;
using Shared.Core;
using Shared.Etcd.Client;
using Xunit;

namespace PgWorker.UnitTests.Provisioning;

// Ротация per-cluster секретов по заявке /pgworker/rotations/<C> — ETCD-FIRST
// (arch/14 §5 I, t05 рев. 8): R2 txn-коммит NEW-четвёрки В НАЧАЛЕ окна (флаг
// rest_window_open), R3 rolling rest-пары каналом БЕЗ пересоздания
// (bootstrap-перерендер ключа ОДНОЙ ноды/тик, реплики вперёд; fake-агент
// публикует статусные ключи по факту «применения»), R4 финальный ALTER
// app/bucket_admin/bucket_mover на мастерах (закрытие окна). В окне 28P01 —
// транзиент (проба initializing→ready waiting-повтор). Окно — по статусным
// ключам (env-инспекции нет): реальный NodeBootstrapDelivery + fake-канал
// на Fakes.FakeEtcd.
public class ClusterSecretRotatorTests
{
    private const string Ep = "http://etcd:2379";
    private static readonly InstallSecrets Secrets = new("su-pw", "sb-pw", "adm-pw", "mov-pw");

    private sealed class DeadHandler : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken ct)
            => Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable));
    }

    // Сид: Active-кластер, 2 шарда с dsn + master-ключи (формат писателя —
    // <host>:<doormanPort> фактической ноды-мастера), тройка per-cluster кредов
    // (ensure читает, txn не ставит), portalloc (мастера — первые ноды,
    // host h1/pg 15000 и h2/pg 15001), заявки ресурсов (arch/14 §2.1 п.4 —
    // без них PgtuneInputsFactory фейлит тик).
    private static void SeedCluster(Fakes.FakeEtcd etcd, string cluster = "shop")
    {
        etcd.Seed($"/clusters/{cluster}/config",
            $$"""{"buckets":2,"dbname":"{{cluster}}","created_unix":1755900000}""");
        etcd.Seed("/clusters/shop/app_user", "app");
        etcd.Seed("/clusters/shop/app_password", "OldPassword000000000000000000A");
        etcd.Seed("/clusters/shop/mover_password", "OldMoverPass0000000000000000000A");
        etcd.Seed("/clusters/shop/bucket_admin_user", "bucket_admin");
        etcd.Seed("/clusters/shop/bucket_admin_password", "OldAdminPass000000000000000A");
        etcd.Seed("/clusters/shop/backup_password", "OldBackupPass0000000000000000000A");
        foreach (var (shard, host, pg, doorman) in new[]
                 {
                     ("shard1", "h1", 15000, 16500), ("shard2", "h2", 15001, 16501),
                 })
        {
            etcd.Seed($"/clusters/shop/shards/{shard}/replicas", "2");
            etcd.Seed($"/clusters/shop/shards/{shard}/nodes/{shard}a/state", "RUNNING");
            etcd.Seed($"/clusters/shop/shards/{shard}/nodes/{shard}b/state", "RUNNING");
            etcd.Seed($"/clusters/shop/shards/{shard}/dsn",
                $"host={host} port={pg} dbname=shop user=bucket_admin password=x");
            // Формат писателя ключа (Patroni-callback): <host>:<doormanPort> ФАКТИЧЕСКОЙ
            // ноды-мастера (shard{N}a) — резолв по уникальному doorman-порту.
            etcd.Seed($"/clusters/shop/shards/{shard}/master", $"{host}:{doorman}");
            etcd.Seed($"/clusters/shop/shards/{shard}/nodes/{shard}a/app_params", "sslmode=require");
            etcd.Seed($"/clusters/shop/shards/{shard}/nodes/{shard}b/app_params", "sslmode=require");
            etcd.Seed($"/service/shop-{shard}/request_cpu", "2");
            etcd.Seed($"/service/shop-{shard}/request_mem", "4Gi");
            // HA-лидер (R3 rolling: реплики вперёд, лидер последним).
            etcd.Seed($"/service/shop-{shard}/leader", $"{shard}a");
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
        // Статусные машины нод: ready (модель работающего кластера, где панель
        // подаёт заявку ротации; гвард «тихого окна» требует готовности).
        foreach (var node in new[] { "shard1a", "shard1b", "shard2a", "shard2b" })
        {
            var shard = node.StartsWith("shard1", StringComparison.Ordinal) ? "shard1" : "shard2";
            etcd.Seed($"/pgworker/nodes/shop/{shard}/{node}/status",
                """{"state":"ready","components":{"pg":"ready","doorman":"ready"},"updated_unix":1}""");
        }
    }

    private static async Task<ClusterSnapshot> Snapshot(Fakes.FakeEtcd etcd)
    {
        var range = await etcd.RangeAsync(Ep, "/clusters/", CancellationToken.None);
        var parsed = ClusterSnapshotParser.ParseClusters(range.Value, out _);
        if (parsed.Value.FirstOrDefault(c => c.Config.Cluster == "shop") is not { } found)
            throw new InvalidOperationException(
                "cluster shop is not parsed; keys: " +
                string.Join(",", range.Value.Select(k => k.Key)));
        return found;
    }

    // Фейк-агент канала: команда читает bootstrap-ключ (его уже положил
    // delivery) и публикует статус ready(applied=mod) — модель мгновенного
    // применения; AppliedNodes — журнал порядка доставки.
    private sealed class FakeChannel(Fakes.FakeEtcd etcd) : INodeChannelClient
    {
        public readonly List<string> AppliedNodes = [];

        public async Task<ChannelReply> HandshakeAsync(
            string baseUrl, string suPassword, string standbyPassword,
            bool isToolStart, CancellationToken ct)
        {
            var node = NodeOf(baseUrl);
            AppliedNodes.Add(node);
            var kv = await ReadBootstrapAsync(node);
            if (kv is null)
                return new ChannelReply(400, false, null, "no bootstrap key");
            await PublishAsync(node, (long)kv.ModRevision);
            return new ChannelReply(200, true, null, null);
        }

        public async Task<ChannelReply> ApplyAsync(string baseUrl, CancellationToken ct)
        {
            var node = NodeOf(baseUrl);
            AppliedNodes.Add(node);
            var kv = await ReadBootstrapAsync(node)
                ?? throw new InvalidOperationException("bootstrap key vanished (fake)");
            await PublishAsync(node, (long)kv.ModRevision);
            return new ChannelReply(200, true, (long)kv.ModRevision, null);
        }

        private async Task<Kv?> ReadBootstrapAsync(string node)
            => (await etcd.GetAsync(Ep,
                $"/clusters/shop/shards/{ShardOf(node)}/nodes/{node}/bootstrap", CancellationToken.None)).Value;

        private async Task PublishAsync(string node, long applied)
            => await etcd.PutAsync(Ep,
                $"/pgworker/nodes/shop/{ShardOf(node)}/{node}/status",
                System.Text.Json.JsonSerializer.Serialize(new Dictionary<string, object?>
                {
                    ["state"] = "ready",
                    ["applied_revision"] = applied,
                    ["components"] = new Dictionary<string, string> { ["pg"] = "ready", ["doorman"] = "ready" },
                    ["updated_unix"] = 1,
                }), null, CancellationToken.None);

        // https://pgw-shop-<shard>-<n>:8081 → (нода, шард) по имени.
        private static string NodeOf(string baseUrl) => baseUrl.Split('-')[^1].Split(':')[0];
        private static string ShardOf(string node) => node.StartsWith("shard1", StringComparison.Ordinal) ? "shard1" : "shard2";
    }

    private sealed record Rig(Fakes.FakeEtcd Etcd, Fakes.FakeSql Sql, ClaimStore Claims,
        WorkJournal Journal, ClusterSecretRotator Rotator, FakeChannel Channel,
        NodeBootstrapDelivery Bootstrap);

    private static async Task<Rig> NewRig(Fakes.FakeEtcd? etcd = null, Fakes.FakeSql? sql = null)
    {
        var store = etcd ?? new Fakes.FakeEtcd();
        if (store.Store.Count == 0)
            SeedCluster(store); // пред-сиднутые etcd (кастомные portalloc/мастера) не перетираем
        var usedSql = sql ?? new Fakes.FakeSql();
        var claims = new ClaimStore("/pgworker", [Ep], store, TimeProvider.System);
        await claims.TryClaimClusterAsync("shop", CancellationToken.None);
        store.Txns.Clear(); // отсечь claim-txn: ассерты — только про txn ротации
        var journal = new WorkJournal("/pgworker", store, [Ep]);
        var probe = new ShardProbe(new HttpClient(new DeadHandler()));
        var channel = new FakeChannel(store);
        var testCa = TestRestCa.Generate();
        var bootstrap = new NodeBootstrapDelivery(
            store, [Ep], channel,
            new RestCertificateCache(testCa.CaPem, testCa.CaKeyPem));
        var rotator = new ClusterSecretRotator(
            store, [Ep], usedSql, probe, claims, journal, Secrets,
            new ClusterSecretEnsurer(store, [Ep]),
            bootstrap, Fakes.PgtuneFactory(), snapshot: null);
        return new Rig(store, usedSql, claims, journal, rotator, channel, bootstrap);
    }

    private static void SeedTicket(Fakes.FakeEtcd etcd, string raw =
        """{"requested_unix":1755900100,"requested_by":"admin"}""")
        => etcd.Seed("/pgworker/rotations/shop", raw);

    // Цикл тиков до journal-фазы done (окно закрыто) либо до первого Failed
    // (транзиент/конфликт — исход различает вызывающий).
    private static async Task<(bool Done, bool Failed)> RunUntilDoneAsync(Rig rig)
    {
        for (var i = 0; i < 10; i++)
        {
            var outcome = await rig.Rotator.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);
            var work = await rig.Journal.ReadAsync("shop", CancellationToken.None);
            if (!outcome.IsSuccess)
                return (Done: false, Failed: true);
            if (work.Value is { Phase: "done" })
                return (Done: true, Failed: false);
        }

        return (Done: false, Failed: false);
    }

    [Fact]
    public async Task Tick_NoTicket_NoOp()
    {
        // Arrange — заявки нет, окна нет
        var rig = await NewRig();

        // Act
        var outcome = await rig.Rotator.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert — Done без мутаций: ensure не вызван (нет txn), SQL не выполнялся
        outcome.IsSuccess.Should().BeTrue();
        outcome.Value.Should().Be(ProcessOutcome.Done);
        rig.Etcd.Txns.Should().BeEmpty();
        rig.Sql.Executed.Should().BeEmpty();
    }

    // ETCD-FIRST, полный проход: R2 txn-коммит NEW-четвёрки В НАЧАЛЕ окна
    // (до резолва мастеров и доставок), R3 rolling (реплики вперёд, ≤1
    // нода/тик), R4 финальные ALTER по NEW из etcd, done закрывает окно.
    [Fact]
    public async Task Rotate_EtcdFirstCommitsUpfront_ThenRolls_ThenAlters()
    {
        // Arrange — заявка стоит; оба шарда с dsn, мастера известны из master-ключей.
        var rig = await NewRig();
        SeedTicket(rig.Etcd);

        // Act
        var (done, failed) = await RunUntilDoneAsync(rig);

        // Assert (1): txn первого тика — состав БЕЗ backup: РОВНО 4 put кред
        // + dsn-перепись каждого dsn-шарда + del заявки; compare — ValueEqual
        // по 4 кредам (OLD) + каждому dsn; backup_password отсутствует.
        var commit = rig.Etcd.Txns.Single(t =>
            t.Success.OfType<TxnOp.Delete>().Any(d => d.Key == "/pgworker/rotations/shop"));
        var putKeys = commit.Success.OfType<TxnOp.Put>().Select(p => p.Key).ToList();
        putKeys.Should().Contain(new[]
        {
            "/clusters/shop/app_password", "/clusters/shop/mover_password",
            "/clusters/shop/bucket_admin_password", "/clusters/shop/rest_password",
        });
        putKeys.Where(k => k.EndsWith("/dsn")).Should().HaveCount(2);
        putKeys.Should().NotContain("/clusters/shop/backup_password");
        commit.Compare.Where(c => c.Key.EndsWith("/dsn")).Should().HaveCount(2);
        commit.Compare.Where(c => c.Key.EndsWith("password")).Should().HaveCount(4);
        commit.Compare.Should().NotContain(c => c.Key.Contains("backup"));
        // (5): заявка снята txn'ом R2.
        rig.Etcd.Store.Should().NotContainKey("/pgworker/rotations/shop");

        // Assert (2): txn коммитится ДО резолва мастеров и ДО доставок канала:
        // R4-ALTER'ы и все доставки — после txn (журналы фейков упорядочены).
        var newApp = rig.Etcd.Store["/clusters/shop/app_password"].Value;
        var newAdmin = rig.Etcd.Store["/clusters/shop/bucket_admin_password"].Value;
        var newMover = rig.Etcd.Store["/clusters/shop/mover_password"].Value;
        newApp.Should().MatchRegex("^[A-Za-z0-9]{32}$").And.NotBe("OldPassword000000000000000000A");
        newAdmin.Should().MatchRegex("^[A-Za-z0-9]{32}$").And.NotBe("OldAdminPass000000000000000A");
        newMover.Should().MatchRegex("^[A-Za-z0-9]{32}$").And.NotBe("OldMoverPass0000000000000000000A");
        // НЕТ backup в etcd-операциях (спека §8-19b).
        rig.Etcd.Store["/clusters/shop/backup_password"].Value
            .Should().Be("OldBackupPass0000000000000000000A");

        // Assert (3): rolling — реплика раньше лидера, ≤1 delivery/тик.
        var shard1Order = rig.Channel.AppliedNodes
            .Where(n => n.StartsWith("shard1", StringComparison.Ordinal)).ToList();
        shard1Order.Should().HaveCount(2, "one node per tick");
        shard1Order.Should().ContainInOrder("shard1b", "shard1a");

        // Assert (4): финальные ALTER — после последнего apply: по 3 ALTER
        // (app/bucket_admin/bucket_mover) на мастера каждого шарда, пароли ==
        // NEW-ключам etcd; НЕТ backup-ALTER.
        done.Should().BeTrue(failed ? "tick failed" : "the window must close");
        failed.Should().BeFalse();
        var sqlTexts = rig.Sql.Executed.Select(e => e.Sql).ToList();
        sqlTexts.Count(s => s.Contains("ALTER ROLE \"app\" PASSWORD")).Should().Be(2);
        sqlTexts.Count(s => s.Contains("ALTER ROLE \"bucket_admin\" PASSWORD")).Should().Be(2);
        sqlTexts.Count(s => s.Contains("ALTER ROLE \"bucket_mover\" PASSWORD")).Should().Be(2);
        sqlTexts.Should().NotContain(s => s.Contains("backup_exec"));
        sqlTexts.Should().Contain(s =>
            s.Contains("ALTER ROLE \"app\" PASSWORD") && s.Contains(newApp));
        sqlTexts.Should().Contain(s =>
            s.Contains("ALTER ROLE \"bucket_admin\" PASSWORD") && s.Contains(newAdmin));
        sqlTexts.Should().Contain(s =>
            s.Contains("ALTER ROLE \"bucket_mover\" PASSWORD") && s.Contains(newMover));
        // ALTER после последней доставки канала.
        rig.Sql.Executed.Should().Contain(e => e.Sql.Contains("ALTER ROLE \"app\" PASSWORD"));

        // Assert (5): журнальный флаг — true после R2 (уже закрыт done'ом —
        // false в финале); фаза done.
        var work = await rig.Journal.ReadAsync("shop", CancellationToken.None);
        work.Value!.Phase.Should().Be("done");
        work.Value.RestWindowOpen.Should().BeFalse("done closes the window");
    }

    // Рейc-регрессия (finding P1): вырожденный host-match резолва мастеров —
    // ETCD-FIRST переносит ALTER в R4; резолв по doorman-порту сохраняется.
    [Fact]
    public async Task Tick_SharedHostMasterKey_ResolvesByDoormanPort()
    {
        // Arrange — заявка; shard1: обе ноды на хосте h1, фактический мастер —
        // shard1b (doorman 16502).
        var etcd = new Fakes.FakeEtcd();
        SeedCluster(etcd);
        etcd.Seed("/pgworker/portalloc/shop", Portalloc.Serialize(new Dictionary<string, NodeAddress>
        {
            ["shard1/shard1a"] = new("h1", new NodePorts(15000, 18000, 16500)),
            ["shard1/shard1b"] = new("h1", new NodePorts(15002, 18002, 16502)),
            ["shard2/shard2a"] = new("h2", new NodePorts(15001, 18001, 16501)),
            ["shard2/shard2b"] = new("h2", new NodePorts(15003, 18003, 16503)),
        }));
        etcd.Seed("/clusters/shop/shards/shard1/master", "h1:16502");
        etcd.Seed("/clusters/shop/shards/shard2/master", "h2:16501");
        var rig = await NewRig(etcd);
        SeedTicket(rig.Etcd);

        // Act
        var (done, failed) = await RunUntilDoneAsync(rig);

        // Assert — txn-коммит UPFRONT (первый тик: креды NEW в etcd, заявка
        // снята); резолв мастера по doorman-порту — в R4 (ALTER-тексты несут
        // Dsn мастера, найденного по порту); окно закрыто.
        done.Should().BeTrue();
        failed.Should().BeFalse();
        var alterDsns = rig.Sql.Executed
            .Where(e => e.Sql.Contains("ALTER ROLE \"app\" PASSWORD"))
            .Select(e => e.Dsn).ToList();
        alterDsns.Should().Contain(dsn => dsn.Contains("Port=15002"),
            "shard1 master is shard1b (doorman 16502)");
        alterDsns.Should().NotContain(dsn => dsn.Contains("Port=15000"),
            "shard1a is a replica — ALTER is impossible there");
    }

    // ETCD-FIRST: мастер шарда недоступен — txn-коммит УЖЕ прошёл (заявка
    // снята, etcd несёт NEW), фаза rotate-waiting-master/<X>, окно живёт
    // (флаг переносится); появление мастера — R4 доводит до done.
    [Fact]
    public async Task Rotate_ShardWithoutMaster_WindowOpen_WaitingMaster()
    {
        // Arrange — у shard2 нет master-ключа и Patroni мёртв (transient)
        var rig = await NewRig();
        rig.Etcd.Store.Remove("/clusters/shop/shards/shard2/master");
        SeedTicket(rig.Etcd);

        // Act: тики rolling по нодам dsn-шардов (shard2 входит в dsn-список);
        // после завершения rolling R4: shard1 ALTER'ится, shard2 — waiting.
        for (var i = 0; i < 6; i++)
            (await rig.Rotator.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None))
                .IsSuccess.Should().BeTrue();

        // Assert: txn-коммит прошёл (заявка СНЯТА — не «TicketAlive»),
        // креды в etcd NEW, фаза rotate-waiting-master/shard2, окно живо.
        rig.Etcd.Store.Should().NotContainKey("/pgworker/rotations/shop");
        rig.Etcd.Store["/clusters/shop/app_password"].Value
            .Should().MatchRegex("^[A-Za-z0-9]{32}$").And.NotBe("OldPassword000000000000000000A");
        rig.Etcd.Store["/clusters/shop/mover_password"].Value
            .Should().MatchRegex("^[A-Za-z0-9]{32}$").And.NotBe("OldMoverPass0000000000000000000A");
        var work = await rig.Journal.ReadAsync("shop", CancellationToken.None);
        work.Value!.Phase.Should().Be("rotate-waiting-master/shard2");
        work.Value.RestWindowOpen.Should().BeTrue("the window stays open while ALTER waits");

        // Появился мастер shard2 → следующий тик доводит до done (окно закрыто).
        rig.Etcd.Seed("/clusters/shop/shards/shard2/master", "h2:16501");
        var (done, failed) = await RunUntilDoneAsync(rig);
        done.Should().BeTrue("the master appeared — R4 completes the window");
        failed.Should().BeFalse();
        (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.Phase.Should().Be("done");
    }

    // Первый тик ETCD-FIRST: txn-коммит + открытие окна (флаг true) + первая
    // доставка — реплика (≤1 нода/тик).
    [Fact]
    public async Task Rotate_FirstTick_CommitsUpfront_AndDeliversReplicaFirst()
    {
        // Arrange: заявка.
        var rig = await NewRig();
        SeedTicket(rig.Etcd);

        // Act
        var outcome = await rig.Rotator.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: txn-коммит (заявка снята, rest_password=NEW), флаг true,
        // доставлена одна нода — реплика shard1b.
        outcome.IsSuccess.Should().BeTrue(outcome.Error?.ToString());
        outcome.Value.Should().Be(ProcessOutcome.Done);
        rig.Etcd.Store.Should().NotContainKey("/pgworker/rotations/shop");
        rig.Etcd.Store["/clusters/shop/rest_password"].Value
            .Should().MatchRegex("^[A-Za-z0-9]{32}$").And.NotBe("Old0Rest0000000000000000000000X");
        var work = await rig.Journal.ReadAsync("shop", CancellationToken.None);
        work.Value!.RestWindowOpen.Should().BeTrue("R2 opened the window");
        work.Value.Phase.Should().StartWith("rotate-rest-rolling/", "phase: {0}; error: {1}",
            work.Value.Phase, outcome.Error?.ToString());
        rig.Channel.AppliedNodes.Should().ContainSingle("phase: {0}; error: {1}",
            work.Value.Phase, outcome.Error?.ToString()).Which.Should().Be("shard1b");
    }

    // Takeover: НОВЫЙ ротатор (экземпляр после смерти воркера) при
    // RestWindowOpen:true БЕЗ заявки продолжает окно — пара из etcd,
    // лидер последним; заявки не требуется.
    [Fact]
    public async Task Rotate_Rolling_ContinuesByWindowFlag_Takeover()
    {
        // Arrange: тик 1 — коммит и первая доставка; окно открыто.
        var rig = await NewRig();
        SeedTicket(rig.Etcd);
        (await rig.Rotator.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None))
            .Value.Should().Be(ProcessOutcome.Done);
        var appliedAfterFirst = rig.Channel.AppliedNodes.Count;

        // Takeover: новый ротатор поверх того же etcd (заявки уже нет).
        var takeover = new ClusterSecretRotator(
            rig.Etcd, [Ep], rig.Sql, new ShardProbe(new HttpClient(new DeadHandler())),
            rig.Claims, rig.Journal, Secrets,
            new ClusterSecretEnsurer(rig.Etcd, [Ep]),
            rig.Bootstrap, Fakes.PgtuneFactory(), snapshot: null);

        // Act: тик takeover-ротатора — окно живо по флагу, rolling продолжается.
        var outcome = await takeover.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: доставлена следующая нода; окно открыто (флаг перенесён).
        outcome.IsSuccess.Should().BeTrue(outcome.Error?.ToString());
        rig.Channel.AppliedNodes.Should().HaveCount(appliedAfterFirst + 1);
        (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.RestWindowOpen
            .Should().BeTrue("supervise/phase writes carry the flag; the window stays open");
    }

    // done закрывает окно (флаг false); повторный тик без заявки — no-op.
    [Fact]
    public async Task Rotate_Done_ClosesWindow()
    {
        // Arrange — окно закрывается fake-агентом (статусы по факту применений).
        var rig = await NewRig();
        SeedTicket(rig.Etcd);

        // Act
        var (done, failed) = await RunUntilDoneAsync(rig);

        // Assert: rest_password==NEW, заявка удалена, флаг false, фаза done;
        // доставки shard1 — реплика первой.
        done.Should().BeTrue();
        failed.Should().BeFalse();
        rig.Etcd.Store["/clusters/shop/rest_password"].Value
            .Should().MatchRegex("^[A-Za-z0-9]{32}$").And.NotBe("Old0Rest0000000000000000000000X");
        rig.Etcd.Store.Should().NotContainKey("/pgworker/rotations/shop");
        var work = await rig.Journal.ReadAsync("shop", CancellationToken.None);
        work.Value!.RestWindowOpen.Should().BeFalse("done closes the window");
        work.Value.Phase.Should().Be("done");
        rig.Channel.AppliedNodes.Where(n => n.StartsWith("shard1", StringComparison.Ordinal))
            .Should().ContainInOrder("shard1b", "shard1a");

        // Повторный тик без заявки — no-op (не no-op только при открытом окне).
        var applied = rig.Channel.AppliedNodes.Count;
        var noop = await rig.Rotator.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);
        noop.IsSuccess.Should().BeTrue();
        noop.Value.Should().Be(ProcessOutcome.Done);
        rig.Channel.AppliedNodes.Should().HaveCount(applied);
    }

    // ETCD-FIRST: заявка есть, клэйм не взят (инвариант мутаций /clusters/).
    [Fact]
    public async Task Tick_ClaimNotMine_MutationsForbidden()
    {
        // Arrange — заявка есть, клэйм не взят
        var etcd = new Fakes.FakeEtcd();
        SeedCluster(etcd);
        SeedTicket(etcd);
        var journal = new WorkJournal("/pgworker", etcd, [Ep]);
        var probe = new ShardProbe(new HttpClient(new DeadHandler()));
        var channel = new FakeChannel(etcd);
        var testCa = TestRestCa.Generate();
        var bootstrap = new NodeBootstrapDelivery(
            etcd, [Ep], channel,
            new RestCertificateCache(testCa.CaPem, testCa.CaKeyPem));
        var rotator = new ClusterSecretRotator(
            etcd, [Ep], new Fakes.FakeSql(), probe,
            new ClaimStore("/pgworker", [Ep], etcd, TimeProvider.System), journal, Secrets,
            new ClusterSecretEnsurer(etcd, [Ep]),
            bootstrap, Fakes.PgtuneFactory(), snapshot: null);

        // Act
        var outcome = await rotator.TickAsync(await Snapshot(etcd), CancellationToken.None);

        // Assert — отказ до любых мутаций
        outcome.IsSuccess.Should().BeFalse();
        etcd.Txns.Should().BeEmpty();
        etcd.Store["/clusters/shop/app_password"].Value
            .Should().Be("OldPassword000000000000000000A");
    }

    // ETCD-FIRST: сравнение dsn закрывает гонку внешней записи dsn: инъекция
    // ПОСЛЕ снапшота, ДО тика — R2-txn compare dsn проигран → commit-conflict;
    // ретрай тиком со свежим снапшотом — до done.
    [Fact]
    public async Task Rotate_ExternalDsnChange_CompareLostRetriable()
    {
        // Arrange — заявка; снапшот ДО инъекции.
        var rig = await NewRig();
        SeedTicket(rig.Etcd);
        var staleSnapshot = await Snapshot(rig.Etcd);

        // Инъекция внешней записи dsn (репарация/оператор) между снапшотом и тиком.
        rig.Etcd.Store["/clusters/shop/shards/shard1/dsn"] =
            new Fakes.FakeEtcd.Entry("host=h9 port=19999 dbname=shop user=bucket_admin password=changed", 9, 9);

        // Act: тик — R2-txn compare dsn проигран.
        var outcome = await rig.Rotator.TickAsync(staleSnapshot, CancellationToken.None);

        // Assert — compare dsn проигран: заявка жива, окно не открыто, креды
        // не перезаписаны нашим txn (etcd OLD).
        outcome.IsSuccess.Should().BeFalse(outcome.Error?.ToString());
        rig.Etcd.Store["/clusters/shop/app_password"].Value
            .Should().Be("OldPassword000000000000000000A");
        rig.Etcd.Store["/clusters/shop/bucket_admin_password"].Value
            .Should().Be("OldAdminPass000000000000000A");
        rig.Etcd.Store.Should().ContainKey("/pgworker/rotations/shop");
        (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.RestWindowOpen
            .Should().NotBeTrue("the conflict happens before the window opens");

        // Ретрай со свежим снапшотом — до done.
        var (done, failed) = await RunUntilDoneAsync(rig);
        done.Should().BeTrue("the retry commits with a fresh snapshot");
        failed.Should().BeFalse();
        rig.Etcd.Store.Should().NotContainKey("/pgworker/rotations/shop");
    }

    // (е1, ETCD-FIRST): кред-compare строится по СВЕЖЕМУ ensure-чтению тика —
    // до-тиковая перезапись креда была бы прочитана ensure'ом как OLD и
    // compare ПРОШЁЛ бы. Поэтому «внешняя перезапись etcdctl» инъецируется
    // хуком OnTxnBeforeCompare (после ensure-чтения, ДО применения txn):
    // compare проигран → commit-conflict, окно НЕ открыто, креды не изменены
    // нашим txn; снятие хука → ретрай со свежими OLD (инъекция стала OLD)
    // достигает done.
    [Fact]
    public async Task Rotate_ExternalCredChange_CompareLostRetriable()
    {
        // Arrange — заявка; снапшот; хук инъекции при R2-txn (R1 ensure при
        // полном сиде делает ранний возврат без txn — creds остаются OLD;
        // хук срабатывает ПОСЛЕ ensure-чтения и ДО применения txn — инъекция
        // «внешнего etcdctl» в этом зазоре).
        var rig = await NewRig();
        SeedTicket(rig.Etcd);
        var staleSnapshot = await Snapshot(rig.Etcd);
        // Дистинкт: инъекция ТОЛЬКО на txn ротатора (R2 несёт del заявки);
        // ensure-txn (put-if-absent R1) хук игнорирует — иначе re-read ensure
        // подхватил бы инъекцию как OLD и compare прошёл бы.
        rig.Etcd.OnTxnBeforeCompare = req =>
        {
            if (req.Success.OfType<TxnOp.Delete>().Any(d => d.Key == "/pgworker/rotations/shop"))
                rig.Etcd.Store["/clusters/shop/app_password"] =
                    new Fakes.FakeEtcd.Entry("ExternallyRewritten00000000X", 9, 9);
        };

        // Act: тик — R2-txn compare app проигран (внешняя перезапись).
        var first = await rig.Rotator.TickAsync(staleSnapshot, CancellationToken.None);

        // Assert: commit-conflict, окно НЕ открыто, креды НЕ изменены нашим
        // txn (app — инъекция, остальные — OLD-сид), заявка жива.
        first.IsSuccess.Should().BeFalse(first.Error?.ToString());
        foreach (var kv in rig.Etcd.Store.Where(k => k.Key.Contains("/clusters/shop/") && !k.Key.Contains("bootstrap") && !k.Key.Contains("app_params")))
            File.AppendAllText("/tmp/e1-dump.txt", $"[e1] {kv.Key} = {kv.Value.Value[..Math.Min(30, kv.Value.Value.Length)]} mod={kv.Value.ModRevision}\n");
        var work = await rig.Journal.ReadAsync("shop", CancellationToken.None);
        work.Value!.Phase.Should().Be("commit-conflict");
        work.Value.RestWindowOpen.Should().NotBeTrue("the conflict happens before the window opens");
        rig.Etcd.Store["/clusters/shop/app_password"].Value
            .Should().Be("ExternallyRewritten00000000X");
        rig.Etcd.Store["/clusters/shop/bucket_admin_password"].Value
            .Should().Be("OldAdminPass000000000000000A");
        rig.Etcd.Store["/clusters/shop/mover_password"].Value
            .Should().Be("OldMoverPass0000000000000000000A");
        // rest_password отсутствовал в сиде — P1.5 ensure сгенерировал его
        // (put-if-absent, mod 38) — наш txn его не менял (32-симв GEN).
        rig.Etcd.Store["/clusters/shop/rest_password"].Value
            .Should().MatchRegex("^[A-Za-z0-9]{32}$");
        rig.Etcd.Store.Should().ContainKey("/pgworker/rotations/shop");

        // Ретрай: хук снят — txn проходит (заявка снята, флаг закрыт).
        rig.Etcd.OnTxnBeforeCompare = null;
        var (done, failed) = await RunUntilDoneAsync(rig);
        done.Should().BeTrue("the retry commits with fresh OLD values");
        failed.Should().BeFalse();
        rig.Etcd.Store.Should().NotContainKey("/pgworker/rotations/shop");
        (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.RestWindowOpen
            .Should().BeFalse("done closes the window");
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

    // AAA (ж): шард без dsn — его ноды гейтом пропускаются (ноды создадутся с
    // NEW после закрытия окна); txn всё равно коммитит креды.
    [Fact]
    public async Task Rotate_ShardWithoutDsn_NotDelivered()
    {
        // Arrange: второй шард без dsn; доставки только для dsn-шардов.
        var rig = await NewRig();
        SeedTicket(rig.Etcd);
        rig.Etcd.Store.Remove("/clusters/shop/shards/shard2/dsn");

        // Act
        var (done, failed) = await RunUntilDoneAsync(rig);

        // Assert: доставки только shard1 (2 ноды); txn коммитит (шард2 — skip).
        done.Should().BeTrue();
        failed.Should().BeFalse();
        rig.Channel.AppliedNodes.Should().OnlyContain(n => n.StartsWith("shard1", StringComparison.Ordinal));
        rig.Channel.AppliedNodes.Should().HaveCount(2);
        rig.Etcd.Store.Should().NotContainKey("/pgworker/rotations/shop");
    }

    // Общий ассерт закрытия окна: заявка снята, rest_password — NEW 32-симв,
    // флаг false (окно закрыто).
    private static async Task AssertWindowClosedAsync(Rig rig)
    {
        rig.Etcd.Store.Should().NotContainKey("/pgworker/rotations/shop", "the window is closed");
        rig.Etcd.Store["/clusters/shop/rest_password"].Value
            .Should().MatchRegex("^[A-Za-z0-9]{32}$");
        (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.RestWindowOpen
            .Should().BeFalse("done closed the window");
    }

    // AAA: restore-шард (PLANNED/RUNNING/REJOINING — те же факты, что надзор)
    // НЕ удерживает окно: его ноды создадутся с NEW после закрытия окна
    // (NEW в etcd с R2).
    [Fact]
    public async Task Rotate_RestoreShard_DoesNotHoldRestWindow()
    {
        // Arrange: shard1 — активная restore (демонтаж владеет restore-процессом);
        // доставки идут только на shard2.
        var etcd = new Fakes.FakeEtcd();
        SeedCluster(etcd);
        etcd.Seed("/pgworker/backups/shop/shard1/restore/r1",
            """{"state":"RUNNING","backup_id":"f1","source":"shard1","target":"shop-new","node":"pgw-r1","requested_unix":1755900000,"requested_by":"admin"}""");
        SeedTicket(etcd);
        var rig = await NewRig(etcd);

        // Act
        var (done, failed) = await RunUntilDoneAsync(rig);

        // Assert: restore-шард пропущен гейтом — окно закрывается.
        done.Should().BeTrue();
        failed.Should().BeFalse();
        rig.Channel.AppliedNodes.Should().OnlyContain(n => n.StartsWith("shard2", StringComparison.Ordinal));
        await AssertWindowClosedAsync(rig);
    }

    // AAA: усыновлённая нода (object) гейтом пропускается — доставки на неё
    // нет; окно не удерживается.
    [Fact]
    public async Task Rotate_AdoptedNode_DoesNotHoldRestWindow()
    {
        // Arrange: обе ноды shard1 — object (внешняя orchestration).
        var rig = await NewRig();
        SeedTicket(rig.Etcd);
        var raw = rig.Etcd.Store["/pgworker/portalloc/shop"].Value;
        var parsed = Portalloc.Parse("shop", raw).Value!;
        var mutable = parsed.ToDictionary(p => p.Key, p => p.Value);
        mutable["shard1/shard1a"] = mutable["shard1/shard1a"] with { Object = "external-a" };
        mutable["shard1/shard1b"] = mutable["shard1/shard1b"] with { Object = "external-b" };
        rig.Etcd.Store["/pgworker/portalloc/shop"] =
            new Fakes.FakeEtcd.Entry(Portalloc.Serialize(mutable), 2, 2);

        // Act
        var (done, failed) = await RunUntilDoneAsync(rig);

        // Assert: object-ноды пропущены гейтом — окно закрывается.
        done.Should().BeTrue();
        failed.Should().BeFalse();
        rig.Channel.AppliedNodes.Should().OnlyContain(n => n.StartsWith("shard2", StringComparison.Ordinal));
        await AssertWindowClosedAsync(rig);
    }
}
