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

// Ротация per-cluster секретов по заявке /pgworker/rotations/<C> (arch/14
// §5 I, t05): ALTER ролей на мастерах → rest-окно доставкой каналом БЕЗ
// пересоздания (bootstrap-перерендер ключа ОДНОЙ ноды/тик, реплики вперёд;
// fake-агент публикует статусные ключи по факту «применения») → атомарный
// txn-коммит (новые креды + перезапись dsn + del заявки); transient-отказы.
// Окно т05 — по статусным ключам (env-инспекции нет): реальный
// NodeBootstrapDelivery + fake-канал на Fakes.FakeEtcd.
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
    // (— ensure читает, txn не ставит), portalloc (мастера — первые ноды,
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
            // HA-лидер (R2-rest порядок: реплики вперёд, лидер последним).
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
        // Статусы нод not_initialized (fake-агент канала публикует ready по
        // факту Handshake — окно закрывается в юнит-контуре).
        foreach (var node in new[] { "shard1a", "shard1b", "shard2a", "shard2b" })
        {
            var shard = node.StartsWith("shard1", StringComparison.Ordinal) ? "shard1" : "shard2";
            etcd.Seed($"/pgworker/nodes/shop/{shard}/{node}/status",
                """{"state":"not_initialized","components":{},"updated_unix":1}""");
        }
    }

    private static async Task<ClusterSnapshot> Snapshot(Fakes.FakeEtcd etcd)
    {
        var range = await etcd.RangeAsync(Ep, "/clusters/", CancellationToken.None);
        var parsed = ClusterSnapshotParser.ParseClusters(range.Value, out _);
        return parsed.Value.Single(c => c.Config.Cluster == "shop");
    }

    // Фейк-агент канала: команда читает bootstrap-ключ (его уже положил
    // delivery) и публикует статус ready(applied=mod) — модель мгновенного
    // применения; AppliedNodes — журнал порядка доставки.
    private sealed class FakeChannel(Fakes.FakeEtcd etcd) : INodeChannelClient
    {
        public readonly List<string> AppliedNodes = [];
        // Инъекция внешней записи ВНУТРИ окна (после R1-чтения, до R3 тика).
        public Action? OnApply;

        private void Inject() => OnApply?.Invoke();

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
            Inject();
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
        if (etcd is null)
            SeedCluster(store);
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

    // Цикл тиков до txn-коммита (заявка снята) либо до первого Failed
    // (транзиент/конфликт — исход различает вызывающий).
    private static async Task<(bool Committed, bool Failed)> RunUntilCommitAsync(Rig rig)
    {
        for (var i = 0; i < 10; i++)
        {
            var outcome = await rig.Rotator.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);
            if (!outcome.IsSuccess)
                return (Committed: false, Failed: true);
            if (!rig.Etcd.Store.ContainsKey("/pgworker/rotations/shop"))
                return (Committed: true, Failed: false);
        }

        return (Committed: false, Failed: false);
    }

    [Fact]
    public async Task Tick_NoTicket_NoOp()
    {
        // Arrange — заявки нет
        var rig = await NewRig();

        // Act
        var outcome = await rig.Rotator.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert — Done без мутаций: ensure не вызван (нет txn), SQL не выполнялся
        outcome.IsSuccess.Should().BeTrue();
        outcome.Value.Should().Be(ProcessOutcome.Done);
        rig.Etcd.Txns.Should().BeEmpty();
        rig.Sql.Executed.Should().BeEmpty();
    }

    [Fact]
    public async Task Tick_Ticket_AltersAllShardsAndCommitsAtomically()
    {
        // Arrange — заявка стоит; оба шарда с dsn, мастера известны из master-ключей.
        // Окно t05: R2 доставляет каналом 4 ноды (по одной/тик), затем R3-коммит.
        var rig = await NewRig();
        SeedTicket(rig.Etcd);

        // Act
        var (committed, failed) = await RunUntilCommitAsync(rig);

        // Assert — ALTER трёх ролей + гвард→ALTER backup_exec на мастерах ОБЕИХ
        // шардов (8 SQL + 2 скаляра-гварда); одна txn: compare OLD (4 креда + 2 dsn)
        // + put новых кредов + перезапись dsn + del заявки.
        committed.Should().BeTrue("rolling over fake statuses must reach the txn");
        failed.Should().BeFalse();
        // Окно t05: проход = несколько тиков, каждый повторяет ALTER со свежими
        // NEW (transient-семантика «повтор с начала»); ассерты — факты покрытия
        // мастеров и коммит-состава, не число повторов.
        var sqlTexts = rig.Sql.Executed.Select(e => e.Sql).ToList();
        sqlTexts.Count(s => s.Contains("ALTER ROLE \"app\" PASSWORD")).Should().BeGreaterThanOrEqualTo(2,
            "ALTER app on both shard masters");
        sqlTexts.Count(s => s.Contains("ALTER ROLE \"bucket_admin\" PASSWORD"))
            .Should().BeGreaterThanOrEqualTo(2);
        sqlTexts.Count(s => s.Contains("ALTER ROLE \"bucket_mover\" PASSWORD"))
            .Should().BeGreaterThanOrEqualTo(2);
        sqlTexts.Count(s => s.Contains("ALTER ROLE \"backup_exec\" PASSWORD"))
            .Should().BeGreaterThanOrEqualTo(2);
        rig.Sql.Scalars.Count(s => s.Sql.Contains("backup_exec"))
            .Should().BeGreaterThanOrEqualTo(2); // гварды

        var newApp = rig.Etcd.Store["/clusters/shop/app_password"].Value;
        var newMover = rig.Etcd.Store["/clusters/shop/mover_password"].Value;
        var newAdmin = rig.Etcd.Store["/clusters/shop/bucket_admin_password"].Value;
        newApp.Should().MatchRegex("^[A-Za-z0-9]{32}$").And.NotBe("OldPassword000000000000000000A");
        newMover.Should().MatchRegex("^[A-Za-z0-9]{32}$").And.NotBe("OldMoverPass0000000000000000000A");
        newAdmin.Should().MatchRegex("^[A-Za-z0-9]{32}$").And.NotBe("OldAdminPass000000000000000A");
        var newBackup = rig.Etcd.Store["/clusters/shop/backup_password"].Value;
        newBackup.Should().MatchRegex("^[A-Za-z0-9]{32}$").And.NotBe("OldBackupPass0000000000000000000A");
        rig.Etcd.Store.Should().NotContainKey("/pgworker/rotations/shop");

        // Коммит-txn узнаём по del заявки (ensure-txn R1 тоже ставит puts)
        var commit = rig.Etcd.Txns.Single(t =>
            t.Success.OfType<TxnOp.Delete>().Any(d => d.Key == "/pgworker/rotations/shop"));
        commit.Compare.Should().Contain(c =>
            c.Key == "/clusters/shop/app_password" && c.Arg == "OldPassword000000000000000000A");
        commit.Compare.Should().Contain(c =>
            c.Key == "/clusters/shop/mover_password" && c.Arg == "OldMoverPass0000000000000000000A");
        commit.Compare.Should().Contain(c =>
            c.Key == "/clusters/shop/bucket_admin_password" && c.Arg == "OldAdminPass000000000000000A");
        commit.Compare.Should().Contain(c =>
            c.Key == "/clusters/shop/backup_password" && c.Arg == "OldBackupPass0000000000000000000A");
        // dsn-сравнение — по прочитанным значениям (гонка репарации → ретрай тиком)
        commit.Compare.Where(c => c.Key.EndsWith("/dsn")).Should().HaveCount(2);
        commit.Success.OfType<TxnOp.Delete>()
            .Should().ContainSingle(d => d.Key == "/pgworker/rotations/shop");
        // dsn перезаписаны: пароль bucket_admin = новый, остальное байт-в-байт
        var dsnPuts = commit.Success.OfType<TxnOp.Put>().Where(p => p.Key.EndsWith("/dsn")).ToList();
        dsnPuts.Should().HaveCount(2);
        dsnPuts.Should().OnlyContain(p =>
            p.Value.EndsWith($"user=bucket_admin password={newAdmin}")
            && p.Value.StartsWith("host="));
        (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.Op
            .Should().Be("rotate-app-password");
    }

    // AAA: R2 backup_exec при отсутствующей роли — гвард создаёт её с NEW-паролем,
    // ротация НЕ падает (регресс-гвард finding 1: Enabled=false, G2 не выполнялся)
    [Fact]
    public async Task Tick_Ticket_BackupExecRoleAbsent_CreatedByGuard()
    {
        // Arrange — заявка; скаляр-гвард backup_exec возвращает CREATE-текст (роли нет)
        var rig = await NewRig();
        SeedTicket(rig.Etcd);
        rig.Sql.ScalarResultBySql = (_, sql) =>
            sql.Contains("\"backup_exec\"")
                ? Result<object?>.Success("SELECT 'CREATE ROLE \"backup_exec\" LOGIN REPLICATION PASSWORD ''x'''")
                : Result<object?>.Success(null);

        // Act
        var (committed, failed) = await RunUntilCommitAsync(rig);

        // Assert — роль создана (CREATE ×2 шарда), ALTER backup_exec не было;
        // ротация успешна: заявка закрыта, backup_password перезаписан.
        committed.Should().BeTrue();
        failed.Should().BeFalse();
        var executed = rig.Sql.Executed.Select(e => e.Sql).ToList();
        executed.Count(s => s.Contains("CREATE ROLE \"backup_exec\"")).Should().BeGreaterThanOrEqualTo(2);
        executed.Should().NotContain(s => s.Contains("ALTER ROLE \"backup_exec\""));
        rig.Etcd.Store.Should().NotContainKey("/pgworker/rotations/shop");
        rig.Etcd.Store["/clusters/shop/backup_password"].Value
            .Should().MatchRegex("^[A-Za-z0-9]{32}$");
    }

    // Деградировавший кейс бага гейта (finding P1): вырожденный master-ключ
    // <host>:0 делал бы обе ноды шарда на одном хосте равнозначными — старый
    // host-match возвращал ПРОИЗВОЛЬНУЮ ноду (ALTER ROLE на реплике →
    // 25006 read-only, ротация зацикливалась). Резолв обязан идти по уникальному
    // doorman-порту из ключа (S4: вырожденные значения недискриминантны; в
    // штатном режиме невозможны — пулер обязателен).
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
        var (committed, failed) = await RunUntilCommitAsync(rig);

        // Assert — ALTER уходит на фактическую ноду-мастера (Port=15002/pg shard1b),
        // а не на первую по порядку portalloc ноду шарда (Port=15000, shard1a)
        committed.Should().BeTrue();
        failed.Should().BeFalse();
        var alterDsns = rig.Sql.Executed
            .Where(e => e.Sql.Contains("ALTER ROLE \"app\" PASSWORD"))
            .Select(e => e.Dsn).ToList();
        alterDsns.Should().NotBeEmpty();
        alterDsns.Should().Contain(dsn => dsn.Contains("Port=15002"), "shard1 master is shard1b (doorman 16502)");
        alterDsns.Should().NotContain(dsn => dsn.Contains("Port=15000"), "shard1a is a replica — ALTER is impossible there");
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

        // Assert — Failed: креды прежние, заявка жива (ретрай тиком с начала)
        outcome.IsSuccess.Should().BeFalse();
        rig.Etcd.Store["/clusters/shop/app_password"].Value
            .Should().Be("OldPassword000000000000000000A");
        rig.Etcd.Store["/clusters/shop/mover_password"].Value
            .Should().Be("OldMoverPass0000000000000000000A");
        rig.Etcd.Store.Should().ContainKey("/pgworker/rotations/shop");
    }

    [Fact]
    public async Task Tick_TicketExternalPasswordChange_CompareLostRetriable()
    {
        // Arrange — внешний etcdctl меняет app_password между чтением и коммитом:
        // инъекция — перезапись ключа при ВТОРОМ ALTER (spec §4.3 R3)
        var rig = await NewRig();
        SeedTicket(rig.Etcd);
        // Инъекция внешней записи app_password ВНУТРИ окна (на первом apply —
        // после R1-чтения creds, до R3 тика, spec §4.3 R3).
        // Инъекция на ПРЕДПОСЛЕДНЕЙ доставке окна: этот же тик дойдёт до R3
        // (creds прочитаны R1 в его начале, до инъекции) → compare проигран.
        rig.Channel.OnApply = () =>
        {
            if (rig.Channel.AppliedNodes.Count == 3)
                rig.Etcd.Seed("/clusters/shop/app_password", "External0000000000000000000000X");
        };

        // Act — повтор окна перечитывает креды (свежие OLD = внешнее значение)
        // и коммитит его (семантика «повтор с начала со свежими OLD», spec §4.3).
        var (committed, failed) = await RunUntilCommitAsync(rig);

        // Assert: коммит свежей пары (внешнее значение стало OLD для R1 тика
        // коммита — «повтор с начала со свежими OLD», spec §4.3 R3).
        committed.Should().BeTrue();
        failed.Should().BeFalse();
        rig.Etcd.Store["/clusters/shop/app_password"].Value
            .Should().MatchRegex("^[A-Za-z0-9]{32}$").And.NotBe("External0000000000000000000000X");
        rig.Etcd.Store.Should().NotContainKey("/pgworker/rotations/shop");
    }

    [Fact]
    public async Task Tick_ExternalDsnChange_CompareLostRetriable()
    {
        // Arrange — внешняя запись dsn (репарация/оператор) между чтением и txn:
        // инъекция при ПЕРВОМ ALTER (§5 I R3: compare по dsn закрывает гонку)
        var rig = await NewRig();
        SeedTicket(rig.Etcd);
        // Тики окна вручную (по доставке на тик); заявка жива до R3.
        for (var i = 0; i < 4; i++)
        {
            var snap = await Snapshot(rig.Etcd);
            (await rig.Rotator.TickAsync(snap, CancellationToken.None))
                .IsSuccess.Should().BeTrue();
        }

        // Тик R3: снапшот ДО инъекции; окно ещё живо (pending до done), R2
        // пусто (все ноды на ревизии NEW) → R3: compare dsn проигрывает
        // (spec §4.3 R3/§5 I R3: compare закрывает гонку записи).
        var r3Snapshot = await Snapshot(rig.Etcd);
        rig.Etcd.Store["/clusters/shop/shards/shard1/dsn"] =
            new Fakes.FakeEtcd.Entry("host=h9 port=19999 dbname=shop user=bucket_admin password=changed", 9, 9);

        // Act: тик R3 — compare dsn проигран.
        var outcome = await rig.Rotator.TickAsync(r3Snapshot, CancellationToken.None);

        // Assert — compare dsn проигран: заявка жива, креды не перезаписаны
        outcome.IsSuccess.Should().BeFalse(outcome.Error?.ToString());
        rig.Etcd.Store["/clusters/shop/app_password"].Value
            .Should().Be("OldPassword000000000000000000A");
        rig.Etcd.Store["/clusters/shop/bucket_admin_password"].Value
            .Should().Be("OldAdminPass000000000000000A");
        rig.Etcd.Store.Should().ContainKey("/pgworker/rotations/shop");
        // commit-conflict сбрасывает окно (повтор — со свежей парой).
        (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.RestPending
            .Should().BeNull("the conflict drops the window");
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

    // ── rest-ветка ротации (четвёртый секрет, arch/14 §5 I; окно t05 —
    // по статусным ключам, доставки каналом БЕЗ пересоздания) ──

    // AAA (а): заявка → фаза rotate-rest-start фиксирует NEW в rest_pending;
    // ПЕРВЫЙ тик доставляет РОВНО ОДНУ ноду и это реплика (≤1 нода/тик);
    // txn НЕ выполняется (окно открыто).
    [Fact]
    public async Task Rotate_RestStart_FixesPendingAndDeliversReplicaFirst()
    {
        // Arrange: заявка.
        var rig = await NewRig();
        SeedTicket(rig.Etcd);

        // Act
        var outcome = await rig.Rotator.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: фаза rotate-rest-start зафиксировала pending NEW (32 симв),
        // доставлена одна нода — реплика shard1b; txn коммита НЕТ, заявка жива.
        outcome.IsSuccess.Should().BeTrue(outcome.Error?.ToString());
        outcome.Value.Should().Be(ProcessOutcome.Done);
        var work = await rig.Journal.ReadAsync("shop", CancellationToken.None);
        work.Value!.RestPending.Should().MatchRegex("^[A-Za-z0-9]{32}$");
        work.Value.Phase.Should().StartWith("rotate-rest-rolling/", "phase: {0}; error: {1}",
            work.Value.Phase, outcome.Error?.ToString());
        rig.Etcd.Store.Should().ContainKey("/pgworker/rotations/shop");
        rig.Channel.AppliedNodes.Should().ContainSingle("phase: {0}; error: {1}",
            work.Value.Phase, outcome.Error?.ToString()).Which.Should().Be("shard1b");
        // txn-коммита не было: rest_password в Store — ensure-значение, НЕ pending
        rig.Etcd.Store["/clusters/shop/rest_password"].Value
            .Should().NotBe(work.Value.RestPending);
    }

    // AAA (б): повторный тик — проход продолжается С ТОЙ ЖЕ парой (NEW не
    // регенерируется), доставляется следующая нода; txn НЕ выполняется.
    [Fact]
    public async Task Rotate_RestRolling_ContinuesSamePending()
    {
        // Arrange: тик 1 зафиксировал pending и доставил первую ноду.
        var rig = await NewRig();
        SeedTicket(rig.Etcd);
        (await rig.Rotator.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None))
            .Value.Should().Be(ProcessOutcome.Done);
        var pending1 = (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.RestPending;
        var appliedAfterFirst = rig.Channel.AppliedNodes.Count;

        // Act: повторный тик (окно ещё открыто).
        (await rig.Rotator.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None))
            .Value.Should().Be(ProcessOutcome.Done);

        // Assert: pending не изменился (та же пара в полёте); доставлена
        // следующая нода; заявка жива.
        var pending2 = (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.RestPending;
        pending2.Should().Be(pending1, "regeneration between ticks would reset the rolling progress");
        rig.Channel.AppliedNodes.Should().HaveCount(appliedAfterFirst + 1);
        rig.Etcd.Store.Should().ContainKey("/pgworker/rotations/shop", "the request is alive");
    }

    // AAA (в): все ноды применили NEW (статусы ready applied==mod NEW-ключа) →
    // txn: compare rest_password==OLD, put NEW, del заявки; pending сброшен.
    [Fact]
    public async Task Rotate_RestCommit_TxnComparePutAndPendingDrop()
    {
        // Arrange — заявка; окно закрывается fake-агентом (статусы по факту
        // применений delivery).
        var rig = await NewRig();
        SeedTicket(rig.Etcd);

        // Act
        var (committed, failed) = await RunUntilCommitAsync(rig);

        // Assert: rest_password==NEW, заявка удалена, pending сброшен; доставки
        // обеих нод shard1 — реплика первой.
        committed.Should().BeTrue();
        failed.Should().BeFalse();
        rig.Etcd.Store["/clusters/shop/rest_password"].Value
            .Should().MatchRegex("^[A-Za-z0-9]{32}$").And.NotBe("Old0Rest0000000000000000000000X");
        rig.Etcd.Store.Should().NotContainKey("/pgworker/rotations/shop");
        var work = await rig.Journal.ReadAsync("shop", CancellationToken.None);
        work.Value!.RestPending.Should().BeNull("the done phase resets pending");
        work.Value.Phase.Should().Be("done");
        rig.Channel.AppliedNodes.Where(n => n.StartsWith("shard1", StringComparison.Ordinal))
            .Should().ContainInOrder("shard1b", "shard1a");
    }

    // AAA (д): потеря pending (журнал перезаписан без поля) — свежая пара,
    // повторный rolling идемпотентен.
    [Fact]
    public async Task Rotate_RestPendingLost_FreshPair()
    {
        // Arrange: тик 1 зафиксировал pending; журнал «потерял» поле.
        var rig = await NewRig();
        SeedTicket(rig.Etcd);
        (await rig.Rotator.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None))
            .Value.Should().Be(ProcessOutcome.Done);
        var pending1 = (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.RestPending;
        await rig.Journal.WritePhaseAsync("shop", "rotate-app-password", "rotate-rest-rolling", "i1", null,
            CancellationToken.None, dropRestPending: true);

        // Act: тик — фиксирует НОВУЮ пару (окно снова открыто — ключи не под NEW).
        var outcome = await rig.Rotator.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: новая пара ≠ потерянная (pending в журнале); txn коммитит её
        // по ходу цикла (идемпотентность).
        outcome.Value.Should().Be(ProcessOutcome.Done);
        var pending2 = (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.RestPending
            ?? rig.Etcd.Store["/clusters/shop/rest_password"].Value;
        pending2.Should().NotBe(pending1).And.MatchRegex("^[A-Za-z0-9]{32}$");
    }

    // AAA (ж): шард без dsn — его ноды гейтом пропускаются (skip; ноды
    // создадутся с новой парой после R3).
    [Fact]
    public async Task Rotate_ShardWithoutDsn_NotDelivered()
    {
        // Arrange: второй шард без dsn; доставки только для dsn-шардов.
        var rig = await NewRig();
        SeedTicket(rig.Etcd);
        rig.Etcd.Store.Remove("/clusters/shop/shards/shard2/dsn");

        // Act
        var (committed, failed) = await RunUntilCommitAsync(rig);

        // Assert: доставки только shard1 (2 ноды); txn коммитит (шард2 — skip).
        committed.Should().BeTrue();
        failed.Should().BeFalse();
        rig.Channel.AppliedNodes.Should().OnlyContain(n => n.StartsWith("shard1", StringComparison.Ordinal));
        rig.Channel.AppliedNodes.Should().HaveCount(2);
        rig.Etcd.Store.Should().NotContainKey("/pgworker/rotations/shop");
    }

    // Общий ассерт коммита: заявка снята, ключ rest_password — NEW, pending сброшен.
    private static async Task AssertWindowCommittedAsync(Rig rig)
    {
        rig.Etcd.Store.Should().NotContainKey("/pgworker/rotations/shop", "the window is closed by the txn commit");
        rig.Etcd.Store["/clusters/shop/rest_password"].Value
            .Should().MatchRegex("^[A-Za-z0-9]{32}$");
        (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.RestPending
            .Should().BeNull("the done phase dropped the window");
    }

    // AAA: restore-шард (PLANNED/RUNNING/REJOINING — те же факты, что надзор)
    // НЕ удерживает окно: его ноды создадутся с новой парой после R3.
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
        var (committed, failed) = await RunUntilCommitAsync(rig);

        // Assert: restore-шард пропущен гейтом — txn коммитит.
        committed.Should().BeTrue();
        failed.Should().BeFalse();
        rig.Channel.AppliedNodes.Should().OnlyContain(n => n.StartsWith("shard2", StringComparison.Ordinal));
        await AssertWindowCommittedAsync(rig);
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
        var (committed, failed) = await RunUntilCommitAsync(rig);

        // Assert: object-ноды пропущены гейтом — txn коммитит.
        committed.Should().BeTrue();
        failed.Should().BeFalse();
        rig.Channel.AppliedNodes.Should().OnlyContain(n => n.StartsWith("shard2", StringComparison.Ordinal));
        await AssertWindowCommittedAsync(rig);
    }
}
