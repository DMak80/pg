using System.Net;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using Shared.Etcd.Client;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Processes;
using PgWorker.Provisioning.Probes;
using Xunit;

namespace PgWorker.UnitTests.Provisioning;

// Ротация per-cluster секретов по заявке /pgworker/rotations/<C> (t02, arch/14
// §5 I): ALTER трёх ролей на мастерах всех шардов с dsn → атомарный txn-коммит
// (новые креды + перезапись dsn + del заявки); transient-отказы.
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
    // (t02 — ensure читает, txn не ставит), portalloc (мастера — первые ноды,
    // host h1/pg 15000 и h2/pg 15001).
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
        WorkJournal Journal, ClusterSecretRotator Rotator, Fakes.FakeDriver Driver);

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
        var driver = new Fakes.FakeDriver
        {
            // t22: инспекция env нод — «rolling завершён» (hash pending журнала;
            // pending нет — считаем конвергентным: ключ кластера уже NEW после
            // прошлых тиков теста).
            InspectEnvOverride = (shard, node) => ReadNodeEnvFromJournal(store, shard, node),
        };
        var rotator = new ClusterSecretRotator(
            store, [Ep], usedSql, driver, probe, claims, journal, Secrets,
            new ClusterSecretEnsurer(store, [Ep]), snapshot: null);
        return new Rig(store, usedSql, claims, journal, rotator, driver);
    }

    // t22: env ноды по журналу ротации — hash(rest_pending) (rolling завершён);
    // pending нет — hash ключа rest_password (конвергентно).
    private static IReadOnlyDictionary<string, string> ReadNodeEnvFromJournal(
        Fakes.FakeEtcd etcd, string shard, string node)
    {
        var pending = ParseRestPending(etcd.Store.GetValueOrDefault("/pgworker/work/shop")?.Value);
        var key = pending ?? etcd.Store.GetValueOrDefault("/clusters/shop/rest_password")?.Value ?? "";
        return new Dictionary<string, string>
        {
            [RestRotation.EnvCert] = "cert",
            [RestRotation.EnvPasswordHash] = RestRotation.PasswordHash(key),
        };
    }

    private static string? ParseRestPending(string? journalJson)
    {
        if (journalJson is null)
            return null;
        try
        {
            using var doc = System.Text.Json.JsonDocument.Parse(journalJson);
            return doc.RootElement.TryGetProperty("rest_pending", out var p) && p.ValueKind == System.Text.Json.JsonValueKind.String
                ? p.GetString()
                : null;
        }
        catch (System.Text.Json.JsonException)
        {
            return null;
        }
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

        // Assert — Done без мутаций: ensure не вызван (нет txn), SQL не выполнялся
        outcome.IsSuccess.Should().BeTrue();
        outcome.Value.Should().Be(ProcessOutcome.Done);
        rig.Etcd.Txns.Should().BeEmpty();
        rig.Sql.Executed.Should().BeEmpty();
    }

    [Fact]
    public async Task Tick_Ticket_AltersAllShardsAndCommitsAtomically()
    {
        // Arrange — заявка стоит; оба шарда с dsn, мастера известны из master-ключей
        var rig = await NewRig();
        SeedTicket(rig.Etcd);

        // Act
        var outcome = await rig.Rotator.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert — ALTER трёх ролей + гвард→ALTER backup_exec на мастерах ОБЕИХ
        // шардов (8 SQL + 2 скаляра-гварда); одна txn: compare OLD (4 креда + 2 dsn)
        // + put новых кредов + перезапись dsn + del заявки
        outcome.IsSuccess.Should().BeTrue();
        var sqlTexts = rig.Sql.Executed.Select(e => e.Sql).ToList();
        sqlTexts.Should().HaveCount(8);
        sqlTexts.Count(s => s.Contains("ALTER ROLE \"app\" PASSWORD")).Should().Be(2);
        sqlTexts.Count(s => s.Contains("ALTER ROLE \"bucket_admin\" PASSWORD")).Should().Be(2);
        sqlTexts.Count(s => s.Contains("ALTER ROLE \"bucket_mover\" PASSWORD")).Should().Be(2);
        sqlTexts.Count(s => s.Contains("ALTER ROLE \"backup_exec\" PASSWORD")).Should().Be(2);
        rig.Sql.Scalars.Count(s => s.Sql.Contains("backup_exec")).Should().Be(2); // гварды

        var newApp = rig.Etcd.Store["/clusters/shop/app_password"].Value;
        var newMover = rig.Etcd.Store["/clusters/shop/mover_password"].Value;
        var newAdmin = rig.Etcd.Store["/clusters/shop/bucket_admin_password"].Value;
        newApp.Should().MatchRegex("^[A-Za-z0-9]{32}$").And.NotBe("OldPassword000000000000000000A");
        newMover.Should().MatchRegex("^[A-Za-z0-9]{32}$").And.NotBe("OldMoverPass0000000000000000000A");
        newAdmin.Should().MatchRegex("^[A-Za-z0-9]{32}$").And.NotBe("OldAdminPass000000000000000A");
        var newBackup = rig.Etcd.Store["/clusters/shop/backup_password"].Value;
        newBackup.Should().MatchRegex("^[A-Za-z0-9]{32}$").And.NotBe("OldBackupPass0000000000000000000A");
        rig.Etcd.Store.Should().NotContainKey("/pgworker/rotations/shop");

        // Коммит-txn узнаём по del заявки (ensure-txn R1 тоже ставит puts — t02)
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
        var outcome = await rig.Rotator.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert — роль создана (CREATE ×2 шарда), ALTER backup_exec не было;
        // ротация успешна: заявка закрыта, backup_password перезаписан.
        outcome.IsSuccess.Should().BeTrue(outcome.Error?.ToString());
        var executed = rig.Sql.Executed.Select(e => e.Sql).ToList();
        executed.Count(s => s.Contains("CREATE ROLE \"backup_exec\"")).Should().Be(2);
        executed.Should().NotContain(s => s.Contains("ALTER ROLE \"backup_exec\""));
        rig.Etcd.Store.Should().NotContainKey("/pgworker/rotations/shop");
        rig.Etcd.Store["/clusters/shop/backup_password"].Value
            .Should().MatchRegex("^[A-Za-z0-9]{32}$");
    }

    // Деградировавший кейс бага t04-гейта (finding P1): при EnableDoorman=false
    // master-ключ вырождается в <host>:0 — оба ноды шарда на одном хосте равнозначны,
    // и старый host-match возвращал ПРОИЗВОЛЬНУЮ ноду (ALTER ROLE на реплике →
    // 25006 read-only, ротация зацикливалась). Резолв обязан идти по уникальному
    // doorman-порту из ключа.
    [Fact]
    public async Task Tick_SharedHostMasterKey_ResolvesByDoormanPort()
    {
        // Arrange — заявка; shard1: обе ноды на хосте h1, фактический мастер —
        // shard1b (doorman 16502); ключ «host:0-вырожденный» не используем, но и
        // host-матч тут тоже промахнулся бы (h1 == Host обеих нод шарда)
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
        var outcome = await rig.Rotator.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert — ALTER уходит на фактическую ноду-мастера (Port=15002/pg shard1b),
        // а не на первую по порядку portalloc ноду шарда (Port=15000, shard1a)
        outcome.IsSuccess.Should().BeTrue(outcome.Error?.ToString());
        var alterDsns = rig.Sql.Executed
            .Where(e => e.Sql.Contains("ALTER ROLE \"app\" PASSWORD"))
            .Select(e => e.Dsn).ToList();
        alterDsns.Should().HaveCount(2);
        alterDsns.Should().Contain(dsn => dsn.Contains("Port=15002"), "мастер shard1 — shard1b (doorman 16502)");
        alterDsns.Should().NotContain(dsn => dsn.Contains("Port=15000"), "shard1a — реплика, ALTER на ней невозможен");
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
    public async Task Tick_ExternalDsnChange_CompareLostRetriable()
    {
        // Arrange — внешняя запись dsn (репарация/оператор) между чтением и txn:
        // инъекция при ПЕРВОМ ALTER (t02 §5 I R3: compare по dsn закрывает гонку)
        var rig = await NewRig();
        SeedTicket(rig.Etcd);
        var alters = 0;
        rig.Sql.OnExecute = _ =>
        {
            if (++alters == 1)
                rig.Etcd.Seed("/clusters/shop/shards/shard1/dsn",
                    "host=h9 port=19999 dbname=shop user=bucket_admin password=changed");
        };

        // Act
        var outcome = await rig.Rotator.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert — compare dsn проигран: заявка жива, креды не перезаписаны
        outcome.IsSuccess.Should().BeFalse();
        rig.Etcd.Store["/clusters/shop/app_password"].Value
            .Should().Be("OldPassword000000000000000000A");
        rig.Etcd.Store["/clusters/shop/bucket_admin_password"].Value
            .Should().Be("OldAdminPass000000000000000A");
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
        var journal = new WorkJournal("/pgworker", etcd, [Ep]);
        var probe = new ShardProbe(new HttpClient(new DeadHandler()));
        var driver = new Fakes.FakeDriver
        {
            InspectEnvOverride = (shard, node) => ReadNodeEnvFromJournal(etcd, shard, node),
        };
        var rotator = new ClusterSecretRotator(
            etcd, [Ep], new Fakes.FakeSql(), driver, probe,
            new ClaimStore("/pgworker", [Ep], etcd, TimeProvider.System), journal, Secrets,
            new ClusterSecretEnsurer(etcd, [Ep]), snapshot: null);

        // Act
        var outcome = await rotator.TickAsync(await Snapshot(etcd), CancellationToken.None);

        // Assert — отказ до любых мутаций
        outcome.IsSuccess.Should().BeFalse();
        etcd.Txns.Should().BeEmpty();
        etcd.Store["/clusters/shop/app_password"].Value
            .Should().Be("OldPassword000000000000000000A");
    }
    // ── t22: rest-ветка ротации (четвёртый секрет, arch/14 §5 I) ──

    // AAA (а): заявка → фаза rotate-rest-start фиксирует NEW в rest_pending;
    // rolling-гейт открыт (нода с чужим hash) — txn НЕ выполняется.
    [Fact]
    public async Task Rotate_RestStart_FixesPendingAndWaitsRolling()
    {
        // Arrange: заявка; инспекция env — нода несёт ЧУЖОЙ hash (rolling идёт).
        var rig = await NewRig();
        SeedTicket(rig.Etcd);
        rig.Driver.InspectEnvOverride = (shard, node) => new Dictionary<string, string>
        {
            [RestRotation.EnvCert] = "cert",
            [RestRotation.EnvPasswordHash] = RestRotation.PasswordHash("Foreign0Pass0000000000000000000X"),
        };

        // Act
        var outcome = await rig.Rotator.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: фаза rotate-rest-start зафиксировала pending NEW (32 симв),
        // txn коммита НЕТ (окно открыто), заявка жива.
        outcome.Value.Should().Be(ProcessOutcome.Done);
        var work = await rig.Journal.ReadAsync("shop", CancellationToken.None);
        work.Value!.RestPending.Should().MatchRegex("^[A-Za-z0-9]{32}$");
        work.Value.Phase.Should().StartWith("rotate-rest-rolling/");
        rig.Etcd.Store.Should().ContainKey("/pgworker/rotations/shop");
        // txn-коммита не было: rest_password в Store — ensure-значение, НЕ pending
        rig.Etcd.Store["/clusters/shop/rest_password"].Value
            .Should().NotBe(work.Value.RestPending);
    }

    // AAA (б): повторный тик при недомигрированной ноде — проход продолжается
    // С ТОЙ ЖЕ парой (NEW не регенерируется), txn НЕ выполняется.
    [Fact]
    public async Task Rotate_RestRolling_ContinuesSamePending()
    {
        // Arrange: тик 1 зафиксировал pending; rolling не завершён.
        var rig = await NewRig();
        SeedTicket(rig.Etcd);
        rig.Driver.InspectEnvOverride = (shard, node) => new Dictionary<string, string>
        {
            [RestRotation.EnvCert] = "cert",
            [RestRotation.EnvPasswordHash] = RestRotation.PasswordHash("Foreign0Pass0000000000000000000X"),
        };
        (await rig.Rotator.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None))
            .Value.Should().Be(ProcessOutcome.Done);
        var pending1 = (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.RestPending;

        // Act: повторный тик (rolling всё ещё открыт).
        (await rig.Rotator.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None))
            .Value.Should().Be(ProcessOutcome.Done);

        // Assert: pending не изменился (та же пара в полёте).
        var pending2 = (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.RestPending;
        pending2.Should().Be(pending1, "регенерация между тиками обнуляла бы прогресс rolling");
        rig.Etcd.Store.Should().ContainKey("/pgworker/rotations/shop", "заявка жива");
    }

    // AAA (в): все ноды несут hash(NEW) → txn: compare rest_password==OLD,
    // put NEW, del заявки; pending сброшен фазой done.
    [Fact]
    public async Task Rotate_RestCommit_TxnComparePutAndPendingDrop()
    {
        // Arrange: тик 1 — pending зафиксирован; rolling завершён (override
        // возвращает hash(pending) — дефолт NewRig-хелпера уже это делает).
        var rig = await NewRig();
        SeedTicket(rig.Etcd);
        // Тик 1: rolling открыт (чужой hash) — pending зафиксирован, txn нет.
        rig.Driver.InspectEnvOverride = (shard, node) => new Dictionary<string, string>
        {
            [RestRotation.EnvCert] = "cert",
            [RestRotation.EnvPasswordHash] = RestRotation.PasswordHash("Foreign0Pass0000000000000000000X"),
        };
        (await rig.Rotator.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None))
            .Value.Should().Be(ProcessOutcome.Done);
        var pending = (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.RestPending;
        pending.Should().NotBeNull("тик 1 зафиксировал пару, txn не было");

        // Act: rolling завершён — повторный тик несёт txn-коммит.
        rig.Driver.InspectEnvOverride = (shard, node) => ReadNodeEnvFromJournal(rig.Etcd, shard, node);
        var outcome = await rig.Rotator.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: rest_password==NEW, заявка удалена, pending сброшен.
        outcome.Value.Should().Be(ProcessOutcome.Done);
        rig.Etcd.Store["/clusters/shop/rest_password"].Value.Should().Be(pending);
        rig.Etcd.Store.Should().NotContainKey("/pgworker/rotations/shop");
        var work = await rig.Journal.ReadAsync("shop", CancellationToken.None);
        work.Value!.RestPending.Should().BeNull("фаза done сбрасывает pending");
        work.Value.Phase.Should().Be("done");
    }

    // AAA (г): проигрыш compare (внешняя запись etcdctl) — commit-conflict,
    // pending сброшен — повторный проход со свежей парой.
    [Fact]
    public async Task Rotate_RestCompareConflict_PendingDropped()
    {
        // Arrange: pending зафиксирован, rolling завершён; ВНЕШНЯЯ запись
        // rest_password мимо ротации.
        var rig = await NewRig();
        SeedTicket(rig.Etcd);
        // Тик 1: rolling открыт (чужой hash) — pending зафиксирован, txn нет.
        rig.Driver.InspectEnvOverride = (shard, node) => new Dictionary<string, string>
        {
            [RestRotation.EnvCert] = "cert",
            [RestRotation.EnvPasswordHash] = RestRotation.PasswordHash("Foreign0Pass0000000000000000000X"),
        };
        (await rig.Rotator.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None))
            .Value.Should().Be(ProcessOutcome.Done);
        // Rolling завершён (default-хелпер); ВНЕШНЯЯ запись dsn ПОСЛЕ чтения
        // снапшота тика — compare dsn проиграет (честный конфликт SQL-секретов).
        rig.Driver.InspectEnvOverride = (shard, node) => ReadNodeEnvFromJournal(rig.Etcd, shard, node);
        var snap = await Snapshot(rig.Etcd);
        rig.Etcd.Store["/clusters/shop/shards/shard1/dsn"] =
            new Fakes.FakeEtcd.Entry("host=h9 dbname=shop user=bucket_admin", 9, 9);

        // Act: тик коммита — compare проигран (dsn изменился).
        var outcome = await rig.Rotator.TickAsync(snap, CancellationToken.None);

        // Assert: commit-conflict + pending сброшен (свежая пара в повторе).
        outcome.IsSuccess.Should().BeFalse();
        var work = await rig.Journal.ReadAsync("shop", CancellationToken.None);
        work.Value!.Phase.Should().Be("commit-conflict");
        work.Value.RestPending.Should().BeNull("повторный проход — со свежей парой");
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

        // Act: тик — фиксирует НОВУЮ пару (rolling по override завершён сразу).
        var outcome = await rig.Rotator.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: новая пара ≠ потерянная; txn коммитит её (идемпотентность).
        outcome.Value.Should().Be(ProcessOutcome.Done);
        rig.Etcd.Store["/clusters/shop/rest_password"].Value
            .Should().NotBe(pending1).And.MatchRegex("^[A-Za-z0-9]{32}$");
    }

    // AAA (ж): шард без dsn — его ноды не инспектируются (skip; ноды создадутся
    // с новой парой после R3).
    [Fact]
    public async Task Rotate_ShardWithoutDsn_NotInspected()
    {
        // Arrange: второй шард без dsn; инспекция только для dsn-шардов.
        var rig = await NewRig();
        SeedTicket(rig.Etcd);
        var inspected = new List<string>();
        rig.Driver.InspectEnvOverride = (shard, node) =>
        {
            inspected.Add(shard);
            return new Dictionary<string, string>
            {
                [RestRotation.EnvCert] = "cert",
                [RestRotation.EnvPasswordHash] = RestRotation.PasswordHash(
                    ParseRestPending(rig.Etcd.Store.GetValueOrDefault("/pgworker/work/shop")?.Value)
                    ?? rig.Etcd.Store.GetValueOrDefault("/clusters/shop/rest_password")?.Value ?? ""),
            };
        };
        // шард2 без dsn: перезапишем пустой строкой-«нет ключа» — уберём dsn.
        rig.Etcd.Store.Remove("/clusters/shop/shards/shard2/dsn");

        // Act
        var outcome = await rig.Rotator.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: инспекция только shard1; txn коммитит (шард2 — skip, R3 не ждёт его).
        outcome.Value.Should().Be(ProcessOutcome.Done);
        inspected.Should().OnlyContain(sh => sh == "shard1");
        rig.Etcd.Store.Should().NotContainKey("/pgworker/rotations/shop");
    }

    // ── t22 рев.3, П2: гварды R2-rest гейта — окно не удерживают ──

    // Общий ассерт коммита: заявка снята, ключ rest_password — NEW, pending сброшен.
    private static async Task AssertWindowCommittedAsync(Rig rig)
    {
        rig.Etcd.Store.Should().NotContainKey("/pgworker/rotations/shop", "окно закрыто txn-коммитом");
        rig.Etcd.Store["/clusters/shop/rest_password"].Value
            .Should().MatchRegex("^[A-Za-z0-9]{32}$");
        (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.RestPending
            .Should().BeNull("фаза done сбросила окно");
    }

    // AAA: restore-шард (PLANNED/RUNNING/REJOINING — те же факты, что надзор)
    // НЕ удерживает окно: его ноды создадутся с новой парой после R3.
    [Fact]
    public async Task Rotate_RestoreShard_DoesNotHoldRestWindow()
    {
        // Arrange: shard1 — активная restore (демонтаж владеет restore-процессом);
        // env shard1 расходится (чужой hash), shard2 конвергентен.
        var etcd = new Fakes.FakeEtcd();
        SeedCluster(etcd);
        etcd.Seed("/pgworker/backups/shop/shard1/restore/r1",
            """{"state":"RUNNING","backup_id":"f1","source":"shard1","target":"shop-new","node":"pgw-r1","requested_unix":1755900000,"requested_by":"admin"}""");
        SeedTicket(etcd);
        var rig = await NewRig(etcd);
        rig.Driver.InspectEnvOverride = (shard, node) => shard == "shard1"
            ? new Dictionary<string, string>
            {
                [RestRotation.EnvCert] = "cert",
                [RestRotation.EnvPasswordHash] = RestRotation.PasswordHash("Stale0Pass0000000000000000000000X"),
            }
            : ReadNodeEnvFromJournal(rig.Etcd, shard, node);

        // Act
        var outcome = await rig.Rotator.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: restore-шард не инспектируется гейтом — txn коммитит.
        outcome.IsSuccess.Should().BeTrue(outcome.Error?.ToString());
        await AssertWindowCommittedAsync(rig);
    }

    // AAA: усыновлённая нода (object) не инспектируется — пустой словарь по
    // каноническому имени не показатель; окно ею не удерживается.
    [Fact]
    public async Task Rotate_AdoptedNode_DoesNotHoldRestWindow()
    {
        // Arrange: обе ноды shard1 — object (внешняя orchestration), env расходится.
        var rig = await NewRig();
        SeedTicket(rig.Etcd);
        var raw = rig.Etcd.Store["/pgworker/portalloc/shop"].Value;
        var parsed = Portalloc.Parse("shop", raw).Value!;
        var mutable = parsed.ToDictionary(p => p.Key, p => p.Value);
        mutable["shard1/shard1a"] = mutable["shard1/shard1a"] with { Object = "external-a" };
        mutable["shard1/shard1b"] = mutable["shard1/shard1b"] with { Object = "external-b" };
        rig.Etcd.Store["/pgworker/portalloc/shop"] =
            new Fakes.FakeEtcd.Entry(Portalloc.Serialize(mutable), 2, 2);
        var inspected = new List<string>();
        rig.Driver.InspectEnvOverride = (shard, node) =>
        {
            if (shard == "shard1")
                inspected.Add(node); // не должен зваться
            return shard == "shard1"
                ? new Dictionary<string, string>
                {
                    [RestRotation.EnvCert] = "cert",
                    [RestRotation.EnvPasswordHash] = RestRotation.PasswordHash("Stale0Pass0000000000000000000000X"),
                }
                : ReadNodeEnvFromJournal(rig.Etcd, shard, node); // shard2 конвергентен
        };

        // Act
        var outcome = await rig.Rotator.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: object-ноды пропущены гейтом — txn коммитит.
        outcome.IsSuccess.Should().BeTrue(outcome.Error?.ToString());
        inspected.Should().BeEmpty("object-ноды не инспектируются");
        await AssertWindowCommittedAsync(rig);
    }

    // AAA: драйвер без честной инспекции (Swarm-заглушка) — окно НЕ удерживается
    // (rolling-шаг там no-op, вечное ожидание лишь блокировало бы ротацию).
    [Fact]
    public async Task Rotate_SwarmInspection_FastPassRestWindow()
    {
        // Arrange: Swarm-семантика инспекции; env расходится у всех нод.
        var rig = await NewRig();
        SeedTicket(rig.Etcd);
        rig.Driver.SupportsRunningInspection = false;
        rig.Driver.InspectEnvOverride = (shard, node) => new Dictionary<string, string>
        {
            [RestRotation.EnvCert] = "cert",
            [RestRotation.EnvPasswordHash] = RestRotation.PasswordHash("Stale0Pass0000000000000000000000X"),
        };

        // Act
        var outcome = await rig.Rotator.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: гейт отключён — txn коммитит (приведение env Swarm-нод — t11).
        outcome.IsSuccess.Should().BeTrue(outcome.Error?.ToString());
        await AssertWindowCommittedAsync(rig);
    }

}
