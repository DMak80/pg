using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using System.Security.Cryptography.X509Certificates;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Core.Tuning;
using PgWorker.Docker.Drivers;
using PgWorker.Etcd.Parsing;
using PgWorker.IntegrationTests.E2e;
using PgWorker.Provisioning.Processes;
using PgWorker.Provisioning.Probes;
using PgWorker.Provisioning.Sql;
using Shared.Core;
using Shared.Etcd.Client;
using Xunit;

namespace PgWorker.IntegrationTests.Docker;

// R2-rest ротации rest_password БЕЗ пересоздания (arch/pgworker/processes/i-secret-rotator.md I, spec §3.7.4):
// заявка → тик ротатора открывает окно (rest_window_open) и начинает доставку
// каналом (bootstrap-перерендер ключа ОДНОЙ ноды + apply) — реплики вперёд,
// лидер последним, ≤1 нода/тик; fake-агент по факту «применения» публикует
// статусный ключ (ready, applied=mod) в etcd; прогресс — немутирующая проба
// IsOnRevisionAsync (content==рендер под NEW); после всех нод — txn-коммит
// R3 (ключ==NEW, заявка снята, pending сброшен). Идемпотентность повторной
// заявки; no-op тика без заявки. Живой etcd (EtcdFixture) + фейк-канал —
// docker-стаб не нужен (env-инспекции больше нет, t05). Полный teardown:
// etcd-префиксы кластера удаляются (DisposeAsync-образец рига).
[Collection(NonE2eCollection.Name)]
public class RestRotationTests(Etcd.EtcdFixture fixture)
{
    private const string Cluster = "shop";
    private const string Shard = "shard1";
    private const string Scope = $"{Cluster}-{Shard}";
    private const string BootstrapA = $"/clusters/{Cluster}/shards/{Shard}/nodes/shard1a/bootstrap";
    private const string BootstrapB = $"/clusters/{Cluster}/shards/{Shard}/nodes/shard1b/bootstrap";
    private const string StatusA = $"/pgworker/nodes/{Cluster}/{Shard}/shard1a/status";
    private const string StatusB = $"/pgworker/nodes/{Cluster}/{Shard}/shard1b/status";

    // ETCD-FIRST (arch/pgworker/processes/i-secret-rotator.md I, t05 рев. 8): R2 txn-коммит NEW-четвёрки В
    // НАЧАЛЕ окна (флаг rest_window_open:true), R3 rolling rest-пары каналом
    // (реплика раньше лидера, ≤1 нода/тик), R4 финальные ALTER по NEW из etcd
    // (закрытие окна). В окне app-креды etcd (NEW) против pg_authid (OLD) —
    // 28P01 транзиент; флаг переносят ВСЕ писатели журнала (в т.ч.
    // supervise-подобные фазовые записи).
    [Fact]
    public async Task Rotation_EtcdFirst_CommitsThenRollsThenAlters()
    {
        var ct = TestContext.Current.CancellationToken;
        var gateway = fixture.Gateway;
        var endpoint = fixture.Endpoint;

        // Arrange: сид контроль-плейна (живой etcd) — dsn-шард из двух нод
        // (shard1a — лидер по master-ключу), заявки ресурсов обязательны
        // (arch/pgworker/01-placement.md §2 — fail-fast без них).
        const string oldPassword = "old0Rest0000000000000000000000A";
        await SeedAsync(gateway, endpoint, oldPassword, ct);
        var claims = new ClaimStore("/pgworker", [endpoint], gateway,
            TimeProvider.System, "instance-test");
        (await claims.TryClaimClusterAsync(Cluster, ct)).Value.Should().BeTrue("the contour claim");

        var channel = new FakeChannel(gateway, endpoint);
        var cache = NewCache();
        var bootstrap = new NodeBootstrapDelivery(
            gateway, [endpoint], channel, cache);
        var rotator = new ClusterSecretRotator(
            gateway, [endpoint], new StubSql(), new ShardProbe(new HttpClient()),
            claims, new WorkJournal("/pgworker", gateway, [endpoint]),
            new InstallSecrets("su-pw", "sb-pw", "adm-pw", "mov-pw"),
            new ClusterSecretEnsurer(gateway, [endpoint]),
            bootstrap,
            new PgtuneInputsFactory(
                new PgtuneSettings(18, "oltp", "ssd", "mid_ram", 60,
                    new HashSet<string>(StringComparer.Ordinal)),
                NullLogger<PgtuneInputsFactory>.Instance));

        // Стационарный контур: bootstrap-ключи обеих нод с OLD-парой внутри
        // spilo_configuration; статусы ready с applied==mod(OLD) — модель
        // работающего кластера до заявки (гвард «тихого окна» требует ready).
        foreach (var node in new[] { "shard1a", "shard1b" })
        {
            var key = $"/clusters/{Cluster}/shards/{Shard}/nodes/{node}/bootstrap";
            await gateway.PutAsync(endpoint, key,
                BootstrapJson(oldPassword, cache, node), null, ct);
            var mod = (await gateway.GetAsync(endpoint, key, ct)).Value!.ModRevision;
            await SeedStatusAsync(gateway, endpoint, node, (long)mod, ct);
        }

        var snapshot = await SnapshotAsync(gateway, endpoint, ct);

        // Т10 (MR-#17): чистка etcd-префиксов в finally — падение ассерта
        // выше НЕ пропускает чистку (префиксы не протухают в etcd фикстуры).
        try
        {
        // Регрессия: тик ДО заявки — мутаций нет.
        var before = await rotator.TickAsync(snapshot, ct);
        before.IsSuccess.Should().BeTrue(before.Error?.ToString());
        (await TicketAsync(gateway, endpoint, ct)).Should().BeNull("no ticket before rotation");
        channel.AppliedNodes.Should().BeEmpty("no delivery before the ticket");
        StubSql.Executed.Clear();

        // Act: заявка → тики до фазы done.
        await SubmitTicketAsync(gateway, endpoint, ct);
        var committed = await RunUntilDoneAsync(rotator, gateway, endpoint, channel, ct);

        // Assert (1): первый тик — txn-коммит ДО первой доставки: ключи etcd
        // NEW, заявка снята той же txn, фаза rotate-etcd-committed, флаг true.
        var appAfter = (await gateway.GetAsync(endpoint,
            $"/clusters/{Cluster}/app_password", ct)).Value!.Value;
        appAfter.Should().MatchRegex("^[A-Za-z0-9]{32}$").And.NotBe("app0Pw00000000000000000000001");
        (await gateway.GetAsync(endpoint,
            $"/clusters/{Cluster}/rest_password", ct)).Value!.Value
            .Should().MatchRegex("^[A-Za-z0-9]{32}$").And.NotBe(oldPassword);

        // Assert (2): rolling — по одной ноде за тик, реплика раньше лидера.
        channel.AppliedNodes.Should().HaveCount(2);
        channel.AppliedNodes.Should().ContainInOrder("shard1b", "shard1a");

        // Assert (4): финальный ALTER — после последнего apply: StubSql — по
        // три ALTER (app/bucket_admin/bucket_mover) на мастера шарда, пароли
        // == NEW-ключам etcd; backup_exec отсутствует.
        var sqlTexts = StubSql.Executed.ToList();
        sqlTexts.Count(s => s.Contains("ALTER ROLE \"app\" PASSWORD")).Should().Be(1);
        sqlTexts.Count(s => s.Contains("ALTER ROLE \"bucket_admin\" PASSWORD")).Should().Be(1);
        sqlTexts.Count(s => s.Contains("ALTER ROLE \"bucket_mover\" PASSWORD")).Should().Be(1);
        sqlTexts.Should().NotContain(s => s.Contains("backup_exec"));
        sqlTexts.Should().Contain(s =>
            s.Contains("ALTER ROLE \"app\" PASSWORD") && s.Contains(appAfter));

        // Assert (5)/(8): done — флаг false; в work-JSON НЕТ легаси-поля pending-пары.
        committed.Should().BeTrue("the window must close within the tick budget");
        var workJson = (await gateway.GetAsync(endpoint, $"/pgworker/work/{Cluster}", ct)).Value!.Value;
        workJson.Should().Contain("\"rest_window_open\":false");
        workJson.Should().NotContain("\"rest_pending\"");
        (await TicketAsync(gateway, endpoint, ct)).Should().BeNull("R2 removed the request");

        // Assert (6): takeover — новый ротатор при открытом флаге продолжает
        // без заявки; здесь окно закрыто — повторный тик без заявки — no-op.
        var noop = await rotator.TickAsync(await SnapshotAsync(gateway, endpoint, ct), ct);
        noop.IsSuccess.Should().BeTrue();
        channel.AppliedNodes.Should().HaveCount(2, "no deliveries without a ticket");

        // Assert (7): вторая заявка — НОВЫЙ проход (коммит + rolling + done).
        await SubmitTicketAsync(gateway, endpoint, ct);
        var committed2 = await RunUntilDoneAsync(rotator, gateway, endpoint, channel, ct);
        committed2.Should().BeTrue("the second pass is committed");
        channel.AppliedNodes.Should().HaveCount(4, "both nodes delivered again");
        var key2 = (await gateway.GetAsync(endpoint, $"/clusters/{Cluster}/rest_password", ct)).Value!.Value;
        key2.Should().NotBe(oldPassword, "a repeated pass — a fresh pair");

        }
        finally
        {
        // Teardown: etcd-префиксы кластера + узлы (по образцу DisposeAsync рига).
        await gateway.DeleteAsync(endpoint, $"/clusters/{Cluster}/", prefix: true, ct);
        await gateway.DeleteAsync(endpoint, $"/pgworker/nodes/{Cluster}/", prefix: true, ct);
        await gateway.DeleteAsync(endpoint, $"/pgworker/work/{Cluster}", prefix: false, ct);
        await gateway.DeleteAsync(endpoint, $"/pgworker/rotations/{Cluster}", prefix: false, ct);
        await gateway.DeleteAsync(endpoint, $"/pgworker/claims/{Cluster}", prefix: false, ct);
        await gateway.DeleteAsync(endpoint, $"/service/{Scope}/", prefix: true, ct);
        }
    }

    // Цикл тиков ротатора до journal-фазы done (окно закрыто ETCD-FIRST-порядком).
    private static async Task<bool> RunUntilDoneAsync(
        ClusterSecretRotator rotator, EtcdGateway gateway, string endpoint,
        FakeChannel channel, CancellationToken ct)
    {
        for (var i = 0; i < 10; i++)
        {
            var rot = await rotator.TickAsync(await SnapshotAsync(gateway, endpoint, ct), ct);
            rot.IsSuccess.Should().BeTrue(rot.Error?.ToString());
            var work = await gateway.GetAsync(endpoint, $"/pgworker/work/{Cluster}", ct);
            if (work.Value is not null && work.Value.Value.Contains("\"phase\":\"done\""))
                return true; // R4 закрыл окно фазой done
        }

        return false;
    }

    // ── Сид/хелперы ──

    private static async Task SeedAsync(
        EtcdGateway gateway, string endpoint, string restPassword, CancellationToken ct)
    {
        await gateway.PutAsync(endpoint, $"/clusters/{Cluster}/config",
            $$"""{"buckets":12,"dbname":"{{Cluster}}","created_unix":1755900000}""", null, ct);
        await gateway.PutAsync(endpoint, $"/clusters/{Cluster}/shards/{Shard}/replicas", "2", null, ct);
        await gateway.PutAsync(endpoint, $"/clusters/{Cluster}/shards/{Shard}/dsn",
            $"host=h1,h1 port=15432,15433 dbname={Cluster} user=bucket_admin password=adm-pw", null, ct);
        // master-ключ byName-формата: резолв мастера из portalloc БЕЗ сети.
        await gateway.PutAsync(endpoint, $"/clusters/{Cluster}/shards/{Shard}/master", "shard1a:16432", null, ct);
        // HA-лидер (R2-rest порядок: реплики вперёд, лидер последним).
        await gateway.PutAsync(endpoint, $"/service/{Scope}/leader", "shard1a", null, ct);
        await gateway.PutAsync(endpoint, $"/clusters/{Cluster}/rest_password", restPassword, null, ct);
        foreach (var node in new[] { "shard1a", "shard1b" })
        {
            await gateway.PutAsync(endpoint,
                $"/clusters/{Cluster}/shards/{Shard}/nodes/{node}/state", "RUNNING", null, ct);
        }
        // portalloc: обе ноды на хосте h1 (адреса снапшота достаточны для topology).
        await gateway.PutAsync(endpoint, $"/pgworker/portalloc/{Cluster}",
            """{"shard1/shard1a":{"host":"h1","ports":{"pg":15432,"patroni":18008,"doorman":16432}},"shard1/shard1b":{"host":"h1","ports":{"pg":15433,"patroni":18009,"doorman":16433}}}""",
            null, ct);
        // Заявки ресурсов ОБЯЗАТЕЛЬНЫ (arch/pgworker/01-placement.md §2) — fail-fast без них.
        await gateway.PutAsync(endpoint, $"/service/{Scope}/request_cpu", "2", null, ct);
        await gateway.PutAsync(endpoint, $"/service/{Scope}/request_mem", "4Gi", null, ct);
        // per-cluster креды приложения (R1 ensure прочтёт/дополнит).
        await gateway.PutAsync(endpoint, $"/clusters/{Cluster}/app_user", "app", null, ct);
        await gateway.PutAsync(endpoint, $"/clusters/{Cluster}/app_password", "app0Pw00000000000000000000001", null, ct);
        await gateway.PutAsync(endpoint, $"/clusters/{Cluster}/mover_password", "mov0Pw00000000000000000000002", null, ct);
        await gateway.PutAsync(endpoint, $"/clusters/{Cluster}/bucket_admin_user", "bucket_admin", null, ct);
        await gateway.PutAsync(endpoint, $"/clusters/{Cluster}/bucket_admin_password", "adm0Pw00000000000000000000003", null, ct);
        await gateway.PutAsync(endpoint, $"/clusters/{Cluster}/backup_password", "bak0Pw00000000000000000000004", null, ct);
    }

    private static async Task SubmitTicketAsync(EtcdGateway gateway, string endpoint, CancellationToken ct)
        => await gateway.PutAsync(endpoint, $"/pgworker/rotations/{Cluster}",
            $$"""{"requested_unix":{{DateTimeOffset.UtcNow.ToUnixTimeSeconds()}}}""", null, ct);

    private static async Task<Kv?> TicketAsync(EtcdGateway gateway, string endpoint, CancellationToken ct)
        => (await gateway.GetAsync(endpoint, $"/pgworker/rotations/{Cluster}", ct)).Value;

    private static async Task<ClusterSnapshot> SnapshotAsync(EtcdGateway gateway, string endpoint, CancellationToken ct)
    {
        var range = await gateway.RangeAsync(endpoint, "/clusters/", ct);
        var parsed = ClusterSnapshotParser.ParseClusters(range.Value, out _);
        return parsed.Value.Single(c => c.Config.Cluster == Cluster);
    }

    private static async Task SeedStatusAsync(
        EtcdGateway gateway, string endpoint, string node, long applied, CancellationToken ct)
    {
        var status = JsonSerializer.Serialize(new Dictionary<string, object?>
        {
            ["state"] = "ready",
            ["applied_revision"] = applied,
            ["components"] = new Dictionary<string, string> { ["pg"] = "ready", ["doorman"] = "ready" },
            ["updated_unix"] = 1,
        });
        await gateway.PutAsync(endpoint,
            $"/pgworker/nodes/{Cluster}/{Shard}/{node}/status", status, null, ct);
    }

    // Детерминированный рендер bootstrap-ключа (тот же NodeBootstrapBuilder,
    // что у delivery — content-сверка пробы сойдётся).
    private static string BootstrapJson(
        string restPassword, RestCertificateCache cache, string node)
    {
        var topology = new ShardTopology(Cluster, Shard, Scope, new Dictionary<string, NodeAddress>());
        var etcdEndpoints = new EtcdEndpoints([]);
        var (certPem, keyPem) = cache.GetOrCreate(Cluster, Shard, node);
        return NodeBootstrapBuilder.Build(topology, etcdEndpoints, syncStrict: false,
            node, certPem, keyPem, restPassword, tuning: null, excludeParams: null);
    }

    // Тестовый CA (парсер TestRestCa юнит-проекта недоступен здесь — локальный).
    private static RestCertificateCache NewCache()
    {
        using var rsa = System.Security.Cryptography.RSA.Create(2048);
        var request = new System.Security.Cryptography.X509Certificates.CertificateRequest(
            $"CN=pgw-test-install-ca-{Guid.NewGuid():N}", rsa,
            System.Security.Cryptography.HashAlgorithmName.SHA256,
            System.Security.Cryptography.RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(
            new System.Security.Cryptography.X509Certificates.X509BasicConstraintsExtension(true, false, 0, true));
        using var ca = request.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(3650));
        return new RestCertificateCache(
            ca.ExportCertificatePem(), ca.GetRSAPrivateKey()!.ExportPkcs8PrivateKeyPem());
    }

    private sealed class StubSql : ISqlExecutor
    {
        public static readonly List<string> Executed = [];

        public Task<Result> ExecuteAsync(string dsn, string sql, CancellationToken ct)
        {
            Executed.Add(sql);
            return Task.FromResult(Result.Success());
        }

        public Task<Result<object?>> ExecuteScalarAsync(string dsn, string sql, CancellationToken ct)
            => Task.FromResult(Result<object?>.Success(null));

        public Task<Result> EnsureDatabaseAsync(string dsn, string dbname, CancellationToken ct)
            => Task.FromResult(Result.Success());
    }

    // Фейк INodeChannelClient + fake-агент: команда читает bootstrap-ключ из
    // etcd и публикует статусный ключ (ready, applied=mod) — модель применения;
    // AppliedNodes — журнал порядка применения (адрес → имя ноды).
    private sealed class FakeChannel(EtcdGateway gateway, string endpoint) : INodeChannelClient
    {
        public readonly List<string> AppliedNodes = [];

        public async Task<ChannelReply> HandshakeAsync(
            string baseUrl, string suPassword, string standbyPassword,
            bool isToolStart, CancellationToken ct)
        {
            var node = NodeOf(baseUrl);
            AppliedNodes.Add(node);
            var key = $"/clusters/{Cluster}/shards/{Shard}/nodes/{node}/bootstrap";
            var kv = (await gateway.GetAsync(endpoint, key, ct)).Value;
            if (kv is null)
                return new ChannelReply(400, false, null, "no bootstrap key");
            var status = JsonSerializer.Serialize(new Dictionary<string, object?>
            {
                ["state"] = "ready",
                ["applied_revision"] = (long)kv.ModRevision,
                ["components"] = new Dictionary<string, string> { ["pg"] = "ready", ["doorman"] = "ready" },
                ["updated_unix"] = 1,
            });
            await gateway.PutAsync(endpoint,
                $"/pgworker/nodes/{Cluster}/{Shard}/{node}/status", status, null, ct);
            return new ChannelReply(200, true, null, null);
        }

        public async Task<ChannelReply> ApplyAsync(string baseUrl, CancellationToken ct)
        {
            var node = NodeOf(baseUrl);
            AppliedNodes.Add(node);
            var key = $"/clusters/{Cluster}/shards/{Shard}/nodes/{node}/bootstrap";
            var kv = (await gateway.GetAsync(endpoint, key, ct)).Value
                ?? throw new InvalidOperationException("bootstrap key vanished (fake)");
            var status = JsonSerializer.Serialize(new Dictionary<string, object?>
            {
                ["state"] = "ready",
                ["applied_revision"] = (long)kv.ModRevision,
                ["components"] = new Dictionary<string, string> { ["pg"] = "ready", ["doorman"] = "ready" },
                ["updated_unix"] = 1,
            });
            await gateway.PutAsync(endpoint,
                $"/pgworker/nodes/{Cluster}/{Shard}/{node}/status", status, null, ct);
            return new ChannelReply(200, true, (long)kv.ModRevision, null);
        }

        // https://pgw-<C>-<X>-<n>:8081 → <n>
        private static string NodeOf(string baseUrl)
            => baseUrl.Split('-')[^1].Split(':')[0];
    }
}
