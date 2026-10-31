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

// R2-rest ротации rest_password БЕЗ пересоздания (arch/14 §5 I, spec §3.7.4):
// заявка → тик ротатора фиксирует NEW в rest_pending и начинает доставку
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

    [Fact]
    public async Task Rotation_RestAppliedWithoutRecreate_CommitsAndIdempotent()
    {
        var ct = TestContext.Current.CancellationToken;
        var gateway = fixture.Gateway;
        var endpoint = fixture.Endpoint;

        // Arrange: сид контроль-плейна (живой etcd) — dsn-шард из двух нод
        // (shard1a — лидер по master-ключу), заявки ресурсов обязательны
        // (arch/14 §2.1 п.4 — fail-fast без них).
        const string oldPassword = "old0Rest0000000000000000000000A";
        await SeedAsync(gateway, endpoint, oldPassword, ct);
        var claims = new ClaimStore("/pgworker", [endpoint], gateway,
            TimeProvider.System, "instance-test");
        (await claims.TryClaimClusterAsync(Cluster, ct)).Value.Should().BeTrue("the contour claim");

        // Фейк-канал: Handshake/Apply по адресу → читает bootstrap-ключ из etcd
        // и запоминает applied_revision; статусы — fake-агент пишет статусные
        // ключи по факту «применения» (модель мгновенной инициализации/apply).
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
        // spilo_configuration; статусы ready с applied==mod(OLD) — точная
        // модель ноды до ротации (регрессия content-пробы: тик ОБЯЗАН начать
        // доставку NEW, не закрыть окно по стационарному applied==mod(OLD)).
        foreach (var node in new[] { "shard1a", "shard1b" })
        {
            var key = $"/clusters/{Cluster}/shards/{Shard}/nodes/{node}/bootstrap";
            await gateway.PutAsync(endpoint, key,
                BootstrapJson(oldPassword, cache, node), null, ct);
            var mod = (await gateway.GetAsync(endpoint, key, ct)).Value!.ModRevision;
            await SeedStatusAsync(gateway, endpoint, node, (long)mod, ct);
        }

        var snapshot = await SnapshotAsync(gateway, endpoint, ct);

        // Регрессия content-пробы (П11.2): тик ДО заявки — мутаций нет
        // (все ноды на ревизии OLD-ключа), окно не открывается.
        var before = await rotator.TickAsync(snapshot, ct);
        before.IsSuccess.Should().BeTrue(before.Error?.ToString());
        (await TicketAsync(gateway, endpoint, ct)).Should().BeNull("no ticket before rotation");
        channel.AppliedNodes.Should().BeEmpty("no delivery before the ticket");

        // Act (а): заявка → ПЕРВЫЙ тик ротатора — rest_pending зафиксирован
        // (NEW), доставлена РОВНО ОДНА нода и это реплика (≤1 нода/тик,
        // реплики вперёд).
        await SubmitTicketAsync(gateway, endpoint, ct);
        var first = await rotator.TickAsync(await SnapshotAsync(gateway, endpoint, ct), ct);
        first.IsSuccess.Should().BeTrue(first.Error?.ToString());
        var (pending, phase) = await ReadWorkAsync(gateway, endpoint, ct);
        pending.Should().NotBeNullOrEmpty(
            "rotate-rest-start records the in-flight pair (tick error: {0}; phase: {1})",
            first.Error?.ToString(), phase);
        pending.Should().NotBe(oldPassword, "the NEW pair differs from the current key");
        channel.AppliedNodes.Should().ContainSingle("exactly one node per tick (spec §3.7.4)");
        channel.AppliedNodes[0].Should().Be("shard1b", "the replica is delivered first");
        (await TicketAsync(gateway, endpoint, ct)).Should().NotBeNull("no commit yet — the request is alive");

        // Act (б): цикл тиков до txn-коммита — лидер применил ПОСЛЕДНИМ.
        var committed = await RunUntilCommitAsync(rotator, gateway, endpoint, channel, ct);

        // Assert (б): txn-коммит — ключ==NEW, заявка снята, pending сброшен;
        // bootstrap-ключи обеих нод несут pending-пароль внутри
        // spilo_configuration; порядок — лидер последним.
        committed.Should().BeTrue("rolling must reach the txn within the tick budget");
        var committedKey = (await gateway.GetAsync(endpoint, $"/clusters/{Cluster}/rest_password", ct)).Value;
        committedKey!.Value.Should().Be(pending, "the txn committed the pending pair");
        (await TicketAsync(gateway, endpoint, ct)).Should().BeNull("the request is removed by the txn");
        (await ReadWorkAsync(gateway, endpoint, ct)).RestPending.Should().BeNull("the done phase reset the window");
        channel.AppliedNodes.Should().HaveCount(2, "both nodes were applied");
        channel.AppliedNodes[^1].Should().Be("shard1a", "the leader is applied last");
        foreach (var bootstrapKey in new[] { BootstrapA, BootstrapB })
        {
            var stored = (await gateway.GetAsync(endpoint, bootstrapKey, ct)).Value!.Value;
            stored.Should().Contain(pending!,
                $"spilo_configuration inside {bootstrapKey} carries the NEW rest_password");
        }

        // (в) идемпотентность: повторная заявка — НОВЫЙ проход (доставки идут),
        // новый коммит; ключ становится второй NEW-парой.
        await SubmitTicketAsync(gateway, endpoint, ct);
        (await rotator.TickAsync(await SnapshotAsync(gateway, endpoint, ct), ct))
            .IsSuccess.Should().BeTrue();
        (await RunUntilCommitAsync(rotator, gateway, endpoint, channel, ct))
            .Should().BeTrue("the second pass is committed");
        var key2 = (await gateway.GetAsync(endpoint, $"/clusters/{Cluster}/rest_password", ct)).Value;
        key2!.Value.Should().NotBe(pending, "a repeated pass — a fresh pair");
        channel.AppliedNodes.Count.Should().BeGreaterThan(2, "the second pass delivered again");

        // (г) no-op тика без заявки: ключ не меняется, окна нет, доставок нет.
        var appliedCount = channel.AppliedNodes.Count;
        var noop = await rotator.TickAsync(await SnapshotAsync(gateway, endpoint, ct), ct);
        noop.IsSuccess.Should().BeTrue(noop.Error?.ToString());
        (await ReadWorkAsync(gateway, endpoint, ct)).RestPending.Should().BeNull("no window");
        channel.AppliedNodes.Should().HaveCount(appliedCount, "no deliveries without a ticket");

        // Teardown: etcd-префиксы кластера + узлы (по образцу DisposeAsync рига).
        await gateway.DeleteAsync(endpoint, $"/clusters/{Cluster}/", prefix: true, ct);
        await gateway.DeleteAsync(endpoint, $"/pgworker/nodes/{Cluster}/", prefix: true, ct);
        await gateway.DeleteAsync(endpoint, $"/pgworker/work/{Cluster}", prefix: false, ct);
        await gateway.DeleteAsync(endpoint, $"/pgworker/rotations/{Cluster}", prefix: false, ct);
        await gateway.DeleteAsync(endpoint, $"/pgworker/claims/{Cluster}", prefix: false, ct);
        await gateway.DeleteAsync(endpoint, $"/service/{Scope}/", prefix: true, ct);
    }

    // Цикл тиков ротатора до txn-коммита; каждый промежуточный тик продолжает
    // проход той же парой (pending неизменен).
    private static async Task<bool> RunUntilCommitAsync(
        ClusterSecretRotator rotator, EtcdGateway gateway, string endpoint,
        FakeChannel channel, CancellationToken ct)
    {
        for (var i = 0; i < 10; i++)
        {
            var rot = await rotator.TickAsync(await SnapshotAsync(gateway, endpoint, ct), ct);
            rot.IsSuccess.Should().BeTrue(rot.Error?.ToString());
            if (await TicketAsync(gateway, endpoint, ct) is null)
                return true; // txn закрыл проход — pending уже сброшен фазой done
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
        // Заявки ресурсов ОБЯЗАТЕЛЬНЫ (arch/14 §2.1 п.4) — fail-fast без них.
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

    private static async Task<(string? RestPending, string? Phase)> ReadWorkAsync(
        EtcdGateway gateway, string endpoint, CancellationToken ct)
    {
        var kv = await gateway.GetAsync(endpoint, $"/pgworker/work/{Cluster}", ct);
        if (kv.Value is null)
            return (null, null);
        using var doc = JsonDocument.Parse(kv.Value.Value);
        var root = doc.RootElement;
        return (root.TryGetProperty("rest_pending", out var pending) && pending.ValueKind == JsonValueKind.String
                ? pending.GetString() : null,
            root.TryGetProperty("phase", out var phase) ? phase.GetString() : null);
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

    // Фейк IClusterDriver: R2-rest доставку делает delivery — драйвер ротатором
    // не вызывается; методы — NotSupportedException (fail-fast при касании).
    private sealed class StubDriver : IClusterDriver
    {
        public bool SupportsRunningInspection => false;
        public Task<Result> EnsureNodeAsync(ShardTopology topology, string nodeName, NodeAddress addr,
            EtcdEndpoints etcd, NodeResources? resources, PgTuneResult? tuning,
            bool syncStrict, CancellationToken ct) => throw new NotSupportedException();
        public Task<Result> RemoveNodeAsync(string cluster, string shard, string nodeName, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<Result> StopNodeAsync(string cluster, string shard, string nodeName, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<Result<IReadOnlyDictionary<string, DiscoveredNode>>> InspectNodesAsync(
            string cluster, IReadOnlyCollection<string> nodeNames, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<string>>> ListNodeObjectsAsync(string cluster, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<Result> RemoveBackupAgentsAsync(string cluster, string? shard, CancellationToken ct)
            => throw new NotSupportedException();
        public IDockerEngine? EngineFor(string host) => null;
        public Task<Result<IReadOnlyList<HostInfo>>> GetHostsAsync(CancellationToken ct)
            => throw new NotSupportedException();
        public Task<Result<IReadOnlySet<(string Host, int Port)>>> GetBusyPortsAsync(CancellationToken ct)
            => throw new NotSupportedException();
    }

    private sealed class StubSql : ISqlExecutor
    {
        public Task<Result> ExecuteAsync(string dsn, string sql, CancellationToken ct)
            => Task.FromResult(Result.Success());

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
