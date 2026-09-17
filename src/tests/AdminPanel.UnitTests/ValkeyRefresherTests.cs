using AdminPanel.Core;
using AdminPanel.Core.Valkey;
using AdminPanel.Core.Valkey.ValkeyAlerting;
using AdminPanel.Etcd;
using Shared.Etcd.Client;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.UnitTests;

// ValkeySnapshotRefresher (arch/02 §11): тик-сборка на fake gateway (4 префикса),
// отказ транспорта роняет тик (прежний снапшот сохраняется), секреты и мердж проб.
public class ValkeyRefresherTests
{
    // Управляемый gateway valkey-домена: четыре префикса + точечные отказы.
    private sealed class ValkeyFakeGateway : IEtcdGateway
    {
        public IReadOnlyList<Kv> ClustersKv { get; set; } = [];

        public IReadOnlyList<Kv> RotationsKv { get; set; } = [];

        public IReadOnlyList<Kv> WorkerApiKv { get; set; } = [];

        public IReadOnlyList<Kv> WorkerApiCertKv { get; set; } = [];

        public List<string> FailEndpoints { get; } = [];

        public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
            => Task.FromResult(FailEndpoints.Contains(endpoint)
                ? Result<IReadOnlyList<Kv>>.Failed(new EtcdUnreachableException(endpoint))
                : Result<IReadOnlyList<Kv>>.Success(prefix switch
                {
                    "/valkey/clusters/" => ClustersKv,
                    "/valkeyworker/rotations/" => RotationsKv,
                    "/valkeyworker/api/" => WorkerApiKv,
                    "/workers/api_tls/valkeyworker" => WorkerApiCertKv,
                    _ => [],
                }));

        // Не используются valkey-тиком — заглушки ради интерфейса.
        public Task<Result<EtcdStatusPayload>> StatusAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<EtcdStatusPayload>.Success(
                new EtcdStatusPayload("3.5.21", 1, 1, 1, 1, null)));

        public Task<Result<IReadOnlyList<EtcdMember>>> MemberListAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<EtcdMember>>.Success([]));

        public Task<Result<IReadOnlyList<EtcdAlarm>>> AlarmAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<EtcdAlarm>>.Success([]));

        public Task<Result<TxnResult>> TxnAsync(string endpoint, TxnRequest req, CancellationToken ct)
            => Task.FromResult(Result<TxnResult>.Failed(new EtcdUnreachableException(endpoint)));

        public Task<Result> PutAsync(string endpoint, string key, string value, long? lease, CancellationToken ct)
            => Task.FromResult(Result.Failed(new EtcdUnreachableException(endpoint)));

        public Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
            => Task.FromResult(Result.Failed(new EtcdUnreachableException(endpoint)));

        public Task<Result<Kv?>> GetAsync(string endpoint, string key, CancellationToken ct)
            => Task.FromResult(Result<Kv?>.Failed(new EtcdUnreachableException(endpoint)));

        public Task<Result<long>> LeaseGrantAsync(string endpoint, int ttlSec, CancellationToken ct)
            => Task.FromResult(Result<long>.Failed(new EtcdUnreachableException(endpoint)));

        public Task<Result> LeaseRevokeAsync(string endpoint, long lease, CancellationToken ct)
            => Task.FromResult(Result.Failed(new EtcdUnreachableException(endpoint)));

        public Task<Result> LeaseKeepaliveAsync(string endpoint, long lease, CancellationToken ct)
            => Task.FromResult(Result.Failed(new EtcdUnreachableException(endpoint)));

        public Task<Result<byte[]>> SnapshotSaveAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<byte[]>.Failed(new EtcdUnreachableException(endpoint)));

        public Task<Result> CompactAsync(string endpoint, long revision, CancellationToken ct)
            => Task.FromResult(Result.Failed(new EtcdUnreachableException(endpoint)));

        public Task<Result> DefragmentAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result.Failed(new EtcdUnreachableException(endpoint)));
    }

    // Читатель проб-стора для тестов мерджа (адаптер подставит Probes).
    private sealed class FixedProbeReader(IReadOnlyList<ValkeyProbeResult>? probes) : IValkeyProbeReader
    {
        public IReadOnlyList<ValkeyProbeResult>? Current { get; } = probes;
    }

    private static ValkeySnapshotRefresher New(
        IEtcdGateway gateway,
        IValkeySnapshotStore store,
        IValkeySecretsStore? secretsStore = null,
        IValkeyProbeReader? probeReader = null,
        params string[] endpoints)
        => new(
            gateway,
            new ValkeyAlertEngine(Options.Create(new ValkeyAlertsOptions())),
            store,
            secretsStore ?? new ValkeySecretsStore(),
            Options.Create(new EtcdOptions { Endpoints = endpoints }),
            Options.Create(new ValkeyPanelOptions()),
            new FixedTimeProvider(),
            NullLogger<ValkeySnapshotRefresher>.Instance,
            probeReader);

    private static ValkeyFakeGateway DemoGateway() => new()
    {
        ClustersKv = EtcdFixtures.LoadKv("Valkey/clusters-canonical.json"),
        RotationsKv = EtcdFixtures.LoadKv("Valkey/rotations.json"),
    };

    // Самоподписанный серт для ключа /workers/api_tls/valkeyworker (PEM — один на класс).
    private static readonly string ServerCertPem = BuildCertPem();

    private static string BuildCertPem()
    {
        using var rsa = System.Security.Cryptography.RSA.Create(2048);
        var request = new System.Security.Cryptography.X509Certificates.CertificateRequest(
            "CN=valkeyworker", rsa, System.Security.Cryptography.HashAlgorithmName.SHA256,
            System.Security.Cryptography.RSASignaturePadding.Pkcs1);
        using var cert = request.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(1));
        return cert.ExportCertificatePem();
    }

    // Arrange: KV-gateway с ключами 4 префиксов (канонические значения + ключ
    // /valkeyworker/api/i1 + ключ серта /workers/api_tls/valkeyworker).
    // Act: RefreshOnceAsync. Assert: снапшот собран: кластеры/ротации/endpoints/cert.
    [Fact]
    public async Task RefreshOnce_Canonical_BuildsSnapshot()
    {
        var gateway = DemoGateway();
        gateway.WorkerApiKv =
        [
            new Kv("/valkeyworker/api/i1", """{"url":"https://valkeyworker:8080","instance":"i1","since_unix":1756000001}""", 9),
        ];
        gateway.WorkerApiCertKv =
        [
            new Kv("/workers/api_tls/valkeyworker",
                $$"""{"cert_pem":"{{ServerCertPem.Replace("\n", "\\n")}}","updated_unix":1756500000,"updated_by":"admin"}""", 10),
        ];
        var store = new ValkeySnapshotStore();
        var refresher = New(gateway, store, endpoints: "http://e1");

        // Act
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert: кластеры/ротации/endpoints/cert — в снапшоте.
        result.IsSuccess.Should().BeTrue();
        var snapshot = store.Current;
        snapshot.Should().NotBeNull();
        snapshot!.EtcdReachable.Should().BeTrue();
        snapshot.ConsecutiveFailures.Should().Be(0);
        // 3 канонических + скелет «unknown» от неизвестного leaf (GetOrAdd до switch —
        // kafka-механика 1:1; пустой Active-кластер без нод).
        snapshot.Clusters.Should().HaveCount(4);
        snapshot.Rotations.Should().HaveCount(2);
        snapshot.WorkerEndpoints.Should().ContainSingle().Which
            .Should().Be(new WorkerEndpoint("i1", "https://valkeyworker:8080", 1756000001));
        snapshot.WorkerApiCert.Should().NotBeNull();
        snapshot.WorkerApiCert!.Thumbprint.Should().HaveLength(64);
        // Кластерных parseError нет; битые ротации rotations.json — ожидаемые записи.
        snapshot.ParseErrors.Select(e => e.Key).Should()
            .OnlyContain(k => k.StartsWith("/valkeyworker/rotations/", StringComparison.Ordinal));
        snapshot.UnknownKeyCount.Should().Be(1); // future_feature
    }

    // Arrange: один из range-запросов (любой префикс) отвечает ошибкой.
    // Act: RefreshOnceAsync. Assert: Result неуспешен; прежний снапшот в сторе
    // (EtcdReachable=false, ConsecutiveFailures=+1, данные прежние).
    [Fact]
    public async Task RefreshOnce_KvFail_FailsTickKeepsPrevious()
    {
        var gateway = DemoGateway();
        var store = new ValkeySnapshotStore();
        var refresher = New(gateway, store, endpoints: "http://e1");
        await refresher.RefreshOnceAsync(CancellationToken.None);
        var builtAt = store.Current!.BuiltAtUtc;
        var clusters = store.Current.Clusters;
        gateway.FailEndpoints.Add("http://e1");

        // Act
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert: прежние данные, Reachable=false, счётчик отказов растёт.
        result.IsSuccess.Should().BeFalse();
        store.Current!.BuiltAtUtc.Should().Be(builtAt);
        store.Current.Clusters.Should().BeSameAs(clusters);
        store.Current.EtcdReachable.Should().BeFalse();
        store.Current.ConsecutiveFailures.Should().Be(1);
    }

    // Arrange: полный набор admin_user+admin_password и частичный (только admin_user).
    // Act: RefreshOnceAsync. Assert: полный — в IValkeySecretsStore; частичный — пропущен без ошибки.
    [Fact]
    public async Task RefreshOnce_Secrets_FullAndPartialSets()
    {
        var gateway = new ValkeyFakeGateway
        {
            ClustersKv =
            [
                new Kv("/valkey/clusters/full/config", """{"nodes":1,"maxmemory_bytes":1,"maxmemory_policy":"noeviction"}""", 1),
                new Kv("/valkey/clusters/full/admin_user", "admin", 2),
                new Kv("/valkey/clusters/full/admin_password", "FullSecret0123456789abcdef", 3),
                new Kv("/valkey/clusters/half/config", """{"nodes":1,"maxmemory_bytes":1,"maxmemory_policy":"noeviction"}""", 4),
                new Kv("/valkey/clusters/half/admin_user", "admin", 5),
            ],
        };
        var store = new ValkeySnapshotStore();
        var secrets = new ValkeySecretsStore();
        var refresher = New(gateway, store, secrets, endpoints: "http://e1");

        // Act
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert: полный набор — в сторе, частичный — пропущен, тик зелёный.
        result.IsSuccess.Should().BeTrue();
        var creds = secrets.Current["full"];
        creds.AdminUser.Should().Be("admin");
        creds.AdminPassword.Should().Be("FullSecret0123456789abcdef");
        secrets.Current.Should().NotContainKey("half");
        store.Current!.ParseErrors.Should().BeEmpty();
    }

    // Arrange: в IValkeyProbeReader лежит живая проба кластера live (Live=true, Error=null)
    // и ошибка для cache. Act: RefreshOnceAsync. Assert: NodesList[].Live/ProbeError
    // переносятся в снапшот; кластер без пробы — Live=null.
    [Fact]
    public async Task RefreshOnce_MergesProbeResults()
    {
        // Канонические кластеры + noprobe (нода без пробы) — для Live=null.
        var gateway = DemoGateway();
        gateway.ClustersKv = [.. gateway.ClustersKv,
            new Kv("/valkey/clusters/noprobe/nodes/node1/state", "RUNNING", 90),
        ];
        var probeReader = new FixedProbeReader(
        [
            new ValkeyProbeResult("live", "node1", true, 1756500100, null),
            new ValkeyProbeResult("cache", "node1", false, 1756500100, "connection refused"),
        ]);
        var store = new ValkeySnapshotStore();
        var refresher = New(gateway, store, probeReader: probeReader, endpoints: "http://e1");

        // Act
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert: live/ProbeError в нужных нодах; noprobe без пробы — Live=null.
        result.IsSuccess.Should().BeTrue();
        var clusters = store.Current!.Clusters;
        clusters.Single(c => c.Name == "live").NodesList.Single().Live.Should().BeTrue();
        var cache = clusters.Single(c => c.Name == "cache").NodesList.Single();
        cache.Live.Should().BeFalse();
        cache.ProbeError.Should().Be("connection refused");
        clusters.Single(c => c.Name == "noprobe").NodesList.Single().Live.Should().BeNull();
    }

    // Arrange: кластеры без проб + список ValkeyProbeResult (живая/ошибка/чужой кластер).
    // Act: прямой вызов ValkeySnapshotRefresher.MergeProbes (public static; spec §5.1
    // «мердж live-проб» в модельных тестах). Assert: Live/ProbeError в нужных нодах,
    // прочие — без изменений, чужой кластер игнорируется.
    [Fact]
    public void MergeProbes_Direct_MergesLiveAndError()
    {
        // Arrange: два кластера по одной ноде; пробы: live-«ok» на a, ошибка на b,
        // чужой кластер «ghost» — игнорируется.
        var clusterA = new ValkeyClusterInfo("a", ValkeyClusterState.Active, 1, 1, "p", 1, "h:1",
            [new ValkeyNodeInfo("node1", "RUNNING", 1, 1, 1)]);
        var clusterB = new ValkeyClusterInfo("b", ValkeyClusterState.Active, 1, 1, "p", 1, "h:2",
            [new ValkeyNodeInfo("node1", "RUNNING", 1, 1, 1)]);
        var probes = new List<ValkeyProbeResult>
        {
            new("a", "node1", true, 100, null),
            new("b", "node1", false, 100, "timeout"),
            new("ghost", "node1", true, 100, null),
        };

        // Act
        var merged = ValkeySnapshotRefresher.MergeProbes([clusterA, clusterB], probes);

        // Assert
        merged.Should().HaveCount(2);
        var a = merged.Single(c => c.Name == "a").NodesList.Single();
        a.Live.Should().BeTrue();
        a.ProbeError.Should().BeNull();
        var b = merged.Single(c => c.Name == "b").NodesList.Single();
        b.Live.Should().BeFalse();
        b.ProbeError.Should().Be("timeout");
        // Чужой кластер не меняет исходные записи (immutable-мердж).
        merged.Single(c => c.Name == "a").Should().NotBeSameAs(clusterA);
    }

    // Arrange: активная ротация кластера live. Act: RefreshOnceAsync.
    // Assert: ValkeyClusterInfo.Rotation заполнен (джойн по имени кластера).
    [Fact]
    public async Task RefreshOnce_JoinsRotationTicket()
    {
        var gateway = DemoGateway();
        var store = new ValkeySnapshotStore();
        var refresher = New(gateway, store, endpoints: "http://e1");

        // Act
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert: заявка live (role app, seed) — в кластере live; у cache — null.
        result.IsSuccess.Should().BeTrue();
        var live = store.Current!.Clusters.Single(c => c.Name == "live");
        live.Rotation.Should().NotBeNull();
        live.Rotation!.Role.Should().Be("app");
        live.Rotation.RequestedBy.Should().Be("seed");
        store.Current.Clusters.Single(c => c.Name == "cache").Rotation.Should().BeNull();
    }
}
