using AdminPanel.Core;
using AdminPanel.Core.S3;
using AdminPanel.Core.S3.S3Alerting;
using AdminPanel.Etcd;
using Shared.Etcd.Client;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.UnitTests;

// S3SnapshotRefresher (arch/02 §12): тик-сборка на fake gateway (4 чтения),
// отказ транспорта роняет тик (прежний снапшот сохраняется), алерты движка.
public class S3RefresherTests
{
    // Управляемый gateway s3-домена: три префикса + точечный ключ серта.
    private sealed class S3FakeGateway : IEtcdGateway
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
                    "/s3/clusters/" => ClustersKv,
                    "/s3worker/rotations/" => RotationsKv,
                    "/s3worker/api/" => WorkerApiKv,
                    "/workers/api_tls/s3worker" => WorkerApiCertKv,
                    _ => [],
                }));

        // Не используются s3-тиком — заглушки ради интерфейса.
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

    private static S3SnapshotRefresher New(
        IEtcdGateway gateway,
        IS3SnapshotStore store,
        AdminPanel.Etcd.Workers.IS3WorkerHealthStore? healthStore = null,
        params string[] endpoints)
        => new(
            gateway,
            new S3AlertEngine(Options.Create(new S3AlertsOptions())),
            store,
            Options.Create(new EtcdOptions { Endpoints = endpoints }),
            Options.Create(new S3PanelOptions()),
            new FixedTimeProvider(),
            NullLogger<S3SnapshotRefresher>.Instance,
            healthStore ?? new AdminPanel.Etcd.Workers.S3WorkerHealthStore());

    private static S3FakeGateway DemoGateway() => new()
    {
        ClustersKv = EtcdFixtures.LoadKv("S3/clusters-canonical.json"),
        RotationsKv = EtcdFixtures.LoadKv("S3/rotations.json"),
    };

    // Самоподписанный серт для ключа /workers/api_tls/s3worker (PEM — один на класс).
    private static readonly string ServerCertPem = BuildCertPem();

    private static string BuildCertPem()
    {
        using var rsa = System.Security.Cryptography.RSA.Create(2048);
        var request = new System.Security.Cryptography.X509Certificates.CertificateRequest(
            "CN=s3worker", rsa, System.Security.Cryptography.HashAlgorithmName.SHA256,
            System.Security.Cryptography.RSASignaturePadding.Pkcs1);
        using var cert = request.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(1));
        return cert.ExportCertificatePem();
    }

    // Arrange: стаб шлюза с каноническими /s3/-ключами + ключ api + ключ серта.
    // Act: RefreshOnceAsync. Assert: снапшот собран: кластеры, ротация сджойнена,
    // WorkerEndpoints/WorkerApiCert заполнены, UnknownKeyCount перенесён.
    [Fact]
    public async Task RefreshOnce_Success_BuildsSnapshot()
    {
        var gateway = DemoGateway();
        gateway.WorkerApiKv =
        [
            new Kv("/s3worker/api/i1", """{"url":"https://s3worker:8080","instance":"i1","since_unix":1760150001}""", 9),
        ];
        gateway.WorkerApiCertKv =
        [
            new Kv("/workers/api_tls/s3worker",
                $$"""{"cert_pem":"{{ServerCertPem.Replace("\n", "\\n")}}","updated_unix":1760150000,"updated_by":"admin"}""", 10),
        ];
        var store = new S3SnapshotStore();
        var refresher = New(gateway, store, endpoints: "http://e1");

        // Act
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert: кластеры/ротация-джойн/endpoints/cert/unknownKeys — в снапшоте.
        result.IsSuccess.Should().BeTrue();
        var snapshot = store.Current;
        snapshot.Should().NotBeNull();
        snapshot!.EtcdReachable.Should().BeTrue();
        snapshot.ConsecutiveFailures.Should().Be(0);
        snapshot.Clusters.Should().HaveCount(4);
        var main = snapshot.Clusters.Single(c => c.Name == "backupsmain");
        main.Rotation.Should().NotBeNull(); // джойн заявки rotations.json по кластеру
        main.Rotation!.RequestedBy.Should().Be("admin");
        snapshot.WorkerEndpoints.Should().ContainSingle().Which
            .Should().Be(new WorkerEndpoint("i1", "https://s3worker:8080", 1760150001));
        snapshot.WorkerApiCert.Should().NotBeNull();
        snapshot.WorkerApiCert!.Thumbprint.Should().HaveLength(64);
        snapshot.UnknownKeyCount.Should().Be(1); // future_feature
        // Кластерных parseError нет; битые ротации rotations.json — ожидаемые записи.
        snapshot.ParseErrors.Select(e => e.Key).Should()
            .OnlyContain(k => k.StartsWith("/s3worker/rotations/", StringComparison.Ordinal));
    }

    // Arrange: первый тик успешен; второй — все range-ответы Failed.
    // Act: два RefreshOnceAsync. Assert: второй вернул Failed, Current — прежний
    // снапшот (BuiltAtUtc не изменился), EtcdReachable=false, ConsecutiveFailures=1.
    [Fact]
    public async Task RefreshOnce_TransportFail_KeepsPreviousData()
    {
        var gateway = DemoGateway();
        var store = new S3SnapshotStore();
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

    // Arrange: пустой /s3worker/api/. Act: RefreshOnceAsync. Assert: WorkerEndpoints
    // пуст, алерт worker-api-unreachable в снапшоте.
    [Fact]
    public async Task RefreshOnce_NoWorkerKeys_AlertsUnreachable()
    {
        var gateway = DemoGateway();
        var store = new S3SnapshotStore();
        var refresher = New(gateway, store, endpoints: "http://e1");

        // Act
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        var snapshot = store.Current;
        snapshot!.WorkerEndpoints.Should().BeEmpty();
        snapshot.Alerts.Should().Contain(a => a.Kind == "worker-api-unreachable"
            && a.Severity == AlertSeverity.Critical && a.Target == "s3worker");
    }
}
