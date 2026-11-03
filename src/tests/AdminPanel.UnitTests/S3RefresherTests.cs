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

        // u13/u14: данные новых префиксов s3-тика (work/leader/claims/instances).
        public Dictionary<string, IReadOnlyList<Kv>> ExtraKv { get; } = [];

        public List<string> FailPrefixes { get; } = [];

        public List<string> FailEndpoints { get; } = [];

        public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
            => Task.FromResult(FailEndpoints.Contains(endpoint) || FailPrefixes.Contains(prefix)
                ? Result<IReadOnlyList<Kv>>.Failed(new EtcdUnreachableException(endpoint))
                : Result<IReadOnlyList<Kv>>.Success(ExtraKv.TryGetValue(prefix, out var extra)
                    ? extra
                    : prefix switch
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

        // Member-API этими тестами не используется — заглушки интерфейса.
        public Task<Result<EtcdMember>> MemberAddAsync(string endpoint, IReadOnlyList<string> peerUrls, bool isLearner, CancellationToken ct)
            => throw new NotSupportedException("member-API не поддерживается этим фейком");

        public Task<Result> MemberRemoveAsync(string endpoint, ulong memberId, CancellationToken ct)
            => throw new NotSupportedException("member-API не поддерживается этим фейком");

        public Task<Result> MemberPromoteAsync(string endpoint, ulong memberId, CancellationToken ct)
            => throw new NotSupportedException("member-API не поддерживается этим фейком");

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

    // u13/u14: новые ключи s3-тика → WorkJournals/Coordination в снапшоте.
    [Fact]
    public async Task Refresh_NewObservabilityKeys_SnapshotCarriesFields()
    {
        // Arrange
        var gateway = DemoGateway();
        gateway.ExtraKv["/s3worker/work/"] =
            [new Kv("/s3worker/work/store",
                """{"op":"sync-probe","phase":"running","instance":"s3w-1","updated_unix":1761900000}""", 1)];
        gateway.ExtraKv["/s3worker/leader"] =
            [new Kv("/s3worker/leader", """{"instance":"s3w-1","since_unix":1761900000}""", 2)];
        gateway.ExtraKv["/s3worker/claims/"] =
            [new Kv("/s3worker/claims/store", """{"instance":"s3w-1"}""", 3)];
        gateway.ExtraKv["/s3worker/instances/"] =
            [new Kv("/s3worker/instances/s3w-1", """{"since_unix":1761900000}""", 4)];
        var store = new S3SnapshotStore();
        var refresher = New(gateway, store, endpoints: "http://etcd1:2379");

        // Act
        var result = await refresher.RefreshOnceAsync(TestContext.Current.CancellationToken);

        // Assert
        result.IsSuccess.Should().BeTrue();
        var snapshot = store.Current!;
        snapshot.WorkJournals.Should().ContainSingle().Which.Cluster.Should().Be("store");
        snapshot.Coordination!.LeaderInstance.Should().Be("s3w-1");
        snapshot.Coordination.Claims.Should().ContainSingle().Which.Cluster.Should().Be("store");
        snapshot.Coordination.Instances.Should().ContainSingle().Which.InstanceId.Should().Be("s3w-1");
    }

    // u13/u14: транспортный провал нового range → отказ тика.
    [Fact]
    public async Task Refresh_NewRangeTransportFail_TickFails()
    {
        // Arrange — первый тик успешен
        var gateway = DemoGateway();
        var store = new S3SnapshotStore();
        var refresher = New(gateway, store, endpoints: "http://etcd1:2379");
        (await refresher.RefreshOnceAsync(TestContext.Current.CancellationToken)).IsSuccess.Should().BeTrue();
        var first = store.Current!;

        // Act
        gateway.FailPrefixes.Add("/s3worker/claims/");
        var second = await refresher.RefreshOnceAsync(TestContext.Current.CancellationToken);

        // Assert
        second.IsSuccess.Should().BeFalse();
        store.Current!.EtcdReachable.Should().BeFalse();
        store.Current.BuiltAtUtc.Should().Be(first.BuiltAtUtc);
    }

    // Перегрузка New (кейсы S10, образец pg RefresherTestHarness.New): подмены
    // алерт-движка (счёт вызовов Evaluate) и времени (движение BuiltAtUtc).
    private static S3SnapshotRefresher New(
        IEtcdGateway gateway,
        IS3SnapshotStore store,
        IS3AlertEngine alertEngine,
        TimeProvider time,
        params string[] endpoints)
        => new(
            gateway,
            alertEngine,
            store,
            Options.Create(new EtcdOptions { Endpoints = endpoints }),
            Options.Create(new S3PanelOptions()),
            time,
            NullLogger<S3SnapshotRefresher>.Instance,
            new AdminPanel.Etcd.Workers.S3WorkerHealthStore());

    // Счётный алертный движок: тихий тик (S10) Evaluate не вызывает — счётчик
    // отражает фактические пересборки с алертным пересчётом.
    private sealed class CountingS3AlertEngine : IS3AlertEngine
    {
        private readonly S3AlertEngine _inner = new(Options.Create(new S3AlertsOptions()));

        public int EvaluateCalls { get; private set; }

        public IReadOnlyList<Alert> Evaluate(S3Snapshot next, S3Snapshot? previous)
        {
            EvaluateCalls++;
            return _inner.Evaluate(next, previous);
        }
    }

    // S10 (arch/02 §12, §5): тихий тик — чтение совпало с кэшем → пересборки нет,
    // S3AlertEngine не вызывается; штамп свежести BuiltAtUtc обновляется,
    // данные — те же ссылки.
    [Fact]
    public async Task Refresh_SecondIdenticalTick_SkipsRebuildAndAlerts()
    {
        // Arrange — первый тик собирает снапшот, время двигается вперёд
        var gateway = DemoGateway();
        var store = new S3SnapshotStore();
        var engine = new CountingS3AlertEngine();
        var time = new FixedTimeProvider();
        var refresher = New(gateway, store, engine, time, "http://e1");
        await refresher.RefreshOnceAsync(CancellationToken.None);
        var first = store.Current!;
        time.Utc = time.Utc.AddSeconds(5);

        // Act — второй тик по неизменному etcd
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert — снапшот не пересобран, алерты не пересчитаны, штамп свежий
        result.IsSuccess.Should().BeTrue();
        engine.EvaluateCalls.Should().Be(1, "тихий тик не запускает алертный пересчёт");
        store.Current!.Clusters.Should().BeSameAs(first.Clusters, "снапшот не пересобирается");
        store.Current.Alerts.Should().BeSameAs(first.Alerts);
        store.Current.BuiltAtUtc.Should().Be(time.Utc, "штамп свежести обновляется");
    }

    // S10: изменение KV (новая заявка ротации) → пересборка и алертный пересчёт.
    [Fact]
    public async Task Refresh_KvChange_RebuildsAndRecomputesAlerts()
    {
        // Arrange — первый тик, затем новая заявка в /s3worker/rotations/
        var gateway = DemoGateway();
        var store = new S3SnapshotStore();
        var engine = new CountingS3AlertEngine();
        var time = new FixedTimeProvider();
        var refresher = New(gateway, store, engine, time, "http://e1");
        await refresher.RefreshOnceAsync(CancellationToken.None);
        var first = store.Current!;
        gateway.RotationsKv = [.. gateway.RotationsKv,
            new Kv("/s3worker/rotations/extra", """{"requested_unix":1760157999,"requested_by":"op"}""", 42)];

        // Act
        await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert — пересборка: новая ссылка модели и новый пересчёт алертов
        store.Current!.Rotations.Should().HaveCount(first.Rotations.Count + 1);
        store.Current.Rotations.Should().NotBeSameAs(first.Rotations);
        engine.EvaluateCalls.Should().Be(2);
    }

    // S10: исчезновение ключа видно в полном range-чтении (ключа нет против
    // кэша) → пересборка.
    [Fact]
    public async Task Refresh_KeyGone_Rebuilds()
    {
        // Arrange — первый тик с ротациями, затем первая заявка снята (ключ удалён)
        var gateway = DemoGateway();
        var store = new S3SnapshotStore();
        var engine = new CountingS3AlertEngine();
        var time = new FixedTimeProvider();
        var refresher = New(gateway, store, engine, time, "http://e1");
        await refresher.RefreshOnceAsync(CancellationToken.None);
        var first = store.Current!;
        gateway.RotationsKv = gateway.RotationsKv.Skip(1).ToArray();

        // Act
        await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert — ротации пересобраны без снятой заявки, пересчёт выполнен
        store.Current!.Rotations.Should().HaveCount(first.Rotations.Count - 1);
        store.Current.Rotations.Should().NotBeSameAs(first.Rotations);
        engine.EvaluateCalls.Should().Be(2);
    }

    // S10/E11: после отказного тика первый успешный при НЕИЗМЕННЫХ данных —
    // тихий тик (тики не классифицируются, гейт — дельта состояния): пересборки
    // и алертного пересчёта нет, доступностная часть восстановлена.
    [Fact]
    public async Task Refresh_RecoveryAfterFail_SameData_QuietTick()
    {
        // Arrange — успешный тик, затем отказ всех endpoints, затем восстановление
        var gateway = DemoGateway();
        var store = new S3SnapshotStore();
        var engine = new CountingS3AlertEngine();
        var time = new FixedTimeProvider();
        var refresher = New(gateway, store, engine, time, "http://e1");
        await refresher.RefreshOnceAsync(CancellationToken.None);
        var first = store.Current!;
        gateway.FailEndpoints.Add("http://e1");
        await refresher.RefreshOnceAsync(CancellationToken.None);
        store.Current!.EtcdReachable.Should().BeFalse();
        gateway.FailEndpoints.Clear();
        time.Utc = time.Utc.AddSeconds(5);

        // Act — восстановительный тик по тем же данным etcd
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert — тихий тик: данные не пересобраны, алертный пересчёт не
        // выполнялся (счёт 2: пересборка т1 + пересчёт отказного т2),
        // доступностная часть свежая.
        result.IsSuccess.Should().BeTrue();
        engine.EvaluateCalls.Should().Be(2, "тихий тик не запускает алертный пересчёт");
        store.Current!.Clusters.Should().BeSameAs(first.Clusters, "снапшот не пересобирается");
        store.Current.EtcdReachable.Should().BeTrue();
        store.Current.ConsecutiveFailures.Should().Be(0);
        store.Current.BuiltAtUtc.Should().Be(time.Utc, "штамп свежести обновляется");
    }

    // S10/E11: после отказного тика первый успешный при ИЗМЕНИВШИХСЯ данных —
    // пересборка (дельта состояния есть).
    [Fact]
    public async Task Refresh_RecoveryAfterFail_DataChanged_Rebuilds()
    {
        // Arrange — успешный тик, отказ; пока endpoints мертвы, в etcd ставится
        // новая заявка ротации
        var gateway = DemoGateway();
        var store = new S3SnapshotStore();
        var engine = new CountingS3AlertEngine();
        var time = new FixedTimeProvider();
        var refresher = New(gateway, store, engine, time, "http://e1");
        await refresher.RefreshOnceAsync(CancellationToken.None);
        var first = store.Current!;
        gateway.FailEndpoints.Add("http://e1");
        await refresher.RefreshOnceAsync(CancellationToken.None);
        gateway.FailEndpoints.Clear();
        gateway.RotationsKv = [.. gateway.RotationsKv,
            new Kv("/s3worker/rotations/extra", """{"requested_unix":1760157999,"requested_by":"op"}""", 42)];

        // Act — восстановительный тик по изменившимся данным
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert — пересборка: новые данные, доступностная часть свежая; счёт 3:
        // пересборка т1 + пересчёт отказного т2 + пересборка т3
        result.IsSuccess.Should().BeTrue();
        engine.EvaluateCalls.Should().Be(3);
        store.Current!.EtcdReachable.Should().BeTrue();
        store.Current.ConsecutiveFailures.Should().Be(0);
        store.Current.Rotations.Should().HaveCount(first.Rotations.Count + 1);
        store.Current.Clusters.Should().NotBeSameAs(first.Clusters, "дельта состояния — пересборка");
    }
}
