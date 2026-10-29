using System.Diagnostics.Metrics;
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using FluentAssertions;
using S3Worker.Core.Model;
using S3Worker.Core.OwnS3;
using S3Worker.IntegrationTests.Api;
using S3Worker.IntegrationTests.Etcd;
using S3Worker.Provisioning;
using S3Worker.Provisioning.Processes;
using Shared.Etcd.Client;
using Shared.Etcd.Coordination;
using Shared.Core;
using Xunit;

namespace S3Worker.IntegrationTests.S3;

// etcd-контур зонда F (spec §8.2): заявка через API → тик → канонические
// ключи + снос заявки; периодический гейт SyncIntervalSec (прогон/тихий тик).
// Сценарий «ошибка записи → заявка жива, ретрай успешен» — юнитом
// SyncProbeTests.Заявка_FailedПрогонаЖива (FakeEtcd.PutFault) — здесь НЕ дублируется.
[Collection(S3EtcdCollection.Name)]
public class SyncProbeEtcdTests(S3EtcdFixture etcd) : IAsyncLifetime
{
    private static readonly string Prefix = "/s3worker";

    // Уникальное имя кластера (изоляция ключей в общем etcd фикстуры).
    private readonly string _cluster = "probe" + Guid.NewGuid().ToString("N")[..6];

    // Управляемые часы: гейт двигается тестом явно.
    private readonly FixedTime _clock = new() { Utc = DateTimeOffset.UtcNow };

    public async ValueTask InitializeAsync()
    {
        // Сид Active-кластера: config без state, обе ноды RUNNING, portalloc;
        // креды — предзаписанный per-cluster secrets-ключ (канон arch/23 §2).
        await etcd.PutAsync($"/s3/clusters/{_cluster}/config",
            """{"platform":"owns3","nodes":2,"created_unix":1760156800}""");
        await etcd.PutAsync($"/s3/clusters/{_cluster}/secrets",
            """{"root_user":"root","root_password":"pw","access_keys":[{"name":"backups","access_key":"bk","secret_key":"sk","policy":"read-write"},{"name":"panel","access_key":"pk","secret_key":"ps","policy":"read-only"}]}""");
        await etcd.PutAsync($"/s3/clusters/{_cluster}/nodes/east1/state", "RUNNING");
        await etcd.PutAsync($"/s3/clusters/{_cluster}/nodes/east2/state", "RUNNING");
        await etcd.PutAsync($"/s3worker/portalloc/{_cluster}",
            """{"east1":{"host":"h1","s3":18001},"east2":{"host":"h1","s3":18002}}""");
    }

    public async ValueTask DisposeAsync()
    {
        // Полная чистка СВОИХ ключей (own-only, docs/e2e-isolation.md).
        var ct = CancellationToken.None;
        await etcd.Gateway.DeleteAsync(etcd.Endpoint, $"/s3/clusters/{_cluster}/", prefix: true, ct);
        await etcd.Gateway.DeleteAsync(etcd.Endpoint, $"/s3worker/probes/{_cluster}", prefix: false, ct);
        await etcd.Gateway.DeleteAsync(etcd.Endpoint, $"/s3worker/portalloc/{_cluster}", prefix: false, ct);
        await etcd.Gateway.DeleteAsync(etcd.Endpoint, $"{Prefix}/work/{_cluster}", prefix: false, ct);
        await etcd.Gateway.DeleteAsync(etcd.Endpoint, $"{Prefix}/claims/{_cluster}", prefix: false, ct);
    }

    // §8.2.1: заявка через API (WAF) → один тик SyncProbeProcess → оба ключа
    // записаны канонически, заявка снесена.
    [Fact]
    public async Task ApiRequest_Tick_KeysWritten_RequestRemoved()
    {
        // Arrange: WAF на etcd фикстуры; стаб нод с взаимными probe-ответами
        // и равными sync-report страницами (конец сразу).
        var stub = new StubNodeClient();
        SeedMirror(stub);
        using var factory = new S3ApiFactory(etcd);
        using var client = factory.CreateClient();
        var probe = NewProbe(stub);

        // Act: POST sync-probe → 202; затем один тик.
        using var response = await client.PostAsync(
            $"/api/s3/clusters/{_cluster}/sync-probe", null, TestContext.Current.CancellationToken);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        response.StatusCode.Should().Be(HttpStatusCode.Accepted, body);
        var result = await probe.TickAsync(Snap(), TestContext.Current.CancellationToken);

        // Assert: ключи есть, JSON парсится, state OK/SYNCED, checked_unix > 0;
        // заявка снесена.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        var connectivity = await etcd.Gateway.GetAsync(
            etcd.Endpoint, $"/s3/clusters/{_cluster}/connectivity", TestContext.Current.CancellationToken);
        connectivity.Value.Should().NotBeNull();
        using (var doc = JsonDocument.Parse(connectivity.Value!.Value))
        {
            doc.RootElement.GetProperty("state").GetString().Should().Be("OK");
            doc.RootElement.GetProperty("checked_unix").GetInt64().Should().BeGreaterThan(0);
        }

        var sync = await etcd.Gateway.GetAsync(
            etcd.Endpoint, $"/s3/clusters/{_cluster}/sync", TestContext.Current.CancellationToken);
        sync.Value.Should().NotBeNull();
        using (var doc = JsonDocument.Parse(sync.Value!.Value))
        {
            doc.RootElement.GetProperty("state").GetString().Should().Be("SYNCED");
        }

        var request = await etcd.Gateway.GetAsync(
            etcd.Endpoint, $"/s3worker/probes/{_cluster}", TestContext.Current.CancellationToken);
        request.Value.Should().BeNull();
    }

    // §8.2.2: периодический гейт — первый тик (ключей нет) прогон, второй
    // немедленно тихий, сдвиг часов ≥ SyncIntervalSec — прогон.
    [Fact]
    public async Task PeriodicGate_Run_QuietTick_Run()
    {
        // Arrange
        var stub = new StubNodeClient();
        SeedMirror(stub);
        var probe = NewProbe(stub);
        var snap = Snap();

        // Act 1 + Assert 1: первый тик — прогон (put обоих ключей).
        var first = await probe.TickAsync(snap, TestContext.Current.CancellationToken);
        first.IsSuccess.Should().BeTrue(first.Error?.Message);
        var connectivity1 = await etcd.Gateway.GetAsync(
            etcd.Endpoint, $"/s3/clusters/{_cluster}/connectivity", TestContext.Current.CancellationToken);
        connectivity1.Value.Should().NotBeNull();
        var checked1 = JsonDocument.Parse(connectivity1.Value!.Value)
            .RootElement.GetProperty("checked_unix").GetInt64();

        // Act 2 + Assert 2: немедленный второй тик — тихий (checked_unix свеж).
        var second = await probe.TickAsync(snap, TestContext.Current.CancellationToken);
        second.IsSuccess.Should().BeTrue(second.Error?.Message);
        var connectivity2 = await etcd.Gateway.GetAsync(
            etcd.Endpoint, $"/s3/clusters/{_cluster}/connectivity", TestContext.Current.CancellationToken);
        JsonDocument.Parse(connectivity2.Value!.Value)
            .RootElement.GetProperty("checked_unix").GetInt64().Should().Be(checked1);

        // Act 3 + Assert 3: сдвиг часов за период — прогон (checked_unix вырос).
        _clock.Utc = _clock.Utc.AddSeconds(61);
        var third = await probe.TickAsync(snap, TestContext.Current.CancellationToken);
        third.IsSuccess.Should().BeTrue(third.Error?.Message);
        var connectivity3 = await etcd.Gateway.GetAsync(
            etcd.Endpoint, $"/s3/clusters/{_cluster}/connectivity", TestContext.Current.CancellationToken);
        JsonDocument.Parse(connectivity3.Value!.Value)
            .RootElement.GetProperty("checked_unix").GetInt64().Should().BeGreaterThan(checked1);
    }

    // Прямой инстанс SyncProbeProcess на etcd фикстуры (клэйм захватывается сразу).
    private SyncProbeProcess NewProbe(IOwnS3NodeClient client)
    {
        var endpoints = new[] { etcd.Endpoint };
        var secrets = new ClusterSecretsStore(etcd.Gateway, endpoints);
        var claims = new ClaimStore(Prefix, endpoints, etcd.Gateway, _clock);
        claims.TryClaimClusterAsync(_cluster, TestContext.Current.CancellationToken)
            .GetAwaiter().GetResult();
        var journal = new WorkJournal(Prefix, etcd.Gateway, endpoints);
        return new SyncProbeProcess(
            etcd.Gateway, endpoints, claims, journal, secrets, client,
            new S3ProvisioningOptions(18000, 18999, 100, 90, 15, null, "owns3:dev", "pgworker-backups",
                SyncIntervalSec: 60, SyncMaxPages: 20),
            _clock, new Meter("S3Worker"));
    }

    // Снапшот под сид (Active, обе ноды RUNNING).
    private S3ClusterSnapshot Snap() => new(
        _cluster,
        new S3ClusterConfig("owns3", 2, null, 1760156800, null),
        [
            new S3NodeSnapshot("east1", "RUNNING", "h1", new S3NodePlacement("h1", null, null), null, null, null),
            new S3NodeSnapshot("east2", "RUNNING", "h1", new S3NodePlacement("h1", null, null), null, null, null),
        ],
        null, null, null);

    // Взаимные probe-ответы + равные страницы с немедленным концом.
    private static void SeedMirror(StubNodeClient stub)
    {
        stub.ProbeByPort[18001] = new OwnS3ProbeReport(true, [new OwnS3ProbeTarget("east2", true, 5)]);
        stub.ProbeByPort[18002] = new OwnS3ProbeReport(true, [new OwnS3ProbeTarget("east1", true, 5)]);
        stub.SyncReportPages[18001] = new Dictionary<string, OwnS3SyncReportPage>
        {
            [""] = new OwnS3SyncReportPage(0, null, 0, null, 0, 0, "0", null),
        };
        stub.SyncReportPages[18002] = new Dictionary<string, OwnS3SyncReportPage>
        {
            [""] = new OwnS3SyncReportPage(0, null, 0, null, 0, 0, "0", null),
        };
    }

    // Управляемые часы (Microsoft.Extensions.TimeProvider.Testing не подключён).
    private sealed class FixedTime : TimeProvider
    {
        public DateTimeOffset Utc { get; set; }

        public override DateTimeOffset GetUtcNow() => Utc;
    }

    // Стаб клиента нод: методы зонда программируются словарями по порту
    // (аналог FakeNodeClient; IntegrationTests не видит internal-фейки юнитов),
    // остальные методы — заглушки (не вызываются этим сценарием).
    private sealed class StubNodeClient : IOwnS3NodeClient
    {
        public readonly Dictionary<int, OwnS3ProbeReport> ProbeByPort = [];
        public readonly Dictionary<int, Dictionary<string, OwnS3SyncReportPage>> SyncReportPages = [];
        public readonly Dictionary<int, IReadOnlyList<OwnS3ListEntry>> Listings = [];

        public Task<Result<bool>> HealthAsync(string host, int port, CancellationToken ct)
            => Task.FromResult(Result<bool>.Success(true));

        public Task<Result<bool>> BucketExistsAsync(
            string host, int port, OwnS3NodeSecrets creds, string bucket, CancellationToken ct)
            => Task.FromResult(Result<bool>.Success(true));

        public Task<Result> EnsureBucketAsync(
            string host, int port, OwnS3NodeSecrets creds, string bucket, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result<OwnS3NodeUsage>> GetUsageAsync(
            string host, int port, OwnS3NodeSecrets creds, CancellationToken ct)
            => Task.FromResult(Result<OwnS3NodeUsage>.Success(new OwnS3NodeUsage(1, 2)));

        public Task<Result<OwnS3ReplicationStatus>> GetReplicationStatusAsync(
            string host, int port, OwnS3NodeSecrets creds, CancellationToken ct)
            => Task.FromResult(Result<OwnS3ReplicationStatus>.Success(new OwnS3ReplicationStatus(false, [])));

        public Task<Result<OwnS3JoinOutcome>> AdminJoinAsync(
            string host, int port, OwnS3NodeSecrets creds, string endpoint, CancellationToken ct)
            => Task.FromResult(Result<OwnS3JoinOutcome>.Success(new OwnS3JoinOutcome(OwnS3JoinKind.Joined)));

        public Task<Result<bool>> DeleteTargetAsync(
            string host, int port, OwnS3NodeSecrets creds, string name, CancellationToken ct)
            => Task.FromResult(Result<bool>.Success(true));

        public Task<Result<bool>> PutDenyListAsync(
            string host, int port, IReadOnlyList<(string Name, string PublicKeyPem)> entries,
            IssuedPkiMaterial orchestrator, CancellationToken ct)
            => Task.FromResult(Result<bool>.Success(true));

        public Task<Result<OwnS3ProbeReport>> ProbeAsync(
            string host, int port, OwnS3NodeSecrets creds, CancellationToken ct)
            => Task.FromResult(ProbeByPort.TryGetValue(port, out var report)
                ? Result<OwnS3ProbeReport>.Success(report)
                : Result<OwnS3ProbeReport>.Failed(new ApplicationException("probe not programmed")));

        public Task<Result<OwnS3SyncReportPage>> GetSyncReportPageAsync(
            string host, int port, OwnS3NodeSecrets creds, string bucket, string? marker, CancellationToken ct)
            => Task.FromResult(
                SyncReportPages.TryGetValue(port, out var pages) && pages.TryGetValue(marker ?? "", out var page)
                    ? Result<OwnS3SyncReportPage>.Success(page)
                    : Result<OwnS3SyncReportPage>.Failed(new ApplicationException("page not programmed")));

        public Task<Result<OwnS3ListPage>> ListObjectEntriesAsync(
            string host, int port, OwnS3NodeSecrets creds, string bucket, string? continuationToken, CancellationToken ct)
            => Task.FromResult(Listings.TryGetValue(port, out var entries)
                ? Result<OwnS3ListPage>.Success(new OwnS3ListPage(entries, null))
                : Result<OwnS3ListPage>.Failed(new ApplicationException("listing not programmed")));
    }
}
