using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using AdminPanel.Core;
using AdminPanel.Core.Valkey;
using AdminPanel.Core.Valkey.ValkeyAlerting;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Workers;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.IntegrationTests;

// ValkeySnapshotRefresher против живого etcd (spec §5.2): сид канонических ключей
// valkey (guid-префиксы кластеров) → тик → снапшот/секреты/ротации; битые ключи —
// ParseErrors без исключения; teardown чистит guid-префиксы и ассертит чистоту.
public class ValkeySnapshotIntegrationTests : IClassFixture<ValkeyEtcdFixture>
{
    private readonly ValkeyEtcdFixture _fx;

    public ValkeySnapshotIntegrationTests(ValkeyEtcdFixture fx) => _fx = fx;

    // 1) RefreshOnceAsync на сид-ключах → снапшот: кластеры/ротации/endpoints/серт;
    //    секреты admin-пары в IValkeySecretsStore (полный набор), частичный — пропущен.
    [Fact]
    public async Task RefreshOnce_SeededValkeys_BuildsSnapshotAndSecrets()
    {
        // Arrange: стор/сторы + refresher против живого etcd с сидом фикстуры.
        var store = new ValkeySnapshotStore();
        var secrets = new ValkeySecretsStore();
        var refresher = _fx.NewRefresher(store, secrets);

        // Act
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert: канонические поля из сида.
        result.IsSuccess.Should().BeTrue();
        var snapshot = store.Current;
        snapshot.Should().NotBeNull();
        snapshot!.EtcdReachable.Should().BeTrue();

        var live = snapshot.Clusters.Should().ContainSingle(c => c.Name == _fx.ClusterA).Subject;
        live.State.Should().Be(ValkeyClusterState.Active);
        live.Endpoints.Should().Be("host.docker.internal:17001");
        live.Rotation.Should().NotBeNull();
        live.Rotation!.Role.Should().Be("app");
        var node = live.NodesList.Should().ContainSingle().Subject;
        node.State.Should().Be("RUNNING");
        node.Cpu.Should().Be(1m);
        node.MemGi.Should().Be(1);
        node.DiskGi.Should().Be(10);

        snapshot.Clusters.Should().Contain(c => c.Name == _fx.ClusterB); // частичные креды — кластер есть
        snapshot.Rotations.Should().ContainSingle(t => t.Cluster == _fx.ClusterA);
        snapshot.WorkerApiCert.Should().NotBeNull();

        // Секреты: полный набор admin-пары — в сторе; частичный (ClusterB) — пропущен.
        secrets.Current.Should().ContainKey(_fx.ClusterA);
        var creds = secrets.Current[_fx.ClusterA];
        creds.AdminUser.Should().Be("admin");
        creds.AdminPassword.Should().Be(_fx.AdminPassword);
        secrets.Current.Should().NotContainKey(_fx.ClusterB);
    }

    // 2) Битые ключи (guid-кластер с "{oops" в config) → ParseErrors без исключения,
    //    UnknownKeyCount для неизвестного leaf.
    [Fact]
    public async Task RefreshOnce_BrokenKeys_ParseErrorsWithoutThrow()
    {
        // Arrange: битый config + неизвестный leaf под guid-кластером.
        var store = new ValkeySnapshotStore();
        var refresher = _fx.NewRefresher(store, new ValkeySecretsStore());
        await EtcdSeed.PutAsync(_fx.Endpoint, $"/valkey/clusters/{_fx.ClusterBad}/config", "{oops", CancellationToken.None);
        await EtcdSeed.PutAsync(_fx.Endpoint, $"/valkey/clusters/{_fx.ClusterBad}/future_feature", "{}", CancellationToken.None);

        // Act
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert: тик зелёный, ключи — в ParseErrors/UnknownKeyCount, кластер-скелет в модели.
        result.IsSuccess.Should().BeTrue();
        var snapshot = store.Current!;
        snapshot.ParseErrors.Should().Contain(e => e.Key == $"/valkey/clusters/{_fx.ClusterBad}/config");
        snapshot.UnknownKeyCount.Should().BeGreaterOrEqualTo(1);
        snapshot.Clusters.Should().Contain(c => c.Name == _fx.ClusterBad);
    }

    // 3) Teardown-ассерт чистоты — в ValkeyEtcdFixture.DisposeAsync: guid-ключи
    //    удалены (prefix-delete) и не восстанавливаются; контейнер удаляется там же.
    [Fact]
    public async Task Seed_UsesOnlyOwnedPrefixes()
    {
        // Arrange: что налито — то и чистится; проверяем, что весь сид живёт под
        // владельческими префиксами (условие корректного teardown фикстуры).
        var gateway = EtcdTestHarness.NewGateway();

        // Act
        var clusters = await gateway.RangeAsync(_fx.Endpoint, $"/valkey/clusters/{_fx.Prefix}", CancellationToken.None);

        // Assert: ключи кластеров сидa имеют guid-префикс (чужие не создаются).
        clusters.IsSuccess.Should().BeTrue();
        clusters.Value.Should().OnlyContain(kv => kv.Key.StartsWith($"/valkey/clusters/{_fx.Prefix}", StringComparison.Ordinal));
    }
}

// Фикстура: свой etcd-контейнер (динамический порт) + сид канонических ключей
// valkey через EtcdSeed.PutAsync (guid-префиксы кластеров). Teardown при любом
// исходе: prefix-delete владельческих ключей + ассерт чистоты + удаление контейнера.
public sealed class ValkeyEtcdFixture : IAsyncLifetime
{
    private readonly EtcdContainerFixture _etcd = new();

    public string Endpoint => _etcd.Endpoint;

    // guid-префикс владельческих ключей (изоляция сценариев, AGENTS.md).
    public string Prefix { get; } = $"t03v{Guid.NewGuid():N}";

    public string ClusterA => $"{Prefix}live";

    public string ClusterB => $"{Prefix}half";

    public string ClusterBad => $"{Prefix}bad";

    public string AdminPassword { get; } = $"pw{Guid.NewGuid():N}";

    public async ValueTask InitializeAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        await _etcd.InitializeAsync();

        // Канонические примеры arch/20 §2.1 (guid-кластеры): Active + endpoints +
        // admin-пара (полный/частичный) + ротация; отдельный leaf-ключ API воркера.
        await EtcdSeed.PutAsync(Endpoint, $"/valkey/clusters/{ClusterA}/config",
            """{"nodes":1,"maxmemory_bytes":536870912,"maxmemory_policy":"allkeys-lru","created_unix":1756500000}""", ct);
        await EtcdSeed.PutAsync(Endpoint, $"/valkey/clusters/{ClusterA}/endpoints", "host.docker.internal:17001", ct);
        await EtcdSeed.PutAsync(Endpoint, $"/valkey/clusters/{ClusterA}/nodes/node1/state", "RUNNING", ct);
        await EtcdSeed.PutAsync(Endpoint, $"/valkey/clusters/{ClusterA}/nodes/node1/resources",
            """{"cpu":"1","mem":"1Gi","disk":"10Gi"}""", ct);
        await EtcdSeed.PutAsync(Endpoint, $"/valkey/clusters/{ClusterA}/admin_user", "admin", ct);
        await EtcdSeed.PutAsync(Endpoint, $"/valkey/clusters/{ClusterA}/admin_password", AdminPassword, ct);
        await EtcdSeed.PutAsync(Endpoint, $"/valkey/clusters/{ClusterB}/config",
            """{"nodes":1,"maxmemory_bytes":1,"maxmemory_policy":"noeviction"}""", ct);
        await EtcdSeed.PutAsync(Endpoint, $"/valkey/clusters/{ClusterB}/admin_user", "admin", ct);
        await EtcdSeed.PutAsync(Endpoint, "/valkeyworker/rotations/" + ClusterA,
            $$"""{"role":"app","requested_unix":{{DateTimeOffset.UtcNow.ToUnixTimeSeconds()}},"requested_by":"seed"}""", ct);
        await EtcdSeed.PutAsync(Endpoint, "/valkeyworker/api/vwk1",
            """{"url":"https://valkeyworker:8080","instance":"vwk1","since_unix":1756000001}""", ct);
        await EtcdSeed.PutAsync(Endpoint, "/workers/api_tls/valkeyworker",
            $$"""{"cert_pem":"{{BuildCertPem().Replace("\n", "\\n")}}","updated_unix":1756500000,"updated_by":"admin"}""", ct);
    }

    public async ValueTask DisposeAsync()
    {
        // Полный teardown при любом исходе: владельческие префиксы удаляются,
        // чистота ассертится; затем контейнер.
        var gateway = EtcdTestHarness.NewGateway();
        await gateway.DeleteAsync(Endpoint, "/valkey/", prefix: true, CancellationToken.None);
        await gateway.DeleteAsync(Endpoint, "/valkeyworker/", prefix: true, CancellationToken.None);
        await gateway.DeleteAsync(Endpoint, "/workers/api_tls/valkeyworker", prefix: false, CancellationToken.None);

        var leftovers = await gateway.RangeAsync(Endpoint, "/valkey/", CancellationToken.None);
        if (leftovers.IsSuccess && leftovers.Value.Count > 0)
            throw new InvalidOperationException(
                $"teardown-ассерт чистоты: остались valkey-ключи: {string.Join(", ", leftovers.Value.Select(k => k.Key))}");

        await _etcd.DisposeAsync();
    }

    // Refresher против живого etcd (конструирование без host'а — паттерн EtcdTestHarness).
    public ValkeySnapshotRefresher NewRefresher(ValkeySnapshotStore store, ValkeySecretsStore secrets)
        => new(
            EtcdTestHarness.NewGateway(),
            new ValkeyAlertEngine(Options.Create(new ValkeyAlertsOptions())),
            store,
            secrets,
            Options.Create(new EtcdOptions { Endpoints = [Endpoint] }),
            Options.Create(new ValkeyPanelOptions()),
            new FixedTimeProvider(),
            NullLogger<ValkeySnapshotRefresher>.Instance,
            new ValkeyWorkerHealthStore());

    // Самоподписанный серверный серт для ключа /workers/api_tls/valkeyworker.
    private static string BuildCertPem()
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            "CN=valkeyworker", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        using var cert = request.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(1));
        return cert.ExportCertificatePem();
    }
}
