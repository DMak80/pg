using System.Text.Json;
using FluentAssertions;
using OwnS3.Protocol.PeerAuth;
using S3Worker.Core.OwnS3;
using S3Worker.Provisioning;
using S3Worker.Provisioning.Processes;
using Xunit;

namespace S3Worker.UnitTests.Provisioning;

// PkiStore — PKI репликации per-cluster в etcd (arch/23 §6, spec §3.2, §8.1.3):
// ленивый выпуск txn put-if-absent (повтор — тот же материал), добор
// оркестратора существующим CA, отказ «CA невосстановим», изоляция trust-доменов
// кластеров, целостность ключ↔сертификат при чтении, канонический JSON arch/23 §2.1.
public class PkiStoreTests
{
    private const string Cluster = "backups";

    private static (PkiStore Store, Fakes.FakeEtcd Etcd, FixedTimeProvider Time) Rig()
    {
        var etcd = new Fakes.FakeEtcd();
        var time = new FixedTimeProvider();
        return (new PkiStore(etcd, ["http://etcd:2379"], time), etcd, time);
    }

    // CA-пара для предзаполнения: CreateCa отдаёт trust-anchor JSON — вытаскиваем PEM.
    private static ClusterCaMaterial NewCa(string name)
    {
        var (caJson, caPem) = PeerCertificateAuthority.CreateCa(name);
        using var doc = JsonDocument.Parse(caJson);
        var publicKeyPem = doc.RootElement.GetProperty("publicKeyPem").GetString()!;
        return new ClusterCaMaterial(new PeerTrustAnchor(name, publicKeyPem), caPem);
    }

    [Fact]
    public async Task RepeatEnsureCluster_SameMaterial()
    {
        // Arrange: свежий etcd — первый ensure выпускает пару CA+оркестратор.
        var (store, etcd, _) = Rig();

        // Act
        var first = await store.EnsureClusterPkiAsync(Cluster, TestContext.Current.CancellationToken);
        var second = await store.EnsureClusterPkiAsync(Cluster, TestContext.Current.CancellationToken);

        // Assert: идемпотентность — материал идентичен; ключи записаны в etcd.
        first.IsSuccess.Should().BeTrue(first.Error?.Message);
        second.IsSuccess.Should().BeTrue(second.Error?.Message);
        second.Value.Ca.Ca.ToJson().Should().Be(first.Value.Ca.Ca.ToJson());
        second.Value.Ca.CaPrivateKeyPem.Should().Be(first.Value.Ca.CaPrivateKeyPem);
        second.Value.Orchestrator.CertificateJson.Should().Be(first.Value.Orchestrator.CertificateJson);
        second.Value.Orchestrator.PrivateKeyPem.Should().Be(first.Value.Orchestrator.PrivateKeyPem);
        etcd.Store.Keys.Should().Contain(S3ProcessCommon.ClusterPkiCaKey(Cluster));
        etcd.Store.Keys.Should().Contain(S3ProcessCommon.OrchestratorPkiKey(Cluster));
    }

    [Fact]
    public async Task OrchestratorCertificateUnlimited_ClusterName()
    {
        // Arrange/Act: выпуск пары кластера.
        var (store, _, _) = Rig();
        var ensured = await store.EnsureClusterPkiAsync(Cluster, TestContext.Current.CancellationToken);

        // Assert: бессрочный (expiresAtUnixMs=0), имя orchestrator, CA = имя кластера.
        ensured.Value.Orchestrator.Certificate.ExpiresAtUnixMs.Should().Be(0);
        ensured.Value.Orchestrator.Certificate.Name.Should().Be("orchestrator");
        ensured.Value.Orchestrator.Certificate.CaName.Should().Be(Cluster);
        ensured.Value.Ca.Ca.Name.Should().Be(Cluster);
    }

    [Fact]
    public async Task PartialPreexistence_CaExists_OrchestratorAdopted()
    {
        // Arrange: в etcd предзаполнен только pki/ca (валидный материал).
        var (store, etcd, time) = Rig();
        var caMaterial = NewCa(Cluster);
        etcd.Seed(S3ProcessCommon.ClusterPkiCaKey(Cluster), PkiStore.SerializeCa(caMaterial));

        // Act
        var ensured = await store.EnsureClusterPkiAsync(Cluster, TestContext.Current.CancellationToken);

        // Assert: оркестратор добран СУЩЕСТВУЮЩИМ CA (publicKeyPem тот же —
        // перевыпуска не было); пара согласована.
        ensured.IsSuccess.Should().BeTrue(ensured.Error?.Message);
        ensured.Value.Ca.Ca.PublicKeyPem.Should().Be(caMaterial.Ca.PublicKeyPem);
        ensured.Value.Orchestrator.Certificate.CaName.Should().Be(Cluster);
        PeerCertificateAuthority.Verify(
            ensured.Value.Orchestrator.Certificate, ensured.Value.Ca.Ca, time.GetUtcNow()).Should().BeTrue();
    }

    [Fact]
    public async Task PartialPreexistence_OrchestratorExists_FailsWithoutIssuingCa()
    {
        // Arrange: предзаполнен только pki/orchestrator (подписан ЧУЖИМ CA — CA нет).
        var (store, etcd, time) = Rig();
        var foreignCa = NewCa("other-domain");
        var (cert, keyPem) = PeerCertificateAuthority.IssueServer(
            "orchestrator", foreignCa.Ca, foreignCa.CaPrivateKeyPem, time.GetUtcNow(), expiresDays: 0);
        etcd.Seed(S3ProcessCommon.OrchestratorPkiKey(Cluster),
            PkiStore.SerializeIssued(new IssuedPkiMaterial(cert, cert.ToJson(), keyPem)));

        // Act
        var ensured = await store.EnsureClusterPkiAsync(Cluster, TestContext.Current.CancellationToken);

        // Assert: Failed с диагностикой «ключ CA невосстановим»; CA не создаётся.
        ensured.IsSuccess.Should().BeFalse();
        ensured.Error!.Message.Should().Contain("unrecoverable");
        etcd.Store.Keys.Should().NotContain(S3ProcessCommon.ClusterPkiCaKey(Cluster));
    }

    [Fact]
    public async Task EnsureNodePki_RepeatSameMaterial_PassesOwnCaVerify()
    {
        // Arrange: пара кластера выпущена.
        var (store, _, time) = Rig();
        var ensured = await store.EnsureClusterPkiAsync(Cluster, TestContext.Current.CancellationToken);

        // Act: два выпуска инстанса (if-absent).
        var first = await store.EnsureNodePkiAsync(Cluster, "east1", ensured.Value.Ca, TestContext.Current.CancellationToken);
        var second = await store.EnsureNodePkiAsync(Cluster, "east1", ensured.Value.Ca, TestContext.Current.CancellationToken);

        // Assert: тот же материал; сертификат проходит Verify своего CA.
        first.IsSuccess.Should().BeTrue(first.Error?.Message);
        second.Value.CertificateJson.Should().Be(first.Value.CertificateJson);
        PeerCertificateAuthority.Verify(first.Value.Certificate, ensured.Value.Ca.Ca, time.GetUtcNow())
            .Should().BeTrue();
    }

    [Fact]
    public async Task TrustDomainIsolation_ForeignCaRejected()
    {
        // Arrange: два кластера с собственными CA.
        var (store, _, time) = Rig();
        var backups = await store.EnsureClusterPkiAsync(Cluster, TestContext.Current.CancellationToken);
        var archive = await store.EnsureClusterPkiAsync("archive", TestContext.Current.CancellationToken);
        var cert = await store.EnsureNodePkiAsync(Cluster, "east1", backups.Value.Ca, TestContext.Current.CancellationToken);

        // Act/Assert: против чужого trust-anchor — false (caName не совпадает);
        // против своего — true (изоляция trust-доменов, spec §3.2).
        PeerCertificateAuthority.Verify(cert.Value.Certificate, archive.Value.Ca.Ca, time.GetUtcNow())
            .Should().BeFalse();
        PeerCertificateAuthority.Verify(cert.Value.Certificate, backups.Value.Ca.Ca, time.GetUtcNow())
            .Should().BeTrue();
    }

    [Fact]
    public async Task SwappedKeyInEtcd_TryReadFailed()
    {
        // Arrange: материал инстанса выпущен, затем ключ в etcd подменён материалом чужого CA.
        var (store, etcd, time) = Rig();
        var ensured = await store.EnsureClusterPkiAsync(Cluster, TestContext.Current.CancellationToken);
        await store.EnsureNodePkiAsync(Cluster, "east1", ensured.Value.Ca, TestContext.Current.CancellationToken);
        var fakeCa = NewCa("fake");
        var (fakeCert, fakeKeyPem) = PeerCertificateAuthority.IssueServer(
            "east1", fakeCa.Ca, fakeCa.CaPrivateKeyPem, time.GetUtcNow(), expiresDays: 0);
        etcd.Store[S3ProcessCommon.NodePkiKey(Cluster, "east1")] = new Fakes.FakeEtcd.Entry(
            PkiStore.SerializeIssued(new IssuedPkiMaterial(fakeCert, fakeCert.ToJson(), fakeKeyPem)), 99, 2);

        // Act
        var read = await store.TryReadNodePkiAsync(Cluster, "east1", TestContext.Current.CancellationToken);

        // Assert: сертификат не проходит верификацию CA кластера (подмена) — Failed.
        read.IsSuccess.Should().BeFalse();
        read.Error!.Message.Should().Contain("CA verification");
    }

    [Fact]
    public async Task TryRead_MissingKey_Null()
    {
        // Arrange: пустой etcd.
        var (store, _, _) = Rig();

        // Act
        var ca = await store.TryReadCaAsync(Cluster, TestContext.Current.CancellationToken);
        var node = await store.TryReadNodePkiAsync(Cluster, "east1", TestContext.Current.CancellationToken);

        // Assert: null (ключа нет — не ошибка).
        ca.IsSuccess.Should().BeTrue();
        ca.Value.Should().BeNull();
        node.IsSuccess.Should().BeTrue();
        node.Value.Should().BeNull();
    }

    [Fact]
    public void CaJsonFormat_CanonicalArch23()
    {
        // Arrange: материал CA.
        var material = NewCa(Cluster);

        // Act
        var json = PkiStore.SerializeCa(material);

        // Assert: поля канона arch/23 §2.1 (смешанный регистр: camelCase + snake_case).
        json.Should().StartWith("""{"name":"backups","publicKeyPem":"-----BEGIN PUBLIC KEY-----""");
        json.Should().Contain("\"private_key_pem\":\"-----BEGIN PRIVATE KEY-----");
    }
}
