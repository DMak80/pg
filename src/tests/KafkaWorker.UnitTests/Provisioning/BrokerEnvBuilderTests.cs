using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using KafkaWorker.Core.Templates;
using KafkaWorker.Provisioning.Processes;
using Xunit;

namespace KafkaWorker.UnitTests.Provisioning;

// Окно ротации CA (t07, arch/16 §2.3): Build с раздельными Signing/Trust CA —
// серт ноды подписан NEW CA, truststore несёт bundle; вне ротации — snap-значения.
public class BrokerEnvBuilderTests
{
    private const string Cluster = "events";

    private static readonly ProvisioningOptions Options =
        new(16000, 16999, BrokerBootSec: 100, NodeDeadSec: 90, null, "apache/kafka:4.0.0");

    private static KafkaClusterSnapshot Snap(string caPem, string caKey) => new(
        Cluster,
        new KafkaClusterConfig(2, 2, 1, 3, 604800000, 1756500000, null),
        [
            new KafkaBrokerDecl("broker1", "RUNNING", "controller", null),
            new KafkaBrokerDecl("broker2", "RUNNING", "broker", null),
        ],
        [], [], 0,
        Endpoints: "h1:16001,h1:16002",
        AppUser: "app", AppPassword: "app-pw",
        AdminUser: "admin", AdminPassword: "admin-pw",
        CaPem: caPem, CaKey: caKey);

    private static NodeAddress Addr() => new("h1", 16001);

    [Fact]
    public void Build_Default_SignsAndTrustsWithSnapshotCa()
    {
        // Arrange — OLD CA в снапшоте (вне ротации)
        var (caPem, caKey) = ClusterPki.GenerateCa(Cluster);
        var snap = Snap(caPem, caKey);
        var cache = new BrokerCertificateCache();

        // Act
        var env = BrokerEnvBuilder.Build(
            snap, "broker1", Addr(), ["app-pw"], ["admin-pw"], Options, cache);

        // Assert — truststore == CA снапшота; серт == кеш-выпуск от того же CA
        var expected = cache.GetOrCreate(Cluster, "broker1", caPem, caKey, "h1:16001");
        env["KAFKA_SSL_TRUSTSTORE_CERTIFICATES"].Should().Be(caPem.Replace("\n", "\\n"));
        env["KAFKA_SSL_KEYSTORE_CERTIFICATE_CHAIN"].Should().Be(expected.CertPem.Replace("\n", "\\n"));
    }

    [Fact]
    public void Build_RotationWindow_SignsWithNewCa_TrustsBundle()
    {
        // Arrange — окно ротации: подпись NEW CA, доверие bundle OLD+NEW
        var (oldPem, oldKey) = ClusterPki.GenerateCa(Cluster);
        var (newPem, newKey) = ClusterPki.GenerateCa(Cluster);
        var bundle = oldPem + "\n" + newPem;
        var snap = Snap(oldPem, oldKey);
        var cache = new BrokerCertificateCache();

        // Act
        var env = BrokerEnvBuilder.Build(
            snap, "broker1", Addr(), ["app-pw"], ["admin-pw"], Options, cache,
            signingCaPem: newPem, signingCaKey: newKey, trustCaPem: bundle);

        // Assert — keystore от NEW (серт другой), truststore == bundle
        var newSigned = cache.GetOrCreate(Cluster, "broker1", newPem, newKey, "h1:16001");
        var oldSigned = cache.GetOrCreate(Cluster, "broker1", oldPem, oldKey, "h1:16001");
        env["KAFKA_SSL_KEYSTORE_CERTIFICATE_CHAIN"].Should().Be(newSigned.CertPem.Replace("\n", "\\n"));
        newSigned.CertPem.Should().NotBe(oldSigned.CertPem, "новая CA даёт другой серт ноды");
        env["KAFKA_SSL_TRUSTSTORE_CERTIFICATES"].Should().Be(bundle.Replace("\n", "\\n"));
    }
}
