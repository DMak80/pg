using FluentAssertions;
using OwnS3.Protocol.PeerAuth;
using S3Worker.Core.OwnS3;
using S3Worker.Provisioning;
using Xunit;

namespace S3Worker.UnitTests.Provisioning;

// InstanceEnvComposer — единая точка сборки env инстанса (arch/24 §4, spec §3.6,
// §8.1.4): клиентская грань профиля + полный набор OWNS3_REPLICATION_* (значения,
// не пути); advertised-endpoint из portalloc/AdvertisedHost строит вызывающий.
public class InstanceEnvComposerTests
{
    [Fact]
    public void Compose_FullEnvSet()
    {
        // Arrange: креды профиля + материал инстанса + trust-anchor CA.
        var secrets = new OwnS3NodeSecrets("root-u", "root-p",
            [new OwnS3AccessKey("ak-1", "sk-1", "read-write"), new OwnS3AccessKey("ak-2", "sk-2", "read-only")]);
        var ca = new PeerTrustAnchor("backups", "-----BEGIN PUBLIC KEY-----CA-----");
        var nodePki = new IssuedPkiMaterial(
            new PeerCertificate("east1", "-----BEGIN PUBLIC KEY-----NODE-----", 1760156800000, 0, "backups", "sig"),
            """{"name":"east1","publicKeyPem":"-----BEGIN PUBLIC KEY-----NODE-----","issuedAtUnixMs":1760156800000,"expiresAtUnixMs":0,"caName":"backups","signature":"sig"}""",
            "-----BEGIN PRIVATE KEY-----NODE-----");
        var composer = new InstanceEnvComposer();

        // Act
        var env = composer.Compose(secrets, nodePki, ca, "http://s3-host:18001");

        // Assert: клиентская грань без изменений + репликация (4 переменные значениями).
        env["OWNS3_ROOT_USER"].Should().Be("root-u");
        env["OWNS3_ROOT_PASSWORD"].Should().Be("root-p");
        env["OWNS3_ACCESS_KEYS__0__ACCESSKEY"].Should().Be("ak-1");
        env["OWNS3_ACCESS_KEYS__0__POLICY"].Should().Be("read-write");
        env["OWNS3_ACCESS_KEYS__1__POLICY"].Should().Be("read-only");
        env["OWNS3_DATA_DIR"].Should().Be("/data");
        env["OWNS3_REPLICATION_CERT"].Should().Be(nodePki.CertificateJson);
        env["OWNS3_REPLICATION_PRIVATE_KEY"].Should().Be(nodePki.PrivateKeyPem);
        env["OWNS3_REPLICATION_TRUST_CA"].Should().Be(ca.ToJson());
        env["OWNS3_REPLICATION_ADVERTISED_ENDPOINT"].Should().Be("http://s3-host:18001");
    }
}
