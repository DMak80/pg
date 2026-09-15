using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Shared.Etcd.Client;
using AdminPanel.Etcd.Parsing;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Парсер ключа /workers/api_tls/<worker> (spec §3.1): метаданные целевого
// серта без PEM-материалов; битый JSON/серт — parseError (тик не роняют).
public class WorkerCertParserTests
{
    // Локальный PKI-хелпер файла: GenerateCa + Issue (SAN: DNS cn + IP 127.0.0.1).
    private static class TestPki
    {
        public static (string CaPem, string CaKeyPem) GenerateCa()
        {
            using var rsa = RSA.Create(2048);
            var request = new CertificateRequest(
                "CN=test-ca", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
            using var ca = request.CreateSelfSigned(
                DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(1));
            return (ca.ExportCertificatePem(), ca.GetRSAPrivateKey()!.ExportPkcs8PrivateKeyPem());
        }

        public static (string CertPem, string KeyPem) Issue(string caPem, string caKeyPem, string commonName)
        {
            using var ca = X509Certificate2.CreateFromPem(caPem);
            using var caKey = RSA.Create();
            caKey.ImportFromPem(caKeyPem);
            using var caWithKey = ca.CopyWithPrivateKey(caKey);
            using var leafKey = RSA.Create(2048);
            var request = new CertificateRequest(
                $"CN={commonName}", leafKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            var san = new SubjectAlternativeNameBuilder();
            san.AddDnsName(commonName);
            san.AddIpAddress(System.Net.IPAddress.Loopback);
            request.CertificateExtensions.Add(san.Build());
            using var cert = request.Create(
                caWithKey, DateTimeOffset.UtcNow.AddDays(-1),
                DateTimeOffset.UtcNow.AddDays(30), RandomNumberGenerator.GetBytes(8));
            return (cert.ExportCertificatePem(), leafKey.ExportPkcs8PrivateKeyPem());
        }
    }

    private static readonly (string CaPem, string CaKeyPem) Ca = TestPki.GenerateCa();

    [Fact]
    public void Parse_NullKv_NoCertNoError()
    {
        // Arrange / Act
        var (cert, error) = WorkerCertParser.Parse("/workers/api_tls/pgworker", null);
        // Assert: ключа нет → unmanaged
        cert.Should().BeNull();
        error.Should().BeNull();
    }

    [Fact]
    public void Parse_ValidKey_MetaDataWithoutPem()
    {
        // Arrange
        var (certPem, keyPem) = TestPki.Issue(Ca.CaPem, Ca.CaKeyPem, "pgworker-api");
        var json = $$"""{"cert_pem":{{System.Text.Json.JsonSerializer.Serialize(certPem)}},"key_pem":{{System.Text.Json.JsonSerializer.Serialize(keyPem)}},"updated_unix":1757000000,"updated_by":"admin"}""";
        // Act
        var (cert, error) = WorkerCertParser.Parse("/workers/api_tls/pgworker", new Kv("/workers/api_tls/pgworker", json, 1));
        // Assert: метаданные; key_pem в модель не попадает
        error.Should().BeNull();
        cert!.Thumbprint.Should().HaveLength(64);
        cert.Subject.Should().Contain("pgworker-api");
        cert.San.Should().Contain("pgworker-api");
        cert.UpdatedUnix.Should().Be(1757000000);
        cert.UpdatedBy.Should().Be("admin");
        cert.NotAfter.Should().BeAfter(cert.NotBefore);
    }

    [Fact]
    public void Parse_BrokenJson_ParseError()
    {
        // Arrange / Act
        var (cert, error) = WorkerCertParser.Parse("/workers/api_tls/pgworker", new Kv("/workers/api_tls/pgworker", "{not-json", 1));
        // Assert: битый ключ — parseError по канону, тик жив
        cert.Should().BeNull();
        error.Should().NotBeNull();
    }

    [Fact]
    public void Parse_BrokenPem_ParseError()
    {
        // Arrange: cert_pem — не сертификат
        var json = """{"cert_pem":"-----BEGIN CERTIFICATE-----\nZZ\n-----END CERTIFICATE-----","key_pem":"x"}""";
        // Act
        var (cert, error) = WorkerCertParser.Parse("/workers/api_tls/pgworker", new Kv("/workers/api_tls/pgworker", json, 1));
        // Assert
        cert.Should().BeNull();
        error.Should().NotBeNull();
    }
}
