using PgWorker.PgNode;
using Xunit;

// AAA: NodeBootstrapData — разбор bootstrap-JSON и валидность для старта
// (spec §3.4: проверки аргументов хендшейка).
public class NodeBootstrapDataTests
{
    private const string CertPem = "-----BEGIN CERTIFICATE-----\nX\n-----END CERTIFICATE-----\n";
    private const string KeyPem = "-----BEGIN PRIVATE KEY-----\nY\n-----END PRIVATE KEY-----\n";

    // Валидная PEM-пара для IsValidForStart=true (RSA-2048, самоподписанный).
    private static (string Cert, string Key) RealPemPair()
    {
        using var rsa = System.Security.Cryptography.RSA.Create(2048);
        var request = new System.Security.Cryptography.X509Certificates.CertificateRequest(
            "CN=test", rsa, System.Security.Cryptography.HashAlgorithmName.SHA256,
            System.Security.Cryptography.RSASignaturePadding.Pkcs1);
        using var cert = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1),
            DateTimeOffset.UtcNow.AddYears(1));
        return (cert.ExportCertificatePem(), rsa.ExportPkcs8PrivateKeyPem());
    }

    [Fact]
    public void Parse_ValidJson_ReturnsData()
    {
        // Arrange: JSON в контракте воркера (NodeBootstrapBuilder).
        var json = System.Text.Json.JsonSerializer.Serialize(new Dictionary<string, string>
        {
            ["spilo_configuration"] = "---\nrestapi:\n",
            ["cert_pem"] = CertPem,
            ["key_pem"] = KeyPem,
        });

        // Act: разбор.
        var data = NodeBootstrapData.Parse(json);

        // Assert: все три поля на месте.
        data.Should().NotBeNull();
        data!.SpiloConfiguration.Should().Contain("restapi:");
        data.CertPem.Should().Be(CertPem);
        data.KeyPem.Should().Be(KeyPem);
    }

    [Fact]
    public void Parse_MalformedJson_ReturnsNull()
    {
        // Arrange: битый JSON (чужой/повреждённый ключ).

        // Act
        var data = NodeBootstrapData.Parse("not a json {");

        // Assert: null — ключ не нашего контракта.
        data.Should().BeNull();
    }

    [Fact]
    public void Parse_EmptyFields_ReturnsNull()
    {
        // Arrange: JSON с пустым полем (неполная доставка).
        var json = """{"spilo_configuration":"","cert_pem":"C","key_pem":"K"}""";

        // Act
        var data = NodeBootstrapData.Parse(json);

        // Assert: null — пустые поля не проходят валидацию контракта.
        data.Should().BeNull();
    }

    [Fact]
    public void IsValidForStart_ValidPemPair_True()
    {
        // Arrange: реальные парсимые PEM-пара + непустой YAML.
        var (cert, key) = RealPemPair();
        var data = new NodeBootstrapData("---\nrestapi:\n", cert, key);

        // Act / Assert: валидна для старта.
        data.IsValidForStart().Should().BeTrue();
    }

    [Fact]
    public void IsValidForStart_GarbageCertificate_False()
    {
        // Arrange: мусорный серт (не парсится в пару).
        var data = new NodeBootstrapData("---\nrestapi:\n", "GARBAGE", "GARBAGE-KEY");

        // Act / Assert: не валидна (хендшейк вернёт Invalid).
        data.IsValidForStart().Should().BeFalse();
    }
}
