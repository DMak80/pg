using OwnS3.Protocol.PeerAuth;

namespace OwnS3.UnitTests.PeerAuth;

// Модель сертификата пира (глава 07 §2): JSON camelCase roundtrip,
// каноническая строка подписи, бессрочность, валидация trust-anchor.
public class PeerCertificateTests
{
    private static readonly PeerCertificate Sample = new(
        "owns3-n1", "-----BEGIN PUBLIC KEY-----\nAAEB\n-----END PUBLIC KEY-----\n",
        1760000000000, 1791536000000, "owns3-ca", "c2ln");

    [Fact]
    public void Json_Roundtrip_CamelCase()
    {
        // Arrange — образцовый сертификат
        // Act — сериализация и обратный разбор
        var json = Sample.ToJson();
        var parsed = PeerCertificate.FromJson(json);

        // Assert — все поля эквивалентны; JSON camelCase (publicKeyPem/issuedAtUnixMs)
        parsed.Should().Be(Sample);
        json.Should().Contain("\"publicKeyPem\":");
        json.Should().Contain("\"issuedAtUnixMs\":");
        json.Should().Contain("\"caName\":");
    }

    [Fact]
    public void CanonicalForm_FiveLines_OrderFixed()
    {
        // Arrange — образцовый сертификат
        // Act — каноническая строка подписи CA
        var canonical = Sample.CanonicalForm();

        // Assert — пять строк в фиксированном порядке (глава 07 §2)
        canonical.Should().Be(
            "owns3-n1\n-----BEGIN PUBLIC KEY-----\nAAEB\n-----END PUBLIC KEY-----\n\n1760000000000\n1791536000000\nowns3-ca");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void ExpiresAt_NonPositive_NeverExpires(long expires)
    {
        // Arrange — сертификат с expiresAtUnixMs ≤ 0 (бессрочный)
        var cert = Sample with { ExpiresAtUnixMs = expires };

        // Act — проверка «истёк» сколь угодно далёким моментом
        var expired = cert.IsExpired(new DateTimeOffset(2199, 1, 1, 0, 0, 0, TimeSpan.Zero));

        // Assert — бессрочный никогда не истекает
        expired.Should().BeFalse();
    }

    [Fact]
    public void IsExpired_Past_Expires()
    {
        // Arrange — expiresAtUnixMs = 1000 мс (1970-01-01T00:00:01Z)
        var cert = Sample with { ExpiresAtUnixMs = 1000 };

        // Act — «сейчас» 2026 год
        var expired = cert.IsExpired(new DateTimeOffset(2026, 10, 14, 0, 0, 0, TimeSpan.Zero));

        // Assert — истёк
        expired.Should().BeTrue();
    }

    [Fact]
    public void FromJson_Malformed_Throws()
    {
        // Arrange — мусорная строка вместо JSON
        // Act
        var act = () => PeerCertificate.FromJson("{не-json");

        // Assert — бросает (fail-fast загрузки), не возвращает null
        act.Should().Throw<Exception>();
    }

    [Fact]
    public void FromJson_EmptyMandatoryFields_Throws()
    {
        // Arrange — JSON с пустым обязательным полем
        // Act
        var act = () => PeerCertificate.FromJson(
            """{"name":"","publicKeyPem":"x","issuedAtUnixMs":1,"expiresAtUnixMs":0,"caName":"ca","signature":"s"}""");

        // Assert — fail-fast: обязательные поля непусты
        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void TrustAnchor_Json_Roundtrip()
    {
        // Arrange — trust-anchor CA
        var trust = new PeerTrustAnchor("owns3-ca", "-----BEGIN PUBLIC KEY-----\nAAEB\n-----END PUBLIC KEY-----\n");

        // Act — сериализация и обратный разбор
        var json = trust.ToJson();
        var parsed = PeerTrustAnchor.FromJson(json);

        // Assert — camelCase-поля, круговой разбор без потерь
        parsed.Should().Be(trust);
        json.Should().Contain("\"name\":");
        json.Should().Contain("\"publicKeyPem\":");
    }

    [Fact]
    public void TrustAnchor_FromJson_Malformed_Throws()
    {
        // Arrange — мусорная строка
        // Act
        var act = () => PeerTrustAnchor.FromJson("не-json");

        // Assert — бросает, не null
        act.Should().Throw<Exception>();
    }
}
