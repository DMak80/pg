using System.Security.Cryptography;
using OwnS3.Protocol.PeerAuth;

namespace OwnS3.UnitTests.PeerAuth;

// Издатель мини-PKI (глава 07 §2): выпуск CA/серверов, верификация
// сертификатов против trust-anchor, формат подписи P1363 fixed-length.
public class PeerCertificateAuthorityTests
{
    private static readonly DateTimeOffset At = new(2026, 10, 14, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public void IssueAndVerify_ValidCertificate_True()
    {
        // Arrange — CA и сертификат сервера, выпущенный этим CA
        var (caJson, caPrivatePem) = PeerCertificateAuthority.CreateCa("owns3-ca");
        var ca = PeerTrustAnchor.FromJson(caJson);
        var (cert, _) = PeerCertificateAuthority.IssueServer("owns3-n1", ca, caPrivatePem, At);

        // Act — верификация против trust-anchor
        var valid = PeerCertificateAuthority.Verify(cert, ca, At);

        // Assert — валиден
        valid.Should().BeTrue();
    }

    [Fact]
    public void Verify_OtherCa_False()
    {
        // Arrange — два независимых CA; серт выпущен CA-A
        var (caAJson, caAPrivatePem) = PeerCertificateAuthority.CreateCa("ca-a");
        var (caBJson, _) = PeerCertificateAuthority.CreateCa("ca-b");
        var caA = PeerTrustAnchor.FromJson(caAJson);
        var caB = PeerTrustAnchor.FromJson(caBJson);
        var (cert, _) = PeerCertificateAuthority.IssueServer("owns3-n1", caA, caAPrivatePem, At);

        // Act — верификация против чужого trust-anchor
        var valid = PeerCertificateAuthority.Verify(cert, caB, At);

        // Assert — отказ (чужой CA)
        valid.Should().BeFalse();
    }

    [Fact]
    public void Verify_Expired_False()
    {
        // Arrange — серт на 1 день от At
        var (caJson, caPrivatePem) = PeerCertificateAuthority.CreateCa("owns3-ca");
        var ca = PeerTrustAnchor.FromJson(caJson);
        var (cert, _) = PeerCertificateAuthority.IssueServer("owns3-n1", ca, caPrivatePem, At, 1);

        // Act — верификация через 2 дня после выпуска
        var valid = PeerCertificateAuthority.Verify(cert, ca, At.AddDays(2));

        // Assert — истёкший невалиден
        valid.Should().BeFalse();
    }

    [Fact]
    public void Verify_TamperedCertificate_False()
    {
        // Arrange — валидный серт, затем подмена полей в JSON (по очереди)
        var (caJson, caPrivatePem) = PeerCertificateAuthority.CreateCa("owns3-ca");
        var ca = PeerTrustAnchor.FromJson(caJson);
        var (cert, _) = PeerCertificateAuthority.IssueServer("owns3-n1", ca, caPrivatePem, At);
        var tamperedName = cert with { Name = "owns3-evil" };
        var tamperedPem = cert with { PublicKeyPem = "-----BEGIN PUBLIC KEY-----\nFAKE\n-----END PUBLIC KEY-----\n" };
        var tamperedIssued = cert with { IssuedAtUnixMs = cert.IssuedAtUnixMs + 1 };

        // Act — верификация каждой подделки
        var nameBad = PeerCertificateAuthority.Verify(tamperedName, ca, At);
        var pemBad = PeerCertificateAuthority.Verify(tamperedPem, ca, At);
        var issuedBad = PeerCertificateAuthority.Verify(tamperedIssued, ca, At);

        // Assert — любая подмена ломает подпись CA
        nameBad.Should().BeFalse();
        pemBad.Should().BeFalse();
        issuedBad.Should().BeFalse();
    }

    [Fact]
    public void Verify_WrongCaName_False()
    {
        // Arrange — trust с тем же публичным ключом CA, но другим именем
        var (caJson, caPrivatePem) = PeerCertificateAuthority.CreateCa("owns3-ca");
        var ca = PeerTrustAnchor.FromJson(caJson);
        var (cert, _) = PeerCertificateAuthority.IssueServer("owns3-n1", ca, caPrivatePem, At);
        var renamedTrust = ca with { Name = "other-ca-name" };

        // Act — верификация против переименованного trust (ключ тот же)
        var valid = PeerCertificateAuthority.Verify(cert, renamedTrust, At);

        // Assert — caName серта ≠ name trust → невалиден (глава 07 §2)
        valid.Should().BeFalse();
    }

    [Fact]
    public void Verify_GarbageBase64Signature_False()
    {
        // Arrange — валидный серт с НЕ-base64 подписью (атакуемый вход join-грани)
        var (caJson, caPrivatePem) = PeerCertificateAuthority.CreateCa("owns3-ca");
        var ca = PeerTrustAnchor.FromJson(caJson);
        var (cert, _) = PeerCertificateAuthority.IssueServer("owns3-n1", ca, caPrivatePem, At);
        var garbageSignature = cert with { Signature = "###" };

        // Act — верификация; FormatException не должен лететь сквозь Result-границу
        var valid = PeerCertificateAuthority.Verify(garbageSignature, ca, At);

        // Assert — «невалиден» (false), без исключения
        valid.Should().BeFalse();
    }

    [Fact]
    public void Verify_BrokenTrustPem_False()
    {
        // Arrange — trust-anchor с битым PEM публичного ключа (ArgumentException при импорте)
        var (caJson, caPrivatePem) = PeerCertificateAuthority.CreateCa("owns3-ca");
        var ca = PeerTrustAnchor.FromJson(caJson);
        var (cert, _) = PeerCertificateAuthority.IssueServer("owns3-n1", ca, caPrivatePem, At);
        var brokenTrust = ca with { PublicKeyPem = "не-pem" };

        // Act — верификация против битого якоря
        var valid = PeerCertificateAuthority.Verify(cert, brokenTrust, At);

        // Assert — «невалиден» (false), без исключения
        valid.Should().BeFalse();
    }

    [Fact]
    public void Signature_P1363_64Bytes()
    {
        // Arrange — серт от CA
        var (caJson, caPrivatePem) = PeerCertificateAuthority.CreateCa("owns3-ca");
        var ca = PeerTrustAnchor.FromJson(caJson);
        var (cert, _) = PeerCertificateAuthority.IssueServer("owns3-n1", ca, caPrivatePem, At);

        // Act — длина подписи в байтах
        var bytes = Convert.FromBase64String(cert.Signature);

        // Assert — P1363 r||s fixed-length: ровно 64 байта (r и s по 32)
        bytes.Should().HaveCount(64);
    }

    [Fact]
    public void IssuedKey_Matches_CertificatePublicKey()
    {
        // Arrange — серт от CA
        var (caJson, caPrivatePem) = PeerCertificateAuthority.CreateCa("owns3-ca");
        var ca = PeerTrustAnchor.FromJson(caJson);
        var (cert, privatePem) = PeerCertificateAuthority.IssueServer("owns3-n1", ca, caPrivatePem, At);

        // Act — импорт публичного ключа из серта и приватного ключа выпуска
        using var fromCert = ECDsa.Create();
        var imported = () => fromCert.ImportFromPem(cert.PublicKeyPem);
        using var issuedKey = ECDsa.Create();
        issuedKey.ImportFromPem(privatePem);
        var issuedExported = issuedKey.ExportSubjectPublicKeyInfoPem();

        // Assert — PEM серта валиден и совпадает с публичным ключом приватного ключа выпуска
        imported.Should().NotThrow();
        issuedExported.Should().Be(cert.PublicKeyPem);
    }

    [Fact]
    public void ServerKey_SignVerify_P256()
    {
        // Arrange — приватный ключ выпуска и публичный ключ серта
        var (caJson, caPrivatePem) = PeerCertificateAuthority.CreateCa("owns3-ca");
        var ca = PeerTrustAnchor.FromJson(caJson);
        var (cert, privatePem) = PeerCertificateAuthority.IssueServer("owns3-n1", ca, caPrivatePem, At);
        var data = "payload"u8.ToArray();
        using var serverKey = ECDsa.Create();
        serverKey.ImportFromPem(privatePem);
        using var certKey = ECDsa.Create();
        certKey.ImportFromPem(cert.PublicKeyPem);

        // Act — подпись приватным ключом, верификация публичным ключом серта
        var signature = serverKey.SignData(data, HashAlgorithmName.SHA256,
            DSASignatureFormat.IeeeP1363FixedFieldConcatenation);
        var verified = certKey.VerifyData(data, signature, HashAlgorithmName.SHA256,
            DSASignatureFormat.IeeeP1363FixedFieldConcatenation);

        // Assert — пара ключей согласована (ECDSA P-256)
        verified.Should().BeTrue();
    }

    [Fact]
    public void DefaultExpiresDays_Is365()
    {
        // Arrange — выпуск без явного срока
        var (caJson, caPrivatePem) = PeerCertificateAuthority.CreateCa("owns3-ca");
        var ca = PeerTrustAnchor.FromJson(caJson);

        // Act
        var (cert, _) = PeerCertificateAuthority.IssueServer("owns3-n1", ca, caPrivatePem, At);

        // Assert — константа 365; expiresAt = at + 365 дней
        PeerCertificateAuthority.DefaultExpiresDays.Should().Be(365);
        cert.ExpiresAtUnixMs.Should().Be(At.AddDays(365).ToUnixTimeMilliseconds());
    }
}
