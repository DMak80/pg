using System.Security.Cryptography;
using OwnS3.Protocol.PeerAuth;
using OwnS3.Replication;

namespace OwnS3.UnitTests.Replication;

// Загрузка identity/trust-anchor из env-конфига (t39, глава 07 §2/spec §4.6):
// значение/путь по прецеденту TLS, fail-fast на любом нарушении материала.
public sealed class PeerIdentityLoaderFixture : IDisposable
{
    public string Dir { get; } = Path.Combine(Path.GetTempPath(), "owns3-test-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        // Teardown: temp-каталог удаляется при любом исходе
        if (Directory.Exists(Dir))
            Directory.Delete(Dir, recursive: true);
    }
}

public class PeerIdentityLoaderTests : IDisposable
{
    private readonly PeerIdentityLoaderFixture _fixture = new();

    // Материал инсталляции: CA + сертификат сервера (те же классы, что CLI).
    private static readonly DateTimeOffset IssuedAt = new(2026, 10, 1, 12, 0, 0, TimeSpan.Zero);

    public void Dispose()
    {
        // Изоляция кейсов: temp-каталог на кейс
        if (Directory.Exists(_fixture.Dir))
            Directory.Delete(_fixture.Dir, recursive: true);
    }

    private static (PeerTrustAnchor Ca, PeerCertificate Cert, string PrivatePem) MakeIdentity(
        string name = "owns3-n1", int expiresDays = PeerCertificateAuthority.DefaultExpiresDays,
        DateTimeOffset? at = null)
    {
        var (caJson, caPem) = PeerCertificateAuthority.CreateCa("owns3-ca");
        var ca = PeerTrustAnchor.FromJson(caJson);
        var (cert, pem) = PeerCertificateAuthority.IssueServer(name, ca, caPem, at ?? IssuedAt, expiresDays);
        return (ca, cert, pem);
    }

    private string WriteFile(string fileName, string content)
    {
        Directory.CreateDirectory(_fixture.Dir);
        var path = Path.Combine(_fixture.Dir, fileName);
        File.WriteAllText(path, content);
        return path;
    }

    [Fact]
    public void Load_ValueAndPath_BothWork()
    {
        // Arrange — материал через значения И через файлы (генерация PeerCertificateAuthority)
        var (_, cert, pem) = MakeIdentity();

        // Act — значение
        var byValue = PeerIdentityLoader.Load(new PeerIdentityOptions
        {
            Certificate = cert.ToJson(),
            PrivateKeyPem = pem,
        });

        // Act / Assert — те же материалы через _PATH дают identity
        var certPath = WriteFile("cert.json", cert.ToJson());
        var keyPath = WriteFile("key.pem", pem);
        var byPath = PeerIdentityLoader.Load(new PeerIdentityOptions
        {
            CertificatePath = certPath,
            PrivateKeyPath = keyPath,
        });

        // Assert — оба варианта: имя и ключ совпадают с сертификатом
        byValue.Name.Should().Be(cert.Name);
        byValue.CertificateJson.Should().Be(cert.ToJson());
        byPath.Name.Should().Be(cert.Name);
        byPath.Key.ExportSubjectPublicKeyInfoPem().Should().Be(cert.PublicKeyPem);
        byValue.Key.Dispose();
        byPath.Key.Dispose();
    }

    [Fact]
    public void Load_PartialPair_Throws()
    {
        // Arrange — только сертификат / только ключ
        var (_, cert, pem) = MakeIdentity();

        // Act / Assert — секция с одним сертификатом → InvalidOperationException
        var actCertOnly = () => PeerIdentityLoader.Load(new PeerIdentityOptions
        {
            Certificate = cert.ToJson(),
        });
        actCertOnly.Should().Throw<InvalidOperationException>().WithMessage("*OWNS3_REPLICATION_*");

        // Act / Assert — секция с одним ключом → InvalidOperationException
        var actKeyOnly = () => PeerIdentityLoader.Load(new PeerIdentityOptions
        {
            PrivateKeyPem = pem,
        });
        actKeyOnly.Should().Throw<InvalidOperationException>().WithMessage("*OWNS3_REPLICATION_*");
    }

    [Fact]
    public void Load_KeyMismatchesCertificate_Throws()
    {
        // Arrange — сертификат n1 и ключ другой пары
        var (_, cert, _) = MakeIdentity("owns3-n1");
        var (_, _, otherPem) = MakeIdentity("owns3-n2");

        // Act
        var act = () => PeerIdentityLoader.Load(new PeerIdentityOptions
        {
            Certificate = cert.ToJson(),
            PrivateKeyPem = otherPem,
        });

        // Assert — fail-fast: ключ не соответствует сертификату
        act.Should().Throw<InvalidOperationException>().WithMessage("*не соответствует*");
    }

    [Fact]
    public void Load_ExpiredCertificate_Throws()
    {
        // Arrange — сертификат со сроком 1 день, выпущен 2 дня назад (уже истёк)
        var yesterday = DateTimeOffset.UtcNow.AddDays(-2);
        var (_, cert, pem) = MakeIdentity(at: yesterday, expiresDays: 1);

        // Act
        var act = () => PeerIdentityLoader.Load(new PeerIdentityOptions
        {
            Certificate = cert.ToJson(),
            PrivateKeyPem = pem,
        });

        // Assert — fail-fast: истёкший expiresAt (глава 07 §2)
        act.Should().Throw<InvalidOperationException>().WithMessage("*истёк*");
    }

    [Fact]
    public void Load_MalformedJson_Throws()
    {
        // Arrange — мусор в Certificate
        var (_, _, pem) = MakeIdentity();

        // Act
        var act = () => PeerIdentityLoader.Load(new PeerIdentityOptions
        {
            Certificate = "{not-json",
            PrivateKeyPem = pem,
        });

        // Assert
        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void Load_NeitherGiven_Throws()
    {
        // Arrange — секция задана, материала нет (null-секция — НЕ этот случай:
        // её обрабатывает Program, identity отсутствует — не ошибка)

        // Act
        var act = () => PeerIdentityLoader.Load(new PeerIdentityOptions());

        // Assert
        act.Should().Throw<InvalidOperationException>().WithMessage("*материала нет*");
    }

    [Fact]
    public void LoadTrustAnchor_NotSet_Null()
    {
        // Arrange — секция не задана / задана пустыми полями

        // Act / Assert
        PeerIdentityLoader.LoadTrustAnchor(null).Should().BeNull();
        PeerIdentityLoader.LoadTrustAnchor(new PeerTrustCaOptions()).Should().BeNull();
    }

    [Fact]
    public void LoadTrustAnchor_Value_Parses()
    {
        // Arrange — trust-anchor CA (тот же, что выпустил сертификат)
        var (ca, _, _) = MakeIdentity();

        // Act
        var trust = PeerIdentityLoader.LoadTrustAnchor(new PeerTrustCaOptions
        {
            Certificate = ca.ToJson(),
        });

        // Assert
        trust.Should().NotBeNull();
        trust!.Name.Should().Be(ca.Name);
        trust.PublicKeyPem.Should().Be(ca.PublicKeyPem);
    }

    [Fact]
    public void Load_NameFromCertificate()
    {
        // Arrange — материал с именем из сертификата
        var (_, cert, pem) = MakeIdentity("owns3-n7");

        // Act
        var identity = PeerIdentityLoader.Load(new PeerIdentityOptions
        {
            Certificate = cert.ToJson(),
            PrivateKeyPem = pem,
        });

        // Assert — Name из сертификата; CertificateJson — исходный JSON
        identity.Name.Should().Be("owns3-n7");
        identity.Name.Should().Be(identity.Certificate.Name);
        identity.CertificateJson.Should().Be(cert.ToJson());
        identity.Key.Dispose();
    }
}
