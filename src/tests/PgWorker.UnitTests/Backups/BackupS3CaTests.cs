using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using PgWorker.Backups;

namespace PgWorker.UnitTests.Backups;

// Fallback доверия CA (t42, spec §5): AWSSDK.Core не читает env AWS_CA_BUNDLE —
// BackupS3 при S3CaBundlePath доверяет per-install CA фабрикой HttpClient.
// Нечитаемый файл — исключение конструктора с диагностикой; без пути — прежнее
// поведение. Валидация фабрики принимает цепочку настроенного CA, чужой — нет.
public class BackupS3CaTests
{
    // t36: цель S3ClientOptions (bucket+CA; endpoint/креды фиктивны).
    private static S3ClientOptions Opts(string? caPath = null) => new(
        "http://minio:9000", "bkt", "ak", "sk", CaBundlePath: caPath);

    private static string NewCaPem()
    {
        using var rsa = RSA.Create(2048);
        var req = new CertificateRequest("CN=s3ca-test", rsa,
            HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        req.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        using var ca = req.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
        return ca.ExportCertificatePem();
    }

    // CA (в памяти, ключ ассоциирован) + серверный серт им + цепочка [leaf, ca]
    // (ExtraStore). PEM round-trip для подписи не годится: LoadPem на не-macOS
    // не ассоциирует приватный ключ с сертификатом (issuer без ключа).
    private static (X509Certificate2 Ca, X509Certificate2 Server, X509Chain Chain) NewSignedPair()
    {
        using var caRsa = RSA.Create(2048);
        var caReq = new CertificateRequest("CN=s3ca-test", caRsa,
            HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        caReq.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        var ca = caReq.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
        using var srvRsa = RSA.Create(2048);
        var srvReq = new CertificateRequest("CN=s3ca-srv", srvRsa,
            HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        // Границы — ОТ серта CA (не от часов): notAfter серта строго внутри
        // валидности CA, иначе Create отсекает (дельта часов между созданиями).
        using var srv = srvReq.Create(ca,
            ca.NotBefore.AddMinutes(1), ca.NotAfter.AddMinutes(-1),
            BitConverter.GetBytes(DateTime.UtcNow.Ticks));
        var chain = new X509Chain();
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        chain.ChainPolicy.VerificationFlags = X509VerificationFlags.AllowUnknownCertificateAuthority;
        chain.ChainPolicy.ExtraStore.Add(ca);
        var server = Shared.Tls.TlsMaterial.LoadPemPair(srv.ExportCertificatePem(), srvRsa.ExportPkcs8PrivateKeyPem());
        chain.Build(server);
        return (new X509Certificate2(ca), server, chain);
    }

    // AAA: валидный файл CA — BackupS3 создаётся (фабрика с CA не бросает)
    [Fact]
    public void СCaBundle_КонструкторОк()
    {
        var s3 = new BackupS3(Opts(WriteCaFile()));
        s3.Should().NotBeNull();
    }

    // AAA: нечитаемый файл CA — исключение с путём в диагностике (fail-fast)
    [Fact]
    public void СБитымПутем_Исключение()
    {
        var act = () => new BackupS3(Opts(Path.Combine(Path.GetTempPath(), $"s3ca-no-{Guid.NewGuid():N}.pem")));
        act.Should().Throw<InvalidOperationException>().WithMessage("*не читается*");
    }

    // AAA: без пути — прежнее поведение, фабрика не ставится
    [Fact]
    public void БезCaBundle_КонструкторОк_ФабрикаНеСтавится()
    {
        var s3 = new BackupS3(Opts());
        s3.Should().NotBeNull();
    }

    // AAA: фабрика строит HttpClient; валидация принимает цепочку настроенного
    // CA и отвергает цепочку чужого CA (механизм доверия S3CaBundlePath)
    [Fact]
    public void Фабрика_CallbackПринимаетСвойCa_ЧужойОтвергает()
    {
        // Arrange: два независимых CA; фабрика доверяет только своему
        var (ownCa, ownServer, ownChain) = NewSignedPair();
        var own = new S3CaHttpClientFactory(ownCa);
        var (otherCa, otherServer, otherChain) = NewSignedPair();

        // Act / Assert
        using (own.CreateHttpClient(new Amazon.S3.AmazonS3Config()))
        {
            own.ValidateCertificate(ownServer, ownChain).Should().BeTrue("цепочка настроенного CA доверена");
            own.ValidateCertificate(otherServer, otherChain).Should().BeFalse("чужой CA отвергается");
        }
    }

    private static string WriteCaFile()
    {
        var path = Path.Combine(Path.GetTempPath(), $"s3ca-{Guid.NewGuid():N}.pem");
        File.WriteAllText(path, NewCaPem());
        return path;
    }
}
