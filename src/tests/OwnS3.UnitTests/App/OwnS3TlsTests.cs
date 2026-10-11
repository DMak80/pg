using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using FluentAssertions;
using OwnS3.App;

namespace OwnS3.UnitTests.App;

// Резолв TLS-материала ownS3 (t42, arch/owns3/05 §4): пусто — HTTP (null);
// валидная пара — сертификат; неполная пара/нет файла/битый PEM — исключение
// (fail-fast старта). PEM приоритетнее _PATH — образец RestTlsMaterial.
public class OwnS3TlsTests
{
    private static (string CertPem, string KeyPem) NewPair()
    {
        using var rsa = RSA.Create(2048);
        var req = new CertificateRequest("CN=os3-tls-test", rsa,
            HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        using var cert = req.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
        return (cert.ExportCertificatePem(), rsa.ExportPkcs8PrivateKeyPem());
    }

    // AAA: материал не задан — HTTP-режим (null), поведение прежнее
    [Fact]
    public void Empty_ReturnsNull_HttpMode()
    {
        OwnS3Tls.Resolve(new TlsOptions()).Should().BeNull();
    }

    // AAA: валидная PEM-пара — сертификат с приватным ключом
    [Fact]
    public void ValidPemPair_ReturnsCertificate()
    {
        var (certPem, keyPem) = NewPair();
        var cert = OwnS3Tls.Resolve(new TlsOptions { ServerCertPem = certPem, ServerKeyPem = keyPem });
        cert.Should().NotBeNull();
        cert!.HasPrivateKey.Should().BeTrue();
    }

    // AAA: только сертификат (значением) — отказ
    [Fact]
    public void OnlyCert_Throws()
    {
        var (certPem, _) = NewPair();
        var act = () => OwnS3Tls.Resolve(new TlsOptions { ServerCertPem = certPem });
        act.Should().Throw<InvalidOperationException>().WithMessage("*OWNS3_TLS*");
    }

    // AAA: только ключ (путём) — отказ
    [Fact]
    public void OnlyKeyPath_Throws()
    {
        var act = () => OwnS3Tls.Resolve(new TlsOptions { ServerKeyPath = "/tmp/no-matter.pem" });
        act.Should().Throw<InvalidOperationException>();
    }

    // AAA: путь задан, файла нет — отказ (не «молча на HTTP»)
    [Fact]
    public void MissingCertFile_Throws()
    {
        var (_, keyPem) = NewPair();
        var act = () => OwnS3Tls.Resolve(new TlsOptions
        {
            ServerCertPath = Path.Combine(Path.GetTempPath(), $"os3-no-{Guid.NewGuid():N}.crt"),
            ServerKeyPem = keyPem,
        });
        act.Should().Throw<InvalidOperationException>().WithMessage("*не читается*");
    }

    // AAA: пара через _PATH-файлы читается
    [Fact]
    public void PathPair_ReadsFiles()
    {
        var (certPem, keyPem) = NewPair();
        var dir = Path.Combine(Path.GetTempPath(), $"os3-tls-{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);
        File.WriteAllText(Path.Combine(dir, "s.crt"), certPem);
        File.WriteAllText(Path.Combine(dir, "s.key"), keyPem);
        OwnS3Tls.Resolve(new TlsOptions
        {
            ServerCertPath = Path.Combine(dir, "s.crt"),
            ServerKeyPath = Path.Combine(dir, "s.key"),
        }).Should().NotBeNull();
    }

    // AAA: PEM-значение приоритетнее _PATH (образец RestTlsMaterial воркеров)
    [Fact]
    public void PemTakesPriorityOverPath()
    {
        var (certPem, keyPem) = NewPair();
        var (_, otherKey) = NewPair(); // «путевой» ключ не подходит к серту
        var dir = Path.Combine(Path.GetTempPath(), $"os3-tls-{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);
        File.WriteAllText(Path.Combine(dir, "k.key"), otherKey);
        OwnS3Tls.Resolve(new TlsOptions
        {
            ServerCertPem = certPem, ServerKeyPem = keyPem, ServerKeyPath = Path.Combine(dir, "k.key"),
        }).Should().NotBeNull("PEM-значения побеждают — путевой ключ не читается");
    }

    // AAA: битый PEM — отказ с диагностикой
    [Fact]
    public void GarbagePem_Throws()
    {
        var act = () => OwnS3Tls.Resolve(new TlsOptions
        {
            ServerCertPem = "not a pem", ServerKeyPem = "not a key",
        });
        act.Should().Throw<InvalidOperationException>().WithMessage("*PEM*");
    }
}
