using System.Text;
using FluentAssertions;
using ValkeyWorker.Core.Valkey;
using ValkeyWorker.Provisioning.Processes;
using Xunit;

namespace ValkeyWorker.UnitTests.Provisioning;

// NodeTlsProvisioner (t06, arch/21 §2/V3): ensure volume+серт ноды,
// переиспользование валидного факта, перевыпуск при чужом CA/SAN-дрейфе/сроке.
public class NodeTlsProvisionerTests
{
    private static readonly FixedTimeProvider Clock = new();

    private const string Image = "valkey/valkey:9.1.2";

    private static (Fakes.FakeDriver Driver, NodeTlsProvisioner Provisioner) NewRig()
    {
        var driver = new Fakes.FakeDriver();
        return (driver, new NodeTlsProvisioner(driver, Image, Clock));
    }

    [Fact]
    public async Task Ensure_WritesVolumeTar()
    {
        // Arrange — пустой volume, валидная CA-пара
        var (driver, provisioner) = NewRig();
        var (caPem, caKeyPem) = ValkeyPki.GenerateCa("c1");

        // Act
        var result = await provisioner.EnsureNodeTlsAsync(
            "c1", "node1", "h1", "localhost", caPem, caKeyPem, TestContext.Current.CancellationToken);

        // Assert — tar с тремя файлами записан (права файлов — 0644: процесс ноды
        // в образе НЕ root, см. NodeTlsProvisioner; кодирование mode — TarArchiveTests).
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        var tar = driver.TlsVolumes[("c1", "h1")];
        var files = TarArchive.Read(tar);
        files.Keys.Should().BeEquivalentTo("node.crt", "node.key", "ca.pem");
        files["ca.pem"].Should().Equal(Encoding.UTF8.GetBytes(caPem));
    }

    [Fact]
    public async Task Ensure_ValidTar_KeyCertMismatch_Reissues()
    {
        // Arrange — валидный tar, но node.key от ЧУЖОГО серта (неатомарная
        // запись т06-ревью): факт невалиден, обязателен перевыпуск
        var (driver, provisioner) = NewRig();
        var (caPem, caKeyPem) = ValkeyPki.GenerateCa("c1");
        await provisioner.EnsureNodeTlsAsync(
            "c1", "node1", "h1", "localhost", caPem, caKeyPem, TestContext.Current.CancellationToken);
        var before = driver.TlsVolumes[("c1", "h1")];

        // Подмена ключа: свежая пара RSA, не связанная с node.crt
        using var foreignRsa = System.Security.Cryptography.RSA.Create(2048);
        var foreignKeyPem = foreignRsa.ExportPkcs8PrivateKeyPem();
        var files = TarArchive.Read(before);
        var tampered = TarArchive.Build(
        [
            new TarArchive.Entry("node.crt", 0b1_1010_0100, files["node.crt"]),
            new TarArchive.Entry("node.key", 0b1_1010_0100, Encoding.UTF8.GetBytes(foreignKeyPem)),
            new TarArchive.Entry("ca.pem", 0b1_1010_0100, files["ca.pem"]),
        ]);
        driver.TlsVolumes[("c1", "h1")] = tampered;

        // Act
        var result = await provisioner.EnsureNodeTlsAsync(
            "c1", "node1", "h1", "localhost", caPem, caKeyPem, TestContext.Current.CancellationToken);

        // Assert — перевыпуск: tar переписан, ключ соответствует серту
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        driver.TlsVolumes[("c1", "h1")].Should().NotBeSameAs(tampered);
        NodeTlsProvisioner.IsValidTar(
            driver.TlsVolumes[("c1", "h1")], "localhost", caPem, Clock).Should().BeTrue();
    }

    [Fact]
    public async Task Ensure_ValidTar_MatchingKey_Reused()
    {
        // Arrange — валидный tar с СОГЛАСОВАННОЙ парой ключ↔серт
        var (driver, provisioner) = NewRig();
        var (caPem, caKeyPem) = ValkeyPki.GenerateCa("c1");
        await provisioner.EnsureNodeTlsAsync(
            "c1", "node1", "h1", "localhost", caPem, caKeyPem, TestContext.Current.CancellationToken);
        var before = driver.TlsVolumes[("c1", "h1")];

        // Act — проверка валидности согласованном набора
        var valid = NodeTlsProvisioner.IsValidTar(before, "localhost", caPem, Clock);

        // Assert — пара ключ↔серт сходится → факт валиден
        valid.Should().BeTrue();
    }

    [Fact]
    public async Task Ensure_ReusesValidCert()
    {
        // Arrange — первый ensure записал валидный серт
        var (driver, provisioner) = NewRig();
        var (caPem, caKeyPem) = ValkeyPki.GenerateCa("c1");
        await provisioner.EnsureNodeTlsAsync(
            "c1", "node1", "h1", "localhost", caPem, caKeyPem, TestContext.Current.CancellationToken);
        var before = driver.TlsVolumes[("c1", "h1")];

        // Act — второй вызов (re-run/пересоздание контейнера)
        var result = await provisioner.EnsureNodeTlsAsync(
            "c1", "node1", "h1", "localhost", caPem, caKeyPem, TestContext.Current.CancellationToken);

        // Assert — переиспользование: tar не переписан.
        result.IsSuccess.Should().BeTrue();
        driver.TlsVolumes[("c1", "h1")].Should().BeSameAs(before);
    }

    [Fact]
    public async Task Ensure_ReissuesOnForeignCa()
    {
        // Arrange — в volume серты чужого CA, ensure с текущим CA кластера
        var (driver, provisioner) = NewRig();
        var (foreignPem, foreignKey) = ValkeyPki.GenerateCa("foreign");
        await provisioner.EnsureNodeTlsAsync(
            "c1", "node1", "h1", "localhost", foreignPem, foreignKey, TestContext.Current.CancellationToken);
        var (caPem, caKeyPem) = ValkeyPki.GenerateCa("c1");

        // Act
        var result = await provisioner.EnsureNodeTlsAsync(
            "c1", "node1", "h1", "localhost", caPem, caKeyPem, TestContext.Current.CancellationToken);

        // Assert — перезапись: ca.pem в volume == текущий CA, серт подписан им.
        result.IsSuccess.Should().BeTrue();
        var files = TarArchive.Read(driver.TlsVolumes[("c1", "h1")]);
        Encoding.UTF8.GetString(files["ca.pem"]).Should().Be(caPem);
        ValkeyPki.TryParseCertificate(Encoding.UTF8.GetString(files["node.crt"]), out var cert).Should().BeTrue();
        using var caCert = System.Security.Cryptography.X509Certificates.X509Certificate2.CreateFromPem(caPem);
        cert!.Issuer.Should().Be(caCert.Subject);
    }

    [Fact]
    public async Task Ensure_ReissuesOnSanDrift()
    {
        // Arrange — серт в volume выпущен под старый advertised-хост
        var (driver, provisioner) = NewRig();
        var (caPem, caKeyPem) = ValkeyPki.GenerateCa("c1");
        await provisioner.EnsureNodeTlsAsync(
            "c1", "node1", "h1", "old.host", caPem, caKeyPem, TestContext.Current.CancellationToken);
        var before = driver.TlsVolumes[("c1", "h1")];

        // Act — advertised сменился
        var result = await provisioner.EnsureNodeTlsAsync(
            "c1", "node1", "h1", "new.host", caPem, caKeyPem, TestContext.Current.CancellationToken);

        // Assert — перезапись (SAN обязан покрывать новый advertised).
        result.IsSuccess.Should().BeTrue();
        driver.TlsVolumes[("c1", "h1")].Should().NotBeSameAs(before);
        var files = TarArchive.Read(driver.TlsVolumes[("c1", "h1")]);
        ValkeyPki.TryParseCertificate(Encoding.UTF8.GetString(files["node.crt"]), out var cert).Should().BeTrue();
        cert!.Extensions.OfType<System.Security.Cryptography.X509Certificates.X509SubjectAlternativeNameExtension>()
            .First().EnumerateDnsNames().Should().Contain("new.host");
    }
}
