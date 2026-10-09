using System.Security.Cryptography.X509Certificates;
using System.Text;
using FluentAssertions;
using ValkeyWorker.Core.Valkey;
using ValkeyWorker.Provisioning.Processes;
using Xunit;

namespace ValkeyWorker.UnitTests.Provisioning;

// NodeTlsProvisioner (env-TLS, arch/21 §2): сборка env VALKEY_TLS_{CERT,KEY,CA}
// со свежим сертом ноды и валидность факта по env (идемпотентность по факту).
public class NodeTlsProvisionerTests
{
    private static readonly FixedTimeProvider Clock = new();

    private static (string CaPem, string CaKeyPem) NewCa(string cluster)
        => ValkeyPki.GenerateCa(cluster);

    // AAA: BuildNodeTlsEnv — три ключа env, CA == входному, серт валиден против CA
    [Fact]
    public void BuildNodeTlsEnv_ТриКлючаИВалидныйСерт()
    {
        // Arrange — валидная CA-пара кластера
        var (caPem, caKeyPem) = NewCa("c1");

        // Act
        var env = NodeTlsProvisioner.BuildNodeTlsEnv(caPem, caKeyPem, "node1", "localhost");

        // Assert — набор ключей канона arch/21 §2 и валидность факта
        env.Keys.Should().BeEquivalentTo("VALKEY_TLS_CERT", "VALKEY_TLS_KEY", "VALKEY_TLS_CA");
        env["VALKEY_TLS_CA"].Should().Be(caPem);
        NodeTlsProvisioner.IsValidNodeEnv(env, "localhost", caPem, Clock).Should().BeTrue();
    }

    // AAA: SAN покрывает advertised-хост — DNS и IP формы
    [Theory]
    [InlineData("cache.example.local")]
    [InlineData("127.0.0.1")]
    public void BuildNodeTlsEnv_SanПокрываетAdvertised(string advertised)
    {
        // Arrange
        var (caPem, caKeyPem) = NewCa("c1");

        // Act
        var env = NodeTlsProvisioner.BuildNodeTlsEnv(caPem, caKeyPem, "node1", advertised);

        // Assert — SAN серта покрывает advertised (DNS|IP по правилу §2)
        ValkeyPki.TryParseCertificate(env["VALKEY_TLS_CERT"], out var cert).Should().BeTrue();
        using (var parsed = cert!)
        {
            var san = parsed.Extensions
                .OfType<X509SubjectAlternativeNameExtension>()
                .First();
            if (System.Net.IPAddress.TryParse(advertised, out var ip))
                san.EnumerateIPAddresses().Should().Contain(ip);
            else
                san.EnumerateDnsNames().Should().Contain(advertised);
        }
    }

    // AAA: key↔cert — приватный ключ env соответствует серту (PKCS#8)
    [Fact]
    public void BuildNodeTlsEnv_КлючСоответствуетСерту()
    {
        // Arrange
        var (caPem, caKeyPem) = NewCa("c1");

        // Act
        var env = NodeTlsProvisioner.BuildNodeTlsEnv(caPem, caKeyPem, "node1", "localhost");

        // Assert — публичные части совпадают
        using var key = System.Security.Cryptography.RSA.Create();
        key.ImportFromPem(env["VALKEY_TLS_KEY"]);
        ValkeyPki.TryParseCertificate(env["VALKEY_TLS_CERT"], out var cert).Should().BeTrue();
        using (var parsed = cert!)
        {
            using var certKey = parsed.GetRSAPublicKey();
            key.ExportSubjectPublicKeyInfo().AsSpan()
                .SequenceEqual(certKey!.ExportSubjectPublicKeyInfo()).Should().BeTrue();
        }
    }

    // AAA: PEM в env — многострочный (переносы в значении; \n-форма etcd — наружи)
    [Fact]
    public void BuildNodeTlsEnv_PemМногстрочный()
    {
        // Arrange
        var (caPem, caKeyPem) = NewCa("c1");

        // Act
        var env = NodeTlsProvisioner.BuildNodeTlsEnv(caPem, caKeyPem, "node1", "localhost");

        // Assert — значение env несёт PEM с реальными переносами строк
        env["VALKEY_TLS_CA"].Should().Contain("\n");
        env["VALKEY_TLS_CERT"].Should().Contain("\n-----END CERTIFICATE-----");
    }

    // ── IsValidNodeEnv ──

    // AAA: валидный env → true; null/нет ключа → false
    [Fact]
    public void IsValidNodeEnv_ВалидныйИОтсутствующий()
    {
        // Arrange
        var (caPem, caKeyPem) = NewCa("c1");
        var env = NodeTlsProvisioner.BuildNodeTlsEnv(caPem, caKeyPem, "node1", "localhost");

        // Act / Assert
        NodeTlsProvisioner.IsValidNodeEnv(env, "localhost", caPem, Clock).Should().BeTrue();
        NodeTlsProvisioner.IsValidNodeEnv(null, "localhost", caPem, Clock).Should().BeFalse();
        NodeTlsProvisioner.IsValidNodeEnv(
            new Dictionary<string, string>(), "localhost", caPem, Clock).Should().BeFalse();
        NodeTlsProvisioner.IsValidNodeEnv(
            new Dictionary<string, string> { ["VALKEY_TLS_CA"] = caPem }, "localhost", caPem, Clock)
            .Should().BeFalse(); // нет CERT/KEY
    }

    // AAA: чужой CA в env → false (сверка Trim-сравнением CA)
    [Fact]
    public void IsValidNodeEnv_ЧужойCa_False()
    {
        // Arrange — env от CA#1, ожидание — CA#2
        var (ca1, ca1Key) = NewCa("c1");
        var (ca2, _) = NewCa("c2");
        var env = NodeTlsProvisioner.BuildNodeTlsEnv(ca1, ca1Key, "node1", "localhost");

        // Act / Assert
        NodeTlsProvisioner.IsValidNodeEnv(env, "localhost", ca2, Clock).Should().BeFalse();
    }

    // AAA: истёкший NotAfter → false
    [Fact]
    public void IsValidNodeEnv_ИстёкшийСерт_False()
    {
        // Arrange — серт валиден по «сейчас», но просрочен по будущим часам
        var (caPem, caKeyPem) = NewCa("c1");
        var env = NodeTlsProvisioner.BuildNodeTlsEnv(caPem, caKeyPem, "node1", "localhost");
        var future = new FixedTimeProvider
        {
            Utc = new DateTimeOffset(2046, 1, 1, 0, 0, 0, TimeSpan.Zero), // серт/CA — 10 лет
        };

        // Act / Assert
        NodeTlsProvisioner.IsValidNodeEnv(env, "localhost", caPem, future).Should().BeFalse();
    }

    // AAA: key≠cert → false (неатомарная запись могла оставить чужую пару)
    [Fact]
    public void IsValidNodeEnv_КлючНеСоответствуетСерту_False()
    {
        // Arrange — подмена ключа свежей парой RSA
        var (caPem, caKeyPem) = NewCa("c1");
        var env = NodeTlsProvisioner.BuildNodeTlsEnv(caPem, caKeyPem, "node1", "localhost");
        using var foreign = System.Security.Cryptography.RSA.Create(2048);
        var tampered = new Dictionary<string, string>(env)
        {
            ["VALKEY_TLS_KEY"] = foreign.ExportPkcs8PrivateKeyPem(),
        };

        // Act / Assert
        NodeTlsProvisioner.IsValidNodeEnv(tampered, "localhost", caPem, Clock).Should().BeFalse();
    }

    // AAA: SAN-drift (advertised сменился, серт выпущен под старый) → false
    [Fact]
    public void IsValidNodeEnv_SanDrift_False()
    {
        // Arrange — серт под old.host, сверка с new.host
        var (caPem, caKeyPem) = NewCa("c1");
        var env = NodeTlsProvisioner.BuildNodeTlsEnv(caPem, caKeyPem, "node1", "old.host");

        // Act / Assert
        NodeTlsProvisioner.IsValidNodeEnv(env, "new.host", caPem, Clock).Should().BeFalse();
    }

    // AAA: битый PEM в env → false (перевыпуск, не исключение)
    [Fact]
    public void IsValidNodeEnv_БитыйPem_False()
    {
        // Arrange
        var (caPem, _) = NewCa("c1");
        var env = new Dictionary<string, string>
        {
            ["VALKEY_TLS_CERT"] = "не PEM",
            ["VALKEY_TLS_KEY"] = "не PEM",
            ["VALKEY_TLS_CA"] = caPem,
        };

        // Act / Assert
        NodeTlsProvisioner.IsValidNodeEnv(env, "localhost", caPem, Clock).Should().BeFalse();
    }

    // AAA: \n-нормализация — env с экранированными переносами сводится к валидному
    [Fact]
    public void IsValidNodeEnv_ПереносыСЭкранированием_Валиден()
    {
        // Arrange — etcd-канон «одной строкой с \n» на границе сверки
        var (caPem, caKeyPem) = NewCa("c1");
        var env = NodeTlsProvisioner.BuildNodeTlsEnv(caPem, caKeyPem, "node1", "localhost");
        var flat = env.ToDictionary(
            p => p.Key, p => p.Value.Replace("\n", "\\n", StringComparison.Ordinal));

        // Act / Assert — нормализация \n → переносы на границе сверки
        NodeTlsProvisioner.IsValidNodeEnv(flat, "localhost", caPem, Clock).Should().BeTrue();
    }
}
