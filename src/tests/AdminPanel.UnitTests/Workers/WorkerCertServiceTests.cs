using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using AdminPanel.Core;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Client;
using AdminPanel.Etcd.Workers;
using FluentAssertions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.UnitTests.Workers;

// Валидатор «серт не затрагивает исходящие» (spec §4.3) и генератор
// self-signed листа (spec §3.3 п.1): все строки таблицы §4.3 + свойства
// сгенерированного серта (SAN/EKU/CA=false/срок 825 дней).
public class WorkerCertServiceTests
{
    private static readonly string[] NoEndpoints = ["http://dead:1"];

    private sealed class StubSecrets : IKafkaSecretsStore
    {
        public IReadOnlyDictionary<string, KafkaClusterSecrets> Current { get; set; } =
            new Dictionary<string, KafkaClusterSecrets>();
        public void Replace(IReadOnlyDictionary<string, KafkaClusterSecrets> secrets) => Current = secrets;
    }

    // Заглушка шлюза: запись в юнитах не вызывается (тестируем валидатор/
    // генератор и гвард worker ДО etcd-обращения; успех означал бы,
    // что гвард не сработал).
    private sealed class DeadGateway : IEtcdGateway
    {
        public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<Kv>>.Failed(new EtcdUnreachableException("dead")));
        public Task<Result<EtcdStatusPayload>> StatusAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<EtcdStatusPayload>.Failed(new EtcdUnreachableException("dead")));
        public Task<Result<IReadOnlyList<EtcdMember>>> MemberListAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<EtcdMember>>.Failed(new EtcdUnreachableException("dead")));
        public Task<Result<IReadOnlyList<EtcdAlarm>>> AlarmAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<EtcdAlarm>>.Failed(new EtcdUnreachableException("dead")));
        public Task<Result<TxnResult>> TxnAsync(string endpoint, IReadOnlyList<TxnCompare> compares, IReadOnlyList<KvPut> puts, CancellationToken ct)
            => Task.FromResult(Result<TxnResult>.Failed(new EtcdUnreachableException("dead")));
        public Task<Result> PutAsync(string endpoint, string key, string value, CancellationToken ct)
            => Task.FromResult(Result.Failed(new EtcdUnreachableException("dead")));
        public Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
            => Task.FromResult(Result.Failed(new EtcdUnreachableException("dead")));
    }

    private static WorkerCertService Service(StubSecrets? secrets = null, WorkerTlsOptions? tls = null)
        => new(
            new DeadGateway(), // запись в юнитах не нужна: тестируем валидатор/генератор
            Options.Create(new EtcdOptions { Endpoints = NoEndpoints }),
            secrets ?? new StubSecrets(),
            Options.Create(new WorkerApiOptions { WorkerTls = tls ?? new WorkerTlsOptions() }),
            // Реальное время: серты TestPki выпускаются от UtcNow — фиксированный
            // TimeProvider уронил бы правило 5 (NotBefore в будущем) раньше целевого.
            TimeProvider.System);

    // ===== Таблица §4.3 =====

    [Fact]
    public void Validate_ValidLeaf_Passes()
    {
        // Arrange: лист с SAN и EKU serverAuth
        var (cert, key) = TestPki.Issue(eku: [TestPki.ServerAuth], san: true);

        // Act / Assert: не кидает, метаданные на месте
        var meta = Service().ValidateAndBuildMeta("pgworker", cert, key);
        meta.Thumbprint.Should().HaveLength(64);
    }

    [Fact]
    public void Validate_BrokenPair_Invalid400()
    {
        // Arrange: правило 4 — ключ не соответствует серту
        var (cert, _) = TestPki.Issue(eku: [TestPki.ServerAuth], san: true);
        var (_, otherKey) = TestPki.Issue(eku: [TestPki.ServerAuth], san: true);

        // Act / Assert
        Assert.Throws<WorkerCertInvalidException>(() => Service().ValidateAndBuildMeta("pgworker", cert, otherKey));
    }

    [Fact]
    public void Validate_Expired_Invalid400()
    {
        // Arrange: правило 5 — NotAfter в прошлом
        var (cert, key) = TestPki.Issue(eku: [TestPki.ServerAuth], san: true, notAfter: DateTimeOffset.UtcNow.AddDays(-1));

        // Act / Assert
        Assert.Throws<WorkerCertInvalidException>(() => Service().ValidateAndBuildMeta("pgworker", cert, key));
    }

    [Fact]
    public void Validate_NoSan_Invalid400()
    {
        // Arrange: правило 6 — ни одного SAN
        var (cert, key) = TestPki.Issue(eku: [TestPki.ServerAuth], san: false);

        // Act / Assert
        Assert.Throws<WorkerCertInvalidException>(() => Service().ValidateAndBuildMeta("pgworker", cert, key));
    }

    [Fact]
    public void Validate_CaTrue_AffectsOutgoing422()
    {
        // Arrange: правило 1 — CA-серт как trust anchor исходящих
        var (ca, caKey) = TestPki.GenerateCa();

        // Act / Assert: 422 с явной формулировкой
        var ex = Assert.Throws<WorkerCertAffectsOutgoingException>(
            () => Service().ValidateAndBuildMeta("pgworker", ca, caKey));
        ex.Message.Should().Contain("подчинёнными сервисами");
    }

    [Fact]
    public void Validate_ClientAuthEku_AffectsOutgoing422()
    {
        // Arrange: правило 2 — EKU clientAuth пригоден в исходящих
        var (cert, key) = TestPki.Issue(eku: [TestPki.ServerAuth, TestPki.ClientAuth], san: true);

        // Act / Assert
        Assert.Throws<WorkerCertAffectsOutgoingException>(() => Service().ValidateAndBuildMeta("pgworker", cert, key));
    }

    [Fact]
    public void Validate_EkuWithoutServerAuth_AffectsOutgoing422()
    {
        // Arrange: правило 2 — EKU задан, serverAuth нет
        var (cert, key) = TestPki.Issue(eku: [TestPki.CodeSigning], san: true);

        // Act / Assert
        Assert.Throws<WorkerCertAffectsOutgoingException>(() => Service().ValidateAndBuildMeta("pgworker", cert, key));
    }

    [Fact]
    public void Validate_ThumbprintMatchesKafkaCa_AffectsOutgoing422()
    {
        // Arrange: правило 3 — серт = per-cluster CA kafka-кластера
        var (ca, caKey) = TestPki.GenerateCa();
        var secrets = new StubSecrets
        {
            Current = new Dictionary<string, KafkaClusterSecrets>
            { ["c1"] = new("c1", "u", "p", ca) },
        };

        // Act / Assert
        Assert.Throws<WorkerCertAffectsOutgoingException>(() => Service(secrets).ValidateAndBuildMeta("pgworker", ca, caKey));
    }

    [Fact]
    public void Validate_ThumbprintMatchesPanelServerCa_AffectsOutgoing422()
    {
        // Arrange: правило 3 — кандидат совпадает по fingerprint с ServerCa панели
        var (ca, caKey) = TestPki.GenerateCa();
        var tls = new WorkerTlsOptions { ServerCaPem = ca };
        var (leaf, leafKey) = TestPki.Issue(eku: [TestPki.ServerAuth], san: true);

        // Act / Assert: CA-кандидат (thumbprint == ServerCa) — отказ
        Assert.Throws<WorkerCertAffectsOutgoingException>(
            () => Service(tls: tls).ValidateAndBuildMeta("pgworker", ca, caKey));
        // sanity: обычный лист при том же конфиге проходит
        Service(tls: tls).ValidateAndBuildMeta("pgworker", leaf, leafKey);
    }

    [Fact]
    public void Validate_KafkaCaBundle_AllPiecesChecked()
    {
        // Arrange: ca_pem в окне ротации — бандл OLD+NEW (arch/15 §2.1);
        // валидируем NEW-пару бандла — она тоже известный материал.
        var (oldCa, _) = TestPki.GenerateCa();
        var (newCa, newKey) = TestPki.GenerateCa();
        var bundle = oldCa + "\n" + newCa;
        var secrets = new StubSecrets
        {
            Current = new Dictionary<string, KafkaClusterSecrets> { ["c1"] = new("c1", "u", "p", bundle) },
        };

        // Act / Assert: NEW-часть бандла тоже опознаётся как известный материал
        Assert.Throws<WorkerCertAffectsOutgoingException>(
            () => Service(secrets).ValidateAndBuildMeta("pgworker", newCa, newKey));
    }

    // ===== Генератор (spec §3.3 п.1) =====

    [Fact]
    public void Generate_SelfSignedLeaf_Properties()
    {
        // Act
        var (certPem, keyPem, meta) = Service().Generate("pgworker", ["worker1.local"]);

        // Assert: SAN = advertise + localhost/127.0.0.1; EKU только serverAuth;
        // CA=false; срок 825 дней; NotBefore ≤ now (сдвиг −5 мин)
        meta.San.Should().Contain("worker1.local").And.Contain("localhost").And.Contain("127.0.0.1");
        using var cert = X509Certificate2.CreateFromPem(certPem, keyPem);
        var eku = cert.Extensions.OfType<X509EnhancedKeyUsageExtension>().Single();
        eku.EnhancedKeyUsages.Cast<Oid>().Select(o => o.Value)
            .Should().BeEquivalentTo(["1.3.6.1.5.5.7.3.1"]);
        cert.Extensions.OfType<X509BasicConstraintsExtension>().Single().CertificateAuthority.Should().BeFalse();
        (cert.NotAfter - cert.NotBefore).Days.Should().BeInRange(820, 830);
// Kind у NotBefore зависит от рантайма — конвертируем с локальным offset.
        new DateTimeOffset(cert.NotBefore, TimeZoneInfo.Local.GetUtcOffset(cert.NotBefore))
            .Should().BeBefore(DateTimeOffset.UtcNow);
        meta.Thumbprint.Should().HaveLength(64);
    }

    [Fact]
    public void Generate_EmptyHosts_LocalhostOnly()
    {
        // Arrange: живых инстансов нет → SAN localhost/127.0.0.1 (spec §4.1 п.1)
        // Act / Assert
        var (_, _, meta) = Service().Generate("pgworker", []);
        meta.San.Should().BeEquivalentTo(["localhost", "127.0.0.1"]);
    }

    [Fact]
    public void Generate_PassesValidation_SelfCheck()
    {
        // Arrange: сгенерированный серт обязан проходить §4.3 (самоконтроль)
        var (certPem, keyPem, _) = Service().Generate("kafkaworker", ["b1"]);

        // Act / Assert: не кидает
        Service().ValidateAndBuildMeta("kafkaworker", certPem, keyPem);
    }

    // ===== Гвард worker (spec §3.3 п.4: 404 до любой записи) =====

    [Fact]
    public async Task Write_UnknownWorker_RejectedWithoutEtcdCall()
    {
        // Arrange: worker вне pgworker|kafkaworker; шлюз DeadGateway — успех
        // возможен только если гвард сработал ДО etcd-обращения
        // Act / Assert: все три write-метода — WorkerNotFoundException
        var put = await Service().PutAsync("foo", "x", "y", "admin", CancellationToken.None);
        put.Error.Should().BeOfType<WorkerNotFoundException>();
        var gen = await Service().GenerateAndPutAsync("foo", [], "admin", CancellationToken.None);
        gen.Error.Should().BeOfType<WorkerNotFoundException>();
        var del = await Service().DeleteAsync("foo", CancellationToken.None);
        del.Error.Should().BeOfType<WorkerNotFoundException>();
    }

    // ===== PKI-хелпер файла (параметры: EKU-набор, CA=TRUE, SAN вкл/выкл, срок) =====

    private static class TestPki
    {
        public const string ServerAuth = "1.3.6.1.5.5.7.3.1";
        public const string ClientAuth = "1.3.6.1.5.5.7.3.2";
        public const string CodeSigning = "1.3.6.1.5.5.7.3.3";

        public static (string CaPem, string CaKeyPem) GenerateCa()
        {
            using var rsa = RSA.Create(2048);
            var request = new CertificateRequest(
                "CN=test-ca", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
            using var ca = request.CreateSelfSigned(
                DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(10));
            return (ca.ExportCertificatePem(), ca.GetRSAPrivateKey()!.ExportPkcs8PrivateKeyPem());
        }

        // Лист: CA-серт или self-signed лист с параметрами под кейс таблицы §4.3.
        public static (string CertPem, string KeyPem) Issue(
            string[]? eku = null, bool san = true, DateTimeOffset? notAfter = null, bool ca = false)
        {
            using var key = RSA.Create(2048);
            var request = new CertificateRequest(
                "CN=test-leaf", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            if (ca)
                request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
            else
                request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, true));
            if (eku is { Length: > 0 })
            {
                var oids = new OidCollection();
                foreach (var o in eku)
                    oids.Add(new Oid(o));
                request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(oids, critical: false));
            }
            if (san)
            {
                var sanBuilder = new SubjectAlternativeNameBuilder();
                sanBuilder.AddDnsName("test-leaf");
                sanBuilder.AddIpAddress(System.Net.IPAddress.Loopback);
                request.CertificateExtensions.Add(sanBuilder.Build());
            }

            // notBefore строго раньше notAfter (кейс expired: notAfter в прошлом).
            var notBefore = notAfter is { } na && na < DateTimeOffset.UtcNow
                ? na.AddDays(-1)
                : DateTimeOffset.UtcNow.AddDays(-1);
            using var cert = request.CreateSelfSigned(notBefore, notAfter ?? DateTimeOffset.UtcNow.AddDays(30));
            return (cert.ExportCertificatePem(), key.ExportPkcs8PrivateKeyPem());
        }
    }
}
