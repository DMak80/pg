using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using AdminPanel.Probes.Kafka;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests.ProbesKafka;

// KafkaClientCache (t11): churn-инцидент 2026-09-02 — «клиент на каждый вызов»
// плодил rd_kafka-инстансы и poll-потоки (~99% ядра на закрытых портах).
// Кэш обязан переиспользовать клиент по (bootstrap, user, password) и
// пересоздавать только при смене кредов/endpoints или Invalidate после фейла.
// Реальные Confluent-клиенты: Build() не ходит по сети — порт берём
// динамически (AGENTS.md: никаких хардкодов хост-портов в тестах).
public class KafkaClientCacheTests : IDisposable
{
    // Динамический порт: никто не слушает → connection refused мгновенно.
    private static int ClosedPort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private readonly KafkaClientCache _cache = new();
    private readonly string _bootstrap = $"127.0.0.1:{ClosedPort()}";

    // Валидный PEM CA: librdkafka парсит ssl.ca.pem при Build() — строка-заглушка
    // роняет создание клиента ещё до сети.
    private static string CaPem()
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            "CN=cache-test-ca", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        using var cert = request.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(1));
        return cert.ExportCertificatePem();
    }

    public void Dispose() => _cache.Dispose();

    [Fact]
    public void SameCreds_AdminReusedAcrossCalls()
    {
        // Arrange: один bootstrap и одни креды.

        // Act: два обращения за клиентом.
        var first = _cache.GetAdmin(_bootstrap, "app", "SecretPassword0123456789", null);
        var second = _cache.GetAdmin(_bootstrap, "app", "SecretPassword0123456789", null);

        // Assert: тот же инстанс — второго нативного клиента не создано.
        second.Should().BeSameAs(first);
        _cache.CreatedClients.Should().Be(1);
    }

    [Fact]
    public void CredsChanged_AdminRecreated()
    {
        // Arrange: смена app_password (ротация кредов кластера).
        var before = _cache.GetAdmin(_bootstrap, "app", "SecretPassword0123456789", null);

        // Act: те же endpoints, новый пароль.
        var after = _cache.GetAdmin(_bootstrap, "app", "RotatedPassword9876543210", null);

        // Assert: другой клиент под новыми кредами (старый не переиспользуется).
        after.Should().NotBeSameAs(before);
        _cache.CreatedClients.Should().Be(2);
    }

    [Fact]
    public void EndpointsChanged_AdminRecreated()
    {
        // Arrange: кластер переехал — другой bootstrap.
        var before = _cache.GetAdmin(_bootstrap, "app", "SecretPassword0123456789", null);

        // Act
        var after = _cache.GetAdmin($"127.0.0.1:{ClosedPort()}", "app", "SecretPassword0123456789", null);

        // Assert
        after.Should().NotBeSameAs(before);
        _cache.CreatedClients.Should().Be(2);
    }

    [Fact]
    public void Invalidate_NextCallBuildsFreshClient()
    {
        // Arrange: фейл пробы инвалидировал закэшированного клиента.
        var before = _cache.GetAdmin(_bootstrap, "app", "SecretPassword0123456789", null);
        _cache.Invalidate(_bootstrap, "app", "SecretPassword0123456789", null);

        // Act
        var after = _cache.GetAdmin(_bootstrap, "app", "SecretPassword0123456789", null);

        // Assert: следующий вызов строит свежий клиент.
        after.Should().NotBeSameAs(before);
        _cache.CreatedClients.Should().Be(2);
    }

    [Fact]
    public void Consumer_CachedIndependentlyFromAdmin()
    {
        // Arrange: admin и consumer — разные нативные клиенты одной пробы.

        // Act
        var admin = _cache.GetAdmin(_bootstrap, "app", "SecretPassword0123456789", null);
        var consumerFirst = _cache.GetConsumer(_bootstrap, "app", "SecretPassword0123456789", null);
        var consumerSecond = _cache.GetConsumer(_bootstrap, "app", "SecretPassword0123456789", null);

        // Assert: consumer переиспользуется и не совпадает с admin-клиентом.
        consumerSecond.Should().BeSameAs(consumerFirst);
        consumerFirst.Should().NotBeSameAs(admin);
        _cache.CreatedClients.Should().Be(2);
    }

    [Fact]
    public void GetAdmin_CacheKeyIncludesCaPem_SameCaReuses_NewCaRecreates()
    {
        // Arrange: два разных CA-PEM (t03: смена CA → пересоздание клиентов).

        // Act: два GetAdmin с CA-A, затем один с CA-B.
        var caA = CaPem();
        var caB = CaPem();
        _cache.GetAdmin(_bootstrap, "admin", "p1", caA);
        _cache.GetAdmin(_bootstrap, "admin", "p1", caA);
        _cache.GetAdmin(_bootstrap, "admin", "p1", caB);

        // Assert: переиспользование при том же CA, пересоздание при смене CA
        // (метрика churn'а CreatedClients — без сетевых подключений).
        _cache.CreatedClients.Should().Be(2);
    }
}
