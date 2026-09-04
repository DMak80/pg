using Confluent.Kafka;
using KafkaWorker.Provisioning.Kafka;
using KafkaWorker.UnitTests.Provisioning;

namespace KafkaWorker.UnitTests.Kafka;

// Кэш AdminClient'ов (t05, spec §3.1): sharable-фабрика per
// (bootstrap,user,password) — «клиент на тик» давал churn rd_kafka-инстансов
// и 100% ядра на лежащем кластере (инцидент as-kafkaworker 2026-09-04).
// Адаптер строит нативный клиент лениво (первая операция) — юниты кэша
// работают без сети.
public class KafkaAdminClientFactoryTests
{
    // AAA: reuse по ключу — тот же адаптер, счётчик не растёт.
    [Fact]
    public void Create_SameKey_ReturnsSameClient()
    {
        // Arrange: фабрика без натива (клиент ленивый).
        var factory = new KafkaAdminClientFactory(TimeSpan.FromSeconds(3));

        // Act: два Create по одному ключу.
        var first = factory.Create("h:9092", "app", "pw", caPem: null);
        var second = factory.Create("h:9092", "app", "pw", caPem: null);

        // Assert: тот же адаптер, клиент создан один.
        second.Should().BeSameAs(first);
        factory.CreatedClients.Should().Be(1);
    }

    // AAA: смена endpoints/кредов = другой ключ = другой клиент.
    [Fact]
    public void Create_DifferentCredentials_DifferentClient()
    {
        // Arrange: фабрика.
        var factory = new KafkaAdminClientFactory(TimeSpan.FromSeconds(3));

        // Act: три разных ключа.
        var a = factory.Create("h:9092", "app", "pw1", caPem: null);
        var b = factory.Create("h:9092", "app", "pw2", caPem: null);
        var c = factory.Create("h2:9092", "app", "pw1", caPem: null);

        // Assert: каждый ключ — свой клиент.
        b.Should().NotBeSameAs(a);
        c.Should().NotBeSameAs(a);
        factory.CreatedClients.Should().Be(3);
    }

    // AAA: пины librdkafka в конфиге (reconnect-шторм дефолтных 100 мс)
    // + t03-канон соединения: SASL_SSL, per-cluster CA — только при caPem.
    [Fact]
    public void BaseAdminConfig_PinsLibrdkafkaBackoffs()
    {
        // Act: конфиг фабрики (без CA и с CA).
        var config = KafkaAdminClientFactory.BaseAdminConfig("h:9092", "app", "pw", caPem: null);
        var withCa = KafkaAdminClientFactory.BaseAdminConfig("h:9092", "app", "pw", "-----BEGIN CERTIFICATE-----");

        // Assert: bootstrap + пины backoff ≥1000 мс; SASL_SSL всегда;
        // ssl.ca.pem ставится ровно при заданном caPem.
        config.BootstrapServers.Should().Be("h:9092");
        config.RetryBackoffMs.Should().Be(1000);
        config.ReconnectBackoffMs.Should().Be(1000);
        config.ReconnectBackoffMaxMs.Should().Be(10000);
        config.SecurityProtocol.Should().Be(SecurityProtocol.SaslSsl);
        config.Get("ssl.ca.pem").Should().BeNull();
        withCa.Get("ssl.ca.pem").Should().Be("-----BEGIN CERTIFICATE-----");
    }

    // AAA: caPem — часть ключа кэша (t03×t05): клиенты кластеров с разным
    // per-cluster CA не шарятся (разный TLS-контекст нативного клиента).
    [Fact]
    public void Create_DifferentCaPem_DifferentClient()
    {
        // Arrange: фабрика.
        var factory = new KafkaAdminClientFactory(TimeSpan.FromSeconds(3));

        // Act: один bootstrap/креды, разные CA.
        var a = factory.Create("h:9092", "app", "pw", caPem: null);
        var b = factory.Create("h:9092", "app", "pw", caPem: "-----BEGIN CERTIFICATE-----");

        // Assert: другой CA — другой клиент.
        b.Should().NotBeSameAs(a);
        factory.CreatedClients.Should().Be(2);
    }

    // AAA: вытеснение по неактивности (FixedTimeProvider — сдвиг на 11 мин).
    [Fact]
    public void Create_EvictsIdleEntries()
    {
        // Arrange: фабрика на управляемых часах.
        var clock = new FixedTimeProvider();
        var factory = new KafkaAdminClientFactory(TimeSpan.FromSeconds(3), clock: clock);
        var first = factory.Create("h:9092", "app", "pw", caPem: null);

        // Act: время дальше окна неактивности + повторный Create.
        clock.Utc += KafkaAdminClientFactory.IdleEvictAfter + TimeSpan.FromMinutes(1);
        var second = factory.Create("h:9092", "app", "pw", caPem: null);

        // Assert: запись вытеснена — построен новый клиент.
        second.Should().NotBeSameAs(first);
        factory.CreatedClients.Should().Be(2);
    }

    // AAA: активный ключ не вытесняется (LastUsed обновляется на Create).
    [Fact]
    public void Create_ActiveKey_NotEvicted()
    {
        // Arrange: фабрика на управляемых часах.
        var clock = new FixedTimeProvider();
        var factory = new KafkaAdminClientFactory(TimeSpan.FromSeconds(3), clock: clock);
        var first = factory.Create("h:9092", "app", "pw", caPem: null);

        // Act: два промежуточных тика внутри окна неактивности.
        clock.Utc += TimeSpan.FromMinutes(5);
        factory.Create("h:9092", "app", "pw", caPem: null);
        clock.Utc += TimeSpan.FromMinutes(5);
        var second = factory.Create("h:9092", "app", "pw", caPem: null);

        // Assert: ключ жив — клиент тот же, пересозданий нет.
        second.Should().BeSameAs(first);
        factory.CreatedClients.Should().Be(1);
    }

    // AAA: Failed операции помечает запись Unhealthy — следующий Create
    // отдаёт свежий клиент (internal NotifyFailed — тот же путь, что зовёт
    // RunAsync при исключении; сети/натива нет — клиент ленивый).
    [Fact]
    public void Create_AfterFailure_CreatesFreshClient()
    {
        // Arrange: клиент в кэше.
        var factory = new KafkaAdminClientFactory(TimeSpan.FromSeconds(3));
        var first = factory.Create("h:9092", "app", "pw", caPem: null);

        // Act: помечаем операцию Failed (путь RunAsync при исключении).
        ((KafkaAdminClient)first).NotifyFailed();
        var second = factory.Create("h:9092", "app", "pw", caPem: null);

        // Assert: unhealthy-запись заменена свежим клиентом.
        second.Should().NotBeSameAs(first);
        factory.CreatedClients.Should().Be(2);
    }

    // AAA: отмена host'а не инвалидирует (IsHostCancellation — условие
    // пометки в RunAsync): OCE при отменённом токене — да; OCE без отмены
    // (чужой cancellation) и обычные исключения — нет.
    [Theory]
    [InlineData(true, true)]   // OCE + ct.IsCancellationRequested → не фейл-пометка
    [InlineData(false, false)] // OCE без отмены → фейл-пометка
    public void IsHostCancellation_Classifies(bool cancelled, bool expected)
    {
        // Arrange: токен в состоянии отмены/без.
        using var cts = new CancellationTokenSource();
        if (cancelled)
            cts.Cancel();

        // Act + Assert: классификация отмены host'а.
        KafkaAdminClient.IsHostCancellation(new OperationCanceledException(), cts.Token)
            .Should().Be(expected);
        KafkaAdminClient.IsHostCancellation(new ApplicationException("down"), cts.Token)
            .Should().BeFalse();
    }

    // AAA: Dispose (shutdown) детерминированно вычищает кэш — повторный
    // Create строит новый клиент (CreatedClients +1), старый Disposeится.
    [Fact]
    public void Dispose_ThenCreate_BuildsFreshClient()
    {
        // Arrange: клиент в кэше.
        var factory = new KafkaAdminClientFactory(TimeSpan.FromSeconds(3));
        var first = factory.Create("h:9092", "app", "pw", caPem: null);

        // Act: shutdown фабрики + повторный Create.
        factory.Dispose();
        var second = factory.Create("h:9092", "app", "pw", caPem: null);

        // Assert: кэш пуст — построен свежий клиент.
        second.Should().NotBeSameAs(first);
        factory.CreatedClients.Should().Be(2);
    }
}
