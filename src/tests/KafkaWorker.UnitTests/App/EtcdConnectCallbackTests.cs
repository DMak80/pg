using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using KafkaWorker.App;
using Xunit;

namespace KafkaWorker.UnitTests.App;

// etcd-клиент против DNS-флейпа Docker embedded DNS (t09; spec §3.3, arch/16 §7):
// PooledConnectionLifetime (пере-резолв после пересоздания etcd-контейнера) +
// IPv4-first последовательный резолв (параллельные A/AAAA флейпят).
public class EtcdConnectCallbackTests
{
    [Fact]
    public void OrderIpv4First_Ipv4BeforeIpv6()
    {
        // Arrange: перемешанные адреса.
        var ipv6 = IPAddress.Parse("fd00::1");
        var ipv4 = IPAddress.Parse("10.0.0.2");
        var mixed = new[] { ipv6, ipv4 };

        // Act
        var ordered = EtcdConnectCallback.OrderIpv4First(mixed);

        // Assert: IPv4 — первый попыткой (Docker embedded DNS держит A-записи).
        ordered[0].Should().Be(ipv4);
        ordered[1].Should().Be(ipv6);
    }

    [Fact]
    public void CreateHandler_ConfiguredWithLifetimeAndCallback()
    {
        // Arrange/Act: фабрика handler'а именованного клиента "etcd".
        var handler = EtcdConnectCallback.CreateHandler();

        // Assert: пул пере-резолвится (5 мин — прецедент DockerEngineFactory),
        // резолв — кастомный IPv4-first.
        handler.PooledConnectionLifetime.Should().Be(TimeSpan.FromMinutes(5));
        handler.ConnectCallback.Should().NotBeNull();
    }

    [Fact]
    public async Task Connect_IpLiteral_GoesStraightToConnect_NoDns()
    {
        // Arrange: IP-литерал (spec §6 «IP-литерал — без DNS») — прямой вызов
        // внутренней механики ветки IPAddress.TryParse (публичного конструктора
        // SocketsHttpConnectionContext в TFM нет); порт закрыт (refused).
        // Act
        var act = () => EtcdConnectCallback.ConnectToAddressesAsync(
            [IPAddress.Parse("127.0.0.1")], 1, TestContext.Current.CancellationToken);

        // Assert: отказ SocketException без участия DNS-резолва (литерал идёт
        // в коннект напрямую).
        await act.Should().ThrowAsync<SocketException>();
    }

    [Fact]
    public async Task ConnectToAddressesAsync_AllDead_ThrowsLast()
    {
        // Arrange: несколько мёртвых адресов (незанятые порты localhost — refused).
        var addresses = new[] { IPAddress.Parse("127.0.0.1"), IPAddress.Parse("127.0.0.1") };

        // Act
        var act = () => EtcdConnectCallback.ConnectToAddressesAsync(
            addresses, 1, TestContext.Current.CancellationToken);

        // Assert: бросок последнего отказа (шлюз обернёт в Result.Failed — проба
        // отдаст структуру), а не «тихое» зависание.
        await act.Should().ThrowAsync<SocketException>();
    }
}
