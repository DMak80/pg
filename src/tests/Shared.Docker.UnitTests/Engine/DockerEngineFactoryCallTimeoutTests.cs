using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Shared.Docker;
using Xunit;

namespace Shared.Docker.UnitTests.Engine;

// Пер-попыточный таймаут docker-транспорта (arch/14 §6, arch/15 §4): константа
// канона «полокна проверки watchdog» и проброс таймаута HttpClient в вызовы
// движка — зависший docker-демон даёт transient-фейл, а не молчание до 100 с
// (окно гонки довыделения против TTL 15 с глобального portalloc-лока).
public class DockerEngineFactoryCallTimeoutTests
{
    [Fact]
    public void CallTimeout_CanonValue_75Seconds()
    {
        // Arrange / Act / Assert: канон — половина окна проверки watchdog (30 с),
        // симметрия etcd/patroni (7.5 с); защита от невнимательной правки.
        DockerEngineFactory.CallTimeout.Should().Be(TimeSpan.FromSeconds(7.5));
    }

    [Fact]
    public async Task Create_SilentServer_PingFailsByTransportTimeout()
    {
        // Arrange: молчащий TCP-сервер (принимает соединения, не отвечает) на
        // динамическом порту; движок с таймаутом 300 мс через internal-перегрузку.
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            var port = ((IPEndPoint)listener.LocalEndpoint).Port;
            var factory = new DockerEngineFactory();
            await using var engine = factory.Create(
                $"tcp://127.0.0.1:{port}", "h", TimeSpan.FromMilliseconds(300));
            var sw = Stopwatch.StartNew();

            // Act: одиночный вызов движка против молчащего сервера.
            var result = await engine.PingAsync(CancellationToken.None);
            sw.Stop();

            // Assert: таймаут транспорта пробрасывается в вызов — Failed быстро,
            // а не молчание до дефолтных 100 с HttpClient.
            result.IsSuccess.Should().BeFalse();
            sw.Elapsed.Should().BeLessThanOrEqualTo(TimeSpan.FromSeconds(3));
        }
        finally
        {
            // Teardown: остановить listener при любом исходе.
            listener.Stop();
        }
    }
}
