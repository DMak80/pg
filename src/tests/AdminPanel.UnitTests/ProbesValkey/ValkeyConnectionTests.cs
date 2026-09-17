using System.Net;
using System.Net.Sockets;
using System.Text;
using AdminPanel.Probes.Valkey;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests.ProbesValkey;

// RESP-миниклиент панели (spec §4.6): AUTH+PING против эфемерного TCP-стаба
// (динамический порт); ошибка/таймаут — Result.Failed, не исключение.
public class ValkeyConnectionTests
{
    // TCP-стаб valkey-ноды: читает AUTH-кадр (7 строк RESP), отвечает сценарием,
    // затем читает PING (3 строки) и отвечает вторым сценарием.
    private sealed class RespStub : IDisposable
    {
        private readonly TcpListener _listener;
        private readonly Task _loop;

        public int Port { get; }

        public string AuthReply { get; set; } = "+OK\r\n";

        public string PingReply { get; set; } = "+PONG\r\n";

        // Принять соединение, но не отвечать (сценарий тишины).
        public bool Silent { get; set; }

        public RespStub()
        {
            _listener = new TcpListener(IPAddress.Loopback, 0);
            _listener.Start();
            Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
            _loop = Task.Run(ServeAsync);
        }

        public void Dispose()
        {
            _listener.Stop();
            try
            {
                _loop.Wait(TimeSpan.FromSeconds(2));
            }
            catch (AggregateException)
            {
                // слушатель закрыт — цикл завершён
            }
        }

        private async Task ServeAsync()
        {
            while (true)
            {
                TcpClient client;
                try
                {
                    client = await _listener.AcceptTcpClientAsync();
                }
                catch (SocketException)
                {
                    break; // listener остановлен
                }

                using (client)
                {
                    if (Silent)
                    {
                        // Приняли и молчим — клиент обязан уйти по таймауту.
                        await Task.Delay(TimeSpan.FromSeconds(5));
                        continue;
                    }

                    var stream = client.GetStream();
                    await ReadFrameAsync(stream, args: 3); // AUTH user password
                    await stream.WriteAsync(Encoding.UTF8.GetBytes(AuthReply));
                    await ReadFrameAsync(stream, args: 1); // PING
                    await stream.WriteAsync(Encoding.UTF8.GetBytes(PingReply));
                }
            }
        }

        // Чтение RESP-массива: строка "*N" + по 2 строки на аргумент.
        private static async Task ReadFrameAsync(NetworkStream stream, int args)
        {
            using var reader = new StreamReader(stream, Encoding.UTF8,
                detectEncodingFromByteOrderMarks: false, bufferSize: 1024, leaveOpen: true);
            for (var i = 0; i < 1 + 2 * args; i++)
                await reader.ReadLineAsync();
        }
    }

    // Свободный порт без слушателя (connection refused).
    private static int DeadPort()
    {
        using var probe = new TcpListener(IPAddress.Loopback, 0);
        probe.Start();
        var port = ((IPEndPoint)probe.LocalEndpoint).Port;
        probe.Stop();
        return port;
    }

    private static ValkeyProbeTarget Target(int port) => new("127.0.0.1", port, "admin", "secret");

    // Arrange: TCP-заглушка, читает AUTH-кадр, отвечает +OK, читает PING, отвечает +PONG.
    // Act: PingAsync. Assert: успех (Live=true).
    [Fact]
    public async Task PingAsync_AuthOkPong_Live()
    {
        using var stub = new RespStub();
        var client = new ValkeyConnection();

        // Act
        var result = await client.PingAsync(Target(stub.Port), TimeSpan.FromSeconds(3), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
    }

    // Arrange: заглушка на AUTH отвечает -WRONGPASS. Act: PingAsync.
    // Assert: Result неуспешен, ошибка содержит "AUTH".
    [Fact]
    public async Task PingAsync_AuthFail_Error()
    {
        using var stub = new RespStub { AuthReply = "-WRONGPASS invalid username-password pair\r\n" };
        var client = new ValkeyConnection();

        // Act
        var result = await client.PingAsync(Target(stub.Port), TimeSpan.FromSeconds(3), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error!.Message.Should().Contain("AUTH").And.Contain("WRONGPASS");
    }

    // Arrange: заглушка принимает соединение и молчит. Act: PingAsync с таймаутом 200 мс.
    // Assert: Result неуспешен (таймаут), не исключение.
    [Fact]
    public async Task PingAsync_Silence_TimesOut()
    {
        using var stub = new RespStub { Silent = true };
        var client = new ValkeyConnection();

        // Act
        var result = await client.PingAsync(Target(stub.Port), TimeSpan.FromMilliseconds(200), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<TimeoutException>();
    }

    // Arrange: порт без слушателя. Act: PingAsync. Assert: Result неуспешен (connection refused).
    [Fact]
    public async Task PingAsync_Refused_Fails()
    {
        var client = new ValkeyConnection();

        // Act
        var result = await client.PingAsync(Target(DeadPort()), TimeSpan.FromSeconds(2), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().NotBeNull();
    }
}
