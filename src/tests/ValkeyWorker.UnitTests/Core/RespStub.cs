using System.Net;
using System.Net.Sockets;
using System.Text;

namespace ValkeyWorker.UnitTests.Core;

// Заглушка RESP-сервера на эфемерном порту (127.0.0.1:0 — литералов нет):
// на каждое соединение читает запросы клиента (RESP-массивы) и на КАЖДУЮ
// команду отвечает очередным кадром из очереди; входящие байты копятся
// для ассертов фрейминга. Chunked-кадр пишется в два захода с паузой —
// клиент обязан дочитать. Клиент молчит (Silent) — таймаут-тест.
internal sealed class RespStub : IAsyncDisposable
{
    private readonly TcpListener _listener;
    private readonly Task _serve;
    private readonly CancellationTokenSource _cts = new();
    private readonly List<string> _frames;
    private readonly List<bool> _chunked;
    private readonly MemoryStream _received = new();

    private RespStub(List<string> frames, List<bool> chunked)
    {
        _frames = frames;
        _chunked = chunked;
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
        _serve = ServeAsync(_cts.Token);
    }

    public int Port { get; }

    // Всё, что клиент записал во все соединения (конкатенация).
    public byte[] Received
    {
        get
        {
            lock (_received)
            {
                return _received.ToArray();
            }
        }
    }

    public static RespStub Start(params string[] frames) => new([.. frames], [.. frames.Select(_ => false)]);

    // Последний кадр — в два захода (клиент дочитывает недописанный кадр).
    public static RespStub StartChunked(params string[] frames) =>
        new([.. frames], [.. frames.Select(_ => false).SkipLast(1).Append(true)]);

    public static RespStub Silent() => new([], []);

    private async Task ServeAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            Socket socket;
            try
            {
                socket = await _listener.AcceptSocketAsync(ct);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            _ = HandleAsync(socket, ct);
        }
    }

    // Обслуживание соединения: парсим запросы (RESP-массив) клиента, на каждый
    // готовый запрос пишем очередной кадр очереди; всё входящее — в Received.
    private async Task HandleAsync(Socket socket, CancellationToken ct)
    {
        using (socket)
        {
            var buffer = new byte[4096];
            var pending = new MemoryStream();
            var frameIndex = 0;
            var argsLeft = 0;
            while (!ct.IsCancellationRequested)
            {
                int read;
                try
                {
                    read = await socket.ReceiveAsync(buffer, ct);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (SocketException)
                {
                    return; // клиент закрыл соединение
                }

                if (read == 0)
                    return; // клиент закрыл соединение после получения ответов

                lock (_received)
                {
                    _received.Write(buffer, 0, read);
                }

                pending.Write(buffer, 0, read);
                var bytes = pending.ToArray();
                pending.SetLength(0);
                pending.Position = 0;

                var offset = 0;
                while (offset < bytes.Length)
                {
                    if (argsLeft == 0)
                    {
                        // ждём '*' начала RESP-массива (иное — не наш клиент)
                        if (bytes[offset] != (byte)'*')
                        {
                            offset++;
                            continue;
                        }

                        // '*<N>\r\n' — считаем строку
                        var lineEnd = IndexOfCrlf(bytes, offset + 1);
                        if (lineEnd < 0)
                            break; // строка не дописана — ждём остальное
                        var count = Encoding.UTF8.GetString(bytes, offset + 1, lineEnd - offset - 1);
                        if (!int.TryParse(count, out argsLeft) || argsLeft <= 0)
                        {
                            offset = lineEnd + 2;
                            continue;
                        }

                        offset = lineEnd + 2;
                        continue;
                    }

                    // аргумент: '$<len>\r\n<data>\r\n'
                    if (bytes[offset] != (byte)'$')
                    {
                        offset++;
                        continue;
                    }

                    var lenEnd = IndexOfCrlf(bytes, offset + 1);
                    if (lenEnd < 0)
                        break;
                    if (!int.TryParse(Encoding.UTF8.GetString(bytes, offset + 1, lenEnd - offset - 1), out var len))
                    {
                        offset = lenEnd + 2;
                        argsLeft--;
                        continue;
                    }

                    var dataEnd = lenEnd + 2 + len;
                    if (dataEnd + 2 > bytes.Length)
                        break; // аргумент не дописан — ждём остальное
                    offset = dataEnd + 2;
                    argsLeft--;
                    if (argsLeft == 0)
                    {
                        // команда готова — отвечаем очередным кадром
                        if (frameIndex < _frames.Count)
                        {
                            var frame = Encoding.UTF8.GetBytes(_frames[frameIndex]);
                            if (_chunked[frameIndex])
                            {
                                // запись в два захода: первые 3 байта, пауза, остаток
                                var split = Math.Min(3, frame.Length);
                                await socket.SendAsync(frame.AsMemory(..split), ct);
                                await Task.Delay(80, ct);
                                await socket.SendAsync(frame.AsMemory(split..), ct);
                            }
                            else
                            {
                                await socket.SendAsync(frame, ct);
                            }
                        }

                        frameIndex++;
                    }
                }

                // остаток непарсенного хвоста — обратно в pending
                pending.Write(bytes, offset, bytes.Length - offset);
            }
        }
    }

    private static int IndexOfCrlf(byte[] bytes, int from)
    {
        for (var i = from; i + 1 < bytes.Length; i++)
        {
            if (bytes[i] == (byte)'\r' && bytes[i + 1] == (byte)'\n')
                return i;
        }

        return -1;
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        try
        {
            await _serve;
        }
        catch (OperationCanceledException)
        {
            // штатная остановка заглушки
        }
        catch (ObjectDisposedException)
        {
        }

        _listener.Stop();
        _cts.Dispose();
    }
}
