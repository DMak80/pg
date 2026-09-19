using System.Net.Sockets;
using System.Text;
using Shared.Core;

namespace AdminPanel.Probes.Valkey;

// RESP-миниклиент панели (spec §4.6): копия паттерна ValkeyWorker.Core/Valkey/
// ValkeyConnection.cs с усечением до AUTH+PING. Одна проба = одно короткоживущее
// TCP-соединение; таймаут connect+команда; ретраев нет — следующий тик петли
// и есть ретрай (симметрия refresher'а). Ошибка сети/протокола → Result.Failed.
public sealed class ValkeyConnection : IValkeyProbeClient
{
    public async Task<Shared.Core.Result> PingAsync(ValkeyProbeTarget target, TimeSpan timeout, CancellationToken ct)
    {
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(timeout);
            using var client = new TcpClient();
            await client.ConnectAsync(target.Host, target.Port, cts.Token);

            // BufferedStream один на соединение: буфер переживает чтение AUTH.
            using var stream = new BufferedStream(client.GetStream(), 8192);

            var auth = await Resp.WriteAndReadAsync(
                stream, ["AUTH", target.AdminUser, target.AdminPassword], cts.Token);
            if (auth is not string)
                return FailedReply("AUTH", auth, target);

            var reply = await Resp.WriteAndReadAsync(stream, ["PING"], cts.Token);
            // +PONG — строковый кадр; всё прочее (включая -ERR) — не жив.
            return auth is not null && reply is "PONG"
                ? Shared.Core.Result.Success()
                : FailedReply("PING", reply, target);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw; // остановка host'а — не «нода молчит»
        }
        catch (OperationCanceledException ex)
        {
            return Shared.Core.Result.Failed(new TimeoutException(
                $"valkey {target.Host}:{target.Port} не ответил за {timeout.TotalSeconds:F1} c (PING)", ex));
        }
        catch (Exception ex)
        {
            return Shared.Core.Result.Failed(new ApplicationException(
                $"valkey {target.Host}:{target.Port} PING: {ex.Message}", ex));
        }
    }

    private static Shared.Core.Result FailedReply(string command, object? reply, ValkeyProbeTarget target)
        => reply is RespError error
            ? Shared.Core.Result.Failed(new ApplicationException($"{command}: {error.Message}"))
            : Shared.Core.Result.Failed(new ApplicationException(
                $"{command}: valkey {target.Host}:{target.Port} — неожиданный ответ сервера"));

    /// <summary>Ошибка протокола (-ERR…): отдельный тип — клиент переводит в Failed.</summary>
    internal sealed record RespError(string Message);

    /// <summary>Парсер/писатель RESP-кадров (internal — юнит-тесты; копия воркерской).</summary>
    internal static class Resp
    {
        // Записать команду RESP-массивом и прочитать один ответ-кадр.
        public static async Task<object?> WriteAndReadAsync(
            Stream stream, IReadOnlyList<string> args, CancellationToken ct)
        {
            var sb = new StringBuilder();
            sb.Append('*').Append(args.Count).Append("\r\n");
            foreach (var arg in args)
            {
                var size = Encoding.UTF8.GetByteCount(arg);
                sb.Append('$').Append(size).Append("\r\n").Append(arg).Append("\r\n");
            }

            var bytes = Encoding.UTF8.GetBytes(sb.ToString());
            await stream.WriteAsync(bytes, ct);
            await stream.FlushAsync(ct);
            return await ReadReplyAsync(stream, ct);
        }

        // Один RESP-объект из потока (типы кадров + - : $ *; чтение до полного
        // кадра — побайтовый reader поверх буферизованного потока, рекурсия
        // для массивов).
        public static async Task<object?> ReadReplyAsync(Stream stream, CancellationToken ct)
        {
            var type = (char)await ReadByteAsync(stream, ct);
            return type switch
            {
                '+' => await ReadLineAsync(stream, ct), // simple string
                '-' => new RespError(await ReadLineAsync(stream, ct)), // error
                ':' => long.TryParse(await ReadLineAsync(stream, ct), out var n) ? n : 0, // integer
                '$' => await ReadBulkAsync(stream, ct), // bulk string / null
                '*' => await ReadArrayAsync(stream, ct), // array
                _ => new RespError($"неизвестный тип RESP-кадра '{type}'"),
            };
        }

        private static async Task<object?> ReadBulkAsync(Stream stream, CancellationToken ct)
        {
            var line = await ReadLineAsync(stream, ct);
            if (!int.TryParse(line, out var size))
                throw new ApplicationException($"RESP bulk: нечисловая длина '{line}'");
            if (size < 0)
                return null; // $-1 — bulk null

            var bytes = new byte[size];
            await ReadExactlyAsync(stream, bytes, ct);
            await ReadCrlfAsync(stream, ct);
            return Encoding.UTF8.GetString(bytes);
        }

        private static async Task<object?> ReadArrayAsync(Stream stream, CancellationToken ct)
        {
            var line = await ReadLineAsync(stream, ct);
            if (!int.TryParse(line, out var count))
                throw new ApplicationException($"RESP array: нечисловая длина '{line}'");

            var items = new List<object?>(Math.Max(count, 0));
            for (var i = 0; i < count; i++)
                items.Add(await ReadReplyAsync(stream, ct));
            return items;
        }

        private static async Task<string> ReadLineAsync(Stream stream, CancellationToken ct)
        {
            var sb = new StringBuilder();
            while (true)
            {
                var b = await ReadByteAsync(stream, ct);
                if (b == '\r')
                {
                    await ExpectLFAsync(stream, ct);
                    return sb.ToString();
                }

                sb.Append((char)b);
            }
        }

        private static async Task ExpectLFAsync(Stream stream, CancellationToken ct)
        {
            var lf = await ReadByteAsync(stream, ct);
            if (lf != '\n')
                throw new ApplicationException("RESP: ожидался \\n после \\r");
        }

        // Терминатор bulk-данных: "\r\n" после ровно size байтов.
        private static async Task ReadCrlfAsync(Stream stream, CancellationToken ct)
        {
            var cr = await ReadByteAsync(stream, ct);
            if (cr != '\r')
                throw new ApplicationException("RESP: ожидался \\r после bulk-данных");
            await ExpectLFAsync(stream, ct);
        }

        private static async Task ReadExactlyAsync(Stream stream, byte[] buffer, CancellationToken ct)
        {
            var offset = 0;
            while (offset < buffer.Length)
            {
                var read = await stream.ReadAsync(buffer.AsMemory(offset..), ct);
                if (read == 0)
                    throw new ApplicationException("RESP: поток закрыт посреди кадра");
                offset += read;
            }
        }

        private static async Task<byte> ReadByteAsync(Stream stream, CancellationToken ct)
        {
            var one = new byte[1];
            var read = await stream.ReadAsync(one, ct);
            if (read == 0)
                throw new ApplicationException("RESP: поток закрыт посреди кадра");
            return one[0];
        }
    }
}
