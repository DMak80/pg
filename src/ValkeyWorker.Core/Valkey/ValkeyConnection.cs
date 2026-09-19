using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Shared.Core;

namespace ValkeyWorker.Core.Valkey;

// RESP-миниклиент (spec §4.3): одна команда = одно короткоживущее TCP-соединение;
// таймаут — connect+команда (это пробы, не ожидания); буферное чтение до
// полного кадра, рекурсивный разбор array; запись — единый кадр-массив;
// ошибка сети/протокола → Result.Failed (S7: слепая проба — не исключение).
public sealed class ValkeyConnection(TimeSpan? connectAndCommandTimeout = null) : IValkeyConnection
{
    // Дефолт 5 с — пробы/команды тиковые, мультиплексирование не нужно.
    private readonly TimeSpan _timeout = connectAndCommandTimeout ?? TimeSpan.FromSeconds(5);

    public async Task<Result> PingAsync(ValkeyEndpoint ep, CancellationToken ct)
        => await ExecuteAsync(ep, ["PING"], reply => IsOk(reply), ct);

    public async Task<Result<IReadOnlyDictionary<string, string>>> ConfigGetAsync(
        ValkeyEndpoint ep, string parameter, CancellationToken ct)
        => await ExecuteAsync(ep, ["CONFIG", "GET", parameter], ProjectConfigGet, ct);

    public async Task<Result> ConfigSetAsync(
        ValkeyEndpoint ep, string parameter, string value, CancellationToken ct)
        => await ExecuteAsync(ep, ["CONFIG", "SET", parameter, value], reply => IsOk(reply), ct);

    public async Task<Result<IReadOnlyList<string>>> AclListAsync(ValkeyEndpoint ep, CancellationToken ct)
        => await ExecuteAsync(ep, ["ACL", "LIST"], ProjectStringList, ct);

    public async Task<Result> AclSetUserAsync(ValkeyEndpoint ep, IReadOnlyList<string> args, CancellationToken ct)
    {
        var command = new List<string> { "ACL", "SETUSER" };
        command.AddRange(args);
        return await ExecuteAsync(ep, command, reply => IsOk(reply), ct);
    }

    // ── Каркас: connect → AUTH → команда → проекция ответа ──

    private async Task<Result<T>> ExecuteAsync<T>(
        ValkeyEndpoint ep, IReadOnlyList<string> command, Func<object?, T> project, CancellationToken ct)
    {
        try
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeout.CancelAfter(_timeout);
            using var client = new TcpClient();
            await client.ConnectAsync(ep.Host, ep.Port, timeout.Token);

            // TLS (t06, arch/21 §6): доверие — ТОЛЬКО per-cluster CA; системные
            // якоря не участвуют (SslPolicyErrors игнорируем — строим свою цепочку
            // CustomRootTrust) + SAN обязан покрывать хост endpoint'а.
            // Валидатор — ЗАМЫКАНИЕ на распарсенный CA и ep.Host (не поле класса:
            // соединение = один endpoint).
            if (!ValkeyPki.TryParseCertificate(ep.CaPem, out var ca) || ca is null)
                return Result<T>.Failed(new ApplicationException(
                    $"valkey {ep.Host}:{ep.Port}: ca_pem — невалидный PEM ({command[0]})"));
            using (ca)
            {
                // Валидатор — ЗАМЫКАНИЕ на распарсенный CA и ep.Host (не поле
                // класса: соединение = один endpoint).
                bool ValidateServerCertificate(
                    object sender, X509Certificate? certificate, X509Chain? chain, SslPolicyErrors errors)
                    => certificate is not null
                       && Shared.Tls.TlsChain.ValidateChain(certificate, ca) // CustomRootTrust + NoCheck
                       && SanMatchesHost(certificate, ep.Host);

                using var ssl = new SslStream(client.GetStream(), false, ValidateServerCertificate);
                var tlsOptions = new SslClientAuthenticationOptions
                {
                    TargetHost = ep.Host,
                    ClientCertificates = null, // клиентские серты — нет (ACL)
                    EnabledSslProtocols = System.Security.Authentication.SslProtocols.None, // дефолты ОС
                    RemoteCertificateValidationCallback = ValidateServerCertificate,
                };
                await ssl.AuthenticateAsClientAsync(tlsOptions, timeout.Token);

                // BufferedStream — один на соединение: буфер переживает чтение AUTH
                // (сервер мог уже отправить оба кадра).
                using var stream = new BufferedStream(ssl, 8192);

                // AUTH выполняется внутри каждой команды (после connect):
                // запрос AUTH user password → ожидание +OK.
                var auth = await Resp.WriteAndReadAsync(
                    stream, ["AUTH", ep.User, ep.Password], timeout.Token);
                if (!IsOk(auth))
                    return FailedReply<T>("AUTH", auth);

                var reply = await Resp.WriteAndReadAsync(stream, command, timeout.Token);
                if (!IsSuccess(reply))
                    return FailedReply<T>(command[0], reply);

                return Result<T>.Success(project(reply));
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw; // остановка host'а — не «нода молчит»
        }
        catch (OperationCanceledException ex)
        {
            return Result<T>.Failed(new TimeoutException(
                $"valkey {ep.Host}:{ep.Port} не ответил за {_timeout.TotalSeconds:F1} c ({command[0]})", ex));
        }
        catch (System.Security.Authentication.AuthenticationException ex)
        {
            // TLS-хендшейк отвергнут (чужой CA / SAN-мисс / протокол) — отдельная ветка
            return Result<T>.Failed(new ApplicationException(
                $"valkey {ep.Host}:{ep.Port} TLS: {ex.Message}", ex));
        }
        catch (Exception ex)
        {
            return Result<T>.Failed(new ApplicationException(
                $"valkey {ep.Host}:{ep.Port} {command[0]}: {ex.Message}", ex));
        }
    }

    // SAN-хост (arch/21 §2): advertised DNS либо IP; ровно один SAN у сертов
    // домена, но сверяем весь список — отказ при отсутствии покрытия.
    private static bool SanMatchesHost(X509Certificate certificate, string host)
    {
        var cert = certificate as X509Certificate2 ?? new X509Certificate2(certificate);
        using var _ = cert;
        var san = cert.Extensions.OfType<X509SubjectAlternativeNameExtension>().FirstOrDefault();
        if (san is null)
            return false;
        if (System.Net.IPAddress.TryParse(host, out var ip))
            return san.EnumerateIPAddresses().Contains(ip);
        return san.EnumerateDnsNames().Contains(host, StringComparer.OrdinalIgnoreCase);
    }

    // Успех = любой не-error кадр (+OK, +PONG, массив, целое, bulk).
    private static bool IsSuccess(object? reply) => reply is not RespError;

    // +OK-семантика для мутаций и PING (строковый кадр без '-').
    private static bool IsOk(object? reply) => reply is string s;

    private static Result<T> FailedReply<T>(string command, object? reply)
        => reply is RespError error
            ? Result<T>.Failed(new ApplicationException($"{command}: {error.Message}"))
            : Result<T>.Failed(new ApplicationException($"{command}: неожиданный ответ сервера"));

    // CONFIG GET → плоский массив [k1, v1, k2, v2…] (RESP2) → словарь;
    // null-элементы (bulk null) → пустая строка (ключ без значения не валиден).
    private static IReadOnlyDictionary<string, string> ProjectConfigGet(object? reply)
    {
        var result = new Dictionary<string, string>();
        if (reply is not List<object?> items)
            return result;

        for (var i = 0; i + 1 < items.Count; i += 2)
        {
            var key = items[i]?.ToString();
            if (!string.IsNullOrEmpty(key))
                result[key] = items[i + 1]?.ToString() ?? "";
        }

        return result;
    }

    // ACL LIST → массив строк-правил.
    private static IReadOnlyList<string> ProjectStringList(object? reply)
        => reply is List<object?> items
            ? [.. items.Select(i => i?.ToString() ?? "").Where(s => s.Length > 0)]
            : [];

    /// <summary>Ошибка протокола (-ERR…): отдельный тип — клиент переводит в Failed.</summary>
    internal sealed record RespError(string Message);

    /// <summary>Парсер/писатель RESP-кадров (internal — юнит-тесты парсера).</summary>
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
