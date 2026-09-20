using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Shared.Core;
using Shared.Tls;

namespace AdminPanel.Probes.Valkey;

// RESP-миниклиент панели (spec §4.6): копия паттерна ValkeyWorker.Core/Valkey/
// ValkeyConnection.cs с усечением до AUTH+PING. Одна проба = одно короткоживущее
// TLS-соединение (t06: доверие — per-cluster ca_pem из secrets-стора, CustomRootTrust
// + SAN-хост endpoint'а; plain-пути нет). Таймаут connect+команда; ретраев нет —
// следующий тик петли и есть ретрай (симметрия refresher'а). Ошибка сети/
// протокола → Result.Failed.
public sealed class ValkeyConnection : IValkeyProbeClient
{
    public async Task<Shared.Core.Result> PingAsync(ValkeyProbeTarget target, TimeSpan timeout, CancellationToken ct)
    {
        // TLS-проба (t06) без ca_pem невозможна: миграция не доиграна/ключ потерян —
        // Failed с диагнозом (алерт valkey-security-missing подхватит HasCaPem=false).
        if (target.CaPem is null)
            return Shared.Core.Result.Failed(new ApplicationException(
                $"valkey {target.Host}:{target.Port}: нет ca_pem — TLS-проба невозможна (миграция t06 не доиграна?)"));

        // Мини-парсер PEM: панель не ссылается на ValkeyWorker.Core; битый PEM →
        // Failed (толерантность как у кредов, arch/02 §11.1).
        if (!TryParseCertificate(target.CaPem, out var ca) || ca is null)
            return Shared.Core.Result.Failed(new ApplicationException(
                $"valkey {target.Host}:{target.Port}: ca_pem — невалидный PEM (PING)"));

        try
        {
            using (ca)
            {
                // Валидатор — ЗАМЫКАНИЕ на распарсенный CA и target.Host.
                bool ValidateServerCertificate(
                    object sender, X509Certificate? certificate, X509Chain? chain, SslPolicyErrors errors)
                    => certificate is not null
                       && TlsChain.ValidateChain(certificate, ca) // CustomRootTrust + NoCheck
                       && SanMatchesHost(certificate, target.Host);

                using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                cts.CancelAfter(timeout);
                using var client = new TcpClient();
                await client.ConnectAsync(target.Host, target.Port, cts.Token);
                using var ssl = new SslStream(client.GetStream(), false, ValidateServerCertificate);
                await ssl.AuthenticateAsClientAsync(new SslClientAuthenticationOptions
                {
                    TargetHost = target.Host,
                    ClientCertificates = null, // клиентские серты — нет (ACL)
                    EnabledSslProtocols = System.Security.Authentication.SslProtocols.None, // дефолты ОС
                    RemoteCertificateValidationCallback = ValidateServerCertificate,
                }, cts.Token);

                // BufferedStream один на соединение: буфер переживает чтение AUTH.
                using var stream = new BufferedStream(ssl, 8192);

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
        catch (System.Security.Authentication.AuthenticationException ex)
        {
            // TLS-хендшейк отвергнут (чужой CA / SAN-мисс) — отдельная ветка
            return Shared.Core.Result.Failed(new ApplicationException(
                $"valkey {target.Host}:{target.Port} TLS: {ex.Message}", ex));
        }
        catch (Exception ex)
        {
            return Shared.Core.Result.Failed(new ApplicationException(
                $"valkey {target.Host}:{target.Port} PING: {ex.Message}", ex));
        }
    }

    // SAN-хост (arch/21 §2): advertised DNS либо IP — сверяем весь список SAN,
    // отказ при отсутствии покрытия (копия воркерского валидатора).
    private static bool SanMatchesHost(X509Certificate certificate, string host)
    {
        var cert = certificate as X509Certificate2;
        var owned = false;
        if (cert is null)
        {
            cert = new X509Certificate2(certificate);
            owned = true;
        }

        try
        {
            var san = cert.Extensions.OfType<X509SubjectAlternativeNameExtension>().FirstOrDefault();
            if (san is null)
                return false;
            if (System.Net.IPAddress.TryParse(host, out var ip))
                return san.EnumerateIPAddresses().Contains(ip);
            return san.EnumerateDnsNames().Contains(host, StringComparer.OrdinalIgnoreCase);
        }
        finally
        {
            if (owned)
                cert.Dispose();
        }
    }

    // PEM-блок CERTIFICATE → DER (без внешних инструментов); мусор → false.
    private static bool TryParseCertificate(string pem, out X509Certificate2? certificate)
    {
        try
        {
            var begin = "-----BEGIN CERTIFICATE-----";
            var end = "-----END CERTIFICATE-----";
            var start = pem.IndexOf(begin, StringComparison.Ordinal);
            var bodyStart = start < 0 ? -1 : start + begin.Length;
            var stop = bodyStart < 0 ? -1 : pem.IndexOf(end, bodyStart, StringComparison.Ordinal);
            if (stop < 0)
                throw new FormatException("PEM не содержит блока CERTIFICATE");
            var base64 = new string(pem[bodyStart..stop].Where(c => !char.IsWhiteSpace(c)).ToArray());
            certificate = X509CertificateLoader.LoadCertificate(Convert.FromBase64String(base64));
            return true;
        }
        catch (Exception e) when (e is ArgumentException or FormatException or CryptographicException)
        {
            certificate = null;
            return false;
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
