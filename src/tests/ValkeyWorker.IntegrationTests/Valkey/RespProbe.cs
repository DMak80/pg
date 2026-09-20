using System.Net.Security;
using System.Text;
using ValkeyWorker.Core.Valkey;

namespace ValkeyWorker.IntegrationTests.Valkey;

// RESP-проба реальной ноды из тестов (одна команда = одно соединение — как
// воркерский клиент): AUTH user pass + произвольная команда, сырой разбор
// кадров. Для SET/GET/CONFIG-матрицы ACL — команды вне воркерского клиента.
public static class RespProbe
{
    public static (bool Ok, string Error, string? Value) Execute(
        string host, int port, string user, string password, params string[] command)
    {
        using var client = new System.Net.Sockets.TcpClient();
        client.ConnectAsync(host, port).GetAwaiter().GetResult();
        using var stream = new BufferedStream(client.GetStream(), 8192);
        return ExecuteOn(stream, user, password, command);
    }

    // TLS-проба (t06): SslStream с валидацией против ca_pem (CustomRootTrust +
    // SAN-хост) — тот же доверительный путь, что у воркера/панели.
    public static (bool Ok, string Error, string? Value) ExecuteTls(
        string host, int port, string user, string password, string caPem, params string[] command)
    {
        using var client = new System.Net.Sockets.TcpClient();
        client.ConnectAsync(host, port).GetAwaiter().GetResult();

        if (!ValkeyPki.TryParseCertificate(caPem, out var ca) || ca is null)
            return (false, "ca_pem: невалидный PEM", null);
        bool ValidateServerCertificate(
            object sender, System.Security.Cryptography.X509Certificates.X509Certificate? certificate,
            System.Security.Cryptography.X509Certificates.X509Chain? chain,
            SslPolicyErrors errors)
            => certificate is not null
               && Shared.Tls.TlsChain.ValidateChain(certificate, ca)
               && SanMatchesHost(certificate, host);

        using var ssl = new SslStream(client.GetStream(), false, ValidateServerCertificate);
        ssl.AuthenticateAsClient(new SslClientAuthenticationOptions
        {
            TargetHost = host,
            RemoteCertificateValidationCallback = ValidateServerCertificate,
        });
        using var stream = new BufferedStream(ssl, 8192);
        return ExecuteOn(stream, user, password, command);
    }

    private static bool SanMatchesHost(
        System.Security.Cryptography.X509Certificates.X509Certificate certificate, string host)
    {
        using var cert = certificate as System.Security.Cryptography.X509Certificates.X509Certificate2
                         ?? new System.Security.Cryptography.X509Certificates.X509Certificate2(certificate);
        var san = cert.Extensions
            .OfType<System.Security.Cryptography.X509Certificates.X509SubjectAlternativeNameExtension>()
            .FirstOrDefault();
        if (san is null)
            return false;
        if (System.Net.IPAddress.TryParse(host, out var ip))
            return san.EnumerateIPAddresses().Contains(ip);
        return san.EnumerateDnsNames().Contains(host, StringComparer.OrdinalIgnoreCase);
    }

    private static (bool Ok, string Error, string? Value) ExecuteOn(
        System.IO.Stream stream, string user, string password, IReadOnlyList<string> command)
    {
        var auth = WriteAndRead(stream, ["AUTH", user, password]);
        if (auth is ValkeyConnection.RespError authError)
            return (false, $"AUTH: {authError.Message}", null);
        if (auth is not string authOk || authOk != "OK")
            return (false, $"AUTH: неожиданный ответ {auth}", null);

        var reply = WriteAndRead(stream, command);
        return reply switch
        {
            ValkeyConnection.RespError error => (false, error.Message, null),
            string s => (true, "", s),
            long n => (true, "", n.ToString()),
            List<object?> list => (true, "", string.Join("|", list.Select(i => i?.ToString() ?? "<null>"))),
            null => (true, "", null),
            _ => (false, $"неизвестный кадр {reply.GetType().Name}", null),
        };
    }

    private static object? WriteAndRead(
        System.IO.Stream stream, IReadOnlyList<string> args)
    {
        var sb = new StringBuilder();
        sb.Append('*').Append(args.Count).Append("\r\n");
        foreach (var arg in args)
        {
            var size = Encoding.UTF8.GetByteCount(arg);
            sb.Append('$').Append(size).Append("\r\n").Append(arg).Append("\r\n");
        }

        var bytes = Encoding.UTF8.GetBytes(sb.ToString());
        stream.Write(bytes);
        stream.Flush();
        return ValkeyConnection.Resp.ReadReplyAsync(stream, TestContext.Current.CancellationToken)
            .GetAwaiter().GetResult();
    }
}

