using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;

namespace ValkeyWorker.UnitTests.Core;

// Фейк-TLS-сервер юнит-тестов ValkeyConnection (t06): TcpListener на
// 127.0.0.1:0 (динамический порт — литералов нет), SslStream поверх
// принятого сокета с серверным сертом. RESP-логика — порт RespStub
// (очередь кадров на каждый запрос, chunked, Received); канон-режим —
// AUTH→+OK, PING→+PONG. Мини-генератор RSA-2048 (копия TestPki интеграции,
// но в юнитах своя — CA per-тест).
internal sealed class TlsTestServer : IAsyncDisposable
{
    private readonly TcpListener _listener;
    private readonly X509Certificate2 _serverCertificate;
    private readonly CancellationTokenSource _cts = new();
    private readonly List<string>? _frames;
    private readonly List<bool>? _chunked;
    private readonly MemoryStream _received = new();

    private TlsTestServer(
        X509Certificate2 serverCertificate, List<string>? frames = null, List<bool>? chunked = null)
    {
        _serverCertificate = serverCertificate;
        _frames = frames;
        _chunked = chunked;
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
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

    // Канон-режим (AUTH→+OK, PING→+PONG) — для тестов доверия CA/SAN.
    public static TlsTestServer Start(string certPem, string keyPem)
        => new(LoadServerCert(certPem, keyPem));

    // Очередь кадров: на каждый RESP-запрос — очередной кадр (исчерпание →
    // повтор последнего). Пустая очередь — соединение обслуживается молча.
    public static TlsTestServer StartWithFrames(string certPem, string keyPem, params string[] frames)
        => new(LoadServerCert(certPem, keyPem), [.. frames], [.. frames.Select(_ => false)]);

    // Последний кадр очереди — в два захода (клиент дочитывает недописанный кадр).
    public static TlsTestServer StartChunked(string certPem, string keyPem, params string[] frames)
        => new(LoadServerCert(certPem, keyPem), [.. frames],
            [.. frames.Select(_ => false).SkipLast(1).Append(true)]);

    // TLS-handshake проходит, но сервер не отвечает (таймаут-тест пробы).
    public static TlsTestServer StartSilent(string certPem, string keyPem)
        => new(LoadServerCert(certPem, keyPem), [], []);

    private static X509Certificate2 LoadServerCert(string certPem, string keyPem)
    {
        var cert = X509Certificate2.CreateFromPem(certPem, keyPem);
        // SslStream требует серт с ключом в том же объекте — Pkcs12-round-trip
        // через loader (конструктор с байтами устарел).
        var pfx = cert.Export(X509ContentType.Pkcs12);
        return X509CertificateLoader.LoadPkcs12(pfx, null);
    }

    // Обслужить ОДНО соединение: TLS-handshake + RESP-цикл. Отказ клиента
    // (отвал до/посреди хендшейка — битый ca_pem, чужой CA) — не исключение.
    public async Task HandleOneAsync()
    {
        Socket socket;
        try
        {
            socket = await _listener.AcceptSocketAsync(_cts.Token);
        }
        catch (OperationCanceledException)
        {
            return;
        }

        try
        {
            using (socket)
            await using (var ns = new NetworkStream(socket, ownsSocket: false))
            {
                using var ssl = new SslStream(ns, leaveInnerStreamOpen: false);
                await ssl.AuthenticateAsServerAsync(_serverCertificate, clientCertificateRequired: false,
                    System.Security.Authentication.SslProtocols.None, checkCertificateRevocation: false);
                await ServeRespAsync(ssl);
            }
        }
        catch (Exception)
        {
            // клиент закрыл соединение на хендшейке — нормальный исход тестов отказа
        }
    }

    // RESP-цикл: читает запросы-массивы, отвечает очередным кадром очереди;
    // null-очередь — канон-режим (AUTH/PING/прочее → +OK/+PONG).
    private async Task ServeRespAsync(SslStream stream)
    {
        try
        {
            var frameIndex = 0;
            while (true)
            {
                var command = await ReadCommandAsync(stream);
                if (command is null)
                    return; // клиент закрыл соединение

                var reply = _frames switch
                {
                    null => CanonReply(command),
                    { Count: > 0 } frames => frames[Math.Min(frameIndex, frames.Count - 1)],
                    _ => null, // silent: соединение обслуживается без ответов
                };
                if (reply is null)
                    return;

                var bytes = Encoding.UTF8.GetBytes(reply);
                if (_chunked is { } chunked && frameIndex < chunked.Count && chunked[frameIndex])
                {
                    // Chunked-кадр: в два захода с паузой — клиент обязан дочитать.
                    await stream.WriteAsync(bytes.AsMemory(0, bytes.Length / 2));
                    await stream.FlushAsync();
                    await Task.Delay(80);
                    await stream.WriteAsync(bytes.AsMemory(bytes.Length / 2));
                }
                else
                {
                    await stream.WriteAsync(bytes);
                }

                await stream.FlushAsync();
                frameIndex++;
            }
        }
        catch (Exception)
        {
            // клиент ушёл/битый кадр — соединение закрыто
        }
    }

    // Канон: AUTH → +OK, PING → +PONG, прочее → +OK.
    private static string CanonReply(string[] command)
        => command[0].ToUpperInvariant() switch
        {
            "PING" => "+PONG\r\n",
            _ => "+OK\r\n",
        };

    // Один RESP-запрос (массив bulk-строк) или null при EOF; входящее — в Received.
    private async Task<string[]?> ReadCommandAsync(SslStream stream)
    {
        var header = await ReadLineAsync(stream);
        if (header is null)
            return null;
        if (!header.StartsWith('*'))
            return null;
        var count = int.Parse(header[1..]);
        var args = new string[count];
        for (var i = 0; i < count; i++)
        {
            var bulk = await ReadLineAsync(stream);
            if (bulk is null || !bulk.StartsWith('$'))
                return null;
            var size = int.Parse(bulk[1..]);
            var bytes = new byte[size];
            await ReadExactlyRecordedAsync(stream, bytes);
            args[i] = Encoding.UTF8.GetString(bytes);
            await ReadCrlfAsync(stream);
        }

        return args;
    }

    private async Task<string?> ReadLineAsync(SslStream stream)
    {
        var sb = new StringBuilder();
        while (true)
        {
            var b = new byte[1];
            var read = await stream.ReadAsync(b);
            if (read == 0)
                return sb.Length == 0 ? null : sb.ToString();
            Record(b[0]);
            if (b[0] == '\r')
            {
                var lf = new byte[1];
                await stream.ReadExactlyAsync(lf);
                Record(lf[0]);
                return sb.ToString();
            }

            sb.Append((char)b[0]);
        }
    }

    // Терминатор bulk-тела "\r\n" после ровно size байтов (входящее — в Received).
    private async Task ReadCrlfAsync(SslStream stream)
    {
        var crlf = new byte[2];
        await stream.ReadExactlyAsync(crlf);
        Record(crlf[0]);
        Record(crlf[1]);
    }

    // Точное чтение bulk-тела с записью входящих байтов в Received.
    private async Task ReadExactlyRecordedAsync(SslStream stream, byte[] buffer)
    {
        var offset = 0;
        while (offset < buffer.Length)
        {
            var read = await stream.ReadAsync(buffer.AsMemory(offset..));
            if (read == 0)
                return;
            for (var i = offset; i < offset + read; i++)
                Record(buffer[i]);
            offset += read;
        }
    }

    private void Record(byte b)
    {
        lock (_received)
        {
            _received.WriteByte(b);
        }
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        _listener.Stop();
        _serverCertificate.Dispose();
        await ValueTask.CompletedTask;
    }

    // ── Мини-PKI юнит-тестов: CA + серверный серт (SAN по запросу) ──

    // Self-signed CA (RSA-2048, BasicConstraints CA).
    public static (string CaPem, string CaKeyPem) CreateCa(string commonName)
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            $"CN={commonName}", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(
            new X509BasicConstraintsExtension(true, false, 0, true));
        using var ca = request.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(5));
        return (PemCert(ca), PemKey(rsa));
    }

    // Серверный серт под CA: SAN — DNS-имя либо IP (для mismatch — заведомо чужой SAN).
    public static (string CertPem, string KeyPem) IssueServerCertificate(
        string caPem, string caKeyPem, string commonName, string? dnsSan = null, IPAddress? ipSan = null)
    {
        using var ca = X509Certificate2.CreateFromPem(caPem, caKeyPem);
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            $"CN={commonName}", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var san = new SubjectAlternativeNameBuilder();
        if (dnsSan is not null)
            san.AddDnsName(dnsSan);
        if (ipSan is not null)
            san.AddIpAddress(ipSan);
        request.CertificateExtensions.Add(san.Build());
        using var cert = request.Create(ca, DateTimeOffset.UtcNow.AddDays(-1),
            DateTimeOffset.UtcNow.AddYears(3), RandomNumberGenerator.GetBytes(16));
        var withKey = cert.CopyWithPrivateKey(rsa);
        return (PemCert(withKey), PemKey(rsa));
    }

    private static string PemCert(X509Certificate2 cert)
        => cert.ExportCertificatePem().ReplaceLineEndings("\n").TrimEnd() + "\n";

    private static string PemKey(RSA rsa)
        => rsa.ExportPkcs8PrivateKeyPem().ReplaceLineEndings("\n").TrimEnd() + "\n";
}
