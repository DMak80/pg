using System.Buffers.Binary;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;

namespace PgWorker.PgNode.BaseBackup;

/// <summary>Источник сообщений BASE_BACKUP (тестовый шов операции полного
/// бэкапа): отправка simple-query команды и чтение одного сообщения;
/// null из <see cref="ReadAsync"/> — чистый EOF соединения.</summary>
public interface IBaseBackupSource : IAsyncDisposable
{
    Task SendAsync(string command, CancellationToken ct);
    Task<PgMessage?> ReadAsync(CancellationToken ct);
}

/// <summary>Низкоуровневый клиент wire-протокола PG v3 для физической
/// репликации (spec t01 §3.1): TCP с keepalive → SSLRequest → TLS (серт
/// источника доверяется — как стрим) → StartupMessage с `replication=database`
/// → аутентификация SCRAM-SHA-256 / cleartext (иное — fail-fast) →
/// ReadyForQuery. Пароль живёт только в памяти; в argv/логи не попадает.
/// Per-read таймаута нет: бюджет операции — у супервизора (spec §6.21).</summary>
public sealed class PgWireReplicationClient : IBaseBackupSource
{
    private const int ProtocolVersion3 = 196608; // 3.0
    private const int SslRequestCode = 80877103;
    private const int MaxFrameLength = 64 * 1024 * 1024;

    private const byte AuthenticationRequest = (byte)'R';
    private const byte PasswordMessage = (byte)'p';
    private const byte ErrorResponse = (byte)'E';
    private const byte ReadyForQuery = (byte)'Z';
    private const byte BackendKeyData = (byte)'K';
    private const byte ParameterStatus = (byte)'S';
    private const byte NoticeResponse = (byte)'N';

    private const int AuthOk = 0;
    private const int AuthCleartextPassword = 3;
    private const int AuthSasl = 10;
    private const int AuthSaslContinue = 11;
    private const int AuthSaslFinal = 12;

    private readonly string _host;
    private readonly int _port;
    private readonly string _user;
    private readonly string _password;

    private TcpClient? _tcp;
    private Stream? _readStream;
    private Stream? _writeStream;

    public PgWireReplicationClient(string host, int port, string user, string password)
    {
        _host = host;
        _port = port;
        _user = user;
        _password = password;
    }

    /// <summary>Тестовый шов: обмен по готовым потокам (фейк-сервер без сети).</summary>
    internal PgWireReplicationClient(
        string host, int port, string user, string password, Stream readStream, Stream writeStream)
        : this(host, port, user, password)
    {
        _readStream = readStream;
        _writeStream = writeStream;
    }

    public async Task ConnectAsync(CancellationToken ct)
    {
        var tcp = new TcpClient();
        // Keepalive на сокете: смерть соединения детектируется транспортом
        // (per-read таймаута нет — spec §6.21).
        tcp.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
        await tcp.ConnectAsync(_host, _port, ct);
        _tcp = tcp;
        var network = tcp.GetStream();

        // SSLRequest: int32(8) + код запроса; ответ — один байт 'S'.
        var sslRequest = new byte[8];
        BinaryPrimitives.WriteInt32BigEndian(sslRequest, 8);
        BinaryPrimitives.WriteInt32BigEndian(sslRequest.AsSpan(4), SslRequestCode);
        await network.WriteAsync(sslRequest, ct);
        var sslResponse = new byte[1];
        await network.ReadExactlyAsync(sslResponse, ct);
        if (sslResponse[0] != (byte)'S')
            throw new ApplicationException(
                $"SSL is not supported by the server: response '{(char)sslResponse[0]}'");

        // TLS-канал: серт источника доверяется (аналог sslmode=require/стрима).
        var ssl = new SslStream(
            network,
            leaveInnerStreamOpen: false,
            (_, _, _, _) => true);
        await ssl.AuthenticateAsClientAsync(
            new SslClientAuthenticationOptions { TargetHost = _host }, ct);
        _readStream = ssl;
        _writeStream = ssl;

        await AuthenticateAsync(ct);
    }

    /// <summary>StartupMessage + цикл аутентификации до ReadyForQuery
    /// (тестовый шов — тот же код, что в <see cref="ConnectAsync"/>).</summary>
    internal async Task AuthenticateAsync(CancellationToken ct)
    {
        var write = Require(_writeStream);
        var startup = BuildStartupMessage();
        await write.WriteAsync(startup, ct);
        await HandleAuthAsync(ct);
    }

    public async Task SendAsync(string command, CancellationToken ct)
    {
        var payload = Encoding.UTF8.GetBytes(command + "\0");
        await WriteFrameAsync(Require(_writeStream), (byte)'Q', payload, ct);
    }

    public async Task<PgMessage?> ReadAsync(CancellationToken ct)
        => await ReadFrameAsync(Require(_readStream), ct);

    public async ValueTask DisposeAsync()
    {
        if (_readStream is not null)
            await _readStream.DisposeAsync();
        _tcp?.Dispose();
    }

    private async Task HandleAuthAsync(CancellationToken ct)
    {
        string? clientFirstBare = null;
        ScramClientFinal? scramFinal = null;
        while (true)
        {
            var message = await ReadAsync(ct)
                          ?? throw new ApplicationException(
                              "connection closed during authentication");
            switch (message.Type)
            {
                case AuthenticationRequest:
                {
                    if (message.Payload.Length < 4)
                        throw new ApplicationException("malformed authentication request");
                    var code = BinaryPrimitives.ReadInt32BigEndian(message.Payload);
                    switch (code)
                    {
                        case AuthOk:
                            break;
                        case AuthCleartextPassword:
                            await WriteFrameAsync(
                                Require(_writeStream), PasswordMessage,
                                Encoding.UTF8.GetBytes(_password + "\0"), ct);
                            break;
                        case AuthSasl:
                        {
                            var mechanism = ChooseMechanism(message.Payload.AsSpan(4));
                            if (mechanism != ScramSha256.Mechanism)
                                throw new ApplicationException(
                                    $"unsupported auth: SCRAM mechanism {mechanism}");
                            var nonce = Convert.ToHexString(
                                RandomNumberGenerator.GetBytes(18)).ToLowerInvariant();
                            clientFirstBare = ScramSha256.ClientFirstBare(nonce);
                            await WriteSaslInitialAsync(mechanism, clientFirstBare, ct);
                            break;
                        }
                        case AuthSaslContinue:
                        {
                            if (clientFirstBare is null)
                                throw new ApplicationException("SCRAM: server-first without client-first");
                            var serverFirst = Encoding.UTF8.GetString(
                                message.Payload, 4, message.Payload.Length - 4);
                            scramFinal = ScramSha256.BuildClientFinal(
                                _password, clientFirstBare, serverFirst);
                            await WriteFrameAsync(
                                Require(_writeStream), PasswordMessage,
                                Encoding.UTF8.GetBytes(scramFinal.Message), ct);
                            break;
                        }
                        case AuthSaslFinal:
                        {
                            var serverFinal = Encoding.UTF8.GetString(
                                message.Payload, 4, message.Payload.Length - 4);
                            if (scramFinal is null
                                || !ScramSha256.VerifyServerFinal(
                                    serverFinal, scramFinal.ServerSignature))
                                throw new ApplicationException("SCRAM server signature mismatch");
                            break;
                        }
                        default:
                            throw new ApplicationException($"unsupported auth: {code}");
                    }

                    break;
                }
                case ReadyForQuery:
                    return; // аутентификация завершена
                case ErrorResponse:
                {
                    var (sqlState, errorMessage) = ParseErrorResponse(message.Payload);
                    throw new ApplicationException(
                        $"connection failed [{sqlState}]: {errorMessage}");
                }
                case BackendKeyData:
                case ParameterStatus:
                case NoticeResponse:
                    break; // служебные — пропуск
                default:
                    throw new ApplicationException(
                        $"unexpected message '{(char)message.Type}' during authentication");
            }
        }
    }

    private async Task WriteSaslInitialAsync(
        string mechanism, string clientFirst, CancellationToken ct)
    {
        var initial = Encoding.UTF8.GetBytes(clientFirst);
        var payload = new byte[mechanism.Length + 1 + 4 + initial.Length];
        Encoding.ASCII.GetBytes(mechanism).CopyTo(payload, 0);
        BinaryPrimitives.WriteInt32BigEndian(
            payload.AsSpan(mechanism.Length + 1), initial.Length);
        initial.CopyTo(payload, mechanism.Length + 5);
        await WriteFrameAsync(Require(_writeStream), PasswordMessage, payload, ct);
    }

    private byte[] BuildStartupMessage()
    {
        using var body = new MemoryStream();
        var version = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(version, ProtocolVersion3);
        body.Write(version);
        WriteCString(body, "user");
        WriteCString(body, _user);
        WriteCString(body, "database");
        WriteCString(body, "postgres");
        // Репликационный режим: BASE_BACKUP доступен только в нём.
        WriteCString(body, "replication");
        WriteCString(body, "database");
        body.WriteByte(0);

        var payload = body.ToArray();
        var frame = new byte[4 + payload.Length];
        BinaryPrimitives.WriteInt32BigEndian(frame, 4 + payload.Length);
        payload.CopyTo(frame, 4);
        return frame;
    }

    // Список SASL-механизмов: нуль-терминированные строки, пустая — конец.
    private static string ChooseMechanism(ReadOnlySpan<byte> payload)
    {
        var names = new List<string>();
        var pos = 0;
        while (pos < payload.Length)
        {
            var end = payload[pos..].IndexOf((byte)0);
            if (end <= 0)
                break;
            names.Add(Encoding.ASCII.GetString(payload.Slice(pos, end)));
            pos += end + 1;
        }

        return names.Contains(ScramSha256.Mechanism)
            ? ScramSha256.Mechanism
            : names.Count > 0 ? names[0] : "";
    }

    /// <summary>Поля ErrorResponse: байт-код + нуль-терминированное значение,
    /// терминатор — нулевой байт. Нужны SQLState ('C') и message ('M').</summary>
    internal static (string SqlState, string Message) ParseErrorResponse(byte[] payload)
    {
        var sqlState = "";
        var message = "";
        var pos = 0;
        while (pos < payload.Length && payload[pos] != 0)
        {
            var code = (char)payload[pos];
            pos++;
            var end = Array.IndexOf(payload, (byte)0, pos);
            if (end < 0)
                end = payload.Length;
            var value = Encoding.UTF8.GetString(payload, pos, end - pos);
            if (code == 'C')
                sqlState = value;
            else if (code == 'M')
                message = value;
            pos = end + 1;
        }

        return (sqlState, message);
    }

    private static void WriteCString(Stream target, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        target.Write(bytes);
        target.WriteByte(0);
    }

    internal static async Task WriteFrameAsync(
        Stream stream, byte type, ReadOnlyMemory<byte> payload, CancellationToken ct)
    {
        var header = new byte[5];
        header[0] = type;
        BinaryPrimitives.WriteInt32BigEndian(header.AsSpan(1), payload.Length + 4);
        await stream.WriteAsync(header, ct);
        if (payload.Length > 0)
            await stream.WriteAsync(payload, ct);
    }

    internal static async Task<PgMessage?> ReadFrameAsync(Stream stream, CancellationToken ct)
    {
        var type = new byte[1];
        var read = await stream.ReadAsync(type, ct);
        if (read == 0)
            return null; // чистый EOF: соединение закрыто между сообщениями
        if (read != 1)
            throw new ApplicationException("malformed frame: type byte is not fully read");

        var lengthBytes = new byte[4];
        await stream.ReadExactlyAsync(lengthBytes, ct);
        var length = BinaryPrimitives.ReadInt32BigEndian(lengthBytes);
        if (length < 4 || length > MaxFrameLength)
            throw new ApplicationException($"malformed frame length: {length}");
        var payload = new byte[length - 4];
        if (payload.Length > 0)
            await stream.ReadExactlyAsync(payload, ct);
        return new PgMessage(type[0], payload);
    }

    private static Stream Require(Stream? stream)
        => stream ?? throw new InvalidOperationException("connection is not established");
}

/// <summary>Разбор SASL-обмена SCRAM-SHA-256 (RFC 5802/7677) — выделен из
/// клиента, чтобы юниты фиксировали порядок склейки authMessage и XOR-proof
/// без сети. Пароль нормализуется FormKC (как требует RFC); серверный серт —
/// без привязки к каналу (-PLUS не поддерживается).</summary>
internal sealed record ScramClientFinal(string Message, byte[] ServerSignature);

internal static class ScramSha256
{
    public const string Mechanism = "SCRAM-SHA-256";

    // client-first-bare: пользователь пуст (startup-user уже известен серверу).
    public static string ClientFirstBare(string nonce) => $"n=,r={nonce}";

    public static string ClientFinalWithoutProof(string combinedNonce) => $"c=biws,r={combinedNonce}";

    public static string AuthMessage(
        string clientFirstBare, string serverFirst, string clientFinalWithoutProof)
        => $"{clientFirstBare},{serverFirst},{clientFinalWithoutProof}";

    public static byte[] SaltedPassword(string password, byte[] salt, int iterations)
        => Rfc2898DeriveBytes.Pbkdf2(
            password.Normalize(NormalizationForm.FormKC),
            salt, iterations, HashAlgorithmName.SHA256, 32);

    public static byte[] ClientKey(byte[] saltedPassword)
        => HMACSHA256.HashData(saltedPassword, "Client Key"u8);

    public static byte[] StoredKey(byte[] clientKey) => SHA256.HashData(clientKey);

    /// <summary>ClientProof = ClientKey XOR HMAC(StoredKey, AuthMessage).</summary>
    public static byte[] ClientProof(byte[] clientKey, byte[] storedKey, string authMessage)
    {
        var signature = HMACSHA256.HashData(storedKey, Encoding.UTF8.GetBytes(authMessage));
        var proof = new byte[clientKey.Length];
        for (var i = 0; i < proof.Length; i++)
            proof[i] = (byte)(clientKey[i] ^ signature[i]);
        return proof;
    }

    /// <summary>ServerSignature = HMAC(HMAC(SaltedPassword, "Server Key"),
    /// AuthMessage) — клиент сверяет серверный финал.</summary>
    public static byte[] ServerSignature(byte[] saltedPassword, string authMessage)
        => HMACSHA256.HashData(
            HMACSHA256.HashData(saltedPassword, "Server Key"u8),
            Encoding.UTF8.GetBytes(authMessage));

    /// <summary>client-final по server-first (r=,s=,i=) + ожидаемая подпись
    /// сервера для проверки server-final.</summary>
    public static ScramClientFinal BuildClientFinal(
        string password, string clientFirstBare, string serverFirst)
    {
        var fields = ParseFields(serverFirst);
        if (!fields.TryGetValue("r", out var combinedNonce)
            || !fields.TryGetValue("s", out var saltBase64)
            || !fields.TryGetValue("i", out var iterationsText)
            || !int.TryParse(iterationsText, System.Globalization.NumberStyles.Integer,
                System.Globalization.CultureInfo.InvariantCulture, out var iterations))
            throw new ApplicationException("SCRAM: malformed server-first message");

        var withoutProof = ClientFinalWithoutProof(combinedNonce);
        var authMessage = AuthMessage(clientFirstBare, serverFirst, withoutProof);
        var salted = SaltedPassword(password, Convert.FromBase64String(saltBase64), iterations);
        var clientKey = ClientKey(salted);
        var proof = ClientProof(clientKey, StoredKey(clientKey), authMessage);
        return new ScramClientFinal(
            withoutProof + ",p=" + Convert.ToBase64String(proof),
            ServerSignature(salted, authMessage));
    }

    /// <summary>Проверка server-final "v=&lt;base64&gt;" (или "e=" — ошибка
    /// сервера); сравнение — constant-time.</summary>
    public static bool VerifyServerFinal(string serverFinal, byte[] expectedSignature)
    {
        if (serverFinal.StartsWith("e=", StringComparison.Ordinal))
            throw new ApplicationException($"SCRAM server error: {serverFinal[2..]}");
        if (!serverFinal.StartsWith("v=", StringComparison.Ordinal))
            throw new ApplicationException("SCRAM: malformed server-final message");
        byte[] actual;
        try
        {
            actual = Convert.FromBase64String(serverFinal[2..]);
        }
        catch (FormatException)
        {
            return false;
        }

        return CryptographicOperations.FixedTimeEquals(actual, expectedSignature);
    }

    // Поля "k=v[,k=v...]" с разделением по первому '=' (значение может
    // содержать '=' — base64-соль).
    private static Dictionary<string, string> ParseFields(string message)
    {
        var fields = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var part in message.Split(','))
        {
            var eq = part.IndexOf('=');
            if (eq > 0)
                fields[part[..eq]] = part[(eq + 1)..];
        }

        return fields;
    }
}
