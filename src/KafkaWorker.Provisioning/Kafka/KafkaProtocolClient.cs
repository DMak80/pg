using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Text;

namespace KafkaWorker.Provisioning.Kafka;

// Протокольный миниклиент AlterPartitionReassignments (arch/16 §2.4; spec t01
// §4.2): одна подача = одно короткоживущее TCP+TLS-соединение (паттерн
// ValkeyConnection.ExecuteAsync), диалог ApiVersions v0 → SaslHandshake v1 →
// SaslAuthenticate v0 (PLAIN) → AlterPartitionReassignments v0 (KIP-455).
// Версии фиксированы (образ pinned apache/kafka:4.0.0, R1) — переговоров нет;
// ApiVersions читается ТОЛЬКО для fail-fast проверки apiKey 45.
internal sealed class KafkaProtocolClient
{
    internal const string ClientId = "kfw-reassign";

    // Верхняя санитарная граница кадра: 16 МБ — огромный запас поверх
    // фактических ответов воркера (батч ≤ 10 партиций даёт килобайтные
    // ответы); битая/враждебная длина не должна давать большую аллокацию
    // до отказа чтения.
    internal const int MaxFrameLength = 16 * 1024 * 1024;

    // Минимальный кадр ответа — ResponseHeader v0 (int32 correlation id).
    private const int MinFrameLength = 4;

    private const short ApiVersionsKey = 18;
    private const short SaslHandshakeKey = 17;
    private const short SaslAuthenticateKey = 36;
    private const short AlterReassignmentsKey = 45;

    // Пустой батч — защитный guard (процесс такие не подаёт).
    public async Task<Result> AlterReassignmentsAsync(
        string bootstrap, string user, string password, string? caPem,
        IReadOnlyList<ReassignMove> moves, TimeSpan connectAndCommandTimeout, CancellationToken ct)
    {
        if (moves.Count == 0)
            return Result.Failed(new ApplicationException("kfw-reassign: empty reassignment batch"));

        // Fail-fast: TLS-валидация брокера требует per-cluster CA; пустое
        // значение — конфигурационная ошибка, а не повод ходить в канал без
        // доверия (admin-пароль по SASL/PLAIN). Пустой/мусорный PEM (0 якорей)
        // и так fail-closed на рукопожатии — здесь закрываем только null.
        if (string.IsNullOrWhiteSpace(caPem))
            return Result.Failed(new ApplicationException(
                "kfw-reassign: ca_pem missing — TLS validation requires the cluster CA"));

        var endpoints = ParseEndpoints(bootstrap);
        if (endpoints.Count == 0)
            return Result.Failed(new ApplicationException($"kfw-reassign: invalid bootstrap '{bootstrap}'"));

        // Якоря доверия (bundle OLD+NEW окна ротации) парсятся ОДИН раз на
        // вызов и диспозятся по его завершении — не на каждую endpoint-попытку.
        // Битый CERTIFICATE-блок — конфигурационная ошибка: fail-fast до сети,
        // симметрично guard'у «ca_pem missing» (Result.Failed, а не исключение
        // в каркас адаптера — unhealthy-пометка не срабатывает, spec §4.4).
        IReadOnlyList<X509Certificate2> anchors;
        try
        {
            anchors = ParseCertificates(caPem);
        }
        catch (Exception ex)
        {
            return Result.Failed(new ApplicationException($"kfw-reassign: ca_pem invalid: {ex.Message}", ex));
        }

        try
        {
            // Failover по списку CLIENT endpoints (spec §7.6): первый полный
            // успех выигрывает; AlterPartitionReassignments форвардится на
            // контроллер — годится любой брокер. Протокольный ответ брокера с
            // ошибкой (top-level/per-partition) НЕ фейловерится: логическая
            // ошибка Kafka одинакова для всех брокеров (запрос форвардится на
            // контроллер) — повтор на следующем endpoint дал бы ту же самую
            // подачу и тот же ответ; Immediate Failed — ровно как один брокер.
            var attemptErrors = new List<string>();
            foreach (var (host, port) in endpoints)
            {
                var attempt = await TryEndpointAsync(
                    host, port, user, password, anchors, moves, connectAndCommandTimeout, ct);
                if (attempt.Outcome.IsSuccess)
                    return Result.Success();

                if (attempt.ProtocolAnswered)
                    return attempt.Outcome;

                attemptErrors.Add(attempt.Outcome.Error!.Message);
            }

            return Result.Failed(new ApplicationException(
                $"kfw-reassign: all bootstrap endpoints failed: {string.Join("; ", attemptErrors)}"));
        }
        finally
        {
            foreach (var anchor in anchors)
                anchor.Dispose();
        }
    }

    // Одна endpoint-попытка: connect + TLS + SASL + round-trip под единым
    // бюджетом (linked CTS). Исход — (Result, ProtocolAnswered): получен ли
    // валидный протокольный ответ брокера на любой из запросов диалога
    // (ошибка ApiVersions/SaslHandshake/SaslAuthenticate, итог
    // AlterReassignments). Транспортные отказы (connect/TLS/чтение кадра/битые
    // данные/correlation mismatch/таймаут) — ProtocolAnswered=false. Отмена
    // host'а пробрасывается (адаптер не помечает клиента unhealthy).
    private static async Task<(Result Outcome, bool ProtocolAnswered)> TryEndpointAsync(
        string host, int port, string user, string password,
        IReadOnlyList<X509Certificate2> anchors,
        IReadOnlyList<ReassignMove> moves, TimeSpan timeout, CancellationToken ct)
    {
        try
        {
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeoutCts.CancelAfter(timeout);
            var token = timeoutCts.Token;

            using var tcp = new TcpClient();
            await tcp.ConnectAsync(host, port, token);

            // TLS: доверие — ВСЕ CERTIFICATE-блоки caPem (bundle OLD+NEW окна
            // ротации) + SAN хоста endpoint (SAN-правило arch/16 §2.3); caPem
            // null отсечён guard'ом выше — валидация всегда включена.
            bool Validate(object sender, X509Certificate? certificate, X509Chain? chain, SslPolicyErrors errors)
                => certificate is not null
                   && Shared.Tls.TlsChain.ValidateChain(certificate, anchors)
                   && SanMatchesHost(certificate, host);

            using var ssl = new SslStream(tcp.GetStream(), false, Validate);
            await ssl.AuthenticateAsClientAsync(new SslClientAuthenticationOptions
            {
                TargetHost = host,
                RemoteCertificateValidationCallback = Validate,
            }, token);

            var correlation = 0;

            // ApiVersions v0 до аутентификации (SASL-листенер отвечает pre-auth):
            // fail-fast по поддержке apiKey 45. Далее все возвраты — после
            // валидного ответа брокера (ProtocolAnswered=true).
            correlation++;
            var versions = new KafkaProtocolWriter();
            versions.WriteRequestHeaderV1(ApiVersionsKey, 0, correlation, ClientId);
            var versionsBody = await RoundTripAsync(
                ssl, versions.ToFrame(), headerV1: false, correlation, "api-versions", token);
            var (supported, versionsError) = ParseApiVersions(versionsBody);
            if (!supported)
                return (Result.Failed(new ApplicationException(
                    $"kfw-reassign {host}:{port} api-versions: {versionsError}")), ProtocolAnswered: true);

            // SaslHandshake v1: механизм PLAIN (задан конфигом кластера; список
            // механизмов ответа читаем, но не сверяем жёстко).
            correlation++;
            var handshake = new KafkaProtocolWriter();
            handshake.WriteRequestHeaderV1(SaslHandshakeKey, 1, correlation, ClientId);
            handshake.WriteString("PLAIN");
            var handshakeBody = await RoundTripAsync(
                ssl, handshake.ToFrame(), headerV1: false, correlation, "sasl-handshake", token);
            var handshakeReader = new KafkaProtocolReader(handshakeBody);
            var handshakeError = handshakeReader.ReadInt16();
            _ = handshakeReader.ReadArrayHeader(); // механизмы — не сверяем
            if (handshakeError != 0)
                return (Result.Failed(new ApplicationException(
                    $"kfw-reassign {host}:{port} sasl-handshake: error {handshakeError}")), ProtocolAnswered: true);

            // SaslAuthenticate v0: PLAIN-токен \0user\0password (пустой authzid).
            correlation++;
            var auth = new KafkaProtocolWriter();
            auth.WriteRequestHeaderV1(SaslAuthenticateKey, 0, correlation, ClientId);
            auth.WriteBytes([0x00, .. Encoding.UTF8.GetBytes(user), 0x00, .. Encoding.UTF8.GetBytes(password)]);
            var authBody = await RoundTripAsync(
                ssl, auth.ToFrame(), headerV1: false, correlation, "sasl-authenticate", token);
            var authReader = new KafkaProtocolReader(authBody);
            var authError = authReader.ReadInt16();
            var authMessage = authReader.ReadString(); // nullable old-style
            _ = authReader.ReadBytes(); // auth_bytes — читаем и игнорируем
            if (authError != 0)
                return (Result.Failed(new ApplicationException(
                    $"kfw-reassign {host}:{port} sasl-authenticate: {authMessage ?? $"error {authError}"}")), ProtocolAnswered: true);

            // AlterPartitionReassignments v0 (flexible, header v2). Идемпотентна
            // (KIP-455): повторная подача того же assignment замещает текущий.
            correlation++;
            var request = BuildAlterReassignmentsRequest(moves, correlation);
            var body = await RoundTripAsync(
                ssl, request, headerV1: true, correlation, "alter-reassignments", token);
            return (ParseAlterReassignmentsResponse(body), ProtocolAnswered: true);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw; // остановка host'а — не «endpoint молчит»
        }
        catch (OperationCanceledException ex)
        {
            return (Result.Failed(new TimeoutException(
                $"kfw-reassign {host}:{port}: endpoint budget {timeout.TotalSeconds:F0} s exhausted", ex)), ProtocolAnswered: false);
        }
        catch (System.Security.Authentication.AuthenticationException ex)
        {
            return (Result.Failed(new ApplicationException(
                $"kfw-reassign {host}:{port} tls: {ex.Message}", ex)), ProtocolAnswered: false);
        }
        catch (Exception ex)
        {
            return (Result.Failed(new ApplicationException(
                $"kfw-reassign {host}:{port}: {ex.Message}", ex)), ProtocolAnswered: false);
        }
    }

    // Записать кадр, прочитать кадр-ответ, разобрать ResponseHeader (v0 для
    // не-flexible, v1 + tagged-скип для flexible) и сверить correlation id.
    private static async Task<byte[]> RoundTripAsync(
        SslStream ssl, byte[] frame, bool headerV1, int expectedCorrelation, string stage, CancellationToken ct)
    {
        await ssl.WriteAsync(frame, ct);
        await ssl.FlushAsync(ct);
        var payload = await ReadFrameAsync(ssl, ct);
        var reader = new KafkaProtocolReader(payload);
        var correlation = headerV1 ? reader.ReadResponseHeaderV1() : reader.ReadResponseHeaderV0();
        if (correlation != expectedCorrelation)
            throw new ApplicationException(
                $"{stage}: correlation mismatch (expected {expectedCorrelation}, got {correlation})");
        return payload[(payload.Length - reader.Remaining)..];
    }

    // Кадр ответа: int32 длина + ровно length байт payload. Длина сверяется
    // с санитарной границей ДО аллокации (см. MaxFrameLength/MinFrameLength).
    // internal — прямой юнит-тест границы (InternalsVisibleTo, класс internal).
    internal static async Task<byte[]> ReadFrameAsync(Stream stream, CancellationToken ct)
    {
        var header = new byte[4];
        await ReadExactlyAsync(stream, header, ct);
        var length = (header[0] << 24) | (header[1] << 16) | (header[2] << 8) | header[3];
        if (length < MinFrameLength || length > MaxFrameLength)
            throw new ApplicationException($"Kafka protocol: invalid frame length {length}");
        var payload = new byte[length];
        await ReadExactlyAsync(stream, payload, ct);
        return payload;
    }

    private static async Task ReadExactlyAsync(Stream stream, byte[] buffer, CancellationToken ct)
    {
        var offset = 0;
        while (offset < buffer.Length)
        {
            var read = await stream.ReadAsync(buffer.AsMemory(offset), ct);
            if (read == 0)
                throw new ApplicationException("Kafka protocol: stream closed mid-frame");
            offset += read;
        }
    }

    // Запрос AlterPartitionReassignments v0 (flexible, header v2). Тело —
    // Приложение A.4: timeout_ms=60000 (апстрим-дефолт; таймаут обработки
    // заявки, НЕ ожидание физического переноса данных), compact-массив топиков,
    // реплики всегда не-null (отмену переезда не используем), пустые
    // tagged-секции. Группировка: OrderBy(Topic).ThenBy(Partition) —
    // детерминированный кадр (golden-тест).
    internal static byte[] BuildAlterReassignmentsRequest(IReadOnlyList<ReassignMove> moves, int correlationId)
    {
        var w = new KafkaProtocolWriter();
        w.WriteRequestHeaderV2(AlterReassignmentsKey, 0, correlationId, ClientId);
        w.WriteInt32(60_000); // timeout_ms
        var topics = moves
            .GroupBy(m => m.Topic, StringComparer.Ordinal)
            .OrderBy(g => g.Key, StringComparer.Ordinal)
            .ToList();
        w.WriteCompactArrayHeader(topics.Count);
        foreach (var topic in topics)
        {
            w.WriteCompactString(topic.Key);
            var partitions = topic.OrderBy(m => m.Partition).ToList();
            w.WriteCompactArrayHeader(partitions.Count);
            foreach (var move in partitions)
            {
                w.WriteInt32(move.Partition);
                w.WriteCompactArrayHeader(move.Replicas.Count); // НЕ null — отмена не используется
                foreach (var replica in move.Replicas)
                    w.WriteInt32(replica);
                w.WriteTaggedSection(); // tagged партиции — после её полей
            }
            // tagged топика — ПОСЛЕ всех его полей (name, partitions): апстрим
            // AlterPartitionReassignmentsRequest.json — tagged-секция структурного
            // объекта одна и всегда в конце.
            w.WriteTaggedSection();
        }
        w.WriteTaggedSection(); // tagged тела
        return w.ToFrame();
    }

    // Ответ AlterPartitionReassignments v0 (тело ПОСЛЕ ResponseHeader v1):
    // throttle_time_ms, error_code, nullable error_message, compact-массив
    // responses (nullable: 0 при top-level ошибке). Top-level ИЛИ любой
    // per-partition error ≠ 0 → единый Result.Failed со сводкой (spec §7.13).
    internal static Result ParseAlterReassignmentsResponse(byte[] body)
    {
        var r = new KafkaProtocolReader(body);
        _ = r.ReadInt32(); // throttle_time_ms
        var error = r.ReadInt16();
        var message = r.ReadCompactString();
        var failures = new List<string>();
        if (error != 0)
            failures.Add($"top-level error {error}" + (message is null ? "" : $" ({message})"));

        var topicsCount = r.ReadCompactArrayHeader();
        for (var t = 0; t < topicsCount; t++)
        {
            var topic = r.ReadCompactString();
            // tagged-секция топика — ПОСЛЕ его полей (name, partitions):
            // апстрим AlterPartitionReassignmentsResponse.json.
            var partitionsCount = r.ReadCompactArrayHeader();
            for (var p = 0; p < partitionsCount; p++)
            {
                var partition = r.ReadInt32();
                var partitionError = r.ReadInt16();
                var partitionMessage = r.ReadCompactString();
                r.SkipTaggedSection(); // tagged партиции — после её полей
                if (partitionError != 0)
                    failures.Add($"{topic}[{partition}]: {partitionError}"
                        + (partitionMessage is null ? "" : $" ({partitionMessage})"));
            }

            r.SkipTaggedSection(); // tagged топика
        }

        r.SkipTaggedSection(); // tagged тела

        return failures.Count == 0
            ? Result.Success()
            : Result.Failed(new ApplicationException(
                $"AlterPartitionReassignments: {string.Join("; ", failures)}"));
    }

    // Ответ ApiVersions v0 (тело ПОСЛЕ ResponseHeader v0): error_code,
    // old-style массив {api_key, min, max}. Fail-fast: apiKey 45 обязан
    // поддерживать версию 0 (spec §4.2).
    internal static (bool Supported, string? Error) ParseApiVersions(byte[] body)
    {
        var r = new KafkaProtocolReader(body);
        var error = r.ReadInt16();
        if (error != 0)
            return (false, $"error {error}");
        var count = r.ReadArrayHeader();
        var hasAlter = false;
        for (var i = 0; i < count; i++)
        {
            var apiKey = r.ReadInt16();
            var min = r.ReadInt16();
            var max = r.ReadInt16();
            if (apiKey == AlterReassignmentsKey && min <= 0 && 0 <= max)
                hasAlter = true;
        }

        return hasAlter
            ? (true, null)
            : (false, "broker does not support AlterPartitionReassignments");
    }

    // Все CERTIFICATE-блоки PEM (bundle окна ротации OLD+NEW → 2 якоря).
    // Битый блок посреди bundle (например, не-base64 тело): уже созданные
    // якоря диспозятся, исходное исключение пробрасывается — вызывающий
    // оформляет fail-fast «ca_pem invalid».
    internal static IReadOnlyList<X509Certificate2> ParseCertificates(string caPem)
    {
        var anchors = new List<X509Certificate2>();
        try
        {
            var lines = caPem.Split('\n');
            var block = new List<string>();
            foreach (var raw in lines)
            {
                var line = raw.Trim();
                if (line.StartsWith("-----BEGIN CERTIFICATE-----", StringComparison.Ordinal))
                {
                    block.Clear();
                    block.Add(line);
                }
                else if (line.StartsWith("-----END CERTIFICATE-----", StringComparison.Ordinal) && block.Count > 0)
                {
                    block.Add(line);
                    // SYSLIB0057: в .NET 10 загрузка серта из байтов — через
                    // X509CertificateLoader (конструктор устарел).
                    anchors.Add(X509CertificateLoader.LoadCertificate(
                        Encoding.UTF8.GetBytes(string.Join('\n', block))));
                    block.Clear();
                }
                else if (block.Count > 0)
                {
                    block.Add(line);
                }
            }
        }
        catch
        {
            foreach (var anchor in anchors)
                anchor.Dispose();
            throw;
        }

        return anchors;
    }

    // SAN-хост (arch/16 §2.3): advertised DNS либо IP; сверяем весь список SAN.
    // Владение: сертификат SslStream — чужой (диспозится SslStream, повторный
    // dispose чужого недопустим) — using только для заново созданной копии.
    private static bool SanMatchesHost(X509Certificate certificate, string host)
    {
        var cert2 = certificate as X509Certificate2;
        using var owned = cert2 is null ? new X509Certificate2(certificate) : null;
        var cert = cert2 ?? owned!;
        var san = cert.Extensions.OfType<X509SubjectAlternativeNameExtension>().FirstOrDefault();
        if (san is null)
            return false;
        if (System.Net.IPAddress.TryParse(host, out var ip))
            return san.EnumerateIPAddresses().Contains(ip);
        return san.EnumerateDnsNames().Contains(host, StringComparer.OrdinalIgnoreCase);
    }

    // bootstrap "host:port[,host:port…]" → endpoints. Порция без порта или с
    // некорректным числом — скипается (валидные соседи живут).
    internal static List<(string Host, int Port)> ParseEndpoints(string bootstrap)
    {
        var endpoints = new List<(string Host, int Port)>();
        foreach (var raw in bootstrap.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var separator = raw.LastIndexOf(':');
            if (separator <= 0 || !int.TryParse(raw[(separator + 1)..], out var port))
                continue;
            endpoints.Add((raw[..separator], port));
        }

        return endpoints;
    }
}
