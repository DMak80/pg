using System.Globalization;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;
using OwnS3.Protocol.Auth;
using OwnS3.Protocol.Errors;
using OwnS3.Protocol.PeerAuth;
using OwnS3.Protocol.Requests;
using OwnS3.Replication.Transport;

namespace OwnS3.Replication;

// Обе стороны join-handshake (t39, глава 07 §2/spec §4.4): JoinAsync — исходящий
// запрос инициатора и проверка ответа соседа; HandleIncoming — входящая обработка
// (до-join аутентификация по сертификату, CA/deny/конфликт, добавление записи,
// подписанный ответ). Исходы — Result-конвенция (глава 01 §4); отдельный класс —
// тестируем без хоста (фейк-транспорт, прецедент ReplicationProber).
// Join не ставит событий репликации (анти-петля) и не меняет курсоры.
public sealed class JoinPeerService(
    PeerIdentity? identity,
    PeerTrustAnchor? trustAnchor,
    ReplicationTargets targets,
    DenyList denyList,
    HttpClient http,
    TimeProvider timeProvider,
    ILogger<JoinPeerService> logger,
    string? advertisedEndpoint)
{
    private static readonly JsonSerializerOptions Json = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.Never,
    };

    // Транспорт join-контура с капом таймаута (по образцу ReplicationHttpClient):
    // дефолтные 100 с HttpClient держали бы синхронный admin-join на «зависшем»
    // соседе; отмена по таймауту — TaskCanceledException при НЕ отменённом внешнем
    // ct → 502 (spec §4.4 п.4), внешняя отмена (graceful stop) — проброс.
    private readonly HttpClient _http = CapRequestTimeout(http);

    private static HttpClient CapRequestTimeout(HttpClient http)
    {
        if (http.Timeout == default || http.Timeout > ReplicationHttpClient.RequestTimeout)
            http.Timeout = ReplicationHttpClient.RequestTimeout;
        return http;
    }

    // Тела join-запроса/ответа (стабильная сериализация camelCase — ответ
    // подписывается как есть, инициатор проверяет подпись по сырому телу).
    private sealed record JoinRequestBody(string AdvertisedEndpoint);

    private sealed record JoinResponseBody(string Name, string AdvertisedEndpoint, string Certificate);

    private sealed record JoinResponse(string Name, string AdvertisedEndpoint, string Certificate);

    // Исходящая сторона (spec §4.4 п.1/п.2/п.4): POST {endpoint}/replication/join
    // с peer-подписью и сертификатом в заголовке; 200 — верификация сертификата
    // соседа против СВОЕГО trust-anchor и подписи тела → добавление записи →
    // PeerJoined; 403/409 проксируются; прочее — 502 (состав не менялся).
    public async Task<Result<PeerJoined>> JoinAsync(string endpoint, CancellationToken ct)
    {
        // Fail-fast инициатора: 400 с пояснением — без trust-anchor инициатор
        // не сможет проверить сертификат ответа соседа (spec §4.4 п.1).
        if (identity is null || trustAnchor is null)
            return Result<PeerJoined>.Failed(new JoinBadConfigurationException(
                "join недоступен: не заданы identity и/или trust-anchor " +
                "(env OWNS3_REPLICATION_CERT/_PRIVATE_KEY, OWNS3_REPLICATION_TRUST_CA)"));
        if (string.IsNullOrWhiteSpace(advertisedEndpoint))
            return Result<PeerJoined>.Failed(new JoinBadConfigurationException(
                "join недоступен: не задан advertised endpoint " +
                "(env OWNS3_REPLICATION_ADVERTISED_ENDPOINT)"));
        // Endpoint из админ-тела — до запроса соседа (spec §4.4 п.1): невалидный —
        // 400-конфигурация, а не UriFormatException сквозь Result-границу (500).
        if (!Uri.TryCreate(endpoint, UriKind.Absolute, out var neighbor)
            || neighbor.Scheme is not ("http" or "https")
            || string.IsNullOrEmpty(neighbor.Host))
            return Result<PeerJoined>.Failed(new JoinBadConfigurationException(
                $"join недоступен: невалидный endpoint соседа «{endpoint}» (ожидалось scheme://host:port)"));

        var body = JsonSerializer.SerializeToUtf8Bytes(new JoinRequestBody(advertisedEndpoint), Json);
        var payloadHash = SigV4Core.HexSha256(body);
        var at = timeProvider.GetUtcNow();
        var peerDate = at.ToString("yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture);
        var (authorization, _, _) = PeerRequestSigner.Sign(identity.Key, identity.Name,
            "POST", "/replication/join", "",
            [("host", neighbor.Authority),
                ("x-owns3-peer-date", peerDate),
                ("x-owns3-peer-content-sha256", payloadHash)],
            payloadHash, at);
        using var request = new HttpRequestMessage(HttpMethod.Post,
            $"{endpoint.TrimEnd('/')}/replication/join")
        {
            Content = new ByteArrayContent(body),
        };
        request.Headers.TryAddWithoutValidation("host", neighbor.Authority);
        request.Headers.TryAddWithoutValidation("x-owns3-peer-date", peerDate);
        request.Headers.TryAddWithoutValidation("x-owns3-peer-content-sha256", payloadHash);
        request.Headers.TryAddWithoutValidation("Authorization", authorization);
        // Сертификат «на проводе» (глава 07 §2): base64 от UTF-8 байтов JSON.
        request.Headers.TryAddWithoutValidation("X-OwnS3-Peer-Certificate",
            Convert.ToBase64String(Encoding.UTF8.GetBytes(identity.CertificateJson)));

        HttpResponseMessage response;
        try
        {
            response = await _http.SendAsync(request, HttpCompletionOption.ResponseContentRead, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw; // внешняя отмена — не исход операции (graceful stop)
        }
        catch (Exception ex)
        {
            return Result<PeerJoined>.Failed(new JoinBadGatewayException(
                $"join к {endpoint}: транспорт — {ex.Message}"));
        }

        using (response)
        {
            // Проксирование исходов соседа (spec §4.4 п.4): отказ/конфликт — тот же код.
            if (response.StatusCode == HttpStatusCode.Forbidden)
                return Result<PeerJoined>.Failed(new JoinRefusedException(
                    $"сосед {endpoint} отказал в join: {await BodyTextAsync(response, ct)}"));
            if (response.StatusCode == HttpStatusCode.Conflict)
                return Result<PeerJoined>.Failed(new JoinConflictException(
                    $"сосед {endpoint} отклонил join: {await BodyTextAsync(response, ct)}"));
            if (!response.IsSuccessStatusCode)
                return Result<PeerJoined>.Failed(new JoinBadGatewayException(
                    $"join к {endpoint}: HTTP {(int)response.StatusCode}"));

            byte[] raw;
            string? signatureHeader;
            try
            {
                raw = await response.Content.ReadAsByteArrayAsync(ct);
                signatureHeader = response.Headers.TryGetValues("X-OwnS3-Peer-Signature", out var values)
                    ? values.FirstOrDefault()
                    : null;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                return Result<PeerJoined>.Failed(new JoinBadGatewayException(
                    $"join к {endpoint}: тело/заголовки ответа не читаются — {ex.Message}"));
            }

            JoinResponse parsed;
            PeerCertificate peerCert;
            try
            {
                parsed = JsonSerializer.Deserialize<JoinResponse>(raw, Json)
                         ?? throw new InvalidOperationException("пустое тело join-ответа");
                peerCert = PeerCertificate.FromJson(Encoding.UTF8.GetString(
                    Convert.FromBase64String(parsed.Certificate)));
            }
            catch (Exception ex)
            {
                return Result<PeerJoined>.Failed(new JoinBadGatewayException(
                    $"join к {endpoint}: невалидный ответ соседа — {ex.Message}"));
            }

            // Сертификат соседа — против СВОЕГО trust-anchor (spec §4.4 п.4).
            if (!PeerCertificateAuthority.Verify(peerCert, trustAnchor, timeProvider.GetUtcNow()))
                return Result<PeerJoined>.Failed(new JoinBadGatewayException(
                    $"join к {endpoint}: сертификат соседа {peerCert.Name} не проходит " +
                    "верификацию trust-anchor (CA/срок)"));

            // Подпись тела — приватным ключом соседа, проверка ключом его сертификата.
            if (signatureHeader is null)
                return Result<PeerJoined>.Failed(new JoinBadGatewayException(
                    $"join к {endpoint}: нет заголовка X-OwnS3-Peer-Signature"));
            bool verified;
            try
            {
                using var peerKey = ECDsa.Create();
                peerKey.ImportFromPem(peerCert.PublicKeyPem);
                verified = peerKey.VerifyData(raw, Convert.FromBase64String(signatureHeader),
                    HashAlgorithmName.SHA256, DSASignatureFormat.IeeeP1363FixedFieldConcatenation);
            }
            catch (Exception ex)
            {
                return Result<PeerJoined>.Failed(new JoinBadGatewayException(
                    $"join к {endpoint}: ключ/подпись соседа не читаются — {ex.Message}"));
            }
            if (!verified)
                return Result<PeerJoined>.Failed(new JoinBadGatewayException(
                    $"join к {endpoint}: подпись тела join-ответа не сходится"));

            // Состав — merge + персист + горячая замена (глава 07 §2); дальше —
            // штатная механика «добавленный сосед» (курсор −1 + reconcile-забор).
            // AddOrReplace валидирует состав ДО персиста (бросок ArgumentException —
            // снапшот не тронут): невалидный advertised из ОТВЕТА соседа (его вина —
            // сосед без своего advertised отвечает «») → 502, состав не менялся
            // (spec §4.4 п.4), а не ArgumentException сквозь Result-границу (500).
            try
            {
                targets.AddOrReplace(new TargetRecord(parsed.Name, parsed.AdvertisedEndpoint,
                    TargetRecord.AuthKey, PublicKeyPem: peerCert.PublicKeyPem));
            }
            catch (ArgumentException ex)
            {
                return Result<PeerJoined>.Failed(new JoinBadGatewayException(
                    $"join к {endpoint}: сосед вернул невалидную запись (advertised " +
                    $"«{parsed.AdvertisedEndpoint}») — {ex.Message}"));
            }
            logger.LogInformation(
                "join: сосед {Name} ({Endpoint}) добавлен в состав (auth=key)",
                parsed.Name, parsed.AdvertisedEndpoint);
            return Result<PeerJoined>.Success(
                new PeerJoined(parsed.Name, parsed.AdvertisedEndpoint, peerCert.PublicKeyPem));
        }
    }

    // Входящая сторона (spec §4.4 п.3, глава 07 §6): до-join аутентификация по
    // сертификату — trust-anchor не задан → отказ (ручной режим); сертификат
    // невалиден (CA/срок/deny-list по имени ИЛИ ключу) → отказ; подпись запроса
    // верифицируется ключом сертификата; конфликт имени (другой ключ) → 409;
    // успех/идемпотентность → запись + подписанный ответ.
    public Result<JoinAccept> HandleIncoming(S3RequestModel model, string certificateBase64,
        string peerAdvertisedEndpoint)
    {
        // trust-anchor не задан → ручной режим состава; identity не задан →
        // нечем подписать ответ. Оба случая — отказ (403 на replication-грани).
        if (identity is null || trustAnchor is null)
            return Result<JoinAccept>.Failed(new JoinRefusedException(
                "join отказан: identity и/или trust-anchor не заданы (ручной режим состава)"));

        PeerCertificate cert;
        try
        {
            cert = PeerCertificate.FromJson(
                Encoding.UTF8.GetString(Convert.FromBase64String(certificateBase64)));
        }
        catch (Exception ex)
        {
            logger.LogWarning("join: сертификат пира не читается — {Message}", ex.Message);
            return Result<JoinAccept>.Failed(new JoinRefusedException(
                $"join отказан: сертификат пира не читается — {ex.Message}"));
        }

        // Сертификат валиден ⟺ caName совпадает ∧ подпись CA верна ∧ не истёк.
        if (!PeerCertificateAuthority.Verify(cert, trustAnchor, timeProvider.GetUtcNow()))
        {
            logger.LogWarning(
                "join: сертификат пира {Name} не проходит верификацию trust-anchor (CA/срок)",
                cert.Name);
            return Result<JoinAccept>.Failed(new JoinRefusedException(
                $"join отказан: сертификат пира {cert.Name} не проходит верификацию trust-anchor (CA/срок)"));
        }

        // Deny-list — по имени ИЛИ публичному ключу (глава 07 §2).
        if (denyList.IsDenied(cert.Name, cert.PublicKeyPem))
        {
            logger.LogWarning("join: пир {Name} отозван (deny-list)", cert.Name);
            return Result<JoinAccept>.Failed(new JoinRefusedException(
                $"join отказан: пир {cert.Name} отозван (deny-list)"));
        }

        using var peerKey = ECDsa.Create();
        try
        {
            peerKey.ImportFromPem(cert.PublicKeyPem);
        }
        catch (Exception ex)
        {
            return Result<JoinAccept>.Failed(new JoinRefusedException(
                $"join отказан: публичный ключ сертификата пира не импортируется — {ex.Message}"));
        }

        // Подпись запроса — публичным ключом сертификата (глава 07 §6);
        // ErrorCode зеркалит каталог подписи (грань отдаст errorCode ?? AccessDenied).
        var verify = new PeerRequestVerifier(timeProvider).Verify(model, peerKey);
        if (verify is not PeerVerifyResult.Ok)
        {
            var code = verify is PeerVerifyResult.Fail fail ? (S3ErrorCode?)fail.Code : null;
            logger.LogWarning("join: подпись запроса пира {Name} не проходит ({Code})",
                cert.Name, code);
            return Result<JoinAccept>.Failed(new JoinRefusedException(
                $"join отказан: подпись запроса пира {cert.Name} не проходит верификацию", code));
        }

        // Конфликт имени: числится как key-запись с ДРУГИМ публичным ключом → 409.
        if (targets.TryGet(cert.Name, out var existing)
            && existing.Auth == TargetRecord.AuthKey
            && !string.Equals(existing.PublicKeyPem, cert.PublicKeyPem, StringComparison.Ordinal))
        {
            logger.LogWarning("join: имя {Name} числится с другим публичным ключом", cert.Name);
            return Result<JoinAccept>.Failed(new JoinConflictException(
                $"join отклонён: имя {cert.Name} числится с другим публичным ключом"));
        }

        // Успех/идемпотентность: merge + персист + горячая замена (глава 07 §2);
        // endpoint пира — его advertised из join-запроса.
        targets.AddOrReplace(new TargetRecord(cert.Name, peerAdvertisedEndpoint, TargetRecord.AuthKey,
            PublicKeyPem: cert.PublicKeyPem));

        // Ответ: тело собирается ОДИН раз и подписывается как есть (spec §4.4 п.3 —
        // «advertisedEndpoint» в ответе — СВОЙ advertised соседа, поле ctor из env).
        // Null advertised у приёмника — вне специфицированных сценариев (оркестратор
        // ставит env всем узлам): инициатор откажет такую запись валидацией endpoint.
        var accept = new JoinAccept(identity.Name, advertisedEndpoint ?? string.Empty,
            Convert.ToBase64String(Encoding.UTF8.GetBytes(identity.CertificateJson)), string.Empty);
        var body = JsonSerializer.Serialize(
            new JoinResponseBody(accept.Name, accept.AdvertisedEndpoint, accept.CertificateBase64),
            Json);
        var signature = Convert.ToBase64String(identity.Key.SignData(Encoding.UTF8.GetBytes(body),
            HashAlgorithmName.SHA256, DSASignatureFormat.IeeeP1363FixedFieldConcatenation));
        logger.LogInformation("join: пир {Name} ({Endpoint}) принят в состав (auth=key)",
            cert.Name, peerAdvertisedEndpoint);
        return Result<JoinAccept>.Success(accept with { SignatureBase64 = signature });
    }

    // Текст тела для пояснения отказа (обрезка — хвост лога без раздувания).
    private static async Task<string> BodyTextAsync(HttpResponseMessage response, CancellationToken ct)
    {
        try
        {
            var text = await response.Content.ReadAsStringAsync(ct);
            return text.Length > 200 ? text[..200] : text;
        }
        catch
        {
            return "<тело не читается>";
        }
    }
}
