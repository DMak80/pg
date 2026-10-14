using OwnS3.Protocol.Errors;

namespace OwnS3.Replication.Transport;

// Новые типизированные ошибки транспорта (конвенция главы 01 §4): кладутся в
// Result.Error, не летят исключениями через границу IReplicationTransport.

/// <summary>Сосед не принял рассылку: 429 (приёмный буфер переполнен),
/// не-200 ответ или обрыв транспорта — вызывающий бэкоффит (глава 07 §8).</summary>
public sealed class PeerRejectedException(string message) : Exception(message);

/// <summary>Probe: имя цели не входит в состав targets снапшота — маркер 400
/// для хендлера (глава 07 §2).</summary>
public sealed class UnknownReplicationTargetException(string message) : Exception(message);

// ── t39: доменные ошибки join-handshake (глава 07 §2, конвенция 01 §4) ──

/// <summary>Конфигурация инициатора join неполна (нет identity, trust-anchor
/// или advertised endpoint) — маркер 400 админ-грани; fail-fast: без
/// trust-anchor инициатор не проверит сертификат ответа соседа (spec §4.4 п.1).</summary>
public sealed class JoinBadConfigurationException(string message) : Exception(message);

/// <summary>Сосед отказал в join — маркер 403: trust-anchor не задан (ручной
/// режим состава), сертификат невалиден (CA/срок/deny-list) или подпись
/// запроса не проходит. ErrorCode — исходный код каталога подписи
/// (SignatureDoesNotMatch/RequestTimeTooSkewed/…), когда отказ вызван именно
/// проверкой подписи запроса; грань маппит errorCode ?? AccessDenied.</summary>
public sealed class JoinRefusedException(string message, S3ErrorCode? errorCode = null)
    : Exception(message)
{
    public S3ErrorCode? ErrorCode { get; } = errorCode;
}

/// <summary>Join-конфликт: имя числится в targets с другим публичным ключом —
/// маркер 409 (глава 07 §2).</summary>
public sealed class JoinConflictException(string message) : Exception(message);

/// <summary>Join не состоялся по вине соседа/транспорта: не-200 (кроме
/// 403/409), битое тело, невалидный сертификат/подпись ответа, обрыв —
/// маркер 502 (глава 07 §2: состав не менялся).</summary>
public sealed class JoinBadGatewayException(string message) : Exception(message);

// ── owns3-denylist-replace: ошибки входящей перезаписи deny-list
// (глава 07 §2/§6, конвенция 01 §4) ──

/// <summary>Перезапись deny-list отказана — маркер 403: конфигурация (ручной
/// режим состава), доверие (CA/истёк/deny-list вызывающего) или подпись запроса.
/// ErrorCode — исходный код каталога подписи для отказов верификации; грань
/// отдаст errorCode ?? AccessDenied (AuthorizationHeaderMalformed → 400).</summary>
public sealed class DenyReplaceRefusedException(string message, S3ErrorCode? errorCode = null)
    : Exception(message)
{
    public S3ErrorCode? ErrorCode { get; } = errorCode;
}

/// <summary>Тело перезаписи malformed/невалидно (не-JSON, нет entries, пустые
/// поля, битый PEM, дубли имён/ключей) — маркер 400 JSON {error}; список
/// не менялся.</summary>
public sealed class DenyReplaceBadRequestException(string message) : Exception(message);

/// <summary>Исход входящей перезаписи: применённый состав (число записей —
/// лог/тесты, глава 07 §10).</summary>
public sealed record DenyListReplaced(int Count);

/// <summary>Исход исходящей стороны JoinAsync: сосед добавлен в состав.</summary>
public sealed record PeerJoined(string Name, string Endpoint, string PublicKeyPem);

/// <summary>Исход входящей стороны HandleIncoming: пира приняли; тело ответа
/// {"name","advertisedEndpoint","certificate"} грань отдаёт как есть вместе
/// с SignatureBase64 в заголовке X-OwnS3-Peer-Signature.</summary>
public sealed record JoinAccept(string Name, string AdvertisedEndpoint,
    string CertificateBase64, string SignatureBase64);
