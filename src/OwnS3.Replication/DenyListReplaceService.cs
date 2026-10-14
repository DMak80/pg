using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;
using OwnS3.Protocol.Errors;
using OwnS3.Protocol.PeerAuth;
using OwnS3.Protocol.Requests;
using OwnS3.Replication.Transport;

namespace OwnS3.Replication;

// Входящая сторона полной перезаписи deny-list (глава 07 §2/§6, PUT
// /replication/denylist): до-join контур — те же примитивы, что у входящего
// join (JoinPeerService.HandleIncoming): сертификат вызывающего →
// PeerCertificateAuthority.Verify против trust-anchor → DenyList.IsDenied →
// PeerRequestVerifier (ключ сертификата) → тело → DenyList.Replace. Проверки
// доверия — ДО разбора тела: malformed-тело от недоверенного источника не
// рулит кодом ответа. Тестируем без хоста (прецедент JoinPeerService);
// собственного транспорта нет — метод только входящий.
public sealed class DenyListReplaceService(
    PeerIdentity? identity,
    PeerTrustAnchor? trustAnchor,
    DenyList denyList,
    TimeProvider timeProvider,
    ILogger<DenyListReplaceService> logger)
{
    private static readonly JsonSerializerOptions Json = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.Never,
    };

    // Тело перезаписи (глава 07 §2): {entries:[{name, publicKeyPem}]};
    // revokedAtUnixMs ставит сервер — в теле не передаётся.
    private sealed record DenyReplaceEntry(string? Name, string? PublicKeyPem);

    private sealed record DenyReplaceBody(IReadOnlyList<DenyReplaceEntry>? Entries);

    // Шаги 1–9 §4.2: конфигурация → сертификат → доверие → подпись → тело.
    public Result<DenyListReplaced> HandleIncoming(S3RequestModel model, string certificateBase64,
        ReadOnlyMemory<byte> rawBody)
    {
        // 1. Ручной режим состава (identity не нужен для проверки — семантика
        //    та же, что у join: без trust-anchor перезапись недоступна).
        if (identity is null || trustAnchor is null)
            return Result<DenyListReplaced>.Failed(new DenyReplaceRefusedException(
                "перезапись deny-list отказана: identity и/или trust-anchor не заданы (ручной режим состава)"));

        // 2. Сертификат вызывающего.
        PeerCertificate cert;
        try
        {
            cert = PeerCertificate.FromJson(
                Encoding.UTF8.GetString(Convert.FromBase64String(certificateBase64)));
        }
        catch (Exception ex)
        {
            logger.LogWarning("deny-list replace: сертификат вызывающего не читается — {Message}", ex.Message);
            return Result<DenyListReplaced>.Failed(new DenyReplaceRefusedException(
                $"перезапись deny-list отказана: сертификат вызывающего не читается — {ex.Message}"));
        }

        // 3. Доверие: подписан CA trust-anchor ∧ не истёк.
        if (!PeerCertificateAuthority.Verify(cert, trustAnchor, timeProvider.GetUtcNow()))
        {
            logger.LogWarning(
                "deny-list replace: сертификат вызывающего {Name} не проходит верификацию trust-anchor (CA/срок)",
                cert.Name);
            return Result<DenyListReplaced>.Failed(new DenyReplaceRefusedException(
                $"перезапись deny-list отказана: сертификат вызывающего {cert.Name} не проходит верификацию trust-anchor (CA/срок)"));
        }

        // 4. Вызывающий не отозван (имя ИЛИ ключ) — отозванный не переписывает
        //    список отзывов.
        if (denyList.IsDenied(cert.Name, cert.PublicKeyPem))
        {
            logger.LogWarning("deny-list replace: вызывающий {Name} отозван (deny-list)", cert.Name);
            return Result<DenyListReplaced>.Failed(new DenyReplaceRefusedException(
                $"перезапись deny-list отказана: вызывающий {cert.Name} отозван (deny-list)"));
        }

        // 5. Подпись запроса — публичным ключом сертификата; ErrorCode зеркалит
        //    каталог подписи (грань отдаст errorCode ?? AccessDenied).
        using var callerKey = ECDsa.Create();
        try
        {
            callerKey.ImportFromPem(cert.PublicKeyPem);
        }
        catch (Exception ex)
        {
            return Result<DenyListReplaced>.Failed(new DenyReplaceRefusedException(
                $"перезапись deny-list отказана: публичный ключ сертификата вызывающего не импортируется — {ex.Message}"));
        }
        var verify = new PeerRequestVerifier(timeProvider).Verify(model, callerKey);
        if (verify is not PeerVerifyResult.Ok)
        {
            var code = verify is PeerVerifyResult.Fail fail ? (S3ErrorCode?)fail.Code : null;
            logger.LogWarning("deny-list replace: подпись запроса {Name} не проходит ({Code})",
                cert.Name, code);
            return Result<DenyListReplaced>.Failed(new DenyReplaceRefusedException(
                $"перезапись deny-list отказана: подпись запроса {cert.Name} не проходит верификацию", code));
        }

        // 6. Тело — ПОСЛЕ доверия/подписи.
        DenyReplaceBody? body;
        try
        {
            body = JsonSerializer.Deserialize<DenyReplaceBody>(rawBody.Span, Json);
        }
        catch (JsonException ex)
        {
            return Result<DenyListReplaced>.Failed(new DenyReplaceBadRequestException(
                $"malformed тело перезаписи deny-list — {ex.Message}"));
        }
        if (body?.Entries is null)
            return Result<DenyListReplaced>.Failed(new DenyReplaceBadRequestException(
                "тело перезаписи deny-list: ожидается {entries:[{name, publicKeyPem}]}"));

        // 7–8. Валидация записей — внутри Replace (ArgumentException до персиста)
        //      → 400-тип с пояснением; снапшот не тронут.
        var entries = body.Entries
            .Select(e => (e.Name ?? string.Empty, e.PublicKeyPem ?? string.Empty))
            .ToList();
        try
        {
            denyList.Replace(entries);
        }
        catch (ArgumentException ex)
        {
            return Result<DenyListReplaced>.Failed(new DenyReplaceBadRequestException(ex.Message));
        }

        // 9. Успех: лог-событие §10 (число записей); исход — count.
        logger.LogInformation("Полная перезапись deny-list: {Count} записей (вызывающий {Caller})",
            entries.Count, cert.Name);
        return Result<DenyListReplaced>.Success(new DenyListReplaced(entries.Count));
    }
}
