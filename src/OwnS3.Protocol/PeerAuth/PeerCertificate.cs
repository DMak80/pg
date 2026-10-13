using System.Text.Json;
using System.Text.Json.Serialization;

namespace OwnS3.Protocol.PeerAuth;

// Мини-сертификат пира ownS3 (глава 07 §2): JSON camelCase; подпись CA —
// P1363 base64 по канонической строке CanonicalForm(); expiresAtUnixMs ≤ 0 —
// бессрочный. X.509 сознательно не вводится.
public sealed record PeerCertificate(
    string Name, string PublicKeyPem, long IssuedAtUnixMs, long ExpiresAtUnixMs,
    string CaName, string Signature)
{
    private static readonly JsonSerializerOptions Json = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.Never,
    };

    public string ToJson() => JsonSerializer.Serialize(this, Json);

    // Бросает при malformed JSON / пустых обязательных полях (fail-fast загрузки).
    public static PeerCertificate FromJson(string json)
    {
        var cert = JsonSerializer.Deserialize<PeerCertificate>(json, Json)
            ?? throw new InvalidOperationException("пустой JSON сертификата пира");
        if (string.IsNullOrWhiteSpace(cert.Name) || string.IsNullOrWhiteSpace(cert.PublicKeyPem)
            || string.IsNullOrWhiteSpace(cert.CaName) || string.IsNullOrWhiteSpace(cert.Signature))
            throw new InvalidOperationException("сертификат пира: обязательные поля пусты");
        return cert;
    }

    // Каноническая строка подписи CA (глава 07 §2).
    public string CanonicalForm() =>
        $"{Name}\n{PublicKeyPem}\n{IssuedAtUnixMs}\n{ExpiresAtUnixMs}\n{CaName}";

    // Истёк ⟺ expiresAt положителен и наступил (≤ 0 — бессрочный, глава 07 §2).
    public bool IsExpired(DateTimeOffset now) =>
        ExpiresAtUnixMs > 0 && now.ToUnixTimeMilliseconds() >= ExpiresAtUnixMs;
}
