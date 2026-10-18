using System.Text.Json;
using System.Text.Json.Serialization;

namespace OwnS3.Protocol.PeerAuth;

// Trust-anchor инсталляции (глава 07 §2): публичный ключ CA — JSON
// `{"name","publicKeyPem"}` camelCase; сертификат пира валиден ⟺
// cert.caName == name ∧ подпись верна ключом publicKeyPem ∧ не истёк.
public sealed record PeerTrustAnchor(string Name, string PublicKeyPem)
{
    private static readonly JsonSerializerOptions Json = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.Never,
    };

    public string ToJson() => JsonSerializer.Serialize(this, Json);

    // Бросает при malformed JSON / пустых обязательных полях (fail-fast загрузки).
    public static PeerTrustAnchor FromJson(string json)
    {
        var trust = JsonSerializer.Deserialize<PeerTrustAnchor>(json, Json)
            ?? throw new InvalidOperationException("empty trust-anchor JSON");
        if (string.IsNullOrWhiteSpace(trust.Name) || string.IsNullOrWhiteSpace(trust.PublicKeyPem))
            throw new InvalidOperationException("trust-anchor: required fields are empty");
        return trust;
    }
}
