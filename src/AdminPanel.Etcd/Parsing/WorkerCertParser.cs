using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using AdminPanel.Core;
using AdminPanel.Etcd.Client;

namespace AdminPanel.Etcd.Parsing;

// Результат разбора ключа серта: метаданные либо parseError (битый JSON/PEM).
public sealed record WorkerCertParseResult(WorkerApiCert? Cert, KeyParseError? Error);

// Чистая функция: Kv ключа /workers/api_tls/<worker> → метаданные целевого
// серта (spec §3.1): key_pem в модель не попадает — парсим только серт.
// Битый JSON/серт — KeyParseError (тик не роняют, толерантность парсеров).
public static class WorkerCertParser
{
    public static WorkerCertParseResult Parse(string key, Kv? kv)
    {
        if (kv is null)
            return new(null, null);
        try
        {
            using var doc = JsonDocument.Parse(kv.Value);
            var root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object
                || !root.TryGetProperty("cert_pem", out var certPem)
                || certPem.ValueKind != JsonValueKind.String)
                return new(null, new(key, "нет поля cert_pem"));

            using var cert = X509Certificate2.CreateFromPem(certPem.GetString()!);
            var san = new List<string>();
            foreach (var ext in cert.Extensions.OfType<X509SubjectAlternativeNameExtension>())
            {
                san.AddRange(ext.EnumerateDnsNames());
                san.AddRange(ext.EnumerateIPAddresses().Select(ip => ip.ToString()));
            }

            return new(new WorkerApiCert(
                Convert.ToHexString(SHA256.HashData(cert.RawData)).ToLowerInvariant(),
                cert.Subject,
                cert.Issuer,
                san,
                // NotBefore/NotAfter — локальные DateTime на host-рантаймах
                // (Kind=Local): приведение к UTC даёт корректный offset 0.
                new DateTimeOffset(cert.NotBefore.ToUniversalTime(), TimeSpan.Zero),
                new DateTimeOffset(cert.NotAfter.ToUniversalTime(), TimeSpan.Zero),
                root.TryGetProperty("updated_unix", out var unix) && unix.ValueKind == JsonValueKind.Number
                    ? unix.GetInt64()
                    : 0,
                root.TryGetProperty("updated_by", out var by) && by.ValueKind == JsonValueKind.String
                    ? by.GetString()
                    : null), null);
        }
        catch (JsonException e)
        {
            return new(null, new(key, $"битый JSON: {e.Message}"));
        }
        catch (Exception e) when (e is ArgumentException or CryptographicException or FormatException)
        {
            return new(null, new(key, $"битый PEM сертификата: {e.Message}"));
        }
    }

    // Ошибки результата для общего списка ParseErrors снапшота (канон тиков).
    public static IReadOnlyList<KeyParseError> ErrorsOf(WorkerCertParseResult r)
        => r.Error is null ? [] : [r.Error];
}
