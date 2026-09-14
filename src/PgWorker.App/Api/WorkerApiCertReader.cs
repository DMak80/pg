using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using PgWorker.Etcd.Client;

namespace PgWorker.App.Api;

// Статус чтения ключа /workers/api_tls/<worker> при старте (spec §3.2 п.1):
// Found — ключ есть и PEM-пара валидна; Missing — ключа нет (env-фоллбек);
// Unreachable — etcd недоступен (env-фоллбек с warning или fail-fast);
// Broken — ключ есть, но JSON/PEM битые (fail-fast: ключ — явное намерение
// оператора, тихий fallback маскирует проблему).
public enum ManagedCertStatus { Found, Missing, Unreachable, Broken }

// Результат чтения managed-серверного серта: PEM-пара при Found, Error — при Broken/Unreachable.
public sealed record ManagedCertRead(ManagedCertStatus Status, string? CertPem, string? KeyPem, string? Error);

// Ридер ключа /workers/api_tls/<worker> ДО поднятия Kestrel (spec §3.2 п.1):
// одиночный /v3/kv/range к первому живому endpoint (формат gateway — EtcdGateway).
// Панель — единственный писатель ключа; воркер его только читает.
public static class WorkerApiCertReader
{
    public const string KeyPrefix = "/workers/api_tls/";

    // Перебор endpoints по одному (failover): первый ответивший выигрывает.
    public static async Task<ManagedCertRead> ReadAsync(string[] endpoints, string worker, CancellationToken ct)
    {
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(3) };
        var gateway = new EtcdGateway(http);
        string? lastError = null;
        foreach (var endpoint in endpoints)
        {
            var result = await gateway.GetAsync(endpoint, KeyPrefix + worker, ct);
            if (!result.IsSuccess)
            {
                lastError = result.Error?.Message;
                continue;
            }

            if (result.Value is null)
                return new(ManagedCertStatus.Missing, null, null, null);
            try
            {
                var (certPem, keyPem) = ParsePayload(result.Value.Value);
                // PEM обязан быть валидной парой — проверяем сразу (fail-fast старта).
                using var _ = CreateCertPair(certPem, keyPem);
                return new(ManagedCertStatus.Found, certPem, keyPem, null);
            }
            catch (Exception e)
            {
                return new(ManagedCertStatus.Broken, null, null, e.Message);
            }
        }

        return new(ManagedCertStatus.Unreachable, null, null,
            lastError ?? "endpoints не заданы");
    }

    // JSON {cert_pem, key_pem} → пара; битый JSON/отсутствие полей — исключение.
    public static (string CertPem, string KeyPem) ParsePayload(string json)
    {
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        if (root.ValueKind != JsonValueKind.Object
            || !root.TryGetProperty("cert_pem", out var cert)
            || cert.ValueKind != JsonValueKind.String
            || !root.TryGetProperty("key_pem", out var key)
            || key.ValueKind != JsonValueKind.String)
            throw new ApplicationException("value ключа обязан быть JSON {cert_pem, key_pem}");
        return (cert.GetString()!, key.GetString()!);
    }

    // Проверка пары PEM: серт + ключ обязаны синтаксически собираться вместе.
    private static X509Certificate2 CreateCertPair(string certPem, string keyPem)
        => X509Certificate2.CreateFromPem(certPem, keyPem);
}
