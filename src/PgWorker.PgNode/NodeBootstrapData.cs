using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace PgWorker.PgNode;

/// <summary>Значение bootstrap-ключа etcd /clusters/&lt;C&gt;/shards/&lt;X&gt;/nodes/&lt;n&gt;/bootstrap
/// (arch/14 §3.2/§4): полный SPILO_CONFIGURATION (rest_password внутри) и
/// серверный серт ноды. Контракт — дубль PgWorker.Core NodeBootstrapBuilder
/// (запрет цикла ссылок, прецедент ApiContracts).</summary>
public sealed record NodeBootstrapData(
    string SpiloConfiguration,
    string CertPem,
    string KeyPem)
{
    /// <summary>null — JSON не парсится (чужой/битый ключ).</summary>
    public static NodeBootstrapData? Parse(string json)
    {
        try
        {
            var dto = JsonSerializer.Deserialize<Dto>(json);
            return dto is { SpiloConfiguration: { Length: > 0 },
                    CertPem: { Length: > 0 }, KeyPem: { Length: > 0 } }
                ? new NodeBootstrapData(dto.SpiloConfiguration, dto.CertPem, dto.KeyPem)
                : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    /// <summary>Валидность для старта (spec §3.4): сертификат+ключ парсятся в
    /// пару, YAML непуст. Значения в текст ошибок не включаются.</summary>
    public bool IsValidForStart()
    {
        try
        {
            // PEM-строки (не пути): CreateFromPem — пары из bootstrap-ключа.
            using var _ = X509Certificate2.CreateFromPem(CertPem, KeyPem);
            return SpiloConfiguration.Trim().Length > 0;
        }
        catch (Exception e) when (
            e is ArgumentException or System.Security.Cryptography.CryptographicException)
        {
            return false;
        }
    }

    private sealed record Dto(
        [property: JsonPropertyName("spilo_configuration")] string? SpiloConfiguration,
        [property: JsonPropertyName("cert_pem")] string? CertPem,
        [property: JsonPropertyName("key_pem")] string? KeyPem);
}
