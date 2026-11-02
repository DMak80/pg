using System.Text.Json;
using System.Text.Json.Serialization;
using PgWorker.Core.Model;
using PgWorker.Core.Tuning;

namespace PgWorker.Core.Templates;

/// <summary>Bootstrap-ключ ноды (arch/14 §3.2/§4, t05): JSON
/// {spilo_configuration, cert_pem, key_pem} — полный SPILO_CONFIGURATION
/// (rest_password внутри) и серверный серт ноды (per-install REST-CA, SAN —
/// arch/14 §2.4 п.6; один серт на REST :8008 / doorman :6432 / mTLS-канал
/// :8081). Пишет ТОЛЬКО воркер; читает агент PgNode; per-install пароли PG
/// сюда НЕ входят (прямая mTLS-передача). Сериализация симметрична —
/// детерминированный вывод без дребезга mod_revision (spec §3.7.2).</summary>
public static class NodeBootstrapBuilder
{
    private static readonly JsonSerializerOptions Options = new()
    {
        WriteIndented = false,
    };

    private sealed record BootstrapData(
        [property: JsonPropertyName("spilo_configuration")] string SpiloConfiguration,
        [property: JsonPropertyName("cert_pem")] string CertPem,
        [property: JsonPropertyName("key_pem")] string KeyPem);

    public static string Build(
        ShardTopology topology, EtcdEndpoints etcd,
        bool syncStrict, string nodeName, string certPem, string keyPem,
        string restPassword, PgTuneResult? tuning = null,
        IReadOnlySet<string>? excludeParams = null) =>
        JsonSerializer.Serialize(new BootstrapData(
            SpiloEnvBuilder.SpiloConfiguration(
                topology, etcd, syncStrict, nodeName, restPassword, tuning, excludeParams),
            certPem, keyPem), Options);
}
