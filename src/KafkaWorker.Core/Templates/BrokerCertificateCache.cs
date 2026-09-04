using System.Collections.Concurrent;
using System.Net;
using System.Security.Cryptography;
using System.Text;

namespace KafkaWorker.Core.Templates;

/// <summary>
/// Кеш серверных сертов нод (R3, arch/16 §2.3): серт генерируется один раз
/// на (кластер, нода, CA) в рамках жизни процесса — повторные сборки env
/// (надзор/ротация/регенерация) дают тот же PEM. Смена CA (hash ключа) —
/// новый серт. DI-синглтон.
/// </summary>
public sealed class BrokerCertificateCache
{
    private readonly ConcurrentDictionary<(string Cluster, string Broker, string CaHash), (string CertPem, string KeyPem)> _certificates = new();

    public (string CertPem, string KeyPem) GetOrCreate(
        string cluster, string brokerName, string caCertPem, string caKeyPem, string advertisedClientHost)
    {
        var caHash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(caKeyPem)));
        return _certificates.GetOrAdd(
            (cluster, brokerName, caHash),
            _ => Issue(caCertPem, caKeyPem, brokerName, advertisedClientHost));
    }

    // SAN-правило arch/16 §2.3: DNS broker<k> (INTERNAL advertised) +
    // advertised-хост CLIENT (DNS либо IP — как резолвят клиенты по endpoints).
    private static (string CertPem, string KeyPem) Issue(
        string caCertPem, string caKeyPem, string brokerName, string advertisedClientHost)
    {
        var host = HostOf(advertisedClientHost);
        IPAddress? ip = IPAddress.TryParse(host, out var parsed) ? parsed : null;
        var dnsNames = ip is null ? new[] { brokerName, host } : new[] { brokerName };
        return ClusterPki.IssueBrokerCertificate(caCertPem, caKeyPem, brokerName, dnsNames, ip);
    }

    // "host.docker.internal:16001" → "host.docker.internal" (порт SAN не входит).
    private static string HostOf(string advertisedClient)
    {
        var separator = advertisedClient.LastIndexOf(':');
        return separator > 0 ? advertisedClient[..separator] : advertisedClient;
    }
}
