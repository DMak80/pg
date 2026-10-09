using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;

namespace PgWorker.Core.Templates;

/// <summary>
/// Кеш серверных сертов REST-эндпоинтов нод (arch/14 §2.1): серт
/// генерируется один раз на (кластер, шард, нода, CA) в рамках жизни
/// процесса — повторные сборки env (надзор/ensure/rebuild) дают тот же PEM.
/// Смена CA (hash ключа) — новый серт. Серт ноды — не etcd-состояние
/// (SAN детерминирован именем); DI-синглтон.
/// </summary>
public sealed class RestCertificateCache(string caCertPem, string caKeyPem)
{
    private readonly ConcurrentDictionary<(string Cluster, string Shard, string Node, string CaHash), (string CertPem, string KeyPem)> _certificates = new();

    /// <summary>Серт ноды: полное имя pgw-{cluster}-{shard}-{node} строится внутри.</summary>
    public (string CertPem, string KeyPem) GetOrCreate(string cluster, string shard, string nodeName)
    {
        var caHash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(caKeyPem)));
        return _certificates.GetOrAdd(
            (cluster, shard, nodeName, caHash),
            _ => RestPki.IssueNodeCertificate(
                caCertPem, caKeyPem, nodeName, NodeFullName(cluster, shard, nodeName)));
    }

    // Канон полного имени ноды (arch/14 §2.1): pgw-<C>-<X>-<n> — alias в
    // per-cluster сети, имя контейнера в pgw-metrics, connect_address REST.
    internal static string NodeFullName(string cluster, string shard, string nodeName)
        => $"pgw-{cluster}-{shard}-{nodeName}";
}
