namespace PgWorker.WalReceiver;

/// <summary>Идентичность приёмника WAL (env контейнера ноды, arch/19 §3):
/// env ставит EnsureNode воркера; невалидный набор — старт-отказ процесса
/// (выход с ненулевым кодом, supervisord с exitcodes=0 его не перезапускает).</summary>
public sealed record WalReceiverIdentity(
    string Cluster,
    string Shard,
    string Node,
    IReadOnlyList<string> EtcdEndpoints,
    string? S3Bucket,
    string? S3CaPem)
{
    /// <summary>null — обязательные env отсутствуют/пусты; errors — имена
    /// отсутствующих ключей (значения env в тексты ошибок не включаются).</summary>
    public static WalReceiverIdentity? Parse(Func<string, string?> env, out List<string> errors)
    {
        var failures = new List<string>();
        string Require(string key)
        {
            var value = env(key);
            if (string.IsNullOrWhiteSpace(value))
            {
                failures.Add($"required env {key} is missing or empty");
                return "";
            }
            return value;
        }

        var cluster = Require("PGW_BACKUP_CLUSTER");
        var shard = Require("PGW_BACKUP_SHARD");
        var node = Require("PGW_BACKUP_NODE");
        var etcd = Require("PGW_ETCD");
        // bucket/CA per-install (arch/19 §7): необязательны — приёмник гейтится
        // etcd-ключами, их отсутствие не старт-отказ.
        var bucket = env("PGW_BACKUP_S3_BUCKET");
        var caPem = env("PGW_BACKUP_S3_CA_PEM");

        errors = failures;
        if (failures.Count > 0)
            return null;

        var endpoints = etcd.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (endpoints.Length == 0)
        {
            errors = ["env PGW_ETCD must be a non-empty comma-separated list of urls"];
            return null;
        }

        return new WalReceiverIdentity(
            cluster, shard, node, endpoints,
            string.IsNullOrWhiteSpace(bucket) ? null : bucket,
            string.IsNullOrWhiteSpace(caPem) ? null : caPem);
    }
}
