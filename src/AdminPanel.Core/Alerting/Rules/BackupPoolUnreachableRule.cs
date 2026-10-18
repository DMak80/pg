using Shared.Core.DI;

namespace AdminPanel.Core.Alerting.Rules;

// backup-pool-unreachable (critical, t36, каталог 03 §4): маркер
// s3-pool-exhausted в error статусов воркера — wal DEGRADED (приёмник в
// ретрай-цикле исчерпания списка) или любой full FAILED (исчерпание
// списка/Failed-резолв пула, arch/19 §4). Единая алертная грань «запись в пул
// не доставлена ни на одну ноду списков».
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class BackupPoolUnreachableRule : IAlertRule
{
    public const string KindName = "backup-pool-unreachable";
    public const string Marker = "s3-pool-exhausted";
    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var cluster in snapshot.Backups)
        {
            var hits = new List<string>();
            // wal DEGRADED с маркером (Shards — словарь wal-статусов, t03-модель).
            foreach (var (shard, wal) in cluster.Shards ?? new Dictionary<string, WalStreamInfo?>())
                if (wal is { State: WalStreamInfoState.Degraded }
                    && wal.Error?.Contains(Marker, StringComparison.Ordinal) == true)
                    hits.Add($"wal {shard}: {wal.Error}");
            // Любой full FAILED с маркером (ShardsFulls — t08-модель полных).
            foreach (var (_, fulls) in cluster.ShardsFulls ?? new Dictionary<string, IReadOnlyList<BackupFullInfo>>())
                foreach (var full in fulls.Where(f =>
                    f.State == "FAILED" && f.Error?.Contains(Marker, StringComparison.Ordinal) == true))
                    hits.Add($"full {full.Id}: {full.Error}");

            if (hits.Count == 0)
                continue;
            yield return new Alert(
                $"{KindName}:{cluster.Cluster}",
                AlertSeverity.Critical,
                KindName,
                cluster.Cluster,
                $"cluster {cluster.Cluster}: S3 pool write was not delivered to any list node ({string.Join("; ", hits.Take(3))})",
                new Dictionary<string, string>(),
                null,
                "check pool nodes (/s3/): the worker did not deliver the write to any list node",
                AlertRemedy.OperatorRunbook,
                "pool is starting/provisioning — the alert will clear itself (retries on tick); otherwise inspect /s3/ nodes via healthz/portalloc");
        }
    }
}
