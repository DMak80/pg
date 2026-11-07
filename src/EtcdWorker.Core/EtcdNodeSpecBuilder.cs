namespace EtcdWorker.Core;

/// <summary>Спецификация managed-узла etcd (arch/etcdworker/01-placement.md §2,
/// arch/etcdnode/01-agent.md §1): контейнер — образ EtcdNode (etcd +
/// supervisord + агент), CMD образа — supervisord (Cmd НЕ передаётся);
/// флаги etcd — env ETCD_ARGS одной строкой; конфигурация агента —
/// env ETCDNODE_* (arch/etcdnode/01-agent.md §3). Флаги без
/// shell-метасимволов — экранирование не требуется.</summary>
public static class EtcdNodeSpecBuilder
{
    public static (string[] Cmd, IReadOnlyDictionary<string, string> Env) Build(
        int k, EtcdProvisioningOptions options, int clientHostPort, string? initialCluster = null)
    {
        var clientAdvertise = options.AdvertisedHost is { Length: > 0 } host
            ? $"http://etcdw-node-{k}:2379,http://{host}:{clientHostPort}"
            : $"http://etcdw-node-{k}:2379";
        var args = new List<string>
        {
            $"--name=etcdw-node-{k}",
            "--data-dir=/etcd-data",
            "--listen-peer-urls=http://0.0.0.0:2380",
            "--listen-client-urls=http://0.0.0.0:2379",
            $"--initial-advertise-peer-urls=http://etcdw-node-{k}:2380",
            $"--advertise-client-urls={clientAdvertise}",
            "--initial-cluster-state=existing",
        };
        if (initialCluster is { Length: > 0 })
            args.Add($"--initial-cluster={initialCluster}");
        args.AddRange([
            "--heartbeat-interval=250",
            "--election-timeout=2000",
            "--auto-compaction-retention=1",
            "--quota-backend-bytes=8589934592", // 8 GiB (arch/etcdworker/01-placement.md)
        ]);

        var env = new Dictionary<string, string>
        {
            ["ETCD_ARGS"] = string.Join(' ', args),
            ["ETCDNODE_NODE_NAME"] = $"etcdw-node-{k}",
            ["ETCDNODE_SNAPSHOTS__INTERVAL_MIN"] = options.SnapshotIntervalMin.ToString(),
            ["ETCDNODE_SNAPSHOTS__MIN_INTERVAL_SEC"] = options.SnapshotMinIntervalSec.ToString(),
            ["ETCDNODE_SNAPSHOTS__RETRY_INTERVAL_SEC"] = options.SnapshotRetryIntervalSec.ToString(),
            ["ETCDNODE_SNAPSHOTS__TIMEOUT_SEC"] = options.SnapshotTimeoutSec.ToString(),
            ["ETCDNODE_SNAPSHOTS__RETENTION_FILES"] = options.SnapshotRetentionFiles.ToString(),
            ["ETCDNODE_SNAPSHOTS__RETENTION_OBJECTS"] = options.SnapshotRetentionObjects.ToString(),
            ["ETCDNODE_SNAPSHOTS__MAINTENANCE_INTERVAL_MIN"] = options.SnapshotMaintenanceIntervalMin.ToString(),
        };
        if (options.SnapshotS3CaPem is { } pem)
            env["ETCDNODE_S3_CA_PEM"] = pem;

        return ([], env); // Cmd пуст: контейнер стартует CMD образа (supervisord)
    }

    /// <summary>Полный initial-cluster для join узла k: started-члены состава
    /// (имя = первое peer-URL-имя; unstarted-learner без имени не представим)
    /// + сам новый узел.</summary>
    public static string InitialCluster(
        IReadOnlyList<Shared.Etcd.Client.EtcdMember> members, int k)
    {
        var entries = members
            .Where(m => m.Name is { Length: > 0 })
            .Select(m => $"{m.Name}={m.PeerUrls.FirstOrDefault()}")
            .Where(e => !e.EndsWith("=", StringComparison.Ordinal))
            .Append($"etcdw-node-{k}=http://etcdw-node-{k}:2380");
        return string.Join(",", entries.Distinct(StringComparer.Ordinal));
    }
}
