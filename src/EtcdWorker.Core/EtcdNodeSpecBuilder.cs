namespace EtcdWorker.Core;

/// <summary>Флаги managed-узла etcd (arch/04 §3, arch/25 §2): peer — только
/// имя узла сети; client — host-публикация; initial-cluster-state=existing
/// (член добавлен member-API ДО старта). initialCluster — полный состав
/// кластера для join (без него etcd строит initial-cluster только из себя и
/// падает «cannot fetch cluster info»).</summary>
public static class EtcdNodeSpecBuilder
{
    public static (string[] Cmd, IReadOnlyDictionary<string, string> Env) Build(
        int k, string? advertisedHost, int clientHostPort, string? initialCluster = null)
    {
        var clientAdvertise = advertisedHost is { Length: > 0 } host
            ? $"http://etcdw-node-{k}:2379,http://{host}:{clientHostPort}"
            : $"http://etcdw-node-{k}:2379";
        var cmd = new List<string>
        {
            "etcd",
            $"--name=etcdw-node-{k}",
            "--data-dir=/etcd-data",
            "--listen-peer-urls=http://0.0.0.0:2380",
            "--listen-client-urls=http://0.0.0.0:2379",
            $"--initial-advertise-peer-urls=http://etcdw-node-{k}:2380",
            $"--advertise-client-urls={clientAdvertise}",
            "--initial-cluster-state=existing",
        };
        if (initialCluster is { Length: > 0 })
            cmd.Add($"--initial-cluster={initialCluster}");
        cmd.AddRange([
            "--heartbeat-interval=250",
            "--election-timeout=2000",
            "--auto-compaction-retention=1",
            "--quota-backend-bytes=8589934592", // 8 GiB (arch/04 §3)
        ]);
        return ([.. cmd], new Dictionary<string, string>());
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
