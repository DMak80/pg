namespace EtcdWorker.Core;

/// <summary>Флаги managed-узла etcd (arch/04 §3, arch/25 §2): peer — только
/// имя узла сети; client — host-публикация; initial-cluster-state=existing
/// (член добавлен member-API ДО старта).</summary>
public static class EtcdNodeSpecBuilder
{
    public static (string[] Cmd, IReadOnlyDictionary<string, string> Env) Build(
        int k, string? advertisedHost, int clientHostPort)
    {
        var clientAdvertise = advertisedHost is { Length: > 0 } host
            ? $"http://etcdw-node-{k}:2379,http://{host}:{clientHostPort}"
            : $"http://etcdw-node-{k}:2379";
        string[] cmd =
        [
            "etcd",
            $"--name=etcdw-node-{k}",
            "--data-dir=/etcd-data",
            "--listen-peer-urls=http://0.0.0.0:2380",
            "--listen-client-urls=http://0.0.0.0:2379",
            $"--initial-advertise-peer-urls=http://etcdw-node-{k}:2380",
            $"--advertise-client-urls={clientAdvertise}",
            "--initial-cluster-state=existing",
            "--heartbeat-interval=250",
            "--election-timeout=2000",
            "--auto-compaction-retention=1",
            "--quota-backend-bytes=8589934592", // 8 GiB (arch/04 §3)
        ];
        return (cmd, new Dictionary<string, string>());
    }
}
