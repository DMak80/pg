namespace AdminPanel.Core;

// Связь scope "<C>-<X>" с кластером/шардом по известным кластерам /clusters/ (arch/02 §2.2):
// префикс "<C>-", suffix обязан быть именем шарда; иначе scope показывается «как есть» с пометкой unmatched.
public static class ScopeMatcher
{
    public static (string? Cluster, string? Shard, bool Matched) Match(
        string scope,
        IReadOnlyList<ClusterInfo> clusters)
    {
        foreach (var cluster in clusters)
        {
            var prefix = cluster.Name + "-";
            if (!scope.StartsWith(prefix, StringComparison.Ordinal))
                continue;

            var suffix = scope[prefix.Length..];
            return cluster.Shards.Any(sh => sh.Name == suffix)
                ? (cluster.Name, suffix, true)
                : (cluster.Name, null, false);
        }

        return (null, null, false);
    }
}
