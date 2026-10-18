using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using Shared.Core.DI;
using Microsoft.Extensions.Options;

namespace AdminPanel.Core.S3.S3Alerting;

// Чистая функция (S3Snapshot next, prev) → Alert[] — каталог arch/03 §12.4.
// sinceUnix — по стабильному id из prev.Alerts (механика ValkeyAlertEngine);
// сортировка severity → kind → target.
public interface IS3AlertEngine
{
    IReadOnlyList<Alert> Evaluate(S3Snapshot next, S3Snapshot? previous);
}

[InjectAsSingleton(typeof(IS3AlertEngine))]
public sealed class S3AlertEngine(IOptions<S3AlertsOptions> options) : IS3AlertEngine
{
    private static readonly IComparer<AlertSeverity> SeverityDescending =
        Comparer<AlertSeverity>.Create((x, y) => y.CompareTo(x));

    private readonly S3AlertsOptions _options = options.Value;

    public IReadOnlyList<Alert> Evaluate(S3Snapshot next, S3Snapshot? previous)
    {
        var nowUnix = next.BuiltAtUtc.ToUnixTimeSeconds();
        return
        [
            .. Enumerate(next, previous)
               .Select(a => a with { SinceUnix = ResolveSince(a, previous, nowUnix) })
               .OrderBy(a => a.Severity, SeverityDescending)
               .ThenBy(a => a.Kind, StringComparer.Ordinal)
               .ThenBy(a => a.Target, StringComparer.Ordinal),
        ];
    }    // Каталог arch/03 §12.4: все 17 kinds.
    private IEnumerable<Alert> Enumerate(S3Snapshot next, S3Snapshot? previous)
    {
        // 1. worker-api-unreachable (critical): нет живых ключей /s3worker/api/
        // (arch/02 §2.3.4) — s3-мутации панели 503; чтение данных не страдает.
        if (next.WorkerEndpoints.Count == 0)
            yield return new Alert(
                "worker-api-unreachable:s3worker",
                AlertSeverity.Critical,
                "worker-api-unreachable",
                "s3worker",
                "S3Worker API unavailable: no live /s3worker/api/ keys — s3 mutations from the panel return 503; data reads are unaffected",
                null,
                null,
                Hint: "the worker sets the lease key at startup; no key means the worker never came up or died ≤15 s ago",
                Remedy: AlertRemedy.OperatorRunbook,
                RemedyText: "start the s3worker container (s3 profile of the deploy setup), check /healthz and S3Worker:Api:AdvertiseUrl");

        // 2. worker-unhealthy (warning): живой ключ, /healthz ≠ 200.
        foreach (var w in next.WorkerHealth.Where(w => w.Status != WorkerHealthStatus.Healthy))
        {
            var what = w.Status == WorkerHealthStatus.Degraded
                ? $"/healthz responds with non-200 ({w.Detail ?? "degraded"})"
                : $"unreachable at the lease key URL ({w.Detail ?? "network error"})";
            yield return new Alert(
                $"worker-unhealthy:s3worker/{w.InstanceId}",
                AlertSeverity.Warning,
                "worker-unhealthy",
                $"s3worker/{w.InstanceId}",
                $"S3Worker instance {w.InstanceId} unhealthy: {what}",
                new Dictionary<string, string>
                {
                    ["url"] = w.Url,
                    ["checked_unix"] = w.CheckedAtUtc.ToUnixTimeSeconds().ToString(),
                },
                null,
                "the lease key is alive but the process health probe is bad; docker-healthcheck will stop the container — the lease will disappear and critical worker-api-unreachable will follow",
                AlertRemedy.OperatorRunbook,
                "check docker logs s3worker and /healthz directly; fix the dependency (etcd/docker) or restart the worker container");
        }

        foreach (var cluster in next.Clusters)
        {
            switch (cluster.State)
            {
                // 3. s3-cluster-not-initialized (info).
                case S3ClusterState.NotInitialized:
                    yield return new Alert(
                        $"s3-cluster-not-initialized:{cluster.Name}",
                        AlertSeverity.Info,
                        "s3-cluster-not-initialized",
                        cluster.Name,
                        $"cluster {cluster.Name} declared (NOT_INITIALIZED): instances are not up",
                        null, null,
                        "cluster declared (config.state=NOT_INITIALIZED): worker provisioning will bring the instances up and move state to Active",
                        AlertRemedy.WorkerAuto,
                        "wait for provisioning — the worker will clear NOT_INITIALIZED; if it stalls, check /s3worker/work/<C>");
                    break;

                // 4. s3-cluster-to-remove (info).
                case S3ClusterState.ToRemove:
                    yield return new Alert(
                        $"s3-cluster-to-remove:{cluster.Name}",
                        AlertSeverity.Info,
                        "s3-cluster-to-remove",
                        cluster.Name,
                        $"cluster {cluster.Name} is being removed (TO_REMOVE): the worker is dismantling it",
                        null, null,
                        "cluster is being removed (config.state=TO_REMOVE): the worker will dismantle the instances and remove the /s3/clusters/<C> prefix",
                        AlertRemedy.WorkerAuto,
                        "the worker dismantles the cluster itself; if it stalls, check /s3worker/work/<C>");
                    break;

                case S3ClusterState.Active:
                    foreach (var alert in ActiveClusterAlerts(cluster, previous, next))
                        yield return alert;
                    break;
            }
        }

        // 15/16. s3-rotation-pending (info) / s3-rotation-stale (warning): живые
        // заявки ротации (снятие — только воркером; отмена из панели НЕТ).
        foreach (var rotation in next.Rotations)
        {
            yield return new Alert(
                $"s3-rotation-pending:{rotation.Cluster}",
                AlertSeverity.Info,
                "s3-rotation-pending",
                rotation.Cluster,
                $"secret rotation of the cluster {rotation.Cluster} profile requested, executed by the worker (rolling re-creation of instances)",
                new Dictionary<string, string>
                {
                    ["requestedBy"] = rotation.RequestedBy ?? "unknown",
                    ["requestedUnix"] = rotation.RequestedUnix.ToString(),
                },
                null,
                "rotation request is alive (key /s3worker/rotations/<C>): the worker will reissue the env profile by rolling re-creation of instances and remove the key; no cancellation from the panel (arch/23 §8)",
                AlertRemedy.WorkerAuto,
                "rotation is executed by the worker, the key will disappear; if it stalls, the worker is lagging — check /s3worker/work/<C>");

            var age = next.BuiltAtUtc.ToUnixTimeSeconds() - rotation.RequestedUnix;
            if (age > _options.RotationStaleSeconds)
                yield return new Alert(
                    $"s3-rotation-stale:{rotation.Cluster}",
                    AlertSeverity.Warning,
                    "s3-rotation-stale",
                    rotation.Cluster,
                    $"secret rotation request of cluster {rotation.Cluster} alive for more than {age} s (threshold {_options.RotationStaleSeconds} s) — the executor does not start",
                    new Dictionary<string, string>
                    {
                        ["ageSec"] = age.ToString(),
                        ["staleSec"] = _options.RotationStaleSeconds.ToString(),
                    },
                    null,
                    $"request not executed for more than {_options.RotationStaleSeconds} s — the worker is lagging or does not see the request; only the worker removes it",
                    AlertRemedy.WorkerAuto,
                    "check /s3worker/work/<C> and S3 node availability; the request will be cleared by execution or by the worker age timeout");
        }

        // 17. s3-key-malformed (warning): parseError-записи (arch/23 §2).
        foreach (var error in next.ParseErrors)
            yield return new Alert(
                $"s3-key-malformed:{error.Key}",
                AlertSeverity.Warning,
                "s3-key-malformed",
                error.Key,
                $"s3 key not parsed: {error.Key}",
                new Dictionary<string, string> { ["reason"] = error.Reason },
                null,
                "s3 key not parsed by the panel parser: the malformed value does not reach the model — the UI is blind to the key; the s3 domain value format canon is arch/23",
                AlertRemedy.OperatorRunbook,
                "eliminate the source of the malformed record (external writer) and bring the value to the arch/23 canon; the next tick will parse the key");
    }

    // 5–14. Алерты Active-кластера.
    private IEnumerable<Alert> ActiveClusterAlerts(
        S3ClusterInfo cluster, S3Snapshot? previous, S3Snapshot next)
    {
        // 6. s3-endpoints-missing (critical): Active с нодами без endpoints.
        if (cluster.NodesList.Count > 0 && string.IsNullOrEmpty(cluster.Endpoints))
            yield return new Alert(
                $"s3-endpoints-missing:{cluster.Name}",
                AlertSeverity.Critical,
                "s3-endpoints-missing",
                cluster.Name,
                $"Active cluster {cluster.Name} has nodes without endpoints — client discovery is impossible",
                null, null,
                "Active cluster with a node up but without endpoints: the worker writes endpoints once nodes come up — without them clients cannot find the storage",
                AlertRemedy.WorkerAuto,
                "the worker will write endpoints after provisioning; if it stalls, check /s3worker/work/<C> and the s3w-<C>-<I> container");

        var nowUnix = next.BuiltAtUtc.ToUnixTimeSeconds();

        // 7/8. s3-connectivity-degraded (warning) / s3-connectivity-down (critical).
        if (cluster.Connectivity?.State == "DEGRADED")
            yield return new Alert(
                $"s3-connectivity-degraded:{cluster.Name}",
                AlertSeverity.Warning,
                "s3-connectivity-degraded",
                cluster.Name,
                $"connectivity of cluster {cluster.Name} DEGRADED — some configured links fail",
                null, null,
                "the connectivity probe saw offline links while nodes are alive: replication over these pairs is not happening (arch/23 §3)",
                AlertRemedy.WorkerAuto,
                "the worker probe will re-transfer the matrix; persistently DEGRADED — check the link nodes and the docker host network");

        if (cluster.Connectivity?.State == "DOWN")
            yield return new Alert(
                $"s3-connectivity-down:{cluster.Name}",
                AlertSeverity.Critical,
                "s3-connectivity-down",
                cluster.Name,
                $"connectivity of cluster {cluster.Name} DOWN — nodes cannot hear each other",
                null, null,
                "the connectivity probe failed on every link: graph replication is not happening, backup data diverges (arch/23 §3)",
                AlertRemedy.OperatorRunbook,
                "check the s3w-<C>-<I> node containers and the docker host network; the probe will re-transfer the matrix after recovery");

        // 9/10. s3-sync-lagging (warning) / s3-sync-diverged (critical).
        if (cluster.Sync?.State == "LAGGING")
            yield return new Alert(
                $"s3-sync-lagging:{cluster.Name}",
                AlertSeverity.Warning,
                "s3-sync-lagging",
                cluster.Name,
                $"cluster {cluster.Name} sync LAGGING — pairs are compared with discrepancies",
                null, null,
                "the probe compared destination/source listings: missing/extra found — replication has not caught up (arch/23 §3)",
                AlertRemedy.WorkerAuto,
                "the worker probe will re-transfer; persistent lag — check the node replication queues");

        if (cluster.Sync?.State == "DIVERGED")
            yield return new Alert(
                $"s3-sync-diverged:{cluster.Name}",
                AlertSeverity.Critical,
                "s3-sync-diverged",
                cluster.Name,
                $"cluster {cluster.Name} sync DIVERGED — pair data diverges",
                null, null,
                "the probe saw missing/extra after catch-up runs: some backups are not replicated across pairs (arch/23 §3)",
                AlertRemedy.OperatorRunbook,
                "check the pair link and the s3w-<C>-<I> node logs; after the fix the probe will re-transfer and the verdict will improve");

        // 11. s3-sync-stale (warning, один на кластер): Active с RUNNING-нодами,
        // зонд молчит (ключей нет) или checked_unix протух.
        if (cluster.NodesList.Any(n => n.State == "RUNNING")
            && (cluster.Connectivity is null
                || cluster.Sync is null
                || StaleChecked(cluster.Connectivity.CheckedUnix, nowUnix)
                || StaleChecked(cluster.Sync.CheckedUnix, nowUnix)))
            yield return new Alert(
                $"s3-sync-stale:{cluster.Name}",
                AlertSeverity.Warning,
                "s3-sync-stale",
                cluster.Name,
                $"connectivity/sync probe of cluster {cluster.Name} is silent (no keys or checked_unix older than {_options.SyncStaleSec} s)",
                null, null,
                "an Active cluster with RUNNING nodes has no fresh connectivity/sync: the worker probe is not writing — observability is blind",
                AlertRemedy.OperatorRunbook,
                "check the worker probe: /s3worker/work/<C> op=sync-probe; a periodic or on-demand probe will refresh checked_unix");

        var prevCluster = previous?.Clusters.FirstOrDefault(c => c.Name == cluster.Name);
        foreach (var node in cluster.NodesList)
        {
            // 5. s3-node-not-running (critical): кроме fresh-PROVISIONING.
            if (node.State is null or "RUNNING")
                continue;
            if (node.State == "PROVISIONING"
                && IsFreshProvisioning(node, prevCluster, previous, next, _options.FreshProvisioningSeconds))
                continue;

            yield return new Alert(
                $"s3-node-not-running:{cluster.Name}/{node.Name}",
                AlertSeverity.Critical,
                "s3-node-not-running",
                $"{cluster.Name}/{node.Name}",
                $"instance {node.Name} of cluster {cluster.Name} is not RUNNING: {node.State}",
                new Dictionary<string, string> { ["state"] = node.State! },
                null,
                "the instance is not RUNNING: worker supervision must bring it back to RUNNING (container restart/re-creation); every declared node must be alive in an Active cluster",
                AlertRemedy.WorkerAuto,
                "supervision will re-create the container; if it stalls, check the s3w-<C>-<I> container");
        }

        // 12. s3-node-disk-usage-high (warning): процент или превышение декларации.
        foreach (var node in cluster.NodesList)
        {
            if (node.Usage is { } usage)
            {
                var overPercent = usage.TotalBytes > 0
                    && usage.UsedBytes * 100 / usage.TotalBytes > _options.DiskUsagePercent;
                var overDeclared = node.Resources?.DiskGi is { } diskGi
                    && usage.UsedBytes > diskGi * 1024L * 1024 * 1024;
                if (overPercent || overDeclared)
                    yield return new Alert(
                        $"s3-node-disk-usage-high:{cluster.Name}/{node.Name}",
                        AlertSeverity.Warning,
                        "s3-node-disk-usage-high",
                        $"{cluster.Name}/{node.Name}",
                        $"disk of instance {node.Name} of cluster {cluster.Name} is {usage.UsedBytes * 100 / Math.Max(1, usage.TotalBytes)}% full (threshold {_options.DiskUsagePercent}%)",
                        new Dictionary<string, string>
                        {
                            ["usedBytes"] = usage.UsedBytes.ToString(),
                            ["totalBytes"] = usage.TotalBytes.ToString(),
                        },
                        null,
                        "instance disk usage exceeded the threshold/declaration: a full disk will stop backup intake",
                        AlertRemedy.OperatorApi,
                        $"increase the disk: PUT /api/s3/clusters/{cluster.Name}/nodes/{node.Name}/resources");
            }
        }

        // 13. s3-nodes-same-host (warning): ≥2 нод на одном непустом placed_on.
        foreach (var host in cluster.NodesList
                     .Where(n => !string.IsNullOrEmpty(n.PlacedOn))
                     .GroupBy(n => n.PlacedOn, StringComparer.Ordinal)
                     .Where(g => g.Count() >= 2))
        {
            var names = string.Join(", ", host.Select(n => n.Name));
            yield return new Alert(
                $"s3-nodes-same-host:{cluster.Name}",
                AlertSeverity.Warning,
                "s3-nodes-same-host",
                cluster.Name,
                $"nodes of cluster {cluster.Name} on the same host {host.Key}: {names}",
                new Dictionary<string, string>
                {
                    ["host"] = host.Key!,
                    ["nodes"] = names,
                },
                null,
                "several cluster nodes are placed on the same docker host: losing the host loses a share of the data; geo-spread is the operator's duty (arch/23 §4)",
                AlertRemedy.OperatorRunbook,
                "geo-spread is the operator's duty, arch/23 §4; drain or move the instance");
        }

        // 14. s3-node-not-in-mesh (warning): нода без единой связи в links.
        var meshed = cluster.Links
            .SelectMany(l => new[] { l.A, l.B })
            .ToHashSet(StringComparer.Ordinal);
        foreach (var node in cluster.NodesList
                     .Where(n => n.State is null or "RUNNING")
                     .Where(n => !meshed.Contains(n.Name)))
            yield return new Alert(
                $"s3-node-not-in-mesh:{cluster.Name}/{node.Name}",
                AlertSeverity.Warning,
                "s3-node-not-in-mesh",
                $"{cluster.Name}/{node.Name}",
                $"instance {node.Name} of cluster {cluster.Name} is outside the replication graph — no links at all",
                null, null,
                "an Active cluster node appears in no links pair: its objects are not replicated — risk of an unreplicated share",
                AlertRemedy.OperatorApi,
                $"add a link: PUT /api/s3/clusters/{cluster.Name}/links");
    }

    private bool StaleChecked(long? checkedUnix, long nowUnix)
        => checkedUnix is null || nowUnix - checkedUnix.Value > _options.SyncStaleSec;

    private static bool IsFreshProvisioning(
        S3NodeInfo node,
        S3ClusterInfo? prevCluster,
        S3Snapshot? previous,
        S3Snapshot next,
        int freshSeconds)
    {
        // Нет prev / в prev нода была не PROVISIONING → статус только что начался.
        if (previous is null || prevCluster is null)
            return true;
        var prevNode = prevCluster.NodesList.FirstOrDefault(n => n.Name == node.Name);
        if (prevNode?.State != "PROVISIONING")
            return true;

        // PROVISIONING наблюдался и тик назад: fresh, пока разница BuiltAtUtc < окна.
        return next.BuiltAtUtc - previous.BuiltAtUtc < TimeSpan.FromSeconds(freshSeconds);
    }

    // sinceUnix: prev нет → null; id был в prev → перенос; новый → now (pg-механика).
    private static long? ResolveSince(Alert alert, S3Snapshot? previous, long nowUnix)
    {
        if (previous is null)
            return null;
        var before = previous.Alerts.FirstOrDefault(a => a.Id == alert.Id);
        return before is null ? nowUnix : before.SinceUnix;
    }
}
