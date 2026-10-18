using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using Shared.Core.DI;
using Microsoft.Extensions.Options;

namespace AdminPanel.Core.Valkey.ValkeyAlerting;

// Чистая функция (ValkeySnapshot next, prev) → Alert[] — каталог arch/03 §8.4
// (t10 — stale-ротации и ticket-expired).
// sinceUnix — по стабильному id из prev.Alerts (механика KafkaAlertEngine);
// сортировка severity → kind → target.
public interface IValkeyAlertEngine
{
    IReadOnlyList<Alert> Evaluate(ValkeySnapshot next, ValkeySnapshot? previous);
}

[InjectAsSingleton(typeof(IValkeyAlertEngine))]
public sealed class ValkeyAlertEngine(IOptions<ValkeyAlertsOptions> options) : IValkeyAlertEngine
{
    private static readonly IComparer<AlertSeverity> SeverityDescending =
        Comparer<AlertSeverity>.Create((x, y) => y.CompareTo(x));

    private readonly ValkeyAlertsOptions _options = options.Value;

    public IReadOnlyList<Alert> Evaluate(ValkeySnapshot next, ValkeySnapshot? previous)
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
    }

    // Каталог arch/03 §8.4: все 8 kinds. Ротационный алерт — только у живого
    // кластера (заявка удаляется исполнением или демонтажем — вечного pending нет).
    private IEnumerable<Alert> Enumerate(ValkeySnapshot next, ValkeySnapshot? previous)
    {
        // worker-api-unreachable (critical): нет живых ключей /valkeyworker/api/
        // (arch/02 §2.3.3) — valkey-мутации панели 503; чтение не страдает.
        if (next.WorkerEndpoints.Count == 0)
            yield return new Alert(
                "worker-api-unreachable:valkeyworker",
                AlertSeverity.Critical,
                "worker-api-unreachable",
                "valkeyworker",
                "ValkeyWorker API unreachable: no live /valkeyworker/api/ keys — valkey mutations from the panel return 503; data reads are unaffected",
                null,
                null,
                Hint: "the worker writes the lease key at startup; missing key means the worker failed to start or died ≤15 s ago",
                Remedy: AlertRemedy.OperatorRunbook,
                RemedyText: "start the worker container (valkey profile of the stand compose), check /healthz and ValkeyWorker:Api:AdvertiseUrl");

        // worker-unhealthy (warning): живой ключ, /healthz ≠ 200.
        foreach (var w in next.WorkerHealth.Where(w => w.Status != WorkerHealthStatus.Healthy))
        {
            var what = w.Status == WorkerHealthStatus.Degraded
                ? $"/healthz returns non-200 ({w.Detail ?? "degraded"})"
                : $"unreachable at the lease-key URL ({w.Detail ?? "network error"})";
            yield return new Alert(
                $"worker-unhealthy:valkeyworker/{w.InstanceId}",
                AlertSeverity.Warning,
                "worker-unhealthy",
                $"valkeyworker/{w.InstanceId}",
                $"ValkeyWorker instance {w.InstanceId} is unhealthy: {what}",
                new Dictionary<string, string>
                {
                    ["url"] = w.Url,
                    ["checked_unix"] = w.CheckedAtUtc.ToUnixTimeSeconds().ToString(),
                },
                null,
                "the lease key is alive but the process health probe is failing; docker-healthcheck will kill the container — the lease will disappear, followed by critical worker-api-unreachable",
                AlertRemedy.OperatorRunbook,
                "check docker logs valkeyworker and /healthz directly; restore the dependency (etcd/docker) or restart the worker container");
        }

        foreach (var cluster in next.Clusters)
        {
            switch (cluster.State)
            {
                case ValkeyClusterState.NotInitialized:
                    yield return new Alert(
                        $"valkey-cluster-not-initialized:{cluster.Name}",
                        AlertSeverity.Info,
                        "valkey-cluster-not-initialized",
                        cluster.Name,
                        $"cluster {cluster.Name} declared (NOT_INITIALIZED): node not provisioned",
                        null, null,
                        "cluster declared (config.state=NOT_INITIALIZED): worker provisioning will bring up the node and move state to ACTIVE",
                        AlertRemedy.WorkerAuto,
                        "wait for node provisioning — the worker will clear NOT_INITIALIZED; if it takes longer than usual, check the worker journal");
                    break;
                case ValkeyClusterState.ToRemove:
                    yield return new Alert(
                        $"valkey-cluster-to-remove:{cluster.Name}",
                        AlertSeverity.Info,
                        "valkey-cluster-to-remove",
                        cluster.Name,
                        $"cluster {cluster.Name} is being removed (TO_REMOVE): the worker is dismantling it",
                        null, null,
                        "cluster being removed (config.state=TO_REMOVE): the worker will dismantle the node and remove the /valkey/clusters/<C> prefix",
                        AlertRemedy.WorkerAuto,
                        "the worker dismantles the cluster on its own; if it hangs, check the worker journal (the container may have failed to be removed)");
                    break;
                case ValkeyClusterState.Active:
                    foreach (var alert in ActiveClusterAlerts(cluster, previous, next))
                        yield return alert;
                    break;
            }
        }

        // valkey-rotation-pending (info): только заявки живых кластеров.
        var alive = next.Clusters.Select(c => c.Name).ToHashSet(StringComparer.Ordinal);
        foreach (var rotation in next.Rotations.Where(r => alive.Contains(r.Cluster)))
            yield return new Alert(
                $"valkey-rotation-pending:{rotation.Cluster}",
                AlertSeverity.Info,
                "valkey-rotation-pending",
                rotation.Cluster,
                $"rotation of the {rotation.Role} password for cluster {rotation.Cluster} is declared and executed by the worker (two-password window, no restarts)",
                new Dictionary<string, string>
                {
                    ["role"] = rotation.Role,
                    ["requestedBy"] = rotation.RequestedBy ?? "unknown",
                    ["requestedUnix"] = rotation.RequestedUnix.ToString(),
                },
                null,
                "the rotation request is alive (key /valkeyworker/rotations/<C>): the worker is executing the two-password window E1–E3 and will remove the key; there is no cancel from the panel (arch/20 §3)",
                AlertRemedy.WorkerAuto,
                "the rotation is executed by the worker, the key will disappear; if it hangs, the worker is stuck — check the journal");

        // ===== t10 (arch/03 §8.4): видимость зависших ротационных заявок =====

        var rotationNowUnix = next.BuiltAtUtc.ToUnixTimeSeconds();

        // valkey-rotation-stale / valkey-ca-rotation-stale (warning): живая заявка
        // старше RotationStaleSeconds — видимость ДО возрастного снятия воркером.
        foreach (var rotation in next.Rotations.Where(r => alive.Contains(r.Cluster)))
        {
            var age = rotationNowUnix - rotation.RequestedUnix;
            if (age > _options.RotationStaleSeconds)
                yield return RotationStaleAlert("valkey-rotation-stale", rotation.Cluster, age, "password rotation");
        }
        foreach (var ca in (next.CaRotations ?? []).Where(r => alive.Contains(r.Cluster)))
        {
            var age = rotationNowUnix - ca.RequestedUnix;
            if (age > _options.RotationStaleSeconds)
                yield return RotationStaleAlert("valkey-ca-rotation-stale", ca.Cluster, age, "CA/cert rotation");
        }

        // valkey-ticket-expired (warning, t10): заявка снята возрастным таймаутом
        // воркера — устранить причину и повторить; гаснет перезаписью исхода done.
        foreach (var outcome in (next.TicketOutcomes ?? []).Where(
                     o => o.Outcome == "expired" && alive.Contains(o.Cluster)))
            yield return new Alert(
                $"valkey-ticket-expired:{outcome.Cluster}",
                AlertSeverity.Warning,
                "valkey-ticket-expired",
                outcome.Cluster,
                $"the request for cluster {outcome.Cluster} was dropped by the worker age timeout (kind={outcome.Kind}, reason: {outcome.Reason ?? "not specified"})",
                new Dictionary<string, string>
                {
                    ["kind"] = outcome.Kind,
                    ["reason"] = outcome.Reason ?? "",
                    ["finishedUnix"] = outcome.FinishedUnix.ToString(),
                },
                null,
                $"fix the cause ({outcome.Reason ?? "see the outcome reason"}) and re-submit the request",
                AlertRemedy.OperatorRunbook,
                "fix the cause of the waiting phase (waiting-cluster — bring the cluster up, migration T will complete it) and re-submit the request; a successful rotation will overwrite the outcome with done and the alert will clear");

        // valkey-key-malformed (warning): parseError-записи (arch/20 §5).
        foreach (var error in next.ParseErrors)
            yield return new Alert(
                $"valkey-key-malformed:{error.Key}",
                AlertSeverity.Warning,
                "valkey-key-malformed",
                error.Key,
                $"valkey key failed to parse: {error.Key}",
                new Dictionary<string, string> { ["reason"] = error.Reason },
                null,
                "the panel parser failed to parse the valkey key: a malformed value does not reach the model — the UI is blind to the key; the value format of the valkey domain is canonical per arch/20",
                AlertRemedy.OperatorRunbook,
                "fix the source of the malformed entry (external writer) and bring the value to the arch/20 canon; the next tick will parse the key");
    }

    // Stale-алерт ротационной заявки (t10): warning с возрастом и остатком до
    // возрастного снятия (связность stale = RotationTicketTimeoutSec/2 — spec §2).
    private Alert RotationStaleAlert(string kind, string cluster, long age, string what)
        => new(
            $"{kind}:{cluster}",
            AlertSeverity.Warning,
            kind,
            cluster,
            $"{what} for cluster {cluster} not started for more than {age} s (threshold {_options.RotationStaleSeconds} s) — the executor has not started",
            new Dictionary<string, string>
            {
                ["ageSec"] = age.ToString(),
                ["staleSec"] = _options.RotationStaleSeconds.ToString(),
            },
            null,
            $"the request has not been started for more than {_options.RotationStaleSeconds} s — {Math.Max(0, 2 * _options.RotationStaleSeconds - age)} s left until the worker age-drop; check the journal (waiting phase)",
            AlertRemedy.WorkerAuto,
            "the worker will drop the request by the age timeout (t10); if it hangs, check the worker journal and cluster availability");

    // valkey-endpoints-missing + valkey-security-missing + valkey-node-not-running
    // (только Active-кластер).
    private IEnumerable<Alert> ActiveClusterAlerts(
        ValkeyClusterInfo cluster, ValkeySnapshot? previous, ValkeySnapshot next)
    {
        if (string.IsNullOrEmpty(cluster.Endpoints))
            yield return new Alert(
                $"valkey-endpoints-missing:{cluster.Name}",
                AlertSeverity.Critical,
                "valkey-endpoints-missing",
                cluster.Name,
                $"Active cluster {cluster.Name} has no endpoints — client discovery is impossible",
                null, null,
                "Active cluster without endpoints: the worker appends endpoints once the node is up — without them clients cannot find the instance; every Active cluster must have endpoints",
                AlertRemedy.WorkerAuto,
                "the worker will append endpoints after provisioning; if it hangs, the node is unreachable for the worker — check the vwk-<C>-node1 container");

        // valkey-security-missing (critical, t06): Active-кластер без ca_pem —
        // миграция TLS не доиграна либо ключ потерян (arch/20 §5).
        if (!cluster.HasCaPem)
            yield return new Alert(
                $"valkey-security-missing:{cluster.Name}",
                AlertSeverity.Critical,
                "valkey-security-missing",
                cluster.Name,
                $"cluster {cluster.Name}: Active without ca_pem — TLS canon is violated",
                null, null,
                "Active cluster without ca_pem: the TLS migration (worker migrate-tls) has not completed or the key is lost; client connections without the TLS canon — risk R5 arch/21",
                AlertRemedy.OperatorRunbook,
                "wait for the worker to complete the migration (journal work/<C>, op=migrate-tls); if it hangs, check the worker and the /valkey/clusters/<C>/ca_pem key in etcd");

        var prevCluster = previous?.Clusters.FirstOrDefault(c => c.Name == cluster.Name);
        foreach (var node in cluster.NodesList)
        {
            if (node.State is null or "RUNNING")
                continue;

            // fresh-PROVISIONING (arch/03 §8.4): подъём только начался — не алертим
            // (порт IsFreshProvisioning kafka: PROVISIONING наблюдался и тик назад,
            // но окно FreshProvisioningSeconds ещё не истекло).
            if (node.State == "PROVISIONING"
                && IsFreshProvisioning(node, prevCluster, previous, next, _options.FreshProvisioningSeconds))
                continue;

            yield return new Alert(
                $"valkey-node-not-running:{cluster.Name}/{node.Name}",
                AlertSeverity.Critical,
                "valkey-node-not-running",
                $"{cluster.Name}/{node.Name}",
                $"node {node.Name} of cluster {cluster.Name} is not RUNNING: {node.State}",
                new Dictionary<string, string> { ["state"] = node.State },
                null,
                "node is not RUNNING: worker supervision must bring the node back to RUNNING (container restart/recreation); every declared node must be alive in an Active cluster",
                AlertRemedy.WorkerAuto,
                "the worker supervises the node (restart/container recreation); if it hangs, check the vwk-<C>-node1 container on the stand");
        }
    }

    private static bool IsFreshProvisioning(
        ValkeyNodeInfo node,
        ValkeyClusterInfo? prevCluster,
        ValkeySnapshot? previous,
        ValkeySnapshot next,
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
    private static long? ResolveSince(Alert alert, ValkeySnapshot? previous, long nowUnix)
    {
        if (previous is null)
            return null;
        var before = previous.Alerts.FirstOrDefault(a => a.Id == alert.Id);
        return before is null ? nowUnix : before.SinceUnix;
    }
}
