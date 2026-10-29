using AdminPanel.Core.Alerting;
using Shared.Core.DI;
using Microsoft.Extensions.Options;

namespace AdminPanel.Core.Kafka.KafkaAlerting;

// Чистая функция (KafkaSnapshot next, prev) → Alert[] — кластерные kinds волны B
// каталога arch/03 §7.4 (probe-алерты волн C добавляются сюда же; —
// ca-rotation-pending, stale-ротации, ticket-expired). sinceUnix —
// по стабильному id из prev.Alerts (механика pg AlertEngine); сортировка
// severity → kind → target.
public interface IKafkaAlertEngine
{
    IReadOnlyList<Alert> Evaluate(
        KafkaSnapshot next, KafkaSnapshot? previous, IReadOnlyCollection<string>? securityReady = null);
}

[InjectAsSingleton(typeof(IKafkaAlertEngine))]
public sealed class KafkaAlertEngine(IOptions<KafkaAlertsOptions> options) : IKafkaAlertEngine
{
    private static readonly IComparer<AlertSeverity> SeverityDescending =
        Comparer<AlertSeverity>.Create((x, y) => y.CompareTo(x));

    private readonly KafkaAlertsOptions _options = options.Value;

    // securityReady : имена кластеров с полным набором admin_user+admin_password+
    // валидного ca_pem в internal-сторе (считает KafkaSnapshotRefresher из ReadSecrets);
    // null — стор недоступен (прямые вызовы Evaluate без данных стора) — правило
    // безопасности не оценивается.
    public IReadOnlyList<Alert> Evaluate(
        KafkaSnapshot next, KafkaSnapshot? previous, IReadOnlyCollection<string>? securityReady = null)
    {
        var nowUnix = next.BuiltAtUtc.ToUnixTimeSeconds();
        return
        [
            .. Enumerate(next, previous, securityReady)
               .Select(a => a with { SinceUnix = ResolveSince(a, previous, nowUnix) })
               .OrderBy(a => a.Severity, SeverityDescending)
               .ThenBy(a => a.Kind, StringComparer.Ordinal)
               .ThenBy(a => a.Target, StringComparer.Ordinal),
        ];
    }

    private IEnumerable<Alert> Enumerate(
        KafkaSnapshot next, KafkaSnapshot? previous, IReadOnlyCollection<string>? securityReady)
    {
        // kafka-security-missing (critical, arch/15 §6): Active-кластер без
        // полного набора admin-кредов/CA в сторе — пробы SASL_SSL не исполнимы
        // (премиграционный кластер ждёт SecurityMigrator).
        if (securityReady is not null)
        {
            var ready = securityReady.ToHashSet(StringComparer.Ordinal);
            foreach (var cluster in next.Clusters.Where(
                c => c.State == KafkaClusterState.Active && !ready.Contains(c.Name)))
                yield return new Alert(
                    $"kafka-security-missing:{cluster.Name}",
                    AlertSeverity.Critical,
                    "kafka-security-missing",
                    cluster.Name,
                    $"Active cluster {cluster.Name} has no admin credentials/CA in etcd — TLS client probes and discovery do not work",
                    null,
                    null,
                    "Active cluster without admin_user/admin_password/ca_pem: worker ensure must write the secrets (or the cluster is pre-migration — SecurityMigrator will perform the migration)",
                    AlertRemedy.WorkerAuto,
                    "worker ensure/migration will add the secrets ; if it hangs — check the worker journal");
        }

        // worker-api-unreachable (critical, task etcd-via-worker-api): нет живых
        // ключей /kafkaworker/api/ (arch/02 §2.3.2) — kafka-мутации панели 503;
        // чтение данных не страдает. Pg-грань — WorkerApiUnreachableRule.
        if (next.WorkerEndpoints.Count == 0)
            yield return new Alert(
                "worker-api-unreachable:kafkaworker",
                AlertSeverity.Critical,
                "worker-api-unreachable",
                "kafkaworker",
                "KafkaWorker API unreachable: no live /kafkaworker/api/ keys — kafka mutations from the panel return 503; data reads are not affected",
                null,
                null,
                Hint: "the worker sets the lease key at startup; no key means the worker did not start or died ≤15 s ago",
                Remedy: AlertRemedy.OperatorRunbook,
                RemedyText: "start the worker container (kafka profile of the stand compose), check /healthz and KafkaWorker:Api:AdvertiseUrl");

        // worker-unhealthy (warning; arch/03 §4, arch/adminpanel/02 §2.3.2): живой
        // ключ /kafkaworker/api/<id>, но опрос /healthz ≠ 200 — процесс нездоров ДО
        // истечения lease (порт WorkerUnhealthyRule pg-грани; docker-health и панель
        // видят одно и то же — расхождений больше нет).
        foreach (var w in next.WorkerHealth.Where(w => w.Status != WorkerHealthStatus.Healthy))
        {
            var what = w.Status == WorkerHealthStatus.Degraded
                ? $"/healthz responds non-200 ({w.Detail ?? "degraded"})"
                : $"unreachable at the lease key URL ({w.Detail ?? "network error"})";
            yield return new Alert(
                $"worker-unhealthy:kafkaworker/{w.InstanceId}",
                AlertSeverity.Warning,
                "worker-unhealthy",
                $"kafkaworker/{w.InstanceId}",
                $"KafkaWorker instance {w.InstanceId} is unhealthy: {what}",
                new Dictionary<string, string>
                {
                    ["url"] = w.Url,
                    ["checked_unix"] = w.CheckedAtUtc.ToUnixTimeSeconds().ToString(),
                },
                null,
                "the lease key is alive but the process health probe is bad: /healthz sections (etcd/docker hosts/loops/snapshot) are degraded; docker-healthcheck kills the container — the lease will disappear next, followed by critical worker-api-unreachable",
                AlertRemedy.OperatorRunbook,
                "check docker logs kafkaworker and /healthz directly (sections etcd-reachable/docker-hosts/loops-alive/snapshot); restore the dependency (etcd/docker) or restart the worker container");
        }

        foreach (var cluster in next.Clusters)
        {
            switch (cluster.State)
            {
                case KafkaClusterState.NotInitialized:
                    yield return new Alert(
                        $"kafka-cluster-not-initialized:{cluster.Name}",
                        AlertSeverity.Info,
                        "kafka-cluster-not-initialized",
                        cluster.Name,
                        $"cluster {cluster.Name} declared (NOT_INITIALIZED): brokers are not up",
                        null,
                null,
                "cluster declared (config.state=NOT_INITIALIZED), brokers are not up: this is a normal lifecycle — worker provisioning will bring the brokers up and move the state to ACTIVE",
                AlertRemedy.WorkerAuto,
                "wait for broker provisioning — the worker will clear NOT_INITIALIZED; hanging longer than usual — check the worker journal");
                    break;
                case KafkaClusterState.ToRemove:
                    yield return new Alert(
                        $"kafka-cluster-to-remove:{cluster.Name}",
                        AlertSeverity.Info,
                        "kafka-cluster-to-remove",
                        cluster.Name,
                        $"cluster {cluster.Name} pending removal (TO_REMOVE): the worker is dismantling it",
                        null,
                null,
                "cluster being removed (config.state=TO_REMOVE): the worker dismantles the brokers and removes the /kafka/clusters/<C> prefix; the note lives until the dismantling completes",
                AlertRemedy.WorkerAuto,
                "the worker dismantles the cluster itself; if it hangs — check the worker journal (brokers/containers may have failed to be removed)");
                    break;
                case KafkaClusterState.Active:
                    foreach (var alert in BrokerAlerts(cluster, previous, next))
                        yield return alert;
                    foreach (var alert in TopicAlerts(cluster, nowUnix: next.BuiltAtUtc.ToUnixTimeSeconds()))
                        yield return alert;
                    foreach (var alert in LifecycleAlerts(cluster, nowUnix: next.BuiltAtUtc.ToUnixTimeSeconds()))
                        yield return alert;
                    foreach (var alert in GroupAlerts(cluster))
                        yield return alert;
                    break;
            }
        }

        // Ротационные алерты — только заявки живых кластеров: демонтаж кластера
        // удаляет заявку (arch/16 X-фазы) — вечного kafka-rotation-pending нет.
        var alive = next.Clusters.Select(c => c.Name).ToHashSet(StringComparer.Ordinal);
        foreach (var rotation in next.Rotations.Where(r => alive.Contains(r.Cluster)))
            yield return new Alert(
                $"kafka-rotation-pending:{rotation.Cluster}",
                AlertSeverity.Info,
                "kafka-rotation-pending",
                rotation.Cluster,
                $"app password rotation of cluster {rotation.Cluster} requested, executed by the worker (phases A/B/C)",
                new Dictionary<string, string>
                {
                    ["requestedBy"] = rotation.RequestedBy ?? "unknown",
                    ["requestedUnix"] = rotation.RequestedUnix.ToString(),
                },
                null,
                "app password rotation requested (key /kafkaworker/rotations/<C>): the worker executes phases A/B/C and will remove the key; every request must be cleared by its executor",
                AlertRemedy.WorkerAuto,
                "the worker executes the rotation (phases A/B/C), the key will disappear; if it hangs — the worker is stuck, check the journal");

        // Ротации admin-пароля (arch/15 §4): порт kafka-rotation-pending —
        // только заявки живых кластеров.
        foreach (var rotation in (next.AdminRotations ?? []).Where(r => alive.Contains(r.Cluster)))
            yield return new Alert(
                $"kafka-admin-rotation-pending:{rotation.Cluster}",
                AlertSeverity.Warning,
                "kafka-admin-rotation-pending",
                rotation.Cluster,
                $"admin password rotation of cluster {rotation.Cluster} requested, executed by the worker (phases A/B/C with rolling broker restarts)",
                new Dictionary<string, string>
                {
                    ["requestedBy"] = rotation.RequestedBy ?? "unknown",
                    ["requestedUnix"] = rotation.RequestedUnix.ToString(),
                },
                null,
                "admin password rotation requested (key /kafkaworker/admin_rotations/<C>): the worker executes phases A/B/C and will remove the key; applications (the app role) are not affected",
                AlertRemedy.WorkerAuto,
                "the worker executes the rotation (phases A/B/C), the key will disappear; if it hangs — the worker is stuck, check the journal");

        // Заявки ребалансировки (arch/03 §7.4) — только живых кластеров.
        foreach (var rebalance in next.Rebalances.Where(r => alive.Contains(r.Cluster)))
            yield return new Alert(
                $"kafka-rebalance-pending:{rebalance.Cluster}",
                AlertSeverity.Info,
                "kafka-rebalance-pending",
                rebalance.Cluster,
                $"partition rebalance of cluster {rebalance.Cluster} requested, executed by the worker",
                new Dictionary<string, string>
                {
                    ["requestedBy"] = rebalance.RequestedBy ?? "unknown",
                    ["requestedUnix"] = rebalance.RequestedUnix.ToString(),
                },
                null,
                "partition rebalance requested (/kafkaworker/rebalances/<C>): the worker submits batches and will clear the request once it converges; every request must complete",
                AlertRemedy.WorkerAuto,
                "the worker submits the batches and clears the request on convergence; if it hangs — check the live brokers (fallback-exec)");

        // ===== (arch/03 §7.4): видимость зависших ротационных заявок =====

        var rotationNowUnix = next.BuiltAtUtc.ToUnixTimeSeconds();

        // kafka-ca-rotation-pending (info): живая заявка CA-ротации — порт
        // kafka-rotation-pending, только живых кластеров.
        foreach (var ca in (next.CaRotations ?? []).Where(r => alive.Contains(r.Cluster)))
            yield return new Alert(
                $"kafka-ca-rotation-pending:{ca.Cluster}",
                AlertSeverity.Info,
                "kafka-ca-rotation-pending",
                ca.Cluster,
                $"CA/cert rotation of cluster {ca.Cluster} requested, executed by the worker (dual-trust window P/D/R/C)",
                new Dictionary<string, string>
                {
                    ["requestedBy"] = ca.RequestedBy ?? "unknown",
                    ["requestedUnix"] = ca.RequestedUnix.ToString(),
                },
                null,
                "CA rotation request is alive (key /kafkaworker/ca_rotations/<C>): the worker plays the dual-trust window (brokers restart one at a time) and will remove the key",
                AlertRemedy.WorkerAuto,
                "the worker executes the rotation, the key will disappear; if it hangs — check the journal (waiting phase)");

        // Stale-алерты ротационных заявок (warning): живая заявка старше
        // RotationStaleSeconds — видимость ДО возрастного снятия воркером.
        // Пароль-ротации app/admin — один алерт на кластер по старейшей заявке;
        // CA и rebalances — свои kinds.
        foreach (var group in next.Rotations
                     .Concat(next.AdminRotations ?? [])
                     .Where(r => alive.Contains(r.Cluster))
                     .GroupBy(r => r.Cluster, StringComparer.Ordinal))
        {
            var age = rotationNowUnix - group.MinBy(r => r.RequestedUnix)!.RequestedUnix;
            if (age > _options.RotationStaleSeconds)
                yield return RotationStaleAlert("kafka-rotation-stale", group.Key, age, "password rotation");
        }
        foreach (var ca in (next.CaRotations ?? []).Where(r => alive.Contains(r.Cluster)))
        {
            var age = rotationNowUnix - ca.RequestedUnix;
            if (age > _options.RotationStaleSeconds)
                yield return RotationStaleAlert("kafka-ca-rotation-stale", ca.Cluster, age, "CA/cert rotation");
        }
        foreach (var rebalance in next.Rebalances.Where(r => alive.Contains(r.Cluster)))
        {
            var age = rotationNowUnix - rebalance.RequestedUnix;
            if (age > _options.RotationStaleSeconds)
                yield return RotationStaleAlert("kafka-rebalance-stale", rebalance.Cluster, age, "partition rebalance");
        }

        // kafka-ticket-expired (warning): заявка снята возрастным таймаутом
        // воркера — устранить причину и повторить; гаснет перезаписью исхода done.
        foreach (var outcome in (next.TicketOutcomes ?? []).Where(
                     o => o.Outcome == "expired" && alive.Contains(o.Cluster)))
            yield return new Alert(
                $"kafka-ticket-expired:{outcome.Cluster}",
                AlertSeverity.Warning,
                "kafka-ticket-expired",
                outcome.Cluster,
                $"request of cluster {outcome.Cluster} cleared by the worker age timeout (kind={outcome.Kind}, reason: {outcome.Reason ?? "not specified"})",
                new Dictionary<string, string>
                {
                    ["kind"] = outcome.Kind,
                    ["reason"] = outcome.Reason ?? "",
                    ["finishedUnix"] = outcome.FinishedUnix.ToString(),
                },
                null,
                $"fix the cause ({outcome.Reason ?? "see reason in the outcome"}) and re-submit the request",
                AlertRemedy.OperatorRunbook,
                "fix the cause of the waiting phase (waiting-cluster — bring the cluster up; waiting-reassignment — wait for the execution) and re-submit the request; a successful rotation will overwrite the outcome with done and the alert will clear");

        // Стагнация reassignment : прогресс жив, но partitions_remaining не
        // двигается дольше ReassignStaleSec. Пара (prev, next): остаток тот же
        // и стоит дольше порога; prev нет — алерт по возрасту updated_unix.
        foreach (var progress in next.Reassignments.Where(p => alive.Contains(p.Cluster)))
        {
            var prevProgress = previous?.Reassignments.FirstOrDefault(p => p.Cluster == progress.Cluster);
            var stale = prevProgress is not null
                ? prevProgress.PartitionsRemaining == progress.PartitionsRemaining
                  && next.BuiltAtUtc.ToUnixTimeSeconds() - progress.UpdatedUnix > _options.ReassignStaleSec
                : next.BuiltAtUtc.ToUnixTimeSeconds() - progress.UpdatedUnix > _options.ReassignStaleSec;
            if (stale)
                yield return new Alert(
                    $"kafka-reassignment-stale:{progress.Cluster}",
                    AlertSeverity.Warning,
                    "kafka-reassignment-stale",
                    progress.Cluster,
                    $"reassignment of cluster {progress.Cluster} is stuck: partitions_remaining={progress.PartitionsRemaining} "
                    + $"unchanged for more than {_options.ReassignStaleSec} s",
                    new Dictionary<string, string>
                    {
                        ["mode"] = progress.Mode,
                        ["partitionsRemaining"] = progress.PartitionsRemaining.ToString(),
                        ["updatedUnix"] = progress.UpdatedUnix.ToString(),
                    },
                null,
                "reassignment progress has not moved for longer than the threshold: partitions_remaining must decrease (the worker submits batches); stagnation means the executor is unavailable",
                AlertRemedy.WorkerAuto,
                "the worker drives the reassignment; if it hangs — check the live brokers and execution (fallback exec via a RUNNING node)");
        }

        foreach (var error in next.ParseErrors)
            yield return new Alert(
                $"kafka-key-malformed:{error.Key}",
                AlertSeverity.Warning,
                "kafka-key-malformed",
                error.Key,
                $"kafka key not parsed: {error.Key}",
                new Dictionary<string, string> { ["reason"] = error.Reason },
                null,
                "kafka key not parsed by the panel parser: the malformed value does not reach the model — the UI is blind to the key; the kafka domain value format is canon arch/15",
                AlertRemedy.OperatorRunbook,
                "fix the source of the malformed record (external writer) and bring the value to the arch/15 canon; the next tick will parse the key");
    }

    // Топиковые алерты волны C (arch/03 §7.4): missing-desired и stale по etcd-
    // данным модели, under-replicated — по runtime-USR из пробы (refresher мерджит).
    private IEnumerable<Alert> TopicAlerts(KafkaClusterInfo cluster, long nowUnix)
    {
        foreach (var topic in cluster.Topics)
        {
            if (topic.Missing)
                yield return new Alert(
                    $"kafka-topic-missing-desired:{cluster.Name}/{topic.Name}",
                    AlertSeverity.Warning,
                    "kafka-topic-missing-desired",
                    $"{cluster.Name}/{topic.Name}",
                    $"topic {topic.Name} of cluster {cluster.Name} is missing in Kafka while the desired request is alive (the request cannot be executed; cancellation will remove the key)",
                    null,
                null,
                "topic missing in Kafka while the desired request is alive: converge cannot be executed (no topic — nothing to configure); a desired without a topic must be cleared or turned into create",
                AlertRemedy.OperatorApi,
                "cancel the request (DELETE /api/kafka/clusters/{c}/topics/{t}/desired) or create the topic (POST .../topics); a live desired on a missing topic means a hang");

            if (topic.Desired?.RequestedUnix is { } requested && nowUnix - requested > _options.StaleDesiredSeconds)
                yield return new Alert(
                    $"kafka-desired-stale:{cluster.Name}/{topic.Name}",
                    AlertSeverity.Warning,
                    "kafka-desired-stale",
                    $"{cluster.Name}/{topic.Name}",
                    $"desired request of topic {topic.Name} of cluster {cluster.Name} not cleared for more than {_options.StaleDesiredSeconds} s — converge is stuck",
                    new Dictionary<string, string>
                    {
                        ["requestedUnix"] = requested.ToString(),
                        ["requestedBy"] = topic.Desired.RequestedBy ?? "unknown",
                    },
                null,
                "desired request not cleared for longer than the threshold: worker converge must apply the desired and remove it from the topic key; a hanging request blocks new config changes",
                AlertRemedy.WorkerAuto,
                "worker converge will apply and clear the request; if it hangs — the worker is stuck or the cluster is unreachable, check the journal");

            if (topic.UnderReplicatedPartitions is > 0)
                yield return new Alert(
                    $"kafka-topic-under-replicated:{cluster.Name}/{topic.Name}",
                    AlertSeverity.Warning,
                    "kafka-topic-under-replicated",
                    $"{cluster.Name}/{topic.Name}",
                    $"partitions of topic {topic.Name} of cluster {cluster.Name} are under-replicated (ISR < RF): {topic.UnderReplicatedPartitions}",
                    new Dictionary<string, string>
                    {
                        ["underReplicatedPartitions"] = topic.UnderReplicatedPartitions.Value.ToString(),
                    },
                null,
                "partitions have ISR < RF (under-replication): replicas lag or are dead — fault tolerance is below the declared level; every partition must keep RF live replicas",
                AlertRemedy.WorkerAuto,
                "the worker will restore the replicas (broker restart/rebalance); if it hangs — check the live brokers, a recreate may be needed");
        }
    }

    // Lifecycle-алерты (arch/03 §7.4): pending-заявки + буксование.
    // Stale-порог тот же, что у desired (StaleDesiredSeconds).
    private IEnumerable<Alert> LifecycleAlerts(KafkaClusterInfo cluster, long nowUnix)
    {
        foreach (var ticket in cluster.LifecycleTickets ?? [])
        {
            if (nowUnix - ticket.RequestedUnix > _options.StaleDesiredSeconds)
            {
                yield return new Alert(
                    $"kafka-lifecycle-stale:{cluster.Name}/{ticket.Topic}",
                    AlertSeverity.Warning,
                    "kafka-lifecycle-stale",
                    $"{cluster.Name}/{ticket.Topic}",
                    $"request {ticket.Op} of topic {ticket.Topic} of cluster {cluster.Name} not executed for more than {_options.StaleDesiredSeconds} s — the worker is stuck or the cluster is unreachable",
                    new Dictionary<string, string>
                    {
                        ["op"] = ticket.Op,
                        ["requestedUnix"] = ticket.RequestedUnix.ToString(),
                        ["requestedBy"] = ticket.RequestedBy ?? "unknown",
                    },
                null,
                "lifecycle request (topic create/delete) not executed for longer than the threshold: the worker tick must execute requests (desired.create/desired.delete); a hanging request blocks topic operations",
                AlertRemedy.WorkerAuto,
                "the worker tick will execute the request; if it hangs — the worker is stuck or the cluster is unreachable (check the journal and the brokers)");
                continue;
            }

            yield return new Alert(
                $"kafka-topic-{ticket.Op}-pending:{cluster.Name}/{ticket.Topic}",
                ticket.Op == "delete" ? AlertSeverity.Warning : AlertSeverity.Info,
                ticket.Op == "delete" ? "kafka-topic-delete-pending" : "kafka-topic-create-pending",
                $"{cluster.Name}/{ticket.Topic}",
                ticket.Op == "delete"
                    ? $"topic deletion request for {ticket.Topic} of cluster {cluster.Name} is alive — the topic and its data will be deleted (can be cancelled before the tick)"
                    : $"topic creation request for {ticket.Topic} of cluster {cluster.Name} is alive — waiting for the worker tick",
                new Dictionary<string, string>
                {
                    ["requestedUnix"] = ticket.RequestedUnix.ToString(),
                    ["requestedBy"] = ticket.RequestedBy ?? "unknown",
                },
                null,
                "a topic deletion request is alive (desired.delete): the topic and its data will be deleted by the worker tick; the cancellation window is until the tick",
                AlertRemedy.WorkerAuto,
                "the worker tick will execute the request (desired.delete); to cancel before the tick — DELETE .../topics/{t}/desired.delete");
        }
    }

    // Групповые алерты волны C: totalLag > GroupLagMessages (данные пробы).
    private IEnumerable<Alert> GroupAlerts(KafkaClusterInfo cluster)
    {
        foreach (var group in cluster.Groups ?? [])
            if (group.TotalLag > _options.GroupLagMessages)
                yield return new Alert(
                    $"kafka-group-lag-high:{cluster.Name}/{group.Group}",
                    AlertSeverity.Warning,
                    "kafka-group-lag-high",
                    $"{cluster.Name}/{group.Group}",
                    $"lag of group {group.Group} of cluster {cluster.Name}: {group.TotalLag} messages (threshold {_options.GroupLagMessages})",
                    new Dictionary<string, string>
                    {
                        ["totalLag"] = group.TotalLag.ToString(),
                        ["members"] = group.Members.ToString(),
                    },
                null,
                "consumer group lag above the threshold: consumption lags behind production — data latency grows as does the risk of loss on failure; the group must keep up with retention",
                AlertRemedy.OperatorRunbook,
                "analyze the group consumers (speed/partitioning/instance liveness) — the panel and the worker do not manage consumers");
    }

    // Stale-алерт ротационной заявки : warning с возрастом и остатком до
    // возрастного снятия (связность stale = RotationTicketTimeoutSec/2 — spec §2).
    private Alert RotationStaleAlert(string kind, string cluster, long age, string what)
        => new(
            $"{kind}:{cluster}",
            AlertSeverity.Warning,
            kind,
            cluster,
            $"{what} of cluster {cluster} not started for more than {age} s (threshold {_options.RotationStaleSeconds} s) — the executor has not started",
            new Dictionary<string, string>
            {
                ["ageSec"] = age.ToString(),
                ["staleSec"] = _options.RotationStaleSeconds.ToString(),
            },
            null,
            $"request not started for more than {_options.RotationStaleSeconds} s — {Math.Max(0, 2 * _options.RotationStaleSeconds - age)} s left until the worker age-based removal; check the journal (waiting phase)",
            AlertRemedy.WorkerAuto,
            "the worker will clear the request by the age timeout ; if it hangs — check the worker journal and cluster availability");

    // kafka-broker-not-running + kafka-endpoints-missing (только Active-кластер).
    private IEnumerable<Alert> BrokerAlerts(
        KafkaClusterInfo cluster,
        KafkaSnapshot? previous,
        KafkaSnapshot next)
    {
        if (string.IsNullOrEmpty(cluster.Endpoints))
            yield return new Alert(
                $"kafka-endpoints-missing:{cluster.Name}",
                AlertSeverity.Critical,
                "kafka-endpoints-missing",
                cluster.Name,
                $"Active cluster {cluster.Name} has no endpoints — client discovery is impossible",
                null,
                null,
                "Active cluster without endpoints: the worker writes endpoints after DescribeCluster — without them clients cannot find the brokers; every Active cluster must have endpoints",
                AlertRemedy.WorkerAuto,
                "the worker will write the endpoints after DescribeCluster; if it hangs — the cluster is unreachable for the worker, check broker liveness");

        var prevCluster = previous?.Clusters.FirstOrDefault(c => c.Name == cluster.Name);
        foreach (var broker in cluster.BrokersList)
        {
            if (broker.State is null or "RUNNING")
                continue;

            // fresh-PROVISIONING (arch/03 §7.4): подъём только начался — не алертим.
            // Возраст оценивается по паре (prev, next): PROVISIONING в обоих снапшотах
            // держится как минимум next.BuiltAtUtc − prev.BuiltAtUtc (точность — тик).
            if (broker.State == "PROVISIONING"
                && IsFreshProvisioning(broker, prevCluster, previous, next, _options.FreshProvisioningSeconds))
                continue;

            yield return new Alert(
                $"kafka-broker-not-running:{cluster.Name}/{broker.Name}",
                AlertSeverity.Critical,
                "kafka-broker-not-running",
                $"{cluster.Name}/{broker.Name}",
                $"broker {broker.Name} of cluster {cluster.Name} is not RUNNING: {broker.State}",
                new Dictionary<string, string> { ["state"] = broker.State },
                null,
                "broker is not RUNNING: worker provisioning/supervision must bring the broker to RUNNING; every declared broker must be alive in an Active cluster",
                AlertRemedy.WorkerAuto,
                "the worker supervises the brokers (restart/container re-creation); if it hangs — check the broker container on the stand");
        }
    }

    private static bool IsFreshProvisioning(
        KafkaBrokerInfo broker,
        KafkaClusterInfo? prevCluster,
        KafkaSnapshot? previous,
        KafkaSnapshot next,
        int freshSeconds)
    {
        // Нет prev / в prev брокер был не PROVISIONING → статус только что начался.
        if (previous is null || prevCluster is null)
            return true;
        var prevBroker = prevCluster.BrokersList.FirstOrDefault(b => b.Name == broker.Name);
        if (prevBroker?.State != "PROVISIONING")
            return true;

        // PROVISIONING наблюдался и тик назад: fresh, пока разница BuiltAtUtc < окна.
        return next.BuiltAtUtc - previous.BuiltAtUtc < TimeSpan.FromSeconds(freshSeconds);
    }

    // sinceUnix: prev нет → null; id был в prev → перенос; новый → now (pg-механика).
    private static long? ResolveSince(Alert alert, KafkaSnapshot? previous, long nowUnix)
    {
        if (previous is null)
            return null;
        var before = previous.Alerts.FirstOrDefault(a => a.Id == alert.Id);
        return before is null ? nowUnix : before.SinceUnix;
    }
}
