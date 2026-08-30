using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;
using Microsoft.Extensions.Options;

namespace AdminPanel.Core.Kafka.KafkaAlerting;

// Чистая функция (KafkaSnapshot next, prev) → Alert[] — кластерные kinds волны B
// каталога arch/03 §7.4 (probe-алерты волн C добавляются сюда же). sinceUnix —
// по стабильному id из prev.Alerts (механика pg AlertEngine); сортировка
// severity → kind → target.
public interface IKafkaAlertEngine
{
    IReadOnlyList<Alert> Evaluate(KafkaSnapshot next, KafkaSnapshot? previous);
}

[InjectAsSingleton(typeof(IKafkaAlertEngine))]
public sealed class KafkaAlertEngine(IOptions<KafkaAlertsOptions> options) : IKafkaAlertEngine
{
    private static readonly IComparer<AlertSeverity> SeverityDescending =
        Comparer<AlertSeverity>.Create((x, y) => y.CompareTo(x));

    private readonly KafkaAlertsOptions _options = options.Value;

    public IReadOnlyList<Alert> Evaluate(KafkaSnapshot next, KafkaSnapshot? previous)
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

    private IEnumerable<Alert> Enumerate(KafkaSnapshot next, KafkaSnapshot? previous)
    {
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
                        $"кластер {cluster.Name} заявлен (NOT_INITIALIZED): брокеры не подняты",
                        null, null);
                    break;
                case KafkaClusterState.ToRemove:
                    yield return new Alert(
                        $"kafka-cluster-to-remove:{cluster.Name}",
                        AlertSeverity.Info,
                        "kafka-cluster-to-remove",
                        cluster.Name,
                        $"кластер {cluster.Name} в удалении (TO_REMOVE): воркер демонтирует",
                        null, null);
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
                $"ротация app-пароля кластера {rotation.Cluster} заявлена, исполняется воркером (фазы A/B/C)",
                new Dictionary<string, string>
                {
                    ["requestedBy"] = rotation.RequestedBy ?? "unknown",
                    ["requestedUnix"] = rotation.RequestedUnix.ToString(),
                },
                null);

        // Заявки ребалансировки (t02, arch/03 §7.4) — только живых кластеров.
        foreach (var rebalance in next.Rebalances.Where(r => alive.Contains(r.Cluster)))
            yield return new Alert(
                $"kafka-rebalance-pending:{rebalance.Cluster}",
                AlertSeverity.Info,
                "kafka-rebalance-pending",
                rebalance.Cluster,
                $"ребалансировка партиций кластера {rebalance.Cluster} заявлена, исполняется воркером",
                new Dictionary<string, string>
                {
                    ["requestedBy"] = rebalance.RequestedBy ?? "unknown",
                    ["requestedUnix"] = rebalance.RequestedUnix.ToString(),
                },
                null);

        // Стагнация reassignment (t02): прогресс жив, но partitions_remaining не
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
                    $"reassignment кластера {progress.Cluster} буксует: partitions_remaining={progress.PartitionsRemaining} "
                    + $"не меняется дольше {_options.ReassignStaleSec} c",
                    new Dictionary<string, string>
                    {
                        ["mode"] = progress.Mode,
                        ["partitionsRemaining"] = progress.PartitionsRemaining.ToString(),
                        ["updatedUnix"] = progress.UpdatedUnix.ToString(),
                    },
                    null);
        }

        foreach (var error in next.ParseErrors)
            yield return new Alert(
                $"kafka-key-malformed:{error.Key}",
                AlertSeverity.Warning,
                "kafka-key-malformed",
                error.Key,
                $"kafka-ключ не разобран: {error.Key}",
                new Dictionary<string, string> { ["reason"] = error.Reason },
                null);
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
                    $"топик {topic.Name} кластера {cluster.Name} отсутствует в Kafka при живой заявке desired (заявка не исполнима; отмена уберёт ключ)",
                    null, null);

            if (topic.Desired?.RequestedUnix is { } requested && nowUnix - requested > _options.StaleDesiredSeconds)
                yield return new Alert(
                    $"kafka-desired-stale:{cluster.Name}/{topic.Name}",
                    AlertSeverity.Warning,
                    "kafka-desired-stale",
                    $"{cluster.Name}/{topic.Name}",
                    $"заявка desired топика {topic.Name} кластера {cluster.Name} не снята дольше {_options.StaleDesiredSeconds} c — converge буксует",
                    new Dictionary<string, string>
                    {
                        ["requestedUnix"] = requested.ToString(),
                        ["requestedBy"] = topic.Desired.RequestedBy ?? "unknown",
                    },
                    null);

            if (topic.UnderReplicatedPartitions is > 0)
                yield return new Alert(
                    $"kafka-topic-under-replicated:{cluster.Name}/{topic.Name}",
                    AlertSeverity.Warning,
                    "kafka-topic-under-replicated",
                    $"{cluster.Name}/{topic.Name}",
                    $"партиции топика {topic.Name} кластера {cluster.Name} недореплицированы (ISR < RF): {topic.UnderReplicatedPartitions}",
                    new Dictionary<string, string>
                    {
                        ["underReplicatedPartitions"] = topic.UnderReplicatedPartitions.Value.ToString(),
                    },
                    null);
        }
    }

    // Lifecycle-алерты (t01, arch/03 §7.4): pending-заявки + буксование.
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
                    $"заявка {ticket.Op} топика {ticket.Topic} кластера {cluster.Name} не исполнена дольше {_options.StaleDesiredSeconds} c — воркер буксует или кластер недоступен",
                    new Dictionary<string, string>
                    {
                        ["op"] = ticket.Op,
                        ["requestedUnix"] = ticket.RequestedUnix.ToString(),
                        ["requestedBy"] = ticket.RequestedBy ?? "unknown",
                    },
                    null);
                continue;
            }

            yield return new Alert(
                $"kafka-topic-{ticket.Op}-pending:{cluster.Name}/{ticket.Topic}",
                ticket.Op == "delete" ? AlertSeverity.Warning : AlertSeverity.Info,
                ticket.Op == "delete" ? "kafka-topic-delete-pending" : "kafka-topic-create-pending",
                $"{cluster.Name}/{ticket.Topic}",
                ticket.Op == "delete"
                    ? $"заявка удаления топика {ticket.Topic} кластера {cluster.Name} жива — топик и данные будут удалены (до тика можно отменить)"
                    : $"заявка создания топика {ticket.Topic} кластера {cluster.Name} жива — ждёт тика воркера",
                new Dictionary<string, string>
                {
                    ["requestedUnix"] = ticket.RequestedUnix.ToString(),
                    ["requestedBy"] = ticket.RequestedBy ?? "unknown",
                },
                null);
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
                    $"лаг группы {group.Group} кластера {cluster.Name}: {group.TotalLag} сообщений (порог {_options.GroupLagMessages})",
                    new Dictionary<string, string>
                    {
                        ["totalLag"] = group.TotalLag.ToString(),
                        ["members"] = group.Members.ToString(),
                    },
                    null);
    }

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
                $"Active-кластер {cluster.Name} без endpoints — дискавери клиентов невозможно",
                null, null);

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
                $"брокер {broker.Name} кластера {cluster.Name} не RUNNING: {broker.State}",
                new Dictionary<string, string> { ["state"] = broker.State },
                null);
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
