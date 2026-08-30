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
