using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using Shared.Core.DI;
using Microsoft.Extensions.Options;

namespace AdminPanel.Core.Valkey.ValkeyAlerting;

// Чистая функция (ValkeySnapshot next, prev) → Alert[] — каталог arch/03 §8.4.
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
                "API ValkeyWorker недоступен: живых ключей /valkeyworker/api/ нет — valkey-мутации из панели 503; чтение данных не страдает",
                null,
                null,
                Hint: "воркер ставит lease-ключ при старте; ключа нет = воркер не поднялся или умер ≤15 c назад",
                Remedy: AlertRemedy.OperatorRunbook,
                RemedyText: "запустите контейнер воркера (профиль valkey стендовой compose), проверьте /healthz и ValkeyWorker:Api:AdvertiseUrl");

        // worker-unhealthy (warning): живой ключ, /healthz ≠ 200.
        foreach (var w in next.WorkerHealth.Where(w => w.Status != WorkerHealthStatus.Healthy))
        {
            var what = w.Status == WorkerHealthStatus.Degraded
                ? $"/healthz отвечает не-200 ({w.Detail ?? "degraded"})"
                : $"недостижим по URL lease-ключа ({w.Detail ?? "network error"})";
            yield return new Alert(
                $"worker-unhealthy:valkeyworker/{w.InstanceId}",
                AlertSeverity.Warning,
                "worker-unhealthy",
                $"valkeyworker/{w.InstanceId}",
                $"инстанс ValkeyWorker {w.InstanceId} нездоров: {what}",
                new Dictionary<string, string>
                {
                    ["url"] = w.Url,
                    ["checked_unix"] = w.CheckedAtUtc.ToUnixTimeSeconds().ToString(),
                },
                null,
                "lease-ключ жив, но health-проба процесса плохая; docker-healthcheck гасит контейнер — за этим последует исчезновение lease и critical worker-api-unreachable",
                AlertRemedy.OperatorRunbook,
                "смотрите docker logs valkeyworker и /healthz напрямую; поднимите зависимость (etcd/docker) или перезапустите контейнер воркера");
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
                        $"кластер {cluster.Name} заявлен (NOT_INITIALIZED): нода не поднята",
                        null, null,
                        "кластер заявлен (config.state=NOT_INITIALIZED): provisioning воркера поднимет ноду и переведёт state в ACTIVE",
                        AlertRemedy.WorkerAuto,
                        "дождитесь provisioning ноды — воркер снимет NOT_INITIALIZED; висит дольше обычного — смотрите journal воркера");
                    break;
                case ValkeyClusterState.ToRemove:
                    yield return new Alert(
                        $"valkey-cluster-to-remove:{cluster.Name}",
                        AlertSeverity.Info,
                        "valkey-cluster-to-remove",
                        cluster.Name,
                        $"кластер {cluster.Name} в удалении (TO_REMOVE): воркер демонтирует",
                        null, null,
                        "кластер в удалении (config.state=TO_REMOVE): воркер демонтирует ноду и уберёт префикс /valkey/clusters/<C>",
                        AlertRemedy.WorkerAuto,
                        "воркер демонтирует кластер сам; висит — проверьте journal воркера (контейнер мог не удалиться)");
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
                $"ротация {rotation.Role}-пароля кластера {rotation.Cluster} заявлена, исполняется воркером (окно двух паролей, без рестартов)",
                new Dictionary<string, string>
                {
                    ["role"] = rotation.Role,
                    ["requestedBy"] = rotation.RequestedBy ?? "unknown",
                    ["requestedUnix"] = rotation.RequestedUnix.ToString(),
                },
                null,
                "заявка ротации жива (ключ /valkeyworker/rotations/<C>): воркер исполняет окно двух паролей E1–E3 и снимет ключ; отмены из панели нет (arch/20 §3)",
                AlertRemedy.WorkerAuto,
                "ротацию исполняет воркер, ключ исчезнет; висит — воркер буксует, проверьте journal");

        // valkey-key-malformed (warning): parseError-записи (arch/20 §5).
        foreach (var error in next.ParseErrors)
            yield return new Alert(
                $"valkey-key-malformed:{error.Key}",
                AlertSeverity.Warning,
                "valkey-key-malformed",
                error.Key,
                $"valkey-ключ не разобран: {error.Key}",
                new Dictionary<string, string> { ["reason"] = error.Reason },
                null,
                "valkey-ключ не разобран парсером панели: битое значение не попадает в модель — UI слеп к ключу; формат значений valkey-домена — канон arch/20",
                AlertRemedy.OperatorRunbook,
                "устраните источник битой записи (внешний писатель) и приведите значение к канону arch/20; повторный тик распарсит ключ");
    }

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
                $"Active-кластер {cluster.Name} без endpoints — дискавери клиентов невозможно",
                null, null,
                "Active-кластер без endpoints: endpoints дописывает воркер по факту подъёма ноды — без них клиенты не найдут инстанс; каждый Active-кластер обязан иметь endpoints",
                AlertRemedy.WorkerAuto,
                "воркер допишет endpoints по факту provisioning; висит — нода недоступна воркеру, проверьте контейнер vwk-<C>-node1");

        // valkey-security-missing (critical, t06): Active-кластер без ca_pem —
        // миграция TLS не доиграна либо ключ потерян (arch/20 §5).
        if (!cluster.HasCaPem)
            yield return new Alert(
                $"valkey-security-missing:{cluster.Name}",
                AlertSeverity.Critical,
                "valkey-security-missing",
                cluster.Name,
                $"кластер {cluster.Name}: Active без ca_pem — TLS-канон не соблюдён",
                null, null,
                "Active-кластер без ca_pem: миграция TLS (migrate-tls воркера) не доиграна или ключ потерян; клиентские подключения без TLS-канона — риск R5 arch/21",
                AlertRemedy.OperatorRunbook,
                "дождитесь доигрывания миграции воркером (journal work/<C>, op=migrate-tls); висит — проверьте воркера и ключ /valkey/clusters/<C>/ca_pem в etcd");

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
                $"нода {node.Name} кластера {cluster.Name} не RUNNING: {node.State}",
                new Dictionary<string, string> { ["state"] = node.State },
                null,
                "нода не в RUNNING: надзор воркера обязан привести ноду в RUNNING (рестарт/пересоздание контейнера); каждая заявленная нода обязана быть жива в Active-кластере",
                AlertRemedy.WorkerAuto,
                "воркер supervises ноду (restart/пересоздание контейнера); висит — проверьте контейнер vwk-<C>-node1 на стенде");
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
