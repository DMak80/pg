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
    IReadOnlyList<Alert> Evaluate(
        KafkaSnapshot next, KafkaSnapshot? previous, IReadOnlyCollection<string>? securityReady = null);
}

[InjectAsSingleton(typeof(IKafkaAlertEngine))]
public sealed class KafkaAlertEngine(IOptions<KafkaAlertsOptions> options) : IKafkaAlertEngine
{
    private static readonly IComparer<AlertSeverity> SeverityDescending =
        Comparer<AlertSeverity>.Create((x, y) => y.CompareTo(x));

    private readonly KafkaAlertsOptions _options = options.Value;

    // securityReady (t03): имена кластеров с полным набором admin_user+admin_password+
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
        // kafka-security-missing (critical, t03, arch/15 §6): Active-кластер без
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
                    $"Active-кластер {cluster.Name} без admin-кредов/CA в etcd — пробы и дискавери TLS-клиентов не работают",
                    null,
                    null,
                    "Active-кластер без admin_user/admin_password/ca_pem: ensure воркера обязан дописать секреты (или кластер премиграционный — SecurityMigrator выполнит миграцию)",
                    AlertRemedy.WorkerAuto,
                    "ensure/миграция воркера дополнят секреты (t03); висит — проверьте journal воркера");
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
                "API KafkaWorker недоступен: живых ключей /kafkaworker/api/ нет — kafka-мутации из панели 503; чтение данных не страдает",
                null,
                null,
                Hint: "воркер ставит lease-ключ при старте; ключа нет = воркер не поднялся или умер ≤15 c назад",
                Remedy: AlertRemedy.OperatorRunbook,
                RemedyText: "запустите контейнер воркера (профиль kafka стендовой compose), проверьте /healthz и KafkaWorker:Api:AdvertiseUrl");

        // worker-unhealthy (warning, t09; arch/03 §4, arch/adminpanel/02 §2.3.2): живой
        // ключ /kafkaworker/api/<id>, но опрос /healthz ≠ 200 — процесс нездоров ДО
        // истечения lease (порт WorkerUnhealthyRule pg-грани; docker-health и панель
        // видят одно и то же — расхождений больше нет).
        foreach (var w in next.WorkerHealth.Where(w => w.Status != WorkerHealthStatus.Healthy))
        {
            var what = w.Status == WorkerHealthStatus.Degraded
                ? $"/healthz отвечает не-200 ({w.Detail ?? "degraded"})"
                : $"недостижим по URL lease-ключа ({w.Detail ?? "network error"})";
            yield return new Alert(
                $"worker-unhealthy:kafkaworker/{w.InstanceId}",
                AlertSeverity.Warning,
                "worker-unhealthy",
                $"kafkaworker/{w.InstanceId}",
                $"инстанс KafkaWorker {w.InstanceId} нездоров: {what}",
                new Dictionary<string, string>
                {
                    ["url"] = w.Url,
                    ["checked_unix"] = w.CheckedAtUtc.ToUnixTimeSeconds().ToString(),
                },
                null,
                "lease-ключ жив, но health-проба процесса плохая: секции /healthz (etcd/docker-хосты/циклы/снапшот) деградированы; docker-healthcheck гасит контейнер — за этим последует исчезновение lease и critical worker-api-unreachable",
                AlertRemedy.OperatorRunbook,
                "смотрите docker logs kafkaworker и /healthz напрямую (секции etcd-reachable/docker-hosts/loops-alive/snapshot); поднимите зависимость (etcd/docker) или перезапустите контейнер воркера");
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
                        $"кластер {cluster.Name} заявлен (NOT_INITIALIZED): брокеры не подняты",
                        null,
                null,
                "кластер заявлен (config.state=NOT_INITIALIZED), брокеры не подняты: это нормальный жизненный цикл — provisioning воркера поднимет брокеров и переведёт state в ACTIVE",
                AlertRemedy.WorkerAuto,
                "дождитесь provisioning брокеров — воркер снимет NOT_INITIALIZED; висит дольше обычного — смотрите journal воркера");
                    break;
                case KafkaClusterState.ToRemove:
                    yield return new Alert(
                        $"kafka-cluster-to-remove:{cluster.Name}",
                        AlertSeverity.Info,
                        "kafka-cluster-to-remove",
                        cluster.Name,
                        $"кластер {cluster.Name} в удалении (TO_REMOVE): воркер демонтирует",
                        null,
                null,
                "кластер в удалении (config.state=TO_REMOVE): воркер демонтирует брокеров и уберёт префикс /kafka/clusters/<C>; заметка живёт до завершения демонтажа",
                AlertRemedy.WorkerAuto,
                "воркер демонтирует кластер сам; висит — проверьте journal воркера (брокеры/контейнеры могли не удалиться)");
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
                null,
                "ротация app-пароля заявлена (ключ /kafkaworker/rotations/<C>): воркер исполняет фазы A/B/C и снимет ключ; каждая заявка обязана сниматься исполнителем",
                AlertRemedy.WorkerAuto,
                "ротацию исполняет воркер (фазы A/B/C), ключ исчезнет; висит — воркер буксует, проверьте journal");

        // Ротации admin-пароля (t03, arch/15 §4): порт kafka-rotation-pending —
        // только заявки живых кластеров.
        foreach (var rotation in (next.AdminRotations ?? []).Where(r => alive.Contains(r.Cluster)))
            yield return new Alert(
                $"kafka-admin-rotation-pending:{rotation.Cluster}",
                AlertSeverity.Warning,
                "kafka-admin-rotation-pending",
                rotation.Cluster,
                $"ротация admin-пароля кластера {rotation.Cluster} заявлена, исполняется воркером (фазы A/B/C с rolling-рестартами брокеров)",
                new Dictionary<string, string>
                {
                    ["requestedBy"] = rotation.RequestedBy ?? "unknown",
                    ["requestedUnix"] = rotation.RequestedUnix.ToString(),
                },
                null,
                "ротация admin-пароля заявлена (ключ /kafkaworker/admin_rotations/<C>): воркер исполняет фазы A/B/C и снимет ключ; приложения (роль app) не затрагиваются",
                AlertRemedy.WorkerAuto,
                "ротацию исполняет воркер (фазы A/B/C), ключ исчезнет; висит — воркер буксует, проверьте journal");

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
                null,
                "ребалансировка партиций заявлена (/kafkaworker/rebalances/<C>): воркер подаёт батчи и снимет заявку по сходимости; каждая заявка обязана завершаться",
                AlertRemedy.WorkerAuto,
                "батчи подаёт воркер, заявку снимет по сходимости; висит — проверьте живые брокеры (fallback-exec)");

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
                null,
                "прогресс reassignment не двигается дольше порога: partitions_remaining обязан убывать (воркер подаёт батчи); стагнация означает недоступность исполнителя",
                AlertRemedy.WorkerAuto,
                "воркер двигает reassignment; висит — проверьте живые брокеры и исполнение (fallback exec через RUNNING-узел)");
        }

        foreach (var error in next.ParseErrors)
            yield return new Alert(
                $"kafka-key-malformed:{error.Key}",
                AlertSeverity.Warning,
                "kafka-key-malformed",
                error.Key,
                $"kafka-ключ не разобран: {error.Key}",
                new Dictionary<string, string> { ["reason"] = error.Reason },
                null,
                "kafka-ключ не разобран парсером панели: битое значение не попадает в модель — UI слеп к ключу; формат значений kafka-домена — канон arch/15",
                AlertRemedy.OperatorRunbook,
                "устраните источник битой записи (внешний писатель) и приведите значение к канону arch/15; повторный тик распарсит ключ");
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
                    null,
                null,
                "топик отсутствует в Kafka при живой заявке desired: converge не исполним (топика нет — конфигуировать нечего); desired без топика обязан сниматься или превращаться в create",
                AlertRemedy.OperatorApi,
                "отмените заявку (DELETE /api/kafka/clusters/{c}/topics/{t}/desired) либо создайте топик (POST .../topics); живой desired на missing-топике — зависание");

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
                null,
                "заявка desired не снята дольше порога: converge воркера обязан применить desired и удалить его из ключа топика; висящая заявка блокирует новые конфиг-изменения",
                AlertRemedy.WorkerAuto,
                "converge воркера применит и снимет заявку; висит — воркер буксует или кластер недоступен, проверьте journal");

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
                null,
                "у партиций ISR < RF (недорепликация): реплики отстали/умерли — устойчивость к отказам ниже заявленной; каждая партиция обязана держать RF живых реплик",
                AlertRemedy.WorkerAuto,
                "воркер восстановит реплики (restart брокера/ребалансировка); висит — проверьте живые брокеры, возможно нужен recreate");
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
                null,
                "lifecycle-заявка (create/delete топика) не исполнена дольше порога: тик воркера обязан исполнять заявки (desired.create/desired.delete); висящая заявка блокирует операции с топиком",
                AlertRemedy.WorkerAuto,
                "тик воркера исполнит заявку; висит — воркер буксует или кластер недоступен (проверьте journal и брокеров)");
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
                null,
                "жива заявка удаления топика (desired.delete): топик и данные будут удалены тиком воркера; окно отмены — до тика",
                AlertRemedy.WorkerAuto,
                "заявку исполнит тик воркера (desired.delete); отменить до тика — DELETE .../topics/{t}/desired.delete");
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
                null,
                "лаг группы потребителей выше порога: потребление отстаёт от продакшена — растёт латентность данных и риск потери при сбое; группа обязана успевать за retention",
                AlertRemedy.OperatorRunbook,
                "разберите потребителей группы (скорость/партиционирование/живость инстансов) — панель и воркер консьюмерами не управляют");
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
                null,
                null,
                "Active-кластер без endpoints: endpoints дописывает воркер по факту DescribeCluster — без них клиенты не найдут брокеров; каждый Active-кластер обязан иметь endpoints",
                AlertRemedy.WorkerAuto,
                "воркер допишет endpoints по факту DescribeCluster; висит — кластер недоступен воркеру, проверьте живость брокеров");

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
                null,
                "брокер не в RUNNING: provisioning/надзор воркера обязан привести брокер в RUNNING; каждый заявленный брокер обязан быть жив в Active-кластере",
                AlertRemedy.WorkerAuto,
                "воркер supervises брокеры (restart/пересоздание контейнера); висит — проверьте контейнер брокера на стенде");
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
