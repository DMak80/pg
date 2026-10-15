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
                "API S3Worker недоступен: живых ключей /s3worker/api/ нет — s3-мутации из панели 503; чтение данных не страдает",
                null,
                null,
                Hint: "воркер ставит lease-ключ при старте; ключа нет = воркер не поднялся или умер ≤15 c назад",
                Remedy: AlertRemedy.OperatorRunbook,
                RemedyText: "запустите контейнер s3worker (профиль s3 deploy-контура), проверьте /healthz и S3Worker:Api:AdvertiseUrl");

        // 2. worker-unhealthy (warning): живой ключ, /healthz ≠ 200.
        foreach (var w in next.WorkerHealth.Where(w => w.Status != WorkerHealthStatus.Healthy))
        {
            var what = w.Status == WorkerHealthStatus.Degraded
                ? $"/healthz отвечает не-200 ({w.Detail ?? "degraded"})"
                : $"недостижим по URL lease-ключа ({w.Detail ?? "network error"})";
            yield return new Alert(
                $"worker-unhealthy:s3worker/{w.InstanceId}",
                AlertSeverity.Warning,
                "worker-unhealthy",
                $"s3worker/{w.InstanceId}",
                $"инстанс S3Worker {w.InstanceId} нездоров: {what}",
                new Dictionary<string, string>
                {
                    ["url"] = w.Url,
                    ["checked_unix"] = w.CheckedAtUtc.ToUnixTimeSeconds().ToString(),
                },
                null,
                "lease-ключ жив, но health-проба процесса плохая; docker-healthcheck гасит контейнер — за этим последует исчезновение lease и critical worker-api-unreachable",
                AlertRemedy.OperatorRunbook,
                "смотрите docker logs s3worker и /healthz напрямую; поднимите зависимость (etcd/docker) или перезапустите контейнер воркера");
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
                        $"кластер {cluster.Name} заявлен (NOT_INITIALIZED): инстансы не подняты",
                        null, null,
                        "кластер заявлен (config.state=NOT_INITIALIZED): provisioning воркера поднимет инстансы и переведёт state в Active",
                        AlertRemedy.WorkerAuto,
                        "дождитесь provisioning — воркер снимет NOT_INITIALIZED; висит — смотрите /s3worker/work/<C>");
                    break;

                // 4. s3-cluster-to-remove (info).
                case S3ClusterState.ToRemove:
                    yield return new Alert(
                        $"s3-cluster-to-remove:{cluster.Name}",
                        AlertSeverity.Info,
                        "s3-cluster-to-remove",
                        cluster.Name,
                        $"кластер {cluster.Name} в удалении (TO_REMOVE): воркер демонтирует",
                        null, null,
                        "кластер в удалении (config.state=TO_REMOVE): воркер демонтирует инстансы и уберёт префикс /s3/clusters/<C>",
                        AlertRemedy.WorkerAuto,
                        "воркер демонтирует кластер сам; висит — проверьте /s3worker/work/<C>");
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
                $"ротация секретов профиля кластера {rotation.Cluster} заявлена, исполняется воркером (rolling-пересоздание инстансов)",
                new Dictionary<string, string>
                {
                    ["requestedBy"] = rotation.RequestedBy ?? "unknown",
                    ["requestedUnix"] = rotation.RequestedUnix.ToString(),
                },
                null,
                "заявка ротации жива (ключ /s3worker/rotations/<C>): воркер переиздаст env-профиль rolling-пересозданием инстансов и снимет ключ; отмены из панели нет (arch/23 §8)",
                AlertRemedy.WorkerAuto,
                "ротацию исполняет воркер, ключ исчезнет; висит — воркер буксует, проверьте /s3worker/work/<C>");

            var age = next.BuiltAtUtc.ToUnixTimeSeconds() - rotation.RequestedUnix;
            if (age > _options.RotationStaleSeconds)
                yield return new Alert(
                    $"s3-rotation-stale:{rotation.Cluster}",
                    AlertSeverity.Warning,
                    "s3-rotation-stale",
                    rotation.Cluster,
                    $"заявка ротации секретов кластера {rotation.Cluster} жива дольше {age} c (порог {_options.RotationStaleSeconds} c) — исполнитель не стартует",
                    new Dictionary<string, string>
                    {
                        ["ageSec"] = age.ToString(),
                        ["staleSec"] = _options.RotationStaleSeconds.ToString(),
                    },
                    null,
                    $"заявка не исполнена дольше {_options.RotationStaleSeconds} c — воркер буксует или не видит заявку; снятие — только воркером",
                    AlertRemedy.WorkerAuto,
                    "проверьте /s3worker/work/<C> и доступность S3-нод; заявка снимется исполнением или возрастным таймаутом воркера");
        }

        // 17. s3-key-malformed (warning): parseError-записи (arch/23 §2).
        foreach (var error in next.ParseErrors)
            yield return new Alert(
                $"s3-key-malformed:{error.Key}",
                AlertSeverity.Warning,
                "s3-key-malformed",
                error.Key,
                $"s3-ключ не разобран: {error.Key}",
                new Dictionary<string, string> { ["reason"] = error.Reason },
                null,
                "s3-ключ не разобран парсером панели: битое значение не попадает в модель — UI слеп к ключу; формат значений s3-домена — канон arch/23",
                AlertRemedy.OperatorRunbook,
                "устраните источник битой записи (внешний писатель) и приведите значение к канону arch/23; повторный тик распарсит ключ");
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
                $"Active-кластер {cluster.Name} с нодами без endpoints — дискавери клиентов невозможно",
                null, null,
                "Active-кластер с поднятой нодой без endpoints: воркер дописывает endpoints по факту подъёма — без них клиенты не найдут хранилище",
                AlertRemedy.WorkerAuto,
                "воркер допишет endpoints по факту provisioning; висит — проверьте /s3worker/work/<C> и контейнер s3w-<C>-<I>");

        var nowUnix = next.BuiltAtUtc.ToUnixTimeSeconds();

        // 7/8. s3-connectivity-degraded (warning) / s3-connectivity-down (critical).
        if (cluster.Connectivity?.State == "DEGRADED")
            yield return new Alert(
                $"s3-connectivity-degraded:{cluster.Name}",
                AlertSeverity.Warning,
                "s3-connectivity-degraded",
                cluster.Name,
                $"связность кластера {cluster.Name} DEGRADED — часть настроенных линков не проходит",
                null, null,
                "зонд связности увидел offline-линки при живых нодах: репликация по этим парам не идёт (arch/23 §3)",
                AlertRemedy.WorkerAuto,
                "зонд воркера перекачает матрицу; устойчиво DEGRADED — проверьте ноды линков и сеть docker-хостов");

        if (cluster.Connectivity?.State == "DOWN")
            yield return new Alert(
                $"s3-connectivity-down:{cluster.Name}",
                AlertSeverity.Critical,
                "s3-connectivity-down",
                cluster.Name,
                $"связность кластера {cluster.Name} DOWN — ноды не слышат друг друга",
                null, null,
                "зонд связности не прошёл ни по одному линку: репликация графов не идёт, данные бэкапов расходятся (arch/23 §3)",
                AlertRemedy.OperatorRunbook,
                "проверьте контейнеры нод s3w-<C>-<I> и сеть docker-хостов; зонд перекачает матрицу после восстановления");

        // 9/10. s3-sync-lagging (warning) / s3-sync-diverged (critical).
        if (cluster.Sync?.State == "LAGGING")
            yield return new Alert(
                $"s3-sync-lagging:{cluster.Name}",
                AlertSeverity.Warning,
                "s3-sync-lagging",
                cluster.Name,
                $"синхронность кластера {cluster.Name} LAGGING — пары сверяются с расхождениями",
                null, null,
                "зонд сверил листинга приёмника/источника: есть missing/extra — репликация не догнала (arch/23 §3)",
                AlertRemedy.WorkerAuto,
                "зонд воркера перекачает; устойчивый лаг — проверьте очередь репликации нод");

        if (cluster.Sync?.State == "DIVERGED")
            yield return new Alert(
                $"s3-sync-diverged:{cluster.Name}",
                AlertSeverity.Critical,
                "s3-sync-diverged",
                cluster.Name,
                $"синхронность кластера {cluster.Name} DIVERGED — данные пар расходятся",
                null, null,
                "зонд увидел missing/extra после догоняющих прогонов: часть бэкапов не реплицируется на пары (arch/23 §3)",
                AlertRemedy.OperatorRunbook,
                "проверьте линк пар и логи нод s3w-<C>-<I>; после починки зонд перекачает и вердикт поднимется");

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
                $"зонд связности/синхронности кластера {cluster.Name} молчит (ключей нет или checked_unix старше {_options.SyncStaleSec} c)",
                null, null,
                "у Active-кластера с RUNNING-нодами нет свежих connectivity/sync: зонд воркера не пишет — наблюдение слепо",
                AlertRemedy.OperatorRunbook,
                "проверьте зонд воркера: /s3worker/work/<C> op=sync-probe; ворот-periodic/внеплановый зонд обновит checked_unix");

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
                $"инстанс {node.Name} кластера {cluster.Name} не RUNNING: {node.State}",
                new Dictionary<string, string> { ["state"] = node.State! },
                null,
                "инстанс не в RUNNING: надзор воркера обязан привести его в RUNNING (рестарт/пересоздание контейнера); каждая заявленная нода обязана быть жива в Active-кластере",
                AlertRemedy.WorkerAuto,
                "надзор C пересоздаст контейнер; висит — проверьте контейнер s3w-<C>-<I>");
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
                        $"диск инстанса {node.Name} кластера {cluster.Name} занят на {usage.UsedBytes * 100 / Math.Max(1, usage.TotalBytes)}% (порог {_options.DiskUsagePercent}%)",
                        new Dictionary<string, string>
                        {
                            ["usedBytes"] = usage.UsedBytes.ToString(),
                            ["totalBytes"] = usage.TotalBytes.ToString(),
                        },
                        null,
                        "занятость диска инстанса превысила порог/декларацию: переполнение остановит приём бэкапов",
                        AlertRemedy.OperatorApi,
                        $"увеличьте диск: PUT /api/s3/clusters/{cluster.Name}/nodes/{node.Name}/resources");
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
                $"ноды кластера {cluster.Name} на одном хосте {host.Key}: {names}",
                new Dictionary<string, string>
                {
                    ["host"] = host.Key!,
                    ["nodes"] = names,
                },
                null,
                "несколько нод кластера размещены на одном docker-хосте: гибель хоста теряет долю данных; гео-раснос — обязанность оператора (arch/23 §4)",
                AlertRemedy.OperatorRunbook,
                "гео-раснос — обязанность оператора, arch/23 §4; вывод/перевоз инстанса");
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
                $"инстанс {node.Name} кластера {cluster.Name} вне графа репликации — ни одной связи в links",
                null, null,
                "нода Active-кластера не встречается ни в одной паре links: её объекты не реплицируются — риск безрепликационной доли",
                AlertRemedy.OperatorApi,
                $"добавьте связь: PUT /api/s3/clusters/{cluster.Name}/links");
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
