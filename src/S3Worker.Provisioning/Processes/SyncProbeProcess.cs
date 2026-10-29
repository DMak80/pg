using System.Diagnostics.Metrics;
using System.Text.Json;
using S3Worker.Core.Model;
using S3Worker.Core.OwnS3;
using Shared.Core;
using Shared.Etcd.Client;

namespace S3Worker.Provisioning.Processes;

/// <summary>
/// Процесс F — SyncProbe (arch/24 §5 F, spec §3.2–§3.5): зонд
/// связности/синхронности ownS3-кластера — читатель факта, не надзиратель.
/// Прогон: probe-опрос всех RUNNING (матрица настроенных линков + пары сверек),
/// статусы приёмников (fetch-очередь для lag_objects), постраничная сверка пар
/// (sync-report → количественный diff листингов при расхождении), запись двух
/// ключей наблюдения (arch/23 §3 — connectivity/sync, один checked_unix) и del
/// заявки /s3worker/probes/&lt;C&gt;. Никакого лечения — доводка состава зона G.
/// Триггеры: заявка ИЛИ истечение гейта SyncIntervalSec (checked_unix ключа
/// connectivity); тихий тик — без journal-записи. Все грани — через
/// IOwnS3NodeClient (единственная точка транспорта); etcd — failover.
/// Ретраи — тиком (поллинг-инвариант: вызов грани ≤ 7.5 с, страниц — бюджет).
/// </summary>
public sealed class SyncProbeProcess(
    IEtcdGateway gateway,
    string[] endpoints,
    ClaimStore claims,
    WorkJournal journal,
    ClusterSecretsStore secrets,
    IOwnS3NodeClient nodeClient,
    S3ProvisioningOptions options,
    TimeProvider clock,
    Meter meter,
    Shared.Core.Hosting.ILoopProgress? progress = null)
{
    public const string Op = "sync-probe";

    // Диагностика усекается как last_error журнала (канон arch/23 §3).
    private const int DiagnosticsLimit = 300;

    private readonly Counter<long> _runs = meter.CreateCounter<long>("s3worker_sync_probe_runs_total");
    private readonly Counter<long> _links = meter.CreateCounter<long>("s3worker_sync_probe_links_total");
    private readonly Counter<long> _pairs = meter.CreateCounter<long>("s3worker_sync_probe_pairs_total");

    public async Task<Result> TickAsync(S3ClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Cluster;

        // Мутации — только держателем живого клэйма (arch/17 S5).
        var claimed = S3ProcessCommon.EnsureClaimed(claims, cluster, Op);
        if (!claimed.IsSuccess)
            return claimed;

        // 0 RUNNING → idle (до гейта; по букве §3.5 п.1) + del заявки:
        // «исполнена — нечем зондировать».
        var running = snap.Nodes.Where(n => n.State == S3NodeStates.Running).Select(n => n.Node)
            .OrderBy(n => n, StringComparer.Ordinal).ToList();
        if (running.Count == 0)
            return await IdleAsync(cluster, deleteRequest: true, warning: null, ct);

        // Креды — per-cluster secrets из etcd (arch/23 §6): грани недостижимы
        // без них — диагностический skip (ретраи тиком; значения секретов
        // в диагностики не попадают); заявка живёт — исполнится после починки.
        var secretsRead = await secrets.TryReadAsync(cluster, ct);
        var diagnostic = secretsRead.Error?.Message
            ?? $"secrets /s3/clusters/{cluster}/secrets not found";
        if (!secretsRead.IsSuccess || secretsRead.Value is null)
        {
            // Фаза называет причину (не «idle»); заявка зонда НЕ сносится —
            // исполнится после починки кластера (ретраи тиком).
            var rejected = await journal.WritePhaseAsync(
                cluster, Op, "invalid-secrets", claims.InstanceId, diagnostic, ct);
            return rejected.IsSuccess ? Result.Success() : rejected;
        }
        var creds = secretsRead.Value.ToNodeSecrets();

        // Заявка + прошлый connectivity (гейт). Отказ чтения etcd → Failed
        // (ретрай тиком, заявка жива).
        var request = await S3ProcessCommon.GetWithFailoverAsync(
            gateway, endpoints, S3ProcessCommon.ProbeRequestKey(cluster), ct);
        if (!request.IsSuccess)
            return Fail(cluster, request.Error!, "read-probe-request");
        var pastConnectivity = await S3ProcessCommon.GetWithFailoverAsync(
            gateway, endpoints, S3ProcessCommon.ConnectivityKey(cluster), ct);
        if (!pastConnectivity.IsSuccess)
            return Fail(cluster, pastConnectivity.Error!, "read-connectivity");

        var now = clock.GetUtcNow().ToUnixTimeSeconds();
        var pastChecked = S3ProcessCommon.ParseCheckedUnix(pastConnectivity.Value?.Value);
        var gated = request.Value is not null
            || pastChecked is null
            || now - pastChecked.Value >= options.SyncIntervalSec;
        if (!gated)
            return Result.Success(); // тихий тик: journal НЕ пишем (§3.5 п.2)

        return await RunAsync(cluster, snap, running, creds!, request.Value is not null, now, ct);
    }

    // Прогон зонда (§3.2–§3.4): probe-опрос → матрица+пары → статусы приёмников →
    // свёртка пар → запись ключей + del заявки + journal.
    private async Task<Result> RunAsync(string cluster, S3ClusterSnapshot snap, List<string> running,
        OwnS3NodeSecrets creds, bool requestPresent, long now, CancellationToken ct)
    {
        var warnings = new List<string>();
        var probeErrors = new List<string>();

        // Прошлое значение sync — прогресс-критерий вердиктов (§3.3). Отказ
        // чтения → Failed: гейт/заявка уже пройдены, ключи старые не трогаем.
        var pastSync = await S3ProcessCommon.GetWithFailoverAsync(
            gateway, endpoints, S3ProcessCommon.SyncKey(cluster), ct);
        if (!pastSync.IsSuccess)
            return Fail(cluster, pastSync.Error!, "read-sync");
        var past = S3ProcessCommon.ParseSyncDiscrepancies(pastSync.Value?.Value);

        // 1. portalloc (адреса опроса; формат arch/23 §8 — словарь по именам).
        var addresses = await ReadPortAllocAsync(cluster, ct);
        if (!addresses.IsSuccess)
            return Fail(cluster, addresses.Error!, "portalloc");

        // 2. Probe-опрос последовательно по RUNNING (Ordinal); внутри ноды
        // параллелит сама грань. Отказ → строки ноды в матрице нет (§3.2 п.4),
        // реконструкции нет.
        var reports = new SortedDictionary<string, OwnS3ProbeReport>(StringComparer.Ordinal);
        foreach (var node in running)
        {
            if (!addresses.Value.TryGetValue(node, out var addr))
            {
                warnings.Add($"{node}: no address in portalloc — transient");
                continue;
            }

            progress?.Mark(); // долгая фаза: таймаут грани до 7.5 с — watchdog не должен увидеть зависание
            var probe = await nodeClient.ProbeAsync(
                options.AdvertisedHost ?? addr.Host, addr.S3Port, creds, ct);
            if (!probe.IsSuccess)
            {
                probeErrors.Add($"{node}: {probe.Error!.Message}");
                continue;
            }

            reports[node] = probe.Value;
        }

        // 3. Матрица — только ФАКТИЧЕСКИЕ targets ответивших нод (состав
        // targets.json, не декларация links); rttMs в etcd не пишется.
        var links = new SortedDictionary<string, SortedDictionary<string, string>>(StringComparer.Ordinal);
        var directed = new SortedSet<(string From, string To)>();
        foreach (var (node, report) in reports)
        {
            foreach (var target in report.Targets)
            {
                var outcome = target.Reachable ? "ok" : "offline";
                if (!links.TryGetValue(node, out var row))
                    links[node] = row = new SortedDictionary<string, string>(StringComparer.Ordinal);
                row[target.Name] = outcome;
                _links.Add(1, new KeyValuePair<string, object?>("result", outcome));
                directed.Add((node, target.Name));
            }
        }

        // 4. Агрегат connectivity (§3.2 п.5).
        var probeError = probeErrors.Count > 0 ? Truncate(string.Join("; ", probeErrors)) : null;
        string connectivityState;
        if (reports.Count == 0)
        {
            // 0 ответивших при непустом составе RUNNING — DOWN, диагностика
            // обязательна (связность не подтверждена ни по одному линку).
            connectivityState = "DOWN";
            probeError ??= Truncate(string.Join("; ", warnings));
        }
        else if (links.Count == 0)
        {
            connectivityState = "OK"; // никому не настроено — пустой состав = OK
        }
        else
        {
            var alive = links.Values.SelectMany(r => r.Values).Count(v => v == "ok");
            var total = links.Values.SelectMany(r => r.Values).Count();
            connectivityState = alive == 0 ? "DOWN" : alive == total ? "OK" : "DEGRADED";
        }

        // 5. Пары сверек: неориентированные из объединения направленных записей;
        // from = Ordinal-min, to = max (детерминизм между прогонами).
        var pairs = new SortedSet<(string From, string To)>();
        foreach (var (a, b) in directed)
            pairs.Add(string.CompareOrdinal(a, b) <= 0 ? (a, b) : (b, a));

        // 6. Статусы приёмников (fetch-очередь для lag_objects) — только если
        // пары есть; один вызов на ноду (кэш; Failed → кэш-промах, транзиент).
        var statusCache = new Dictionary<string, IReadOnlyList<OwnS3ReplicationNeighbor>>(StringComparer.Ordinal);

        // 7. Свёртка пар (§3.3): sync-report → diff листингов при расхождении.
        var resultPairs = new List<S3ProcessCommon.SyncPairValue>();
        var lastErrorParts = new List<string>();
        var uncovTotal = 0;
        foreach (var (from, to) in pairs)
        {
            var outcome = await ComparePairAsync(from, to, addresses.Value, creds, statusCache, warnings, ct);
            if (outcome.Failure is not null)
            {
                // Пара не сверена — в pairs не попадает, агрегат не лучше LAGGING.
                lastErrorParts.Add(outcome.Failure);
                uncovTotal++;
                continue;
            }

            var missing = outcome.Missing;
            var extra = outcome.Extra;
            var lagBytes = outcome.LagBytes;
            var lagObjects = outcome.LagObjects;

            // Вердикты (§3.3): synced / lagging / diverged — свидетельство
            // прогресса: непустая fetch-очередь приёмника ИЛИ сокращение
            // missing+extra против прошлого прогона. Прошлого нет (первый
            // прогон) при пустой очереди → lagging (не diverged).
            var verdictKind = missing == 0 && extra == 0
                ? "synced"
                : VerdictOf(missing, extra, lagObjects, past, (from, to));

            _pairs.Add(1, new KeyValuePair<string, object?>("result", verdictKind));
            resultPairs.Add(new S3ProcessCommon.SyncPairValue(
                from, to, missing, extra, lagBytes, lagObjects > 0 ? lagObjects : null));
        }

        var syncState = resultPairs.Any(p => p.Missing + p.Extra > 0)
            && resultPairs.Any(p => verdictOf(p) == "diverged")
            ? "DIVERGED"
            : resultPairs.Any(p => verdictOf(p) != "synced") || uncovTotal > 0
                ? "LAGGING"
                : "SYNCED";

        // 8. Запись ключей (один checked_unix), del заявки, journal (§3.4).
        var linksForJson = new Dictionary<string, IReadOnlyDictionary<string, string>>(StringComparer.Ordinal);
        foreach (var (node, row) in links)
            linksForJson[node] = row; // SortedDictionary-строка реализует IReadOnlyDictionary
        var connectivityJson = S3ProcessCommon.SerializeConnectivity(
            connectivityState, now, linksForJson, probeError);
        var putConnectivity = await PutWithFailoverAsync(S3ProcessCommon.ConnectivityKey(cluster), connectivityJson, ct);
        if (!putConnectivity.IsSuccess)
            return Fail(cluster, putConnectivity.Error!, "write-connectivity");

        var syncJson = S3ProcessCommon.SerializeSync(syncState, now, resultPairs,
            lastErrorParts.Count > 0 ? Truncate(string.Join("; ", lastErrorParts)) : null);
        var putSync = await PutWithFailoverAsync(S3ProcessCommon.SyncKey(cluster), syncJson, ct);
        if (!putSync.IsSuccess)
            return Fail(cluster, putSync.Error!, "write-sync");

        if (requestPresent)
        {
            // Заявку сносим только после успеха записи обеих половин (§3.4).
            var deleted = await DeleteWithFailoverAsync(S3ProcessCommon.ProbeRequestKey(cluster), ct);
            if (!deleted.IsSuccess)
                return Fail(cluster, deleted.Error!, "delete-probe-request");
        }

        var written = await journal.WritePhaseAsync(
            cluster, Op, "probed", claims.InstanceId,
            warnings.Count > 0 ? Truncate(string.Join("; ", warnings)) : null, ct);
        if (!written.IsSuccess)
            return written;
        _runs.Add(1, new KeyValuePair<string, object?>("result", "ok"));
        return Result.Success();

        // Вердикт свёрнутой пары (для агрегата — по фактическим счётчикам).
        string verdictOf(S3ProcessCommon.SyncPairValue p)
            => VerdictOf(p.Missing, p.Extra, p.LagObjects ?? 0, past, (p.From, p.To));
    }

    // Свёртка одной пары (§3.3): прет-чек sync-report постранично от пустого
    // маркера у обеих сторон; равные страницы → следующая; расхождение/конец
    // одной из сторон → количественный diff полных листингов. Любая сторона
    // свёртки не уложилась в бюджет страниц или отказала → пара НЕ сверена
    // (failure-текст для last_error, диагностика в warnings).
    private async Task<PairOutcome> ComparePairAsync(
        string from, string to,
        IReadOnlyDictionary<string, NodeAddress> addresses,
        OwnS3NodeSecrets creds,
        Dictionary<string, IReadOnlyList<OwnS3ReplicationNeighbor>> statusCache,
        List<string> warnings, CancellationToken ct)
    {
        if (!addresses.TryGetValue(from, out var fromAddr) || !addresses.TryGetValue(to, out var toAddr))
        {
            warnings.Add($"pair {from}↔{to}: no address in portalloc — transient");
            return new PairOutcome(0, 0, null, 0, $"pair {from}↔{to}: no address in portalloc");
        }

        var fromHost = options.AdvertisedHost ?? fromAddr.Host;
        var toHost = options.AdvertisedHost ?? toAddr.Host;
        var pagesFrom = 0;
        var pagesTo = 0;

        // Прет-чек sync-report: страницы равны ⟺ равны все поля (свёртка
        // (key, modTime) + состав) и nextMarker согласован.
        string? marker = null;
        OwnS3SyncReportPage fromPage;
        OwnS3SyncReportPage toPage;
        while (true)
        {
            if (pagesFrom >= options.SyncMaxPages || pagesTo >= options.SyncMaxPages)
            {
                return new PairOutcome(0, 0, null, 0,
                    $"listing of pair {from}↔{to} exceeds page budget");
            }

            progress?.Mark(); // долгая фаза: таймаут грани до 7.5 с
            var fromPageResult = await nodeClient.GetSyncReportPageAsync(
                fromHost, fromAddr.S3Port, creds, options.Bucket, marker, ct);
            progress?.Mark();
            var toPageResult = await nodeClient.GetSyncReportPageAsync(
                toHost, toAddr.S3Port, creds, options.Bucket, marker, ct);
            pagesFrom++;
            pagesTo++;
            if (!fromPageResult.IsSuccess || !toPageResult.IsSuccess)
            {
                var detail = fromPageResult.Error?.Message ?? toPageResult.Error!.Message;
                warnings.Add($"sync-report {from}/{to}: {detail} — transient");
                return new PairOutcome(0, 0, null, 0, $"pair {from}↔{to}: sync-report unavailable");
            }

            fromPage = fromPageResult.Value!;
            toPage = toPageResult.Value!;
            var fromEnd = string.IsNullOrEmpty(fromPage.NextMarker);
            var toEnd = string.IsNullOrEmpty(toPage.NextMarker);
            if (PagesEqual(fromPage, toPage) && fromEnd == toEnd)
            {
                if (fromEnd)
                {
                    return new PairOutcome(0, 0, null, 0, null); // synced
                }

                marker = fromPage.NextMarker; // общий nextMarker — следующая страница
                continue;
            }

            break; // расхождение (первая неравная страница ИЛИ одна кончилась, другая нет)
        }

        // Количественный diff: полные постраничные листинги обеих сторон, бюджет
        // SyncMaxPages НА СТОРОНУ (pagesFrom/pagesTo независимо — spec §3.3);
        // отказ листинга любой стороны → пара не сверена.
        var fromListing = await FullListingAsync(
            fromHost, fromAddr.S3Port, creds, warnings, ct, pagesFrom);
        if (fromListing.Failure is not ListingFailure.None)
            return new PairOutcome(0, 0, null, 0, ListingFailureText(fromListing.Failure, from, to));

        var toListing = await FullListingAsync(
            toHost, toAddr.S3Port, creds, warnings, ct, pagesTo);
        if (toListing.Failure is not ListingFailure.None)
            return new PairOutcome(0, 0, null, 0, ListingFailureText(toListing.Failure, from, to));

        var fromMap = fromListing.Map;
        var toMap = toListing.Map;

        // Элемент сверки — (key, modTimeUnixMs) (как свёртка aggregateHash):
        // одинаковый ключ с другим modTime попадает в ОБА множества.
        long missing = 0;
        long extra = 0;
        foreach (var key in fromMap.Keys)
            if (!toMap.ContainsKey(key))
                missing++;
        foreach (var key in toMap.Keys)
            if (!fromMap.ContainsKey(key))
                extra++;
        long? lagBytes = missing > 0
            ? fromMap.Where(kv => !toMap.ContainsKey(kv.Key)).Sum(kv => kv.Value)
            : null;

        // lag_objects — fetch-очередь приёмника (статус ноды to, сосед from);
        // null-известность → 0 (поле не пишется, вердикт без свидетельства).
        if (!statusCache.TryGetValue(to, out var neighbors))
        {
            if (!addresses.TryGetValue(to, out _))
            {
                warnings.Add($"status {to}: no address in portalloc — transient");
            }
            else
            {
                progress?.Mark(); // долгая фаза: таймаут грани до 7.5 с
                var status = await nodeClient.GetReplicationStatusAsync(toHost, toAddr.S3Port, creds, ct);
                if (status.IsSuccess)
                    statusCache[to] = status.Value!.Neighbors;
                else
                    warnings.Add($"status {to}: {status.Error!.Message} — transient");
            }
        }

        var lagObjects = statusCache.TryGetValue(to, out var list)
            ? list.FirstOrDefault(n => n.Name == from)?.FetchQueueDepth ?? 0
            : 0;
        return new PairOutcome(missing, extra, lagBytes, lagObjects, null);
    }

    // Результат свёртки пары: Failure != null — пара НЕ сверена (бюджет/отказы).
    private sealed record PairOutcome(
        long Missing, long Extra, long? LagBytes, long LagObjects, string? Failure);

    // Вид отказа листинга стороны (структурированно: текст с именами пары
    // формирует ComparePairAsync единым форматом spec §3.3).
    private enum ListingFailure
    {
        None,
        Budget,
        Transport,
    }

    // Единый формат диагностики несвёрнутой пары (spec §3.3).
    private static string ListingFailureText(ListingFailure failure, string from, string to)
        => failure == ListingFailure.Budget
            ? $"listing of pair {from}↔{to} exceeds page budget"
            : $"pair {from}↔{to}: listing unavailable";

    // Полный постраничный листинг стороны (diff): словарь (key, modTime) → size,
    // счётчик страниц СВОЕЙ стороны (бюджет на сторону — независим) и вид отказа.
    private async Task<(Dictionary<(string Key, long Mod), long> Map, int Pages, ListingFailure Failure)> FullListingAsync(
        string host, int port, OwnS3NodeSecrets creds, List<string> warnings, CancellationToken ct,
        int pagesTaken)
    {
        var map = new Dictionary<(string Key, long Mod), long>();
        var pages = pagesTaken;
        string? token = null;
        while (true)
        {
            if (pages >= options.SyncMaxPages)
                return (map, pages, ListingFailure.Budget);

            progress?.Mark(); // долгая фаза: таймаут грани до 7.5 с
            var page = await nodeClient.ListObjectEntriesAsync(host, port, creds, options.Bucket, token, ct);
            pages++;
            if (!page.IsSuccess)
            {
                warnings.Add($"listing {host}:{port}: {page.Error!.Message} — transient");
                return (map, pages, ListingFailure.Transport);
            }

            foreach (var entry in page.Value!.Entries)
                map[(entry.Key, entry.ModTimeUnixMs)] = entry.Size;
            token = page.Value.NextContinuationToken;
            if (token is null)
                return (map, pages, ListingFailure.None);
        }
    }

    // Вердикт пары (§3.3): synced — 0/0; lagging — есть свидетельство прогресса
    // (fetch-очередь приёмника непуста ИЛИ missing+extra сократились против
    // прошлого прогона); diverged — расхождение без прогресса (устойчивое);
    // прошлого нет (первый прогон) — lagging, не diverged.
    private static string VerdictOf(
        long missing, long extra, long lagObjects,
        IReadOnlyDictionary<(string From, string To), long>? past,
        (string From, string To) pair)
    {
        if (missing == 0 && extra == 0)
            return "synced";
        long? pastTotal = null;
        if (past is not null && past.TryGetValue(pair, out var pastValue))
            pastTotal = pastValue;
        if (lagObjects > 0 || (pastTotal is { } p && missing + extra < p))
            return "lagging";
        return pastTotal is not null ? "diverged" : "lagging";
    }

    // Страницы sync-report равны ⟺ равны все поля свёртки.
    private static bool PagesEqual(OwnS3SyncReportPage x, OwnS3SyncReportPage y)
        => x.Count == y.Count
           && x.FirstKey == y.FirstKey
           && x.FirstModTime == y.FirstModTime
           && x.LastKey == y.LastKey
           && x.LastModTime == y.LastModTime
           && x.TotalSize == y.TotalSize
           && x.AggregateHash == y.AggregateHash
           && string.Equals(x.NextMarker ?? "", y.NextMarker ?? "", StringComparison.Ordinal);

    // Диагностический idle (0 RUNNING / профиль недоступен): journal-фаза;
    // deleteRequest — заявка сносится (отказ del → Failed — заявка обязана
    // быть снесена, ретрай тиком).
    private async Task<Result> IdleAsync(string cluster, bool deleteRequest, string? warning, CancellationToken ct)
    {
        if (deleteRequest)
        {
            var deleted = await DeleteWithFailoverAsync(S3ProcessCommon.ProbeRequestKey(cluster), ct);
            if (!deleted.IsSuccess)
                return Fail(cluster, deleted.Error!, "delete-probe-request");
        }

        var written = await journal.WritePhaseAsync(cluster, Op, "idle", claims.InstanceId, warning, ct);
        if (!written.IsSuccess)
            return written;
        _runs.Add(1, new KeyValuePair<string, object?>("result", "idle"));
        return Result.Success();
    }

    // Journal-запись отказа + Failed (CancellationToken.None — образец G.Fail:
    // last_error должен доехать даже при остановке host'а).
    private Result Fail(string cluster, Exception error, string phase)
    {
        journal.WritePhaseAsync(cluster, Op, "probe-failed", claims.InstanceId, error.Message, CancellationToken.None)
            .GetAwaiter().GetResult();
        _runs.Add(1, new KeyValuePair<string, object?>("result", "failed"));
        return Result.Failed(error);
    }

    private static string Truncate(string s) => s.Length > DiagnosticsLimit ? s[..DiagnosticsLimit] : s;

    // Failover-запись/удаление ключа (первый успешный endpoint).
    private async Task<Result> PutWithFailoverAsync(string key, string value, CancellationToken ct)
    {
        Result last = default;
        foreach (var endpoint in endpoints)
        {
            var put = await gateway.PutAsync(endpoint, key, value, null, ct);
            if (put.IsSuccess)
                return put;
            last = put;
        }

        return last.Error ?? new EtcdUnreachableException("etcd endpoints are not set");
    }

    private async Task<Result> DeleteWithFailoverAsync(string key, CancellationToken ct)
    {
        Result last = default;
        foreach (var endpoint in endpoints)
        {
            var del = await gateway.DeleteAsync(endpoint, key, prefix: false, ct);
            if (del.IsSuccess)
                return del;
            last = del;
        }

        return last.Error ?? new EtcdUnreachableException("etcd endpoints are not set");
    }

    // portalloc-чтение (образец MeshGovernanceProcess.ReadPortAllocAsync).
    private async Task<Result<IReadOnlyDictionary<string, NodeAddress>>> ReadPortAllocAsync(
        string cluster, CancellationToken ct)
    {
        var result = await S3ProcessCommon.GetWithFailoverAsync(
            gateway, endpoints, S3ProcessCommon.PortAllocKey(cluster), ct);
        if (!result.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(result.Error!);
        if (result.Value is not { } kv)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(
                new Dictionary<string, NodeAddress>());
        try
        {
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(
                S3ProcessCommon.ParsePortAlloc(kv.Value));
        }
        catch (JsonException ex)
        {
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(ex);
        }
    }
}
