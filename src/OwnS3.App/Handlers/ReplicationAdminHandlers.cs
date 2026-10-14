using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OwnS3.App.Pipeline;
using OwnS3.Protocol.Auth;
using OwnS3.Replication.Inbound;
using OwnS3.Replication.Outbox;
using OwnS3.Replication.Reconcile;
using OwnS3.Replication.Transport;

// Явная квалификация SyncReportHash ниже (namespace OwnS3.Replication).
using OwnS3.Storage;

namespace OwnS3.App.Handlers;

// Админ-грань mesh-репликации (t35, глава 07 §2/§10; глава 05 §7): управление
// составом без даунтайма (PUT targets — 202), статус со счётчиками исходов,
// внеплановый reconcile (202), сверка листингов (sync-report с XOR-свёрткой),
// активная проба линков (probe — синхронный JSON-отчёт). t39: join-trigger
// (POST join), удаление с отзывом (DELETE targets/{name}[?revoke=true]),
// deny-list (GET/DELETE denylist[/{name}]), статус с полем auth per-target.
// Права admin/root — проверяет S3Middleware (прочим, включая replication, 403).
public sealed class ReplicationAdminHandlers(OwnS3.Replication.ReplicationTargets targets,
    OwnS3.Replication.Cursors.TargetCursors cursors, OutboxJournal journal,
    OwnS3.Replication.ReplicationMetrics metrics, FetchQueueRegistry fetchQueues,
    ReconcileRunner reconcile, IReplicaStore store, OwnS3.Replication.ReplicationOptions options,
    OwnS3.Replication.ReplicationProber prober, OwnS3.Replication.DenyList denyList,
    OwnS3.Replication.JoinPeerService joinService, ILogger<ReplicationAdminHandlers> logger)
{
    private static readonly JsonSerializerOptions Json = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    // PUT /admin/replication/targets: идемпотентная замена полного списка соседей;
    // персист targets.json (tmp+rename), hot-apply; ответ 202 Accepted.
    public async Task HandlePutTargetsAsync(HttpContext context)
    {
        PutTargetsRequest? request;
        try
        {
            request = await JsonSerializer.DeserializeAsync<PutTargetsRequest>(context.Request.Body, Json,
                context.RequestAborted);
        }
        catch (JsonException)
        {
            request = null;
        }
        if (request?.Targets is null)
        {
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            await context.Response.WriteAsJsonAsync(new { error = "malformed targets body" }, context.RequestAborted);
            return;
        }
        try
        {
            targets.Replace(request.Targets
                .Select(t => new OwnS3.Replication.TargetRecord(t.Name, t.Endpoint,
                    t.Auth ?? OwnS3.Replication.TargetRecord.AuthLegacy, t.AccessKey, t.SecretKey,
                    t.PublicKeyPem))
                .ToList());
        }
        catch (ArgumentException ex)
        {
            // Невалидный состав (дубль имени/кредов, битый PEM) → 400 (глава 07 §2)
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            await context.Response.WriteAsJsonAsync(new { error = ex.Message }, context.RequestAborted);
            return;
        }
        context.Response.StatusCode = StatusCodes.Status202Accepted;
    }

    // POST /admin/replication/join (t39, глава 07 §2/spec §4.4): join-handshake с
    // соседом, исполнение синхронное; Success → 200 {joined, peer}; конфигурация
    // инициатора неполна → 400; отказ соседа 403/409 проксируется; 502 — прочее.
    public async Task HandleJoinTriggerAsync(HttpContext context)
    {
        JoinTriggerRequest? request;
        try
        {
            request = await JsonSerializer.DeserializeAsync<JoinTriggerRequest>(context.Request.Body, Json,
                context.RequestAborted);
        }
        catch (JsonException)
        {
            request = null;
        }
        if (string.IsNullOrWhiteSpace(request?.Endpoint))
        {
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            await context.Response.WriteAsJsonAsync(new { error = "malformed join body" }, context.RequestAborted);
            return;
        }
        var outcome = await joinService.JoinAsync(request.Endpoint, context.RequestAborted);
        if (outcome.IsSuccess)
        {
            context.Response.StatusCode = StatusCodes.Status200OK;
            await context.Response.WriteAsJsonAsync(new { joined = true, peer = outcome.Value.Name },
                Json, context.RequestAborted);
            return;
        }
        switch (outcome.Error)
        {
            case JoinBadConfigurationException badConfig:
                // Нет identity/trust-anchor/advertised — fail-fast 400 с пояснением
                // (spec §4.4 п.1: без trust-anchor инициатор не проверит сертификат ответа).
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                await context.Response.WriteAsJsonAsync(new { error = badConfig.Message }, context.RequestAborted);
                return;
            case JoinRefusedException refused:
                context.Response.StatusCode = StatusCodes.Status403Forbidden;
                await context.Response.WriteAsJsonAsync(new { error = refused.Message }, context.RequestAborted);
                return;
            case JoinConflictException conflict:
                context.Response.StatusCode = StatusCodes.Status409Conflict;
                await context.Response.WriteAsJsonAsync(new { error = conflict.Message }, context.RequestAborted);
                return;
            default:
                // JoinBadGatewayException и прочее — 502 (состав не менялся).
                context.Response.StatusCode = StatusCodes.Status502BadGateway;
                await context.Response.WriteAsJsonAsync(new { error = outcome.Error!.Message },
                    context.RequestAborted);
                return;
        }
    }

    // DELETE /admin/replication/targets/{name}[?revoke=true] (t39, глава 07 §2/spec §4.5):
    // 202 — удаление из состава (курсор сохраняется — возврат продолжится с места);
    // revoke=true — дополнительно запись в локальный deny-list (только key-записи);
    // неизвестное имя → 404; revoke для legacy → 400 (креды отлучаются удалением записи).
    public async Task HandleDeleteTargetAsync(HttpContext context, string name, bool revoke)
    {
        if (!targets.TryGet(name, out var record))
        {
            context.Response.StatusCode = StatusCodes.Status404NotFound;
            await context.Response.WriteAsJsonAsync(new { error = $"unknown replication target: {name}" },
                context.RequestAborted);
            return;
        }
        if (revoke)
        {
            if (record.Auth != OwnS3.Replication.TargetRecord.AuthKey)
            {
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                await context.Response.WriteAsJsonAsync(new
                {
                    error = "revoke применим только к key-записям — legacy-креды отлучаются удалением записи",
                }, context.RequestAborted);
                return;
            }
            denyList.Add(name, record.PublicKeyPem!);
            // Лог-событие отзыва (глава 07 §10); удаление из состава пишет ReplicationTargets.Remove.
            logger.LogInformation("Отзыв соседа {Name} (deny-list, revoke=true)", name);
        }
        targets.Remove(name);
        context.Response.StatusCode = StatusCodes.Status202Accepted;
    }

    // GET /admin/replication/denylist (t39, глава 07 §2): список отозванных.
    public async Task HandleDenyListAsync(HttpContext context)
    {
        context.Response.StatusCode = StatusCodes.Status200OK;
        await context.Response.WriteAsJsonAsync(new
        {
            entries = denyList.Snapshot.Select(e => new
            {
                name = e.Name,
                publicKeyPem = e.PublicKeyPem,
                revokedAtUnixMs = e.RevokedAtUnixMs,
            }).ToList(),
        }, Json, context.RequestAborted);
    }

    // DELETE /admin/replication/denylist/{name} (t39, глава 07 §2): снятие отзыва —
    // 202 (несуществующее имя тоже 202: идемпотентность, arch «202 — снятие отзыва»).
    public Task HandleDenyRemoveAsync(HttpContext context, string name)
    {
        denyList.Remove(name);
        context.Response.StatusCode = StatusCodes.Status202Accepted;
        return Task.CompletedTask;
    }

    // GET /admin/replication/status: активность, состояние журнала, стейт/счётчики
    // по каждому соседу (глава 07 §10).
    public async Task HandleStatusAsync(HttpContext context)
    {
        var payload = new
        {
            active = targets.Active,
            outbox = new
            {
                bytes = journal.TotalBytes,
                budgetBytes = options.OutboxBudgetBytes,
                budgetExceeded = journal.BudgetExceeded,
                lastSeqNo = journal.LastSeqNo,
            },
            neighbors = targets.Snapshot.Select(t => new
            {
                name = t.Name,
                auth = t.Auth,
                online = metrics.LinkOnlineOf(t.Name) == 1,
                acceptedSeqNo = cursors.GetAccepted(t.Name),
                queueDepth = metrics.QueueDepthOf(t.Name),
                deliveryLagSeconds = metrics.DeliveryLagOf(t.Name),
                fetchQueueDepth = fetchQueues.ForSource(t.Name).Depth,
                fetchLagSeconds = metrics.FetchLagOf(t.Name),
                fetchPaused = metrics.FetchPausedOf(t.Name) == 1,
                reconcileRunning = reconcile.IsRunning(t.Name),
                reconcilePosition = reconcile.ProcessedKeys(t.Name),
                counters = metrics.SnapshotCounters(t.Name),
            }).ToList(),
        };
        context.Response.StatusCode = StatusCodes.Status200OK;
        await context.Response.WriteAsJsonAsync(payload, Json, context.RequestAborted);
    }

    // POST /admin/replication/reconcile: внеплановый проход по всем соседям — 202;
    // идущий продолжается, параллельный не создаётся (глава 07 §9). Проход — фон
    // БЕЗ привязки к HTTP-соединению (завершение запроса проход не обрывает —
    // по прецеденту координатора).
    public Task HandleReconcileAsync(HttpContext context)
    {
        foreach (var target in targets.Snapshot)
            reconcile.Trigger(target.Name, CancellationToken.None);
        context.Response.StatusCode = StatusCodes.Status202Accepted;
        return Task.CompletedTask;
    }

    // POST /admin/replication/probe?target=<name> (глава 07 §2): активная проба
    // линков — СИНХРОННЫЙ ответ (не 202/фон — в отличие от reconcile); без
    // target — все соседи, неизвестное имя — 400; неактивная репликация —
    // осмысленный {active:false, targets:[]} (не ошибка).
    public async Task HandleProbeAsync(HttpContext context)
    {
        var targetName = context.Request.Query["target"].FirstOrDefault();
        var probe = await prober.ProbeAsync(string.IsNullOrEmpty(targetName) ? null : targetName,
            context.RequestAborted);
        if (!probe.IsSuccess && probe.Error is UnknownReplicationTargetException)
        {
            // Имя не из состава — 400 с пояснением (глава 07 §2)
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            await context.Response.WriteAsJsonAsync(
                new { error = $"unknown replication target: {targetName}" }, context.RequestAborted);
            return;
        }
        probe.Throw(); // прочие отказы — прежний путь (catch-all → 500)
        var report = probe.Value;
        context.Response.StatusCode = StatusCodes.Status200OK;
        await context.Response.WriteAsJsonAsync(
            new ProbeResponse(report.Active,
                report.Targets.Select(o => new ProbeTargetDto(o.Name, o.Reachable, o.RttMs)).ToList()),
            Json, context.RequestAborted);
    }

    // GET /admin/replication/sync-report?bucket=&prefix=&marker=: отчёт листинга ЭТОГО
    // сервера со свёрткой aggregateHash (XOR 64-бит FNV-1a пар (key, modTimeUnixMs)) —
    // попарная сверка серверов (глава 07 §10).
    public async Task HandleSyncReportAsync(HttpContext context, string requestId, string? errorHostId)
    {
        var bucket = context.Request.Query["bucket"].FirstOrDefault();
        if (string.IsNullOrEmpty(bucket))
        {
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            await context.Response.WriteAsJsonAsync(new { error = "bucket query parameter required" },
                context.RequestAborted);
            return;
        }
        var prefix = context.Request.Query["prefix"].FirstOrDefault() ?? string.Empty;
        var marker = context.Request.Query["marker"].FirstOrDefault();
        var page = store.ListReplicationEntries(bucket, marker, maxKeys: 1000);
        if (!page.IsSuccess)
        {
            // Кворумный отказ листинга → 503 + XML (канон 06 §7, 03 §5)
            await S3ResultTranslator.WriteAsync(context, page.Error!, requestId, errorHostId);
            return;
        }
        var filtered = page.Value.Entries.Where(e => e.Key.StartsWith(prefix, StringComparison.Ordinal)).ToList();
        var hash = OwnS3.Replication.SyncReportHash.Compute(filtered.Select(e => (e.Key, e.ModTime.ToUnixTimeMilliseconds())));
        var report = new
        {
            bucket,
            prefix,
            marker,
            firstKey = filtered.Count > 0 ? filtered[0].Key : null,
            firstModTime = filtered.Count > 0 ? filtered[0].ModTime.ToUnixTimeMilliseconds() : 0,
            lastKey = filtered.Count > 0 ? filtered[^1].Key : null,
            lastModTime = filtered.Count > 0 ? filtered[^1].ModTime.ToUnixTimeMilliseconds() : 0,
            count = filtered.Count,
            totalSize = filtered.Sum(e => e.Size),
            aggregateHash = hash,
            nextMarker = page.Value.NextMarker,
        };
        context.Response.StatusCode = StatusCodes.Status200OK;
        await context.Response.WriteAsJsonAsync(report, Json, context.RequestAborted);
    }

    private sealed record PutTargetsRequest(List<TargetDto> Targets);

    // Запись состава (глава 07 §2): auth absent → legacy (обратная совместимость
    // клиентов); key — publicKeyPem обязателен (валидация в ReplicationTargets.Validate).
    private sealed record TargetDto(string Name, string Endpoint, string? Auth,
        string? AccessKey, string? SecretKey, string? PublicKeyPem);

    // Тело join-trigger (spec §4.4 п.1).
    private sealed record JoinTriggerRequest(string Endpoint);

    // rttMs присутствует только при полученном HTTP-ответе (WhenWritingNull).
    private sealed record ProbeResponse(bool Active, IReadOnlyList<ProbeTargetDto> Targets);
    private sealed record ProbeTargetDto(string Name, bool Reachable,
        [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] long? RttMs);
}
