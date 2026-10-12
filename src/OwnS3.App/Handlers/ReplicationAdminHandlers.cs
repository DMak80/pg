using System.Text.Json;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Options;
using OwnS3.Protocol.Auth;
using OwnS3.Replication.Inbound;
using OwnS3.Replication.Outbox;
using OwnS3.Replication.Reconcile;

// Явная квалификация SyncReportHash ниже (namespace OwnS3.Replication).
using OwnS3.Storage;

namespace OwnS3.App.Handlers;

// Админ-грань mesh-репликации (t35, глава 07 §2/§10; глава 05 §7): управление
// составом без даунтайма (PUT targets — 202), статус со счётчиками исходов,
// внеплановый reconcile (202), сверка листингов (sync-report с XOR-свёрткой).
// Права admin/root — проверяет S3Middleware (прочим, включая replication, 403).
public sealed class ReplicationAdminHandlers(OwnS3.Replication.ReplicationTargets targets,
    OwnS3.Replication.Cursors.TargetCursors cursors, OutboxJournal journal,
    OwnS3.Replication.ReplicationMetrics metrics, FetchQueueRegistry fetchQueues,
    ReconcileRunner reconcile, IReplicaStore store, OwnS3.Replication.ReplicationOptions options)
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
                .Select(t => new OwnS3.Replication.TargetRecord(t.Name, t.Endpoint, t.AccessKey, t.SecretKey))
                .ToList());
        }
        catch (ArgumentException ex)
        {
            // Невалидный состав (дубль имени) → 400 (глава 07 §2)
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            await context.Response.WriteAsJsonAsync(new { error = ex.Message }, context.RequestAborted);
            return;
        }
        context.Response.StatusCode = StatusCodes.Status202Accepted;
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

    // GET /admin/replication/sync-report?bucket=&prefix=&marker=: отчёт листинга ЭТОГО
    // сервера со свёрткой aggregateHash (XOR 64-бит FNV-1a пар (key, modTimeUnixMs)) —
    // попарная сверка серверов (глава 07 §10).
    public async Task HandleSyncReportAsync(HttpContext context)
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
        var entries = store.ListReplicationEntries(bucket, marker, maxKeys: 1000, out var nextMarker);
        var filtered = entries.Where(e => e.Key.StartsWith(prefix, StringComparison.Ordinal)).ToList();
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
            nextMarker,
        };
        context.Response.StatusCode = StatusCodes.Status200OK;
        await context.Response.WriteAsJsonAsync(report, Json, context.RequestAborted);
    }

    private sealed record PutTargetsRequest(List<TargetDto> Targets);
    private sealed record TargetDto(string Name, string Endpoint, string AccessKey, string SecretKey);
}
