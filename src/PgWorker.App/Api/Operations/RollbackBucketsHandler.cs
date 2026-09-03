using System.Text.Json;
using PgWorker.Core;
using PgWorker.Core.Writing;
using PgWorker.Etcd.Client;

namespace PgWorker.App.Api.Operations;

// Тело POST /api/clusters/{cluster}/moves/rollback (arch/02 §9.7.2). Buckets
// nullable: null/отсутствие поля ловит валидатор (400), а не NRE.
public sealed record RollbackBucketsRequest(IReadOnlyList<int>? Buckets);

// Ответ 201: queued поставлены сейчас, skipped — идентичные op=rollback стояли.
public sealed record RollbackQueuedDto(
    string Cluster, IReadOnlyList<int> Queued, IReadOnlyList<int> Skipped);

// Валидация тела (§9.7.2): 400 с errors по полям.
public static class RollbackBucketsValidator
{
    public static IReadOnlyList<ValidationError> Validate(RollbackBucketsRequest request)
    {
        var errors = new List<ValidationError>();
        if (request.Buckets is null || request.Buckets.Count == 0)
            errors.Add(new("buckets", "выберите хотя бы один бакет"));
        else if (request.Buckets.Distinct().Count() != request.Buckets.Count)
            errors.Add(new("buckets", "дубликаты бакетов не допускаются"));
        return errors;
    }
}

// Заявки на откат (t07): откат возвращает бакет на прежний шард по живой
// обратной подписке — куда, определяет воркер (SQL-факт). Общий протокол
// постановки — MoveTickets (§9.7 п.3–5); guard'ы — Д4 (перепроверит процесс).
public sealed class RollbackBucketsHandler(IEtcdGateway gateway, string[] endpoints, TimeProvider time)
{
    public async Task<Result<RollbackQueuedDto>> HandleAsync(
        string cluster, RollbackBucketsRequest command, string requestedBy, CancellationToken ct)
    {
        // 1) Валидация тела (400) и каноничность кластера (404).
        var errors = RollbackBucketsValidator.Validate(command);
        if (errors.Count > 0)
            return Result<RollbackQueuedDto>.Failed(new MoveOpValidationException(errors));
        if (!CreateClusterLimits.NamePattern().IsMatch(cluster))
            return Result<RollbackQueuedDto>.Failed(new ClusterNotFoundException(cluster));

        // 2) Guard-данные кластера одним range: сбой → 503; нет config → 404;
        //    state не null → 409; битый → 503.
        var data = await ClusterGuardData.ReadAsync(gateway, endpoints, cluster, ct);
        if (!data.IsSuccess)
            return Result<RollbackQueuedDto>.Failed(data.Error!);
        var info = data.Value;
        if (info.ConfigRaw is null)
            return Result<RollbackQueuedDto>.Failed(new ClusterNotFoundException(cluster));
        string? state;
        int bucketsCount;
        try
        {
            state = ReadState(info.ConfigRaw);
            bucketsCount = ReadBuckets(info.ConfigRaw);
        }
        catch (JsonException)
        {
            return Result<RollbackQueuedDto>.Failed(new InvalidClusterConfigException(cluster));
        }
        if (state is not null)
            return Result<RollbackQueuedDto>.Failed(new ClusterNotActiveException(cluster, state));

        // 3) Guard'ы (Д4): нешардированный; каждый бакет в диапазоне, с routing,
        //    в ACTIVE — rollback возможен только из ACTIVE (§9.7.2).
        if (bucketsCount == 1 && info.Shards.Count <= 1)
            return Result<RollbackQueuedDto>.Failed(new NonShardedClusterException(cluster));
        var ordered = command.Buckets!.Distinct().OrderBy(id => id).ToList();
        foreach (var id in ordered)
        {
            if (id < 0 || id >= bucketsCount || !info.Routing.TryGetValue(id, out var owner))
                return Result<RollbackQueuedDto>.Failed(
                    BucketNotActiveForMoveOpException.OutOfRange("rollback", id));
            var bucketState = info.Status.TryGetValue(id, out var st)
                ? st.State ?? ClusterGuardData.ActiveState
                : ClusterGuardData.ActiveState;
            if (bucketState != ClusterGuardData.ActiveState)
                return Result<RollbackQueuedDto>.Failed(
                    BucketNotActiveForMoveOpException.RollbackOrFinalize("rollback", id, owner, bucketState));
        }

        // 4) Очередь напрямую (§9.7 п.3): идентичная op=rollback → skipped;
        //    иная → 409; база — глобальный max (п.4).
        var queue = await MoveTickets.ReadQueueAsync(gateway, endpoints, cluster, ct);
        if (!queue.IsSuccess)
            return Result<RollbackQueuedDto>.Failed(queue.Error!);
        var skipped = new List<int>();
        var toQueue = new List<int>();
        foreach (var id in ordered)
        {
            if (queue.Value.Mine.TryGetValue($"bucket_{id}", out var existing))
            {
                if (existing.Op == "rollback")
                    skipped.Add(id);
                else
                    return Result<RollbackQueuedDto>.Failed(
                        new MoveRequestConflictException($"bucket_{id}", existing.Op, existing.To));
            }
            else
            {
                toQueue.Add(id);
            }
        }

        // 5) base = max(now, maxUnix+1), k-я заявка — base+k; txn-клэйм per key.
        var now = time.GetUtcNow().ToUnixTimeSeconds();
        var unixBase = Math.Max(now, queue.Value.MaxUnix + 1);
        var queued = new List<int>();
        foreach (var (id, k) in toQueue.Select((b, i) => (b, i)))
        {
            var key = $"/pgworker/moves/{cluster}/bucket_{id}";
            var json = JsonSerializer.Serialize(
                new MoveTickets.TicketBody("rollback", null, null, null, unixBase + k, requestedBy),
                MoveTickets.TicketJson);
            var claim = await MoveTickets.ClaimAsync(gateway, endpoints, key, json, ct);
            if (!claim.IsSuccess)
                return Result<RollbackQueuedDto>.Failed(claim.Error!); // 503, без компенсации (Д5)
            if (!claim.Value.Succeeded)
                return Result<RollbackQueuedDto>.Failed(new MoveClaimLostException(id));
            queued.Add(id);
        }

        return Result<RollbackQueuedDto>.Success(new RollbackQueuedDto(cluster, queued, skipped));
    }

    private static string? ReadState(string raw)
    {
        using var doc = JsonDocument.Parse(raw);
        return doc.RootElement.TryGetProperty("state", out var s) && s.ValueKind == JsonValueKind.String
            ? s.GetString()
            : null;
    }

    private static int ReadBuckets(string raw)
    {
        using var doc = JsonDocument.Parse(raw);
        return doc.RootElement.TryGetProperty("buckets", out var b) && b.ValueKind == JsonValueKind.Number
            ? b.GetInt32()
            : 0;
    }
}
