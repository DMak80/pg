using System.Text.Json;
using PgWorker.Core;
using PgWorker.Core.Writing;
using PgWorker.Etcd.Client;

namespace PgWorker.App.Api.Operations;

// Тело POST /api/clusters/{cluster}/moves/finalize (arch/02 §9.7.3).
public sealed record FinalizeBucketRequest(int? Bucket, string? OldShard);

// Ответ 201: одиночная заявка уборки артефактов на oldShard.
public sealed record BucketFinalizeQueuedDto(string Cluster, int Bucket, string OldShard);

// Валидация тела (§9.7.3): bucket обязателен, oldShard непустой.
public static class FinalizeBucketValidator
{
    public static IReadOnlyList<ValidationError> Validate(FinalizeBucketRequest request)
    {
        var errors = new List<ValidationError>();
        if (request.Bucket is null)
            errors.Add(new("bucket", "укажите бакет"));
        if (string.IsNullOrWhiteSpace(request.OldShard))
            errors.Add(new("oldShard", "шард обязателен"));
        return errors;
    }
}

// Заявка уборки пост-переездных артефактов на старом шарде (t07): DROP SCHEMA
// СО ДАННЫМИ на oldShard — необратимо (UI предупреждает). Общий протокол
// постановки — MoveTickets (§9.7 п.3–5); guard'ы — Д4 (перепроверит процесс).
public sealed class FinalizeBucketHandler(IEtcdGateway gateway, string[] endpoints, TimeProvider time)
{
    public async Task<Result<BucketFinalizeQueuedDto>> HandleAsync(
        string cluster, FinalizeBucketRequest command, string requestedBy, CancellationToken ct)
    {
        // 1) Валидация тела (400) и каноничность кластера (404).
        var errors = FinalizeBucketValidator.Validate(command);
        if (errors.Count > 0)
            return Result<BucketFinalizeQueuedDto>.Failed(new MoveOpValidationException(errors));
        if (!CreateClusterLimits.NamePattern().IsMatch(cluster))
            return Result<BucketFinalizeQueuedDto>.Failed(new ClusterNotFoundException(cluster));

        // 2) Guard-данные кластера одним range: сбой → 503; нет config → 404;
        //    state не null → 409; битый → 503.
        var data = await ClusterGuardData.ReadAsync(gateway, endpoints, cluster, ct);
        if (!data.IsSuccess)
            return Result<BucketFinalizeQueuedDto>.Failed(data.Error!);
        var info = data.Value;
        if (info.ConfigRaw is null)
            return Result<BucketFinalizeQueuedDto>.Failed(new ClusterNotFoundException(cluster));
        string? state;
        int bucketsCount;
        try
        {
            state = ReadState(info.ConfigRaw);
            bucketsCount = ReadBuckets(info.ConfigRaw);
        }
        catch (JsonException)
        {
            return Result<BucketFinalizeQueuedDto>.Failed(new InvalidClusterConfigException(cluster));
        }
        if (state is not null)
            return Result<BucketFinalizeQueuedDto>.Failed(new ClusterNotActiveException(cluster, state));

        // 3) Guard'ы (Д4): нешардированный; бакет в диапазоне, с routing, ACTIVE.
        if (bucketsCount == 1 && info.Shards.Count <= 1)
            return Result<BucketFinalizeQueuedDto>.Failed(new NonShardedClusterException(cluster));
        var id = command.Bucket!.Value;
        if (id < 0 || id >= bucketsCount || !info.Routing.TryGetValue(id, out var owner))
            return Result<BucketFinalizeQueuedDto>.Failed(
                BucketNotActiveForMoveOpException.OutOfRange("finalize", id));
        var bucketState = info.Status.TryGetValue(id, out var st)
            ? st.State ?? ClusterGuardData.ActiveState
            : ClusterGuardData.ActiveState;
        if (bucketState != ClusterGuardData.ActiveState)
            return Result<BucketFinalizeQueuedDto>.Failed(
                BucketNotActiveForMoveOpException.RollbackOrFinalize("finalize", id, owner, bucketState));

        // 4) Guard'ы oldShard: существует (404), ≠ текущего владельца (409);
        //    TO_REMOVE-приёмник допустим (финализация перед демонтажем).
        if (!info.Shards.Contains(command.OldShard!))
            return Result<BucketFinalizeQueuedDto>.Failed(new ShardNotFoundException(cluster, command.OldShard!));
        if (command.OldShard == owner)
            return Result<BucketFinalizeQueuedDto>.Failed(
                new FinalizeTargetIsOwnerException(cluster, id, command.OldShard!));

        // 5) Очередь напрямую (§9.7 п.3): идентичная (op=finalize + тот же
        //    old_shard) → 201 без записи; иная → 409.
        var queue = await MoveTickets.ReadQueueAsync(gateway, endpoints, cluster, ct);
        if (!queue.IsSuccess)
            return Result<BucketFinalizeQueuedDto>.Failed(queue.Error!);
        if (queue.Value.Mine.TryGetValue($"bucket_{id}", out var existing))
        {
            if (existing.Op == "finalize" && existing.OldShard == command.OldShard)
                return Result<BucketFinalizeQueuedDto>.Success(
                    new BucketFinalizeQueuedDto(cluster, id, command.OldShard!));
            return Result<BucketFinalizeQueuedDto>.Failed(
                new MoveRequestConflictException($"bucket_{id}", existing.Op, existing.To));
        }

        // 6) Клэйм одной заявки (k=0): force не предусмотрен семантикой finalize.
        var now = time.GetUtcNow().ToUnixTimeSeconds();
        var unix = Math.Max(now, queue.Value.MaxUnix + 1);
        var key = $"/pgworker/moves/{cluster}/bucket_{id}";
        var json = JsonSerializer.Serialize(
            new MoveTickets.TicketBody("finalize", null, command.OldShard, null, unix, requestedBy),
            MoveTickets.TicketJson);
        var claim = await MoveTickets.ClaimAsync(gateway, endpoints, key, json, ct);
        if (!claim.IsSuccess)
            return Result<BucketFinalizeQueuedDto>.Failed(claim.Error!); // 503, без компенсации (Д5)
        if (!claim.Value.Succeeded)
            return Result<BucketFinalizeQueuedDto>.Failed(new MoveClaimLostException(id));

        return Result<BucketFinalizeQueuedDto>.Success(new BucketFinalizeQueuedDto(cluster, id, command.OldShard!));
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
