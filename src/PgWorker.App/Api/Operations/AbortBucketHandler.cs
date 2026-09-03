using System.Text.Json;
using PgWorker.Core;
using PgWorker.Core.Writing;
using PgWorker.Etcd.Client;
using PgWorker.Moves;

namespace PgWorker.App.Api.Operations;

// Тело POST /api/clusters/{cluster}/moves/abort (arch/02 §9.7.4): force —
// nullable (null = false; в JSON пишется только true).
public sealed record AbortBucketRequest(int? Bucket, bool? Force);

// Ответ 201.
public sealed record BucketAbortQueuedDto(string Cluster, int Bucket, bool Force);

// Валидация тела (§9.7.4): bucket обязателен.
public static class AbortBucketValidator
{
    public static IReadOnlyList<ValidationError> Validate(AbortBucketRequest request)
    {
        var errors = new List<ValidationError>();
        if (request.Bucket is null)
            errors.Add(new("bucket", "укажите бакет"));
        return errors;
    }
}

// Заявка на отмену переезда (t07): быстрые пред-проверки семантики force по
// прямым чтениям etcd (Д4: свежесть AbortMinAgeSec по updated_unix,
// routing==target); авторитетно перепроверит AbortSequence. Порог — из
// MovesRuntimeOptions (единый источник с процессом, appsettings PgWorker:Moves).
public sealed class AbortBucketHandler(
    IEtcdGateway gateway, string[] endpoints, TimeProvider time, MovesRuntimeOptions moves)
{
    private static readonly HashSet<string> MoveStates = ["SYNCING", "FROZEN", "ABORTING"];

    public async Task<Result<BucketAbortQueuedDto>> HandleAsync(
        string cluster, AbortBucketRequest command, string requestedBy, CancellationToken ct)
    {
        // 1) Валидация тела (400) и каноничность кластера (404).
        var errors = AbortBucketValidator.Validate(command);
        if (errors.Count > 0)
            return Result<BucketAbortQueuedDto>.Failed(new MoveOpValidationException(errors));
        if (!CreateClusterLimits.NamePattern().IsMatch(cluster))
            return Result<BucketAbortQueuedDto>.Failed(new ClusterNotFoundException(cluster));

        // 2) Guard-данные кластера (как rollback/finalize).
        var data = await ClusterGuardData.ReadAsync(gateway, endpoints, cluster, ct);
        if (!data.IsSuccess)
            return Result<BucketAbortQueuedDto>.Failed(data.Error!);
        var info = data.Value;
        if (info.ConfigRaw is null)
            return Result<BucketAbortQueuedDto>.Failed(new ClusterNotFoundException(cluster));
        string? state;
        int bucketsCount;
        try
        {
            using var doc = JsonDocument.Parse(info.ConfigRaw);
            state = doc.RootElement.TryGetProperty("state", out var s) && s.ValueKind == JsonValueKind.String
                ? s.GetString() : null;
            bucketsCount = doc.RootElement.TryGetProperty("buckets", out var b) && b.ValueKind == JsonValueKind.Number
                ? b.GetInt32() : 0;
        }
        catch (JsonException)
        {
            return Result<BucketAbortQueuedDto>.Failed(new InvalidClusterConfigException(cluster));
        }
        if (state is not null)
            return Result<BucketAbortQueuedDto>.Failed(new ClusterNotActiveException(cluster, state));

        // 3) Guard'ы (§9.7.4): бакет в диапазоне и с routing; статус жив и state
        //    ∈ SYNCING/FROZEN/ABORTING (ACTIVE/NOT_INITIALIZED — 409 с подсказкой).
        var id = command.Bucket!.Value;
        var force = command.Force == true;
        if (id < 0 || id >= bucketsCount || !info.Routing.TryGetValue(id, out var owner))
            return Result<BucketAbortQueuedDto>.Failed(
                BucketNotActiveForMoveOpException.OutOfRange("abort", id));
        if (!info.Status.TryGetValue(id, out var status) || status.State == ClusterGuardData.ActiveState)
            return Result<BucketAbortQueuedDto>.Failed(BucketNotActiveForMoveOpException.AbortActive(id));
        if (status.State == "NOT_INITIALIZED")
            return Result<BucketAbortQueuedDto>.Failed(BucketNotActiveForMoveOpException.AbortNotInitialized(id));
        if (!MoveStates.Contains(status.State!))
            return Result<BucketAbortQueuedDto>.Failed(
                BucketNotActiveForMoveOpException.AbortActive(id));

        // 4) Пред-проверки force (порт AbortSequence; отсутствие updated_unix у
        //    старого ключа — пропускаем, авторитетно решит процесс, спека §5.3):
        //    свежесть статуса и routing==target.
        if (!force && status.UpdatedUnix is { } updated)
        {
            var age = time.GetUtcNow().ToUnixTimeSeconds() - updated;
            if (age < moves.AbortMinAgeSec)
                return Result<BucketAbortQueuedDto>.Failed(
                    new MoveStatusFreshException(age, moves.AbortMinAgeSec));
        }
        if (!force && status.Target is { } target && target == owner)
            return Result<BucketAbortQueuedDto>.Failed(new MoveAlreadyFlippedException(target));

        // 5) Очередь: идентичная (op=abort + тот же force) → 201 без записи;
        //    иная → 409 (панель не перезаписывает чужие заявки, §9.7).
        var queue = await MoveTickets.ReadQueueAsync(gateway, endpoints, cluster, ct);
        if (!queue.IsSuccess)
            return Result<BucketAbortQueuedDto>.Failed(queue.Error!);
        if (queue.Value.Mine.TryGetValue($"bucket_{id}", out var existing))
        {
            if (existing.Op == "abort" && existing.Force == force)
                return Result<BucketAbortQueuedDto>.Success(new BucketAbortQueuedDto(cluster, id, force));
            return Result<BucketAbortQueuedDto>.Failed(
                new MoveRequestConflictException($"bucket_{id}", existing.Op, existing.To));
        }

        // 6) Клэйм одной заявки: force:true пишется, false — опускается (§4.2).
        var now = time.GetUtcNow().ToUnixTimeSeconds();
        var unix = Math.Max(now, queue.Value.MaxUnix + 1);
        var key = $"/pgworker/moves/{cluster}/bucket_{id}";
        var json = JsonSerializer.Serialize(
            new MoveTickets.TicketBody("abort", null, null, force ? true : null, unix, requestedBy),
            MoveTickets.TicketJson);
        var claim = await MoveTickets.ClaimAsync(gateway, endpoints, key, json, ct);
        if (!claim.IsSuccess)
            return Result<BucketAbortQueuedDto>.Failed(claim.Error!); // 503, без компенсации (Д5)
        if (!claim.Value.Succeeded)
            return Result<BucketAbortQueuedDto>.Failed(new MoveClaimLostException(id));

        return Result<BucketAbortQueuedDto>.Success(new BucketAbortQueuedDto(cluster, id, force));
    }
}
