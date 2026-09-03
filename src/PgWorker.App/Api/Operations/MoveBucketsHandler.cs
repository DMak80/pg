using System.Text.Json;
using System.Text.RegularExpressions;
using PgWorker.Core;
using PgWorker.Core.Writing;
using PgWorker.Etcd.Client;

namespace PgWorker.App.Api.Operations;

// Тело POST /api/clusters/{cluster}/moves (arch/02 §9.7). Buckets nullable:
// null/отсутствие поля ловит валидатор (400), а не NRE. Дубль панельного DTO осознан (t08).
public sealed record MoveBucketsRequest(string From, string To, IReadOnlyList<int>? Buckets);

// Ответ 201: queued поставлены сейчас, skipped — идентичные уже стояли.
public sealed record MovesQueuedDto(
    string Cluster, string From, string To,
    IReadOnlyList<int> Queued, IReadOnlyList<int> Skipped);

// Валидация тела (arch/02 §9.7 п.2): 400 с errors по полям (перенос панели 1:1).
public static class MoveBucketsValidator
{
    public static IReadOnlyList<ValidationError> Validate(MoveBucketsRequest request)
    {
        var errors = new List<ValidationError>();
        var fromEmpty = string.IsNullOrWhiteSpace(request.From);
        var toEmpty = string.IsNullOrWhiteSpace(request.To);
        if (fromEmpty)
            errors.Add(new("from", "шард-источник обязателен"));
        if (toEmpty)
            errors.Add(new("to", "шард-приёмник обязателен"));
        if (!fromEmpty && !toEmpty && request.From == request.To)
            errors.Add(new("to", "приёмник должен отличаться от источника"));
        if (request.Buckets is null || request.Buckets.Count == 0)
            errors.Add(new("buckets", "выберите хотя бы один бакет"));
        else if (request.Buckets.Distinct().Count() != request.Buckets.Count)
            errors.Add(new("buckets", "дубликаты бакетов не допускаются"));
        return errors;
    }
}

// Заявки переездов через API воркера (task etcd-via-worker-api): порт панельного
// MoveBucketsCommandHandler; guards на прямых чтениях etcd (ClusterGuardData).
// requested_by — заголовок X-Requested-By (панель шлёт оператора), fallback "api"
// (у панели — ClaimsPrincipal; значения etcd не меняются, spec §3.7).
// Сбой посередине — БЕЗ компенсации: частичная очередь валидна, повтор досдаст
// остаток (spec Д5). Без ретраев: повтор = новый POST.
public sealed partial class MoveBucketsHandler(IEtcdGateway gateway, string[] endpoints, TimeProvider time)
{
    // Паттерн имени шарда (как DeleteShardHandler: без дефиса).
    [GeneratedRegex("^[a-z][a-z0-9_]{0,30}$")]
    private static partial Regex ShardNamePattern();

    public async Task<Result<MovesQueuedDto>> HandleAsync(
        string cluster, MoveBucketsRequest command, string requestedBy, CancellationToken ct)
    {
        var (from, to) = (command.From, command.To);

        // 1) Валидация тела (400) и каноничность имён (404).
        var errors = MoveBucketsValidator.Validate(new MoveBucketsRequest(from, to, command.Buckets));
        if (errors.Count > 0)
            return Result<MovesQueuedDto>.Failed(new MoveBucketsValidationException(errors));
        if (!CreateClusterLimits.NamePattern().IsMatch(cluster)
            || !ShardNamePattern().IsMatch(from) || !ShardNamePattern().IsMatch(to))
            return Result<MovesQueuedDto>.Failed(new ClusterNotFoundException(cluster));

        // 2) Guard-данные кластера одним range: сбой → 503; нет config → 404;
        //    state не null → 409; битый → 503.
        var data = await ClusterGuardData.ReadAsync(gateway, endpoints, cluster, ct);
        if (!data.IsSuccess)
            return Result<MovesQueuedDto>.Failed(data.Error!);
        var info = data.Value;
        if (info.ConfigRaw is null)
            return Result<MovesQueuedDto>.Failed(new ClusterNotFoundException(cluster));
        string? state;
        int bucketsCount;
        try
        {
            state = ReadState(info.ConfigRaw);
            bucketsCount = ReadBuckets(info.ConfigRaw);
        }
        catch (JsonException)
        {
            return Result<MovesQueuedDto>.Failed(new InvalidClusterConfigException(cluster));
        }
        if (state is not null)
            return Result<MovesQueuedDto>.Failed(new ClusterNotActiveException(cluster, state));

        // 3) Guard'ы (Д4-паттерн: быстро оператору, авторитетно перепроверит воркер).
        if (bucketsCount == 1 && info.Shards.Count <= 1)
            return Result<MovesQueuedDto>.Failed(new NonShardedClusterException(cluster));
        if (!info.Shards.Contains(from))
            return Result<MovesQueuedDto>.Failed(new ShardNotFoundException(cluster, from));
        if (!info.Shards.Contains(to))
            return Result<MovesQueuedDto>.Failed(new ShardNotFoundException(cluster, to));
        if (info.ShardStates.TryGetValue(to, out var targetState) && targetState == "TO_REMOVE")
            return Result<MovesQueuedDto>.Failed(new MoveTargetRemovingException(cluster, to));

        var ordered = command.Buckets!.Distinct().OrderBy(id => id).ToList();
        foreach (var id in ordered)
        {
            if (id < 0 || id >= bucketsCount || !info.Routing.TryGetValue(id, out var owner))
                return Result<MovesQueuedDto>.Failed(new BucketNotOnSourceException(id, null, "OUT_OF_RANGE"));
            if (owner != from)
                return Result<MovesQueuedDto>.Failed(new BucketNotOnSourceException(id, owner, "OUT_OF_RANGE"));
            var bucketState = info.Status.TryGetValue(id, out var st)
                ? st.State ?? ClusterGuardData.ActiveState
                : ClusterGuardData.ActiveState;
            if (bucketState != ClusterGuardData.ActiveState)
                return Result<MovesQueuedDto>.Failed(
                    new BucketNotOnSourceException(id, owner, bucketState));
        }

        // 4) Очередь напрямую, один range по всему префиксу (arch/02 §9.7 п.3):
        //    идентичная заявка → skipped; иная → 409 до записей; база — глобальный max.
        var queue = await MoveTickets.ReadQueueAsync(gateway, endpoints, cluster, ct);
        if (!queue.IsSuccess)
            return Result<MovesQueuedDto>.Failed(queue.Error!);

        var skipped = new List<int>();
        var toQueue = new List<int>();
        foreach (var id in ordered)
        {
            if (queue.Value.Mine.TryGetValue($"bucket_{id}", out var existing))
            {
                if (existing.Op == "move" && existing.To == to)
                    skipped.Add(id);
                else
                    return Result<MovesQueuedDto>.Failed(
                        new MoveRequestConflictException($"bucket_{id}", existing.Op, existing.To));
            }
            else
            {
                toQueue.Add(id);
            }
        }

        // 5) base = max(now, maxUnix+1), заявка k-я по порядку — base+k (§9.7 п.4);
        //    txn-клэйм per key: compare NotExists + put (защита от перезаписи чужой).
        var now = time.GetUtcNow().ToUnixTimeSeconds();
        var unixBase = Math.Max(now, queue.Value.MaxUnix + 1);
        var queued = new List<int>();
        foreach (var (id, k) in toQueue.Select((b, i) => (b, i)))
        {
            var key = $"/pgworker/moves/{cluster}/bucket_{id}";
            var body = JsonSerializer.Serialize(
                new MoveTickets.TicketBody("move", to, null, null, unixBase + k, requestedBy),
                MoveTickets.TicketJson);
            var claim = await MoveTickets.ClaimAsync(gateway, endpoints, key, body, ct);
            if (!claim.IsSuccess)
                return Result<MovesQueuedDto>.Failed(claim.Error!); // 503, без компенсации (Д5)
            if (!claim.Value.Succeeded)
                return Result<MovesQueuedDto>.Failed(new MoveClaimLostException(id));
            queued.Add(id);
        }

        return Result<MovesQueuedDto>.Success(new MovesQueuedDto(cluster, from, to, queued, skipped));
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
