using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using AdminPanel.Api.Inspection; // BucketStates (канон имён состояний бакета)
using AdminPanel.Core;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Client;
using AdminPanel.Etcd.Parsing;
using AdminPanel.Etcd.Writing;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Operations;

// Тело POST /api/clusters/{cluster}/moves (arch/03 §1.5). Buckets nullable:
// null/отсутствие поля ловит валидатор (400), а не NRE (решение при планировании).
public sealed record MoveBucketsRequest(string From, string To, IReadOnlyList<int>? Buckets);

// Ответ 201: queued поставлены сейчас, skipped — идентичные уже стояли (arch/03 §1.5).
public sealed record MovesQueuedDto(
    string Cluster, string From, string To,
    IReadOnlyList<int> Queued, IReadOnlyList<int> Skipped);

// Пятая мутация панели — заявки на переезды бакетов (arch/02 §9.7).
public sealed record MoveBucketsCommand(
    string Cluster, string From, string To, IReadOnlyList<int> Buckets, string RequestedBy)
    : ICommand<MovesQueuedDto>;

public sealed class MoveBucketsValidationException(IReadOnlyList<ValidationError> errors)
    : Exception("параметры переноса бакетов некорректны")
{
    public IReadOnlyList<ValidationError> Errors { get; } = errors;
}

// Приёмник в демонтаже: на удаляемый шард везть нельзя (arch/02 §9.7 п.2; источник
// TO_REMOVE допустим — эвакуация перед демонтажем, spec Д9).
public sealed class MoveTargetRemovingException(string cluster, string shard)
    : Exception($"шард-приёмник {cluster}/{shard} удаляется (TO_REMOVE) — выберите другой приёмник");

// Бакет не годен для переезда с источника: не его владелец / не ACTIVE / вне диапазона.
public sealed class BucketNotOnSourceException(int bucket, string? owner, string state)
    : Exception($"бакет {bucket} не доступен для переезда (владелец: {owner ?? "—"}, состояние: {state})");

// На бакете уже стоит иная заявка — панель чужие не перезаписывает (arch/02 §9.7 п.3).
public sealed class MoveRequestConflictException(string bucket, string op, string? to)
    : Exception($"на {bucket} уже стоит заявка (op={op}, to={to ?? "—"}) — дождитесь её обработки или уберите ключ");

// Txn-клэйм не сошёлся: конкурентная заявка заняла ключ между чтением и записью.
public sealed class MoveClaimLostException(int bucket)
    : Exception($"конкурентная заявка заняла bucket_{bucket} между чтением и записью — повторите запрос");

// Валидация тела (arch/02 §9.7 п.2): 400 с errors по полям.
public static class MoveBucketsValidator
{
    public static IReadOnlyList<ValidationError> Validate(MoveBucketsRequest request)
    {
        var errors = new List<ValidationError>();
        if (string.IsNullOrWhiteSpace(request.From))
            errors.Add(new("from", "шард-источник обязателен"));
        if (string.IsNullOrWhiteSpace(request.To))
            errors.Add(new("to", "шард-приёмник обязателен"));
        if (request.From == request.To && request.From.Length > 0)
            errors.Add(new("to", "приёмник должен отличаться от источника"));
        if (request.Buckets is null || request.Buckets.Count == 0)
            errors.Add(new("buckets", "выберите хотя бы один бакет"));
        else if (request.Buckets.Distinct().Count() != request.Buckets.Count)
            errors.Add(new("buckets", "дубликаты бакетов не допускаются"));
        return errors;
    }
}

// Guard'ы по снапшоту + очередь напрямую + txn-клэйм per key (arch/02 §9.7;
// spec §4.3). Сбой посередине — БЕЗ компенсации: частичная очередь валидна,
// повтор досдаст остаток (spec Д5). Без ретраев: повтор = новый POST.
[InjectAsScoped]
public sealed partial class MoveBucketsCommandHandler(
    ISnapshotStore store, IEtcdGateway gateway, TimeProvider time)
    : ICommandHandler<MoveBucketsCommand, MovesQueuedDto>
{
    // Паттерн имени шарда (как DeleteShardCommand: без дефиса).
    [GeneratedRegex("^[a-z][a-z0-9_]{0,30}$")]
    private static partial Regex ShardNamePattern();

    // Канон тела заявки PgWorker: только нужные поля, snake_case (spec §4.3 шаг 6).
    private static readonly JsonSerializerOptions TicketJson = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    private sealed record TicketBody(
        [property: JsonPropertyName("op")] string Op,
        [property: JsonPropertyName("to")] string To,
        [property: JsonPropertyName("requested_unix")] long RequestedUnix,
        [property: JsonPropertyName("requested_by")] string RequestedBy);

    public async ValueTask<Result<MovesQueuedDto>> Handle(MoveBucketsCommand command, CancellationToken ct)
    {
        var (cluster, from, to) = (command.Cluster, command.From, command.To);

        // 1) Валидация тела (400) и каноничность имён (404 — панель такие не создавала).
        var errors = MoveBucketsValidator.Validate(new MoveBucketsRequest(from, to, command.Buckets));
        if (errors.Count > 0)
            return Result<MovesQueuedDto>.Failed(new MoveBucketsValidationException(errors));
        if (!CreateClusterLimits.NamePattern().IsMatch(cluster)
            || !ShardNamePattern().IsMatch(from) || !ShardNamePattern().IsMatch(to))
            return Result<MovesQueuedDto>.Failed(new ClusterNotFoundException(cluster));

        // 2) Активный endpoint из снапшота.
        var snapshot = store.Current;
        if (snapshot?.Etcd.ActiveEndpoint is not { } endpoint)
            return Result<MovesQueuedDto>.Failed(new EtcdWriteUnavailableException());

        // 3) Config напрямую: сбой → 503; нет → 404; state не null → 409; битый → 503.
        var config = await ReadKeyAsync(endpoint, $"/clusters/{cluster}/config", ct);
        if (!config.IsSuccess)
            return Result<MovesQueuedDto>.Failed(config.Error!);
        if (config.Value is null)
            return Result<MovesQueuedDto>.Failed(new ClusterNotFoundException(cluster));
        string? state;
        try
        {
            using var doc = JsonDocument.Parse(config.Value);
            state = doc.RootElement.TryGetProperty("state", out var s) && s.ValueKind == JsonValueKind.String
                ? s.GetString()
                : null;
        }
        catch (JsonException)
        {
            return Result<MovesQueuedDto>.Failed(new InvalidClusterConfigException(cluster));
        }

        if (state is not null)
            return Result<MovesQueuedDto>.Failed(new ClusterNotActiveException(cluster, state));

        // 4) Guard'ы по снапшоту (Д4-паттерн DeleteShard: быстро оператору,
        //    авторитетно перепроверит PgWorker).
        var info = snapshot.Clusters.FirstOrDefault(c => c.Name == cluster);
        if (info is null)
            return Result<MovesQueuedDto>.Failed(new ShardPrecheckUnavailableException());
        if (info.BucketsCount == 1 && info.Shards.Count <= 1)
            return Result<MovesQueuedDto>.Failed(new NonShardedClusterException(cluster));
        if (info.Shards.All(s => s.Name != from))
            return Result<MovesQueuedDto>.Failed(new ShardNotFoundException(cluster, from));
        if (info.Shards.FirstOrDefault(s => s.Name == to) is not { } target)
            return Result<MovesQueuedDto>.Failed(new ShardNotFoundException(cluster, to));
        if (target.State == ShardState.ToRemove)
            return Result<MovesQueuedDto>.Failed(new MoveTargetRemovingException(cluster, to));

        var ordered = command.Buckets.Distinct().OrderBy(id => id).ToList();
        foreach (var id in ordered)
        {
            var bucket = info.Buckets.FirstOrDefault(b => b.Id == id);
            if (id < 0 || id >= info.BucketsCount || bucket is null)
                return Result<MovesQueuedDto>.Failed(new BucketNotOnSourceException(id, null, "OUT_OF_RANGE"));
            if (bucket.Owner != from)
                return Result<MovesQueuedDto>.Failed(
                    new BucketNotOnSourceException(id, bucket.Owner, BucketStates.Name(bucket.State)));
            if (bucket.State != BucketState.Active)
                return Result<MovesQueuedDto>.Failed(
                    new BucketNotOnSourceException(id, bucket.Owner, BucketStates.Name(bucket.State)));
        }

        // 5) Очередь напрямую, один range по всему префиксу (arch/02 §9.7 п.3):
        //    идентичная заявка → skipped; иная → 409 до записей; база — глобальный max.
        var movesRange = await gateway.RangeAsync(endpoint, MovesQueueParser.Prefix, ct);
        if (!movesRange.IsSuccess)
            return Result<MovesQueuedDto>.Failed(movesRange.Error!);
        var parsed = MovesQueueParser.Parse(movesRange.Value);
        var mine = parsed.Tickets
            .Where(t => t.Cluster == cluster)
            .ToDictionary(t => t.Bucket);
        var maxUnix = parsed.Tickets.Count == 0 ? 0 : parsed.Tickets.Max(t => t.RequestedUnix);

        var skipped = new List<int>();
        var toQueue = new List<int>();
        foreach (var id in ordered)
        {
            if (mine.TryGetValue($"bucket_{id}", out var existing))
            {
                if (existing.Op == "move" && existing.To == to)
                    skipped.Add(id);
                else
                    return Result<MovesQueuedDto>.Failed(
                        new MoveRequestConflictException($"bucket_{id}", existing.Op, existing.To));
            }
            else
                toQueue.Add(id);
        }

        // 6) base = max(now, maxUnix+1), заявка k-я по порядку — base+k (arch/02 §9.7 п.4);
        //    txn-клэйм per key: compare version==0 + put (защита от перезаписи чужой).
        var now = time.GetUtcNow().ToUnixTimeSeconds();
        var unixBase = Math.Max(now, maxUnix + 1);
        var queued = new List<int>();
        foreach (var (id, k) in toQueue.Select((b, i) => (b, i)))
        {
            var key = $"/pgworker/moves/{cluster}/bucket_{id}";
            var body = JsonSerializer.Serialize(
                new TicketBody("move", to, unixBase + k, command.RequestedBy), TicketJson);
            var claim = await gateway.TxnAsync(endpoint, [new TxnCompare(key, 0)], [new KvPut(key, body)], ct);
            if (!claim.IsSuccess)
                return Result<MovesQueuedDto>.Failed(claim.Error!); // 503, без компенсации (Д5)
            if (!claim.Value.Succeeded)
                return Result<MovesQueuedDto>.Failed(new MoveClaimLostException(id));
            queued.Add(id);
        }

        return Result<MovesQueuedDto>.Success(new MovesQueuedDto(cluster, from, to, queued, skipped));
    }

    // Точечное чтение ключа через range (образец AddShardCommand):
    // Failed → 503 у эндпоинта; Success(null) — ровно «ключа нет».
    private async Task<Result<string?>> ReadKeyAsync(string endpoint, string key, CancellationToken ct)
    {
        var range = await gateway.RangeAsync(endpoint, key, ct);
        if (!range.IsSuccess)
            return Result<string?>.Failed(range.Error!);
        return Result<string?>.Success(range.Value.FirstOrDefault(kv => kv.Key == key)?.Value);
    }
}
