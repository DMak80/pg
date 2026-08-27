using System.Text.Json;
using System.Text.RegularExpressions;
using AdminPanel.Core;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Client;
using AdminPanel.Etcd.Writing;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Operations;

// Команда демонтажа шарда — четвёртая мутация панели (arch/02 §9.6, t06):
// one-way маркер shards/<X>/state=TO_REMOVE; очистку выполняет PgWorker.
public sealed record DeleteShardCommand(string Cluster, string Shard) : ICommand<ShardDeletedDto>;

public sealed record ShardDeletedDto(string Cluster, string Shard, string State);

public sealed class ShardNotFoundException(string cluster, string shard)
    : Exception($"шард {cluster}/{shard} не найден (replicas-ключ отсутствует)");

// Быстрая серверная пред-проверка guard'ов (Д4): PgWorker перепроверит авторитетно.
public sealed class ShardRemoveBlockedException(string reason) : Exception(reason)
{
    public static ShardRemoveBlockedException Buckets(int count)
        => new($"на шарде {count} бакетов — сначала явно перевезите (UI переездов — t07)");

    public static ShardRemoveBlockedException UnfinishedMove()
        => new("незавершённый переезд бакета — завершите/отмените");

    public static ShardRemoveBlockedException LastShard()
        => new("нельзя снять последний шард — для полного демонтажа удалите кластер");

    public static ShardRemoveBlockedException Quarantine()
        => new("шард в карантине после эвакуации — сначала разбор данных");
}

// Снапшот отстаёт (кластер в etcd есть, в снапшоте нет) — повтор запроса.
public sealed class ShardPrecheckUnavailableException()
    : Exception("снапшот панели отстаёт — повторите запрос");

// Читает config/replicas напрямую у etcd, пред-проверяет guard'ы по снапшоту
// (Д4) и ставит маркер TO_REMOVE (arch/02 §9.6). Без txn: конкурентные
// PUT маркера сходятся к одному значению. Без ретраев — повтор = новый DELETE.
[InjectAsScoped]
public sealed partial class DeleteShardCommandHandler(ISnapshotStore store, IEtcdGateway gateway)
    : ICommandHandler<DeleteShardCommand, ShardDeletedDto>
{
    public const string ToRemoveState = "TO_REMOVE"; // канон маркера (§9.6)

    // Паттерн имени шарда (§9.5/pg §4.1): без дефиса.
    [GeneratedRegex("^[a-z][a-z0-9_]{0,30}$")]
    private static partial Regex ShardNamePattern();

    public async ValueTask<Result<ShardDeletedDto>> Handle(DeleteShardCommand command, CancellationToken ct)
    {
        var (cluster, shard) = (command.Cluster, command.Shard);

        // 1) Имена канонические, иначе 404 (такие панель создать не могла).
        if (!CreateClusterLimits.NamePattern().IsMatch(cluster)
            || !ShardNamePattern().IsMatch(shard))
            return Result<ShardDeletedDto>.Failed(new ClusterNotFoundException(cluster));

        // 2) Активный endpoint.
        var snapshot = store.Current;
        if (snapshot?.Etcd.ActiveEndpoint is not { } endpoint)
            return Result<ShardDeletedDto>.Failed(new EtcdWriteUnavailableException());

        // 3) Config напрямую: нет → 404; сбой чтения → 503; не Active → 409.
        var config = await ReadKeyAsync(endpoint, $"/clusters/{cluster}/config", ct);
        if (!config.IsSuccess)
            return Result<ShardDeletedDto>.Failed(config.Error!); // 503 (etcd недоступен)
        if (config.Value is null)
            return Result<ShardDeletedDto>.Failed(new ClusterNotFoundException(cluster));
        string? state;
        try
        {
            state = ReadState(config.Value);
        }
        catch (JsonException)
        {
            return Result<ShardDeletedDto>.Failed(new InvalidClusterConfigException(cluster)); // 503
        }

        if (state is not null)
            return Result<ShardDeletedDto>.Failed(new ClusterNotActiveException(cluster, state));

        // 4) Шард существует (replicas-ключ) иначе 404; сбой чтения → 503.
        var replicas = await ReadKeyAsync(endpoint, $"/clusters/{cluster}/shards/{shard}/replicas", ct);
        if (!replicas.IsSuccess)
            return Result<ShardDeletedDto>.Failed(replicas.Error!); // 503
        if (replicas.Value is null)
            return Result<ShardDeletedDto>.Failed(new ShardNotFoundException(cluster, shard));

        // 5) Пред-проверки guard'ов по данным снапшота (Д4: быстро оператору;
        //    гонки ловят G3/G4 PgWorker — маркер-состояние ждёт бесконечно).
        //    Переезд: owner ИЛИ target СТАТУСА, плюс routing-owner (зеркало G4:
        //    после flip routing уже уехал, а зависший статус держит старый шард
        //    в Move.Owner — §4.4 «owner ИЛИ target»).
        var info = snapshot.Clusters.FirstOrDefault(c => c.Name == cluster);
        if (info is null)
            return Result<ShardDeletedDto>.Failed(new ShardPrecheckUnavailableException());
        var shardInfo = info.Shards.FirstOrDefault(s => s.Name == shard);
        // Нешардированная БД (arch/03 §2): демонтаж единственного вырожденного
        // шарда = удаление кластера; guard до бакетов — единственный бакет solo
        // лежит на единственном шарде, сообщение про «последний шард» сбивает.
        if (info.BucketsCount == 1 && info.Shards.Count <= 1)
            return Result<ShardDeletedDto>.Failed(new NonShardedClusterException(cluster));
        var owned = info.Buckets.Count(b => b.Owner == shard);
        if (owned > 0)
            return Result<ShardDeletedDto>.Failed(ShardRemoveBlockedException.Buckets(owned));
        if (info.Buckets.Any(b => b.State is BucketState.Syncing or BucketState.Frozen or BucketState.Aborting
                && (b.Owner == shard || b.Move?.Owner == shard || b.Move?.Target == shard)))
            return Result<ShardDeletedDto>.Failed(ShardRemoveBlockedException.UnfinishedMove());
        if (info.Shards.Count <= 1)
            return Result<ShardDeletedDto>.Failed(ShardRemoveBlockedException.LastShard());
        if (shardInfo?.Nodes.Any(n => n.State == "QUARANTINED") == true)
            return Result<ShardDeletedDto>.Failed(ShardRemoveBlockedException.Quarantine());

        // 6) PUT маркера; уже TO_REMOVE → идемпотентный успех без записи (§9.6);
        //    сбой чтения state-ключа → 503 (не пишем поверх нечитанного).
        var markerKey = $"/clusters/{cluster}/shards/{shard}/state";
        var marker = await ReadKeyAsync(endpoint, markerKey, ct);
        if (!marker.IsSuccess)
            return Result<ShardDeletedDto>.Failed(marker.Error!); // 503
        if (marker.Value == ToRemoveState)
            return Result<ShardDeletedDto>.Success(new ShardDeletedDto(cluster, shard, ToRemoveState));

        var put = await gateway.PutAsync(endpoint, markerKey, ToRemoveState, ct);
        if (!put.IsSuccess)
            return Result<ShardDeletedDto>.Failed(put.Error!);
        return Result<ShardDeletedDto>.Success(new ShardDeletedDto(cluster, shard, ToRemoveState));
    }

    // Точечное чтение ключа через range (образец §9.4). Failed → 503 у эндпоинта;
    // Success(null) — ровно «ключа нет» (404-путь вызывающего), §6.1.
    private async Task<Result<string?>> ReadKeyAsync(string endpoint, string key, CancellationToken ct)
    {
        var range = await gateway.RangeAsync(endpoint, key, ct);
        if (!range.IsSuccess)
            return Result<string?>.Failed(range.Error!); // 503: etcd недоступен
        return Result<string?>.Success(range.Value.FirstOrDefault(kv => kv.Key == key)?.Value);
    }

    private static string? ReadState(string raw)
    {
        using var doc = JsonDocument.Parse(raw);
        return doc.RootElement.TryGetProperty("state", out var s) && s.ValueKind == JsonValueKind.String
            ? s.GetString()
            : null;
    }
}
