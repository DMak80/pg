using System.Text.Json;
using System.Text.RegularExpressions;
using PgWorker.Core;
using PgWorker.Core.Writing;
using PgWorker.Etcd.Client;

namespace PgWorker.App.Api.Operations;

// Ответ 201 POST /api/clusters/{cluster}/shards (arch/02 §9.5; дубль панельного DTO осознан, t08).
public sealed record ShardAddedDto(
    string Cluster, string Name, int Replicas,
    string RequestCpu, string RequestMem, string RequestDisk, string State);

// Добавление шарда через API воркера (task etcd-via-worker-api): порт панельного
// AddShardCommandHandler; guards читают etcd напрямую (панель брала снапшот).
// Клэйм имени → пакет PUT → компенсация при сбое (arch/02 §9.5). Без ретраев:
// повтор = новый POST; повтор вычислит ТО ЖЕ имя (max по префиксу).
public sealed partial class AddShardHandler(IEtcdGateway gateway, string[] endpoints)
{
    private const int MaxShards = CreateClusterLimits.MaxShards;

    // Имя существующего шарда: shard<k> (§9.1).
    [GeneratedRegex("^shard(\\d+)$")]
    private static partial Regex PanelShardPattern();

    public async Task<Result<ShardAddedDto>> HandleAsync(string cluster, AddShardRequest command, CancellationToken ct)
    {
        // 1) Валидация (replicas 0 = поле отсутствовало → дефолт 2, §9.3).
        var request = command with { Replicas = command.Replicas == 0 ? 2 : command.Replicas };
        var errors = AddShardValidator.Validate(request);
        if (errors.Count > 0)
            return Result<ShardAddedDto>.Failed(new AddShardValidationException(errors));

        // 2) Config напрямую: имя каноническое (иначе 404), ключа нет → 404,
        //    сбой чтения → 503, state не Active → 409 (§9.5).
        if (!CreateClusterLimits.NamePattern().IsMatch(cluster))
            return Result<ShardAddedDto>.Failed(new ClusterNotFoundException(cluster));
        var config = await ReadKeyAsync($"/clusters/{cluster}/config", ct);
        if (!config.IsSuccess)
            return Result<ShardAddedDto>.Failed(config.Error!); // 503 (etcd недоступен)
        if (config.Value is null)
            return Result<ShardAddedDto>.Failed(new ClusterNotFoundException(cluster));
        string? rawState;
        int declaredBuckets;
        try
        {
            rawState = ReadStateField(config.Value);
            declaredBuckets = ReadBucketsField(config.Value);
        }
        catch (JsonException)
        {
            return Result<ShardAddedDto>.Failed(new InvalidClusterConfigException(cluster)); // 503
        }

        if (rawState is not null)
            return Result<ShardAddedDto>.Failed(new ClusterNotActiveException(cluster, rawState));

        // 3) Имя shard<max+1> по фактическому префиксу shards/ (range).
        //    «Существующий» шард = replicas + (nodes ИЛИ dsn/master/state):
        //    недодекларация (выжил только replicas после провалившейся
        //    компенсации) НЕ считается — повтор вычислит ТО ЖЕ имя и проиграет
        //    клэйм → 409 (молча создать «другой» шард повтор не может, §9.5).
        var shardsRange = await EtcdFailover.CallAsync(endpoints,
            endpoint => gateway.RangeAsync(endpoint, $"/clusters/{cluster}/shards/", ct));
        if (!shardsRange.IsSuccess)
            return Result<ShardAddedDto>.Failed(shardsRange.Error!);
        var replicasShards = new HashSet<string>();
        var anchoredShards = new HashSet<string>();
        foreach (var kv in shardsRange.Value)
        {
            var segments = kv.Key.Split('/');
            if (segments.Length < 6 || segments[3] != "shards" || segments[4].Length == 0)
                continue;
            if (segments.Length == 6)
            {
                if (segments[5] == "replicas")
                    replicasShards.Add(segments[4]);
                else if (segments[5] is "dsn" or "master" or "state")
                    anchoredShards.Add(segments[4]);
            }
            else if (segments.Length == 8 && segments[5] == "nodes")
                anchoredShards.Add(segments[4]);
        }

        var max = replicasShards.Where(anchoredShards.Contains)
            .Select(name => PanelShardPattern().Match(name))
            .Where(m => m.Success)
            .Select(m => int.Parse(m.Groups[1].Value))
            .DefaultIfEmpty(0)
            .Max();
        // Нешардированная БД (arch/03 §2): 1 бакет + не более 1 существующего
        // шарда — добавление шарда превратило бы её в шардированную мимо
        // типа, заявленного при создании (02 §9.1 признак в etcd не хранится).
        if (declaredBuckets == 1 && replicasShards.Count <= 1)
            return Result<ShardAddedDto>.Failed(new NonShardedClusterException(cluster));
        if (max + 1 > MaxShards)
            return Result<ShardAddedDto>.Failed(new ShardLimitReachedException(cluster));
        var shard = $"shard{max + 1}";

        // 4) Клэйм-txn имени (§9.5): compare NotExists(replicas) + put replicas.
        var plan = ShardScalePlan.Build(cluster, shard, request);
        var claim = await EtcdFailover.CallAsync(endpoints, endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of([TxnCompare.NotExists(plan.ReplicasKey)], [new TxnOp.Put(plan.ReplicasKey, plan.ReplicasValue, null)]),
            ct));
        if (!claim.IsSuccess)
            return Result<ShardAddedDto>.Failed(claim.Error!);
        if (!claim.Value.Succeeded)
            return Result<ShardAddedDto>.Failed(new ShardNameTakenException(cluster, shard));

        // 5) Пакет PUT; сбой посередине → компенсация best-effort (§9.5).
        foreach (var put in plan.Puts)
        {
            var putResult = await EtcdFailover.CallAsync(endpoints,
                endpoint => gateway.PutAsync(endpoint, put.Key, put.Value, null, ct));
            if (putResult.IsSuccess)
                continue;

            await EtcdFailover.CallAsync(endpoints, endpoint => gateway.DeleteAsync(
                endpoint, $"/clusters/{cluster}/shards/{shard}/", prefix: true, ct));
            foreach (var key in plan.RequestKeys)
                await EtcdFailover.CallAsync(endpoints,
                    endpoint => gateway.DeleteAsync(endpoint, key, prefix: false, ct));
            return Result<ShardAddedDto>.Failed(putResult.Error!);
        }

        return Result<ShardAddedDto>.Success(new ShardAddedDto(
            cluster, shard, request.Replicas,
            plan.CanonicalCpu, plan.CanonicalMem, plan.CanonicalDisk,
            ShardScalePlan.NotInitialized));
    }

    // Точечное чтение ключа через range (gateway без GetAsync — образец §9.4).
    // Различаем сбой и отсутствие (§6.1): Failed → 503, Success(null) — «ключа нет».
    private async Task<Result<string?>> ReadKeyAsync(string key, CancellationToken ct)
    {
        var range = await EtcdFailover.CallAsync(endpoints,
            endpoint => gateway.RangeAsync(endpoint, key, ct));
        if (!range.IsSuccess)
            return Result<string?>.Failed(range.Error!); // 503: etcd недоступен
        return Result<string?>.Success(range.Value.FirstOrDefault(kv => kv.Key == key)?.Value);
    }

    // state из config-JSON; битый JSON ловит вызывающий (JsonException → 503).
    private static string? ReadStateField(string raw)
    {
        using var doc = JsonDocument.Parse(raw);
        return doc.RootElement.TryGetProperty("state", out var state)
            && state.ValueKind == JsonValueKind.String
            ? state.GetString()
            : null;
    }

    // buckets из config-JSON (0 — поля нет у легаси-конфига: трактуем как
    // шардированную, guard не срабатывает).
    private static int ReadBucketsField(string raw)
    {
        using var doc = JsonDocument.Parse(raw);
        return doc.RootElement.TryGetProperty("buckets", out var buckets)
            && buckets.ValueKind == JsonValueKind.Number
            ? buckets.GetInt32()
            : 0;
    }
}
