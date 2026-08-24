using System.Text.Json;
using System.Text.RegularExpressions;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Client;
using AdminPanel.Etcd.Writing;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Operations;

// Команда добавления шарда — третья мутация панели (arch/02 §9.5, t06).
public sealed record AddShardCommand(string Cluster, AddShardRequest Request) : ICommand<ShardAddedDto>;

// Ответ 201 POST /api/clusters/{cluster}/shards (arch/03 §1.3).
public sealed record ShardAddedDto(
    string Cluster, string Name, int Replicas,
    string RequestCpu, string RequestMem, string RequestDisk, string State);

public sealed class AddShardValidationException(IReadOnlyList<ValidationError> errors)
    : Exception("параметры добавления шарда некорректны")
{
    public IReadOnlyList<ValidationError> Errors { get; } = errors;
}

// Кластер не Active: NOT_INITIALIZED («дождитесь инициализации») или TO_REMOVE
// («кластер удаляется») — подсказка оператору по state (§9.5/§9.6).
public sealed class ClusterNotActiveException(string name, string state)
    : Exception(state == "NOT_INITIALIZED"
        ? $"кластер {name} ещё инициализируется (NOT_INITIALIZED) — дождитесь инициализации"
        : $"кластер {name} удаляется (TO_REMOVE) — операция запрещена");

// Клэйм-txn имени не сошёлся: конкурентный POST занял имя (arch/02 §9.5).
public sealed class ShardNameTakenException(string cluster, string shard)
    : Exception($"имя шарда {cluster}/{shard} занято (replicas-ключ присутствует)");

// shard<max+1> превысил предел числа шардов (§9.3: ≤128).
public sealed class ShardLimitReachedException(string cluster)
    : Exception($"кластер {cluster} достиг предела числа шардов (128)");

// Клэйм имени → пакет PUT → компенсация при сбое (arch/02 §9.5). Без ретраев:
// повтор = новый POST от пользователя; повтор вычислит ТО ЖЕ имя (max по префиксу).
[InjectAsScoped]
public sealed partial class AddShardCommandHandler(ISnapshotStore store, IEtcdGateway gateway)
    : ICommandHandler<AddShardCommand, ShardAddedDto>
{
    private const int MaxShards = CreateClusterLimits.MaxShards;

    // Имя существующего шарда панели: shard<k> (§9.1).
    [GeneratedRegex("^shard(\\d+)$")]
    private static partial Regex PanelShardPattern();

    public async ValueTask<Result<ShardAddedDto>> Handle(AddShardCommand command, CancellationToken ct)
    {
        var cluster = command.Cluster;

        // 1) Валидация (replicas 0 = поле отсутствовало → дефолт 2, §9.3).
        var request = command.Request with { Replicas = command.Request.Replicas == 0 ? 2 : command.Request.Replicas };
        var errors = AddShardValidator.Validate(request);
        if (errors.Count > 0)
            return Result<ShardAddedDto>.Failed(new AddShardValidationException(errors));

        // 2) Активный endpoint из снапшота.
        var snapshot = store.Current;
        if (snapshot?.Etcd.ActiveEndpoint is not { } endpoint)
            return Result<ShardAddedDto>.Failed(new EtcdWriteUnavailableException());

        // 3) Config напрямую: имя каноническое (иначе 404), ключа нет → 404,
        //    сбой чтения → 503, state не Active → 409 (§9.5).
        if (!CreateClusterLimits.NamePattern().IsMatch(cluster))
            return Result<ShardAddedDto>.Failed(new ClusterNotFoundException(cluster));
        var config = await ReadKeyAsync(endpoint, $"/clusters/{cluster}/config", ct);
        if (!config.IsSuccess)
            return Result<ShardAddedDto>.Failed(config.Error!); // 503 (etcd недоступен)
        if (config.Value is null)
            return Result<ShardAddedDto>.Failed(new ClusterNotFoundException(cluster));
        string? rawState;
        try
        {
            rawState = ReadStateField(config.Value);
        }
        catch (JsonException)
        {
            return Result<ShardAddedDto>.Failed(new InvalidClusterConfigException(cluster)); // 503
        }

        if (rawState is not null)
            return Result<ShardAddedDto>.Failed(new ClusterNotActiveException(cluster, rawState));

        // 4) Имя shard<max+1> по фактическому префиксу shards/ (range).
        //    «Существующий» шард = replicas + (nodes ИЛИ dsn/master/state):
        //    недодекларация (выжил только replicas после провалившейся
        //    компенсации) НЕ считается — повтор вычислит ТО ЖЕ имя и проиграет
        //    клэйм → 409 (молча создать «другой» шард повтор не может, §9.5).
        var shardsRange = await gateway.RangeAsync(endpoint, $"/clusters/{cluster}/shards/", ct);
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
        if (max + 1 > MaxShards)
            return Result<ShardAddedDto>.Failed(new ShardLimitReachedException(cluster));
        var shard = $"shard{max + 1}";

        // 5) Клэйм-txn имени (§9.5): compare version(replicas)==0 + put replicas.
        var plan = ShardScalePlan.Build(cluster, shard, request);
        var claim = await gateway.TxnAsync(
            endpoint, [new TxnCompare(plan.ReplicasKey, 0)], [new KvPut(plan.ReplicasKey, plan.ReplicasValue)], ct);
        if (!claim.IsSuccess)
            return Result<ShardAddedDto>.Failed(claim.Error!);
        if (!claim.Value.Succeeded)
            return Result<ShardAddedDto>.Failed(new ShardNameTakenException(cluster, shard));

        // 6) Пакет PUT; сбой посередине → компенсация best-effort (§9.5).
        foreach (var put in plan.Puts)
        {
            var putResult = await gateway.PutAsync(endpoint, put.Key, put.Value, ct);
            if (putResult.IsSuccess)
                continue;

            await gateway.DeleteAsync(endpoint, $"/clusters/{cluster}/shards/{shard}/", prefix: true, ct);
            foreach (var key in plan.RequestKeys)
                await gateway.DeleteAsync(endpoint, key, prefix: false, ct);
            return Result<ShardAddedDto>.Failed(putResult.Error!);
        }

        return Result<ShardAddedDto>.Success(new ShardAddedDto(
            cluster, shard, request.Replicas,
            plan.CanonicalCpu, plan.CanonicalMem, plan.CanonicalDisk,
            ShardScalePlan.NotInitialized));
    }

    // Точечное чтение ключа через range (gateway без GetAsync — образец §9.4).
    // Различаем сбой и отсутствие (§6.1): Failed → эндпоинт ответит 503,
    // Success(null) — ровно «ключа нет» (404-путь вызывающего).
    private async Task<Result<string?>> ReadKeyAsync(string endpoint, string key, CancellationToken ct)
    {
        var range = await gateway.RangeAsync(endpoint, key, ct);
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
}
