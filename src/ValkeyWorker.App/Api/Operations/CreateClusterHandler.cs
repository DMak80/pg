using System.Text.Json;
using Shared.Core;
using Shared.Core.Writing;
using Shared.Etcd.Client;
using ValkeyWorker.Core.Writing;

namespace ValkeyWorker.App.Api.Operations;

// Ответ 201 POST /api/valkey/clusters (arch/21 §1.1; сигнатуры — порт kfw).
public sealed record ValkeyClusterCreatedDto(
    string Name,
    string State,
    int Nodes,
    long MaxmemoryBytes,
    string MaxmemoryPolicy,
    string Cpu,
    string MemGi,
    string DiskGi);

// Создание valkey-кластера через API воркера: валидация → клэйм-txn
// put-if-absent config (state=NOT_INITIALIZED, nodes=1) → put
// nodes/node1/state + nodes/node1/resources; сбой → компенсация префиксом.
// Без ретраев: повтор = новый POST от клиента.
public sealed class CreateClusterHandler(IEtcdGateway gateway, string[] endpoints, TimeProvider time)
{
    public async Task<Result<ValkeyClusterCreatedDto>> HandleAsync(
        CreateValkeyClusterRequest request, CancellationToken ct)
    {
        // 1) Валидация (сервер — источник истины).
        var errors = Validate(request);
        if (errors.Count > 0)
            return Result<ValkeyClusterCreatedDto>.Failed(new ValkeyValidationException(errors));

        var name = request.Name!;
        var nodes = request.Nodes ?? ValkeyLimits.DefNodes;
        var maxmemoryBytes = request.MaxmemoryBytes ?? ValkeyLimits.DefMaxmemoryBytes;
        var policy = request.MaxmemoryPolicy ?? ValkeyLimits.DefMaxmemoryPolicy;
        var cpu = ValkeyLimits.Canonical(request.Resources?.Cpu ?? ValkeyLimits.DefCpu);
        var memGi = request.Resources?.MemGi ?? ValkeyLimits.DefMemGi;
        var diskGi = request.Resources?.DiskGi ?? ValkeyLimits.DefDiskGi;

        // 2) Клэйм имени: compare NotExists + put config NOT_INITIALIZED
        //    (канон arch/20 §2.1 — заявка несёт state).
        var configValue = new ValkeyConfigJson(
            nodes, maxmemoryBytes, policy, time.GetUtcNow().ToUnixTimeSeconds(),
            "NOT_INITIALIZED").Serialize();
        var configKey = $"/valkey/clusters/{name}/config";
        var claim = await ValkeyEtcdFailover.CallAsync(endpoints, endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of(
                [TxnCompare.NotExists(configKey)],
                [new TxnOp.Put(configKey, configValue, null)]),
            ct));
        if (!claim.IsSuccess)
            return Result<ValkeyClusterCreatedDto>.Failed(claim.Error!);
        if (!claim.Value.Succeeded)
            return Result<ValkeyClusterCreatedDto>.Failed(new ValkeyClusterAlreadyExistsException(name));

        // 3) Пакет PUT nodes/node1/{state,resources}; сбой → компенсация префиксом.
        var puts = new[]
        {
            (ValkeyApiHelpers.NodeKey(name, "node1", "state"), "NOT_INITIALIZED"),
            (ValkeyApiHelpers.NodeKey(name, "node1", "resources"),
                JsonSerializer.Serialize(new ValkeyResourcesJson(cpu, $"{memGi}Gi", $"{diskGi}Gi"))),
        };
        foreach (var (key, value) in puts)
        {
            var putResult = await ValkeyEtcdFailover.CallAsync(endpoints,
                endpoint => gateway.PutAsync(endpoint, key, value, null, ct));
            if (putResult.IsSuccess)
                continue;

            await ValkeyEtcdFailover.CallAsync(endpoints, endpoint => gateway.DeleteAsync(
                endpoint, $"/valkey/clusters/{name}/", prefix: true, ct));
            return Result<ValkeyClusterCreatedDto>.Failed(putResult.Error!);
        }

        return Result<ValkeyClusterCreatedDto>.Success(new ValkeyClusterCreatedDto(
            name, "NOT_INITIALIZED", nodes, maxmemoryBytes, policy, cpu, $"{memGi}Gi", $"{diskGi}Gi"));
    }

    // Чистая функция валидации создания (pg §9.3 форматы + инвариант R3).
    internal static List<ValidationError> Validate(CreateValkeyClusterRequest request)
    {
        var errors = new List<ValidationError>();
        if (request.Name is null || !ValkeyLimits.ClusterPattern().IsMatch(request.Name))
            errors.Add(new("name", "name: [a-z][a-z0-9_]{0,62} (строчные, без дефиса)"));

        var nodes = request.Nodes ?? ValkeyLimits.DefNodes;
        if (nodes != ValkeyLimits.MaxNodes)
            errors.Add(new("nodes", $"nodes: только {ValkeyLimits.MaxNodes} в v1 (реплики — roadmap)"));

        var maxmemory = request.MaxmemoryBytes ?? ValkeyLimits.DefMaxmemoryBytes;
        if (maxmemory < ValkeyLimits.MinMaxmemoryBytes)
            errors.Add(new("maxmemoryBytes", $"maxmemoryBytes: > {ValkeyLimits.MinMaxmemoryBytes - 1}"));
        if (!ValkeyLimits.IsKnownPolicy(request.MaxmemoryPolicy))
            errors.Add(new("maxmemoryPolicy",
                $"maxmemoryPolicy: одно из {string.Join('|', ValkeyWriting.KnownPolicies.OrderBy(p => p, StringComparer.Ordinal))}"));

        var cpu = request.Resources?.Cpu ?? ValkeyLimits.DefCpu;
        var memGi = request.Resources?.MemGi ?? ValkeyLimits.DefMemGi;
        var diskGi = request.Resources?.DiskGi ?? ValkeyLimits.DefDiskGi;
        if (cpu is < ValkeyLimits.MinCpu or > ValkeyLimits.MaxCpu)
            errors.Add(new("cpu", $"cpu: {ValkeyLimits.MinCpu}..{ValkeyLimits.MaxCpu} ядер"));
        if (memGi is < ValkeyLimits.MinGiB or > ValkeyLimits.MaxGiB)
            errors.Add(new("memGi", $"memGi: {ValkeyLimits.MinGiB}..{ValkeyLimits.MaxGiB} GiB"));
        if (diskGi is < ValkeyLimits.MinGiB or > ValkeyLimits.MaxGiB)
            errors.Add(new("diskGi", $"diskGi: {ValkeyLimits.MinGiB}..{ValkeyLimits.MaxGiB} GiB"));

        // Инвариант R3 (на валидных mem/maxmemory).
        if (maxmemory >= ValkeyLimits.MinMaxmemoryBytes
            && memGi is >= ValkeyLimits.MinGiB and <= ValkeyLimits.MaxGiB
            && !ValkeyLimits.MaxmemoryFitsMem(maxmemory, memGi))
            errors.Add(new("maxmemoryBytes",
                $"maxmemoryBytes ({maxmemory}) обязан быть < mem-лимита ({memGi}Gi) — риск OOM-килла (R3)"));

        return errors;
    }
}

// JSON заявки ресурсов ноды: {"cpu":"2","mem":"4Gi","disk":"40Gi"} (pg §9.3).
public sealed record ValkeyResourcesJson(
    [property: System.Text.Json.Serialization.JsonPropertyName("cpu")] string Cpu,
    [property: System.Text.Json.Serialization.JsonPropertyName("mem")] string Mem,
    [property: System.Text.Json.Serialization.JsonPropertyName("disk")] string Disk);
