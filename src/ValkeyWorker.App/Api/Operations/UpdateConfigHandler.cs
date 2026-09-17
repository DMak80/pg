using Shared.Core;
using Shared.Etcd.Client;
using ValkeyWorker.Core.Writing;

namespace ValkeyWorker.App.Api.Operations;

// Ответ 200 PUT /api/valkey/clusters/{c}/config (сигнатуры — порт kfw).
public sealed record ValkeyConfigUpdatedDto(
    string Cluster, long MaxmemoryBytes, string MaxmemoryPolicy);

// Конфиг-мутация maxmemory_bytes/maxmemory_policy (spec §4.7): RMW-txn по
// mod_revision прочитанного config; применит converge D (CONFIG SET без
// рестартов). Валидация на эффективных значениях + инвариант R3 против
// ТЕКУЩИХ resources etcd.
public sealed class UpdateConfigHandler(IEtcdGateway gateway, string[] endpoints)
{
    public async Task<Result<ValkeyConfigUpdatedDto>> HandleAsync(
        string cluster, ValkeyConfigUpdateRequest request, CancellationToken ct)
    {
        var config = await ValkeyApiHelpers.ReadConfigAsync(gateway, endpoints, cluster, ct);
        if (config.Error is not null)
            return Result<ValkeyConfigUpdatedDto>.Failed(config.Error);
        if (config.Value is null)
            return Result<ValkeyConfigUpdatedDto>.Failed(new ValkeyClusterNotFoundException(cluster));

        // Валидация (правила канона; policy null = без изменений → канон-дефолт уже в etcd).
        var errors = new List<ValidationError>();
        if (request.MaxmemoryBytes is { } bytes && bytes < ValkeyLimits.MinMaxmemoryBytes)
            errors.Add(new("maxmemoryBytes", $"maxmemoryBytes: > {ValkeyLimits.MinMaxmemoryBytes - 1}"));
        if (request.MaxmemoryPolicy is { } policy && !ValkeyLimits.IsKnownPolicy(policy))
            errors.Add(new("maxmemoryPolicy",
                $"maxmemoryPolicy: одно из {string.Join('|', ValkeyWriting.KnownPolicies.OrderBy(p => p, StringComparer.Ordinal))}"));

        var effectiveBytes = request.MaxmemoryBytes ?? config.Value.MaxmemoryBytes;
        // Инвариант R3 против текущих resources etcd (мем может отличаться от снапшота).
        var memGi = await ReadMemGiAsync(cluster, ct);
        if (memGi is { } mem
            && effectiveBytes >= ValkeyLimits.MinMaxmemoryBytes
            && !ValkeyLimits.MaxmemoryFitsMem(effectiveBytes, mem))
            errors.Add(new("maxmemoryBytes",
                $"maxmemoryBytes ({effectiveBytes}) обязан быть < mem-лимита ({mem}Gi) — риск OOM-килла (R3)"));
        if (errors.Count > 0)
            return Result<ValkeyConfigUpdatedDto>.Failed(new ValkeyValidationException(errors));

        // RMW-txn: compare mod_revision == прочитанного → put канонического JSON
        // (state сохраняется как есть).
        var updated = config.Value with
        {
            MaxmemoryBytes = request.MaxmemoryBytes ?? config.Value.MaxmemoryBytes,
            MaxmemoryPolicy = request.MaxmemoryPolicy ?? config.Value.MaxmemoryPolicy,
        };
        var key = ValkeyApiHelpers.ConfigKey(cluster);
        var txn = await ValkeyEtcdFailover.CallAsync(endpoints, endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of(
                [TxnCompare.ModRevisionEqual(key, config.Revision!.Value)],
                [new TxnOp.Put(key, updated.Serialize(), null)]),
            ct));
        if (!txn.IsSuccess)
            return Result<ValkeyConfigUpdatedDto>.Failed(txn.Error!);
        if (!txn.Value.Succeeded)
            return Result<ValkeyConfigUpdatedDto>.Failed(
                new ValkeyConcurrentWriteException(key));

        return Result<ValkeyConfigUpdatedDto>.Success(
            new ValkeyConfigUpdatedDto(cluster, updated.MaxmemoryBytes, updated.MaxmemoryPolicy));
    }

    // Текущий memGi из resources node1 (null — ключа нет/битый — инвариант не сверяется).
    private async Task<int?> ReadMemGiAsync(string cluster, CancellationToken ct)
    {
        var resources = await ValkeyApiHelpers.ReadKeyAsync(
            gateway, endpoints, ValkeyApiHelpers.NodeKey(cluster, "node1", "resources"), ct);
        if (!resources.IsSuccess || resources.Value is not { } kv)
            return null;
        try
        {
            using var doc = System.Text.Json.JsonDocument.Parse(kv.Value);
            return doc.RootElement.TryGetProperty("mem", out var mem) && mem.ValueKind == System.Text.Json.JsonValueKind.String
                ? mem.GetString() is { } raw && raw.EndsWith("Gi", StringComparison.Ordinal)
                  && int.TryParse(raw[..^2], out var gi)
                    ? gi
                    : null
                : null;
        }
        catch (System.Text.Json.JsonException)
        {
            return null;
        }
    }
}
