using System.Text.Json;
using Shared.Core;
using Shared.Etcd.Client;

namespace ValkeyWorker.App.Api.Operations;

// Ответ 200 PUT /api/valkey/clusters/{c}/nodes/{node}/resources (порт kfw).
public sealed record ValkeyResourcesUpdatedDto(
    string Cluster, string Node, string Cpu, string MemGi, string DiskGi);

// Мутация лимитов ноды (spec §4.7; v1 — только node1): клэйм-txn NotExists НЕ
// нужен (нода декларирована при создании) — put resources, применит
// автоконверге надзора (пересоздание, одно за тик). Идемпотентен.
public sealed class UpdateResourcesHandler(IEtcdGateway gateway, string[] endpoints)
{
    public async Task<Result<ValkeyResourcesUpdatedDto>> HandleAsync(
        string cluster, string node, ValkeyResourcesUpdateRequest request, CancellationToken ct)
    {
        // v1: standalone node1 (реплики — roadmap).
        if (node != "node1")
            return Result<ValkeyResourcesUpdatedDto>.Failed(new ValkeyNodeNotFoundException(cluster, node));

        var config = await ValkeyApiHelpers.ReadConfigAsync(gateway, endpoints, cluster, ct);
        if (config.Error is not null)
            return Result<ValkeyResourcesUpdatedDto>.Failed(config.Error);
        if (config.Value is null)
            return Result<ValkeyResourcesUpdatedDto>.Failed(new ValkeyClusterNotFoundException(cluster));

        // Текущие ресурсы — дефолты для неуказанных полей (идемпотентность PUT).
        var current = await ReadCurrentAsync(cluster, node, ct);
        var cpu = request.Cpu ?? current?.Cpu ?? ValkeyLimits.DefCpu;
        var memGi = request.MemGi ?? current?.MemGi ?? ValkeyLimits.DefMemGi;
        var diskGi = request.DiskGi ?? current?.DiskGi ?? ValkeyLimits.DefDiskGi;

        // Валидация (границы pg §9.3).
        var errors = new List<ValidationError>();
        if (cpu < ValkeyLimits.MinCpu || cpu > ValkeyLimits.MaxCpu)
            errors.Add(new("cpu", $"cpu: {ValkeyLimits.MinCpu}..{ValkeyLimits.MaxCpu} ядер"));
        if (memGi is < ValkeyLimits.MinGiB or > ValkeyLimits.MaxGiB)
            errors.Add(new("memGi", $"memGi: {ValkeyLimits.MinGiB}..{ValkeyLimits.MaxGiB} GiB"));
        if (diskGi is < ValkeyLimits.MinGiB or > ValkeyLimits.MaxGiB)
            errors.Add(new("diskGi", $"diskGi: {ValkeyLimits.MinGiB}..{ValkeyLimits.MaxGiB} GiB"));

        // Инвариант R3 против ТЕКУЩЕГО maxmemory config.
        if (memGi is >= ValkeyLimits.MinGiB and <= ValkeyLimits.MaxGiB
            && config.Value.MaxmemoryBytes >= ValkeyLimits.MinMaxmemoryBytes
            && !ValkeyLimits.MaxmemoryFitsMem(config.Value.MaxmemoryBytes, memGi))
            errors.Add(new("memGi",
                $"memGi ({memGi}) обязан быть > maxmemory_bytes ({config.Value.MaxmemoryBytes}) — риск OOM-килла (R3)"));
        if (errors.Count > 0)
            return Result<ValkeyResourcesUpdatedDto>.Failed(new ValkeyValidationException(errors));

        var canonicalCpu = ValkeyLimits.Canonical(cpu);
        var put = await ValkeyEtcdFailover.CallAsync(endpoints, endpoint => gateway.PutAsync(
            endpoint,
            ValkeyApiHelpers.NodeKey(cluster, node, "resources"),
            JsonSerializer.Serialize(new ValkeyResourcesJson(canonicalCpu, $"{memGi}Gi", $"{diskGi}Gi")),
            null, ct));
        if (!put.IsSuccess)
            return Result<ValkeyResourcesUpdatedDto>.Failed(put.Error!);

        return Result<ValkeyResourcesUpdatedDto>.Success(
            new ValkeyResourcesUpdatedDto(cluster, node, canonicalCpu, $"{memGi}Gi", $"{diskGi}Gi"));
    }

    private async Task<(decimal Cpu, int MemGi, int DiskGi)?> ReadCurrentAsync(
        string cluster, string node, CancellationToken ct)
    {
        var resources = await ValkeyApiHelpers.ReadKeyAsync(
            gateway, endpoints, ValkeyApiHelpers.NodeKey(cluster, node, "resources"), ct);
        if (!resources.IsSuccess || resources.Value is not { } kv)
            return null;
        try
        {
            using var doc = JsonDocument.Parse(kv.Value);
            var root = doc.RootElement;
            decimal? cpu = root.TryGetProperty("cpu", out var c) && c.ValueKind == JsonValueKind.String
                           && decimal.TryParse(c.GetString(), System.Globalization.NumberStyles.Number,
                               System.Globalization.CultureInfo.InvariantCulture, out var cpuValue)
                ? cpuValue
                : null;
            int? memGi = ParseGi(root, "mem");
            int? diskGi = ParseGi(root, "disk");
            return cpu is null || memGi is null || diskGi is null ? null : (cpu.Value, memGi.Value, diskGi.Value);
        }
        catch (JsonException)
        {
            return null;
        }

        static int? ParseGi(JsonElement root, string field)
            => root.TryGetProperty(field, out var v) && v.ValueKind == JsonValueKind.String
               && v.GetString() is { } raw && raw.EndsWith("Gi", StringComparison.Ordinal)
               && int.TryParse(raw[..^2], out var gi)
                ? gi
                : null;
    }
}
