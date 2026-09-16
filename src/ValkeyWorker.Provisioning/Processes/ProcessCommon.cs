using System.Globalization;
using Shared.Core;
using Shared.Etcd.Client;
using ValkeyWorker.Core.Model;

namespace ValkeyWorker.Provisioning.Processes;

/// <summary>Исход процесса (arch/21 §5): для цикла ReconcileLoop.</summary>
public enum ProcessOutcome
{
    /// <summary>Продолжить следующими тиками (ждём заявок панели, подъёма ноды).</summary>
    InProgress,

    /// <summary>Цель процесса достигнута.</summary>
    Done,
}

/// <summary>
/// Параметры процессов (appsettings ValkeyWorker:Docker/Thresholds): диапазон
/// клиентских портов, бюджет готовности ноды (V4), порог смерти ноды,
/// advertised-хост (null → имя docker-хоста размещения, arch/21 §2), образ ноды.
/// </summary>
public sealed record ValkeyProvisioningOptions(
    int PortRangeFrom,
    int PortRangeTo,
    int NodeBootSec,
    int NodeDeadSec,
    string? AdvertisedClientHost,
    string NodeImage)
{
    public static ValkeyProvisioningOptions Default { get; } =
        new(17000, 17999, 120, 90, null, "valkey/valkey:9.1.2");
}

/// <summary>
/// Общие хелперы процессов (arch/21 §5): чтение/запись state ноды, claimed-check,
/// парсер строк ресурсов канона pg §9.3.
/// </summary>
public static class ProcessCommon
{
    public static string NodeStateKey(string cluster, string node)
        => $"/valkey/clusters/{cluster}/nodes/{node}/state";

    public static string ConfigKey(string cluster) => $"/valkey/clusters/{cluster}/config";

    public static string EndpointsKey(string cluster) => $"/valkey/clusters/{cluster}/endpoints";

    public static string PortAllocKey(string cluster) => $"/valkeyworker/portalloc/{cluster}";

    public static string RotationKey(string cluster) => $"/valkeyworker/rotations/{cluster}";

    // Текущий state ноды (null — ключа нет).
    public static async Task<Result<string?>> ReadNodeStateAsync(
        IEtcdGateway gateway, string[] endpoints, string cluster, string node, CancellationToken ct)
    {
        Result<Kv?>? last = null;
        foreach (var endpoint in endpoints)
        {
            var read = await gateway.GetAsync(endpoint, NodeStateKey(cluster, node), ct);
            if (!read.IsSuccess)
            {
                last = read;
                continue;
            }

            var value = read.Value?.Value;
            return Result<string?>.Success(string.IsNullOrWhiteSpace(value) ? null : value.Trim());
        }

        return Result<string?>.Failed(last!.Error!);
    }

    // Запись state ноды (failover по endpoints).
    public static async Task<Result> WriteNodeStateAsync(
        IEtcdGateway gateway, string[] endpoints, string cluster, string node, string state, CancellationToken ct)
    {
        Result? last = null;
        foreach (var endpoint in endpoints)
        {
            var put = await gateway.PutAsync(endpoint, NodeStateKey(cluster, node), state, null, ct);
            if (put.IsSuccess)
                return put;
            last = put;
        }

        return last!;
    }

    // Claimed-check: мутации — только держателем живого клэйма (arch/21 §6).
    public static Result EnsureClaimed(ClaimStore claims, string cluster, string op)
        => claims.IsMine(cluster)
            ? Result.Success()
            : Result.Failed(new ApplicationException(
                $"{op} {cluster}: клэйм не наш (или потерян) — мутации запрещены"));

    /// <summary>
    /// Ресурсы декларации → лимиты контейнера (cpu/mem; disk — инфо, не парсится).
    /// cpu — decimal-строка инвариантной культуры ("2", "0.5"; БЕЗ суффиксов);
    /// mem — "&lt;n&gt;Gi" → n × 2^30; незнакомый формат → null (процесс не ставит
    /// лимит — валидация API гарантирует канон на входе, толерантность чтения
    /// сохраняется, arch/20 §5).
    /// </summary>
    public static (decimal? Cpu, long? MemBytes)? ParseResources(ValkeyResources? r)
    {
        if (r is null)
            return null;

        decimal? cpu = null;
        long? mem = null;
        if (r.Cpu is { } cpuRaw
            && decimal.TryParse(cpuRaw.Trim(), NumberStyles.Number, CultureInfo.InvariantCulture, out var cpuValue))
            cpu = cpuValue;
        if (r.Mem is { } memRaw
            && memRaw.Trim().EndsWith("Gi", StringComparison.Ordinal)
            && int.TryParse(memRaw.Trim()[..^2], NumberStyles.Integer, CultureInfo.InvariantCulture, out var gi))
            mem = gi * 1024L * 1024 * 1024;

        return (cpu, mem);
    }
}
