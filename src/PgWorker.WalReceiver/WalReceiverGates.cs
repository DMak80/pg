using Shared.Core;
using Shared.Etcd.Client;

namespace PgWorker.WalReceiver;

/// <summary>Снимок стоп-гейтов приёмника (arch/19 §3 «Стоп-семантика»):
/// Disabled — глобальный ключ /pgworker/backups-disabled жив (Backups:Enabled=false
/// воркера); Paused — wal-ключ шарда в STOPPED (карантин) или WAL_CHAIN_ERROR
/// (разрыв цепочки); ChainError — wal-ключ именно в WAL_CHAIN_ERROR (Paused=true;
/// для гейта ОПЕРАЦИИ полного НЕ стоп-гейт — пересъём полным единственное
/// лечение разрыва, arch/19 §2); Unknown — транспортный отказ etcd при чтении
/// (значения полей не доверенные: живой цикл работает по последним известным
/// гейтам, перепроверка через 5 c — etcd-моргание стрим не рвёт, слот держит
/// WAL).</summary>
public sealed record WalReceiverGateState(bool Disabled, bool Paused, bool ChainError, bool Unknown);

/// <summary>Читатель стоп-гейтов etcd (arch/19 §4): чтение обоих ключей —
/// failover-перебор endpoints (образец WalStatusWriter.ReadAsync).</summary>
public static class WalReceiverGates
{
    private const string DisabledKey = "/pgworker/backups-disabled";

    /// <summary>Чтение обоих гейтов. disabled: ключ есть → Disabled. wal:
    /// значение содержит "state":"STOPPED"/"state":"WAL_CHAIN_ERROR" → Paused;
    /// "state":"WAL_CHAIN_ERROR" → ChainError. Любой транспортный отказ etcd →
    /// Unknown=true (гейты не доверенные).</summary>
    public static async Task<WalReceiverGateState> ReadAsync(
        IEtcdGateway etcd, string[] endpoints, string cluster, string shard, CancellationToken ct)
    {
        var disabled = await ReadDisabledAsync(etcd, endpoints, ct);
        if (!disabled.IsSuccess)
            return new WalReceiverGateState(Disabled: false, Paused: false, ChainError: false, Unknown: true);

        var wal = await ReadWalAsync(etcd, endpoints, cluster, shard, ct);
        if (!wal.IsSuccess)
            return new WalReceiverGateState(Disabled: false, Paused: false, ChainError: false, Unknown: true);

        return new WalReceiverGateState(
            Disabled: disabled.Value is not null,
            Paused: wal.Value is { } value && IsPaused(value),
            ChainError: wal.Value is { } chainValue && IsChainError(chainValue),
            Unknown: false);
    }

    /// <summary>Проба ПОЛНОГО стоп-гейта из живого стрима (arch/19 §3):
    /// true — глобальный ключ /pgworker/backups-disabled жив ИЛИ wal-ключ
    /// шарда в STOPPED/WAL_CHAIN_ERROR: живой стрим обязан замирать по ЛЮБОМУ из
    /// гейтов (исход Reconnect — цикл перепроверит и уйдёт в
    /// waiting_disabled/paused), а не ждать обрыва транспорта. Транспортный
    /// отказ etcd → false (стрим не рвём по морганию: перепроверка следующим
    /// периодом пробы; последнее известное — канон живого цикла). Это гейт
    /// СТРИМА — к операции полного не применяется (arch/19 §2: при
    /// WAL_CHAIN_ERROR пересъём не блокируется).</summary>
    public static async Task<bool> IsStopGatedAsync(
        IEtcdGateway etcd, string[] endpoints, string cluster, string shard, CancellationToken ct)
    {
        var disabled = await ReadDisabledAsync(etcd, endpoints, ct);
        if (!disabled.IsSuccess)
            return false;
        if (disabled.Value is not null)
            return true;

        var wal = await ReadWalAsync(etcd, endpoints, cluster, shard, ct);
        return wal.IsSuccess && wal.Value is { } value && IsPaused(value);
    }

    // GET /pgworker/backups-disabled — failover; Result<Kv?>.Failed — transport.
    private static async Task<Result<Kv?>> ReadDisabledAsync(
        IEtcdGateway etcd, string[] endpoints, CancellationToken ct)
    {
        Result<Kv?> last = default;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.GetAsync(endpoint, DisabledKey, ct);
            if (!result.IsSuccess)
            {
                last = result; // отказ — пробуем следующий endpoint (failover)
                continue;
            }
            return result; // null — ключа нет (не ошибка)
        }

        return Result<Kv?>.Failed(last.Error
            ?? new EtcdUnreachableException("etcd endpoints are not set"));
    }

    // GET /pgworker/backups/<C>/<X>/wal — failover; null — ключа нет (не ошибка).
    private static async Task<Result<string?>> ReadWalAsync(
        IEtcdGateway etcd, string[] endpoints, string cluster, string shard, CancellationToken ct)
    {
        Result<Kv?> last = default;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.GetAsync(endpoint, $"/pgworker/backups/{cluster}/{shard}/wal", ct);
            if (!result.IsSuccess)
            {
                last = result; // отказ — пробуем следующий endpoint (failover)
                continue;
            }
            return Result<string?>.Success(result.Value?.Value);
        }

        return Result<string?>.Failed(last.Error
            ?? new EtcdUnreachableException("etcd endpoints are not set"));
    }

    // Стоп-семантика wal-ключа: STOPPED (карантин) или WAL_CHAIN_ERROR (разрыв
    // цепочки) — пауза. Писатель значения один (WalStatusWriter — компактный
    // JSON), подстрока "state":"…" надёжна; всё прочее (ACTIVE/DEGRADED/чужое)
    // — работаем.
    private static bool IsPaused(string walJson) =>
        walJson.Contains("\"state\":\"STOPPED\"", StringComparison.Ordinal) ||
        IsChainError(walJson);

    // WAL_CHAIN_ERROR — разрыв цепочки WAL в архиве: стрим паузится, а гейтом
    // ОПЕРАЦИИ полного НЕ является (пересъём — единственное лечение, arch/19 §2).
    private static bool IsChainError(string walJson) =>
        walJson.Contains("\"state\":\"WAL_CHAIN_ERROR\"", StringComparison.Ordinal);
}
