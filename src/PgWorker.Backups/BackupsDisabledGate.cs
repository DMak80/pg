using System.Text.Json;
using Shared.Core;
using Shared.Etcd.Client;

namespace PgWorker.Backups;

/// <summary>Глобальный стоп-ключ /pgworker/backups-disabled (arch/19 §4,
/// решение spec §7 №7): plain (TTL нет, вне per-cluster префиксов — пустой
/// per-cluster префикс панели трактовали бы как «подсистема не включена»).
/// Писатель/стирающий — PgWorker: держатель глобального лидера при каждом тике
/// лидера при Backups:Enabled=false поддерживает ключ (put), при Enabled=true —
/// зеркально стирает (идемпотентный del в каждом тике — stale-ключ от прежнего
/// лидера/режима не выживает); каждый инстанс при старте с Enabled=true тоже
/// снимает ключ (404 — успех по семантике идемпотентности движка). Приёмники
/// читают ключ сами — воркер больше ничего с ним не делает.</summary>
public sealed class BackupsDisabledGate(IEtcdGateway etcd)
{
    public const string Key = "/pgworker/backups-disabled";

    /// <summary>Поставить ключ (failover — единая точка, arch/17 S7): plain put
    /// {"updated_unix":T} — отметка поддержки (панель/диагностика).</summary>
    public Task<Result> PutDisabledAsync(string[] endpoints, long unix, CancellationToken ct)
    {
        var json = JsonSerializer.Serialize(new { updated_unix = unix });
        return EtcdEndpointFailover.CallAsync(endpoints, endpoint => etcd.PutAsync(endpoint, Key, json, lease: null, ct));
    }

    /// <summary>Снять ключ (failover — единая точка, arch/17 S7): del; отсутствие
    /// ключа — успех (идемпотентность старта с Enabled=true).</summary>
    public Task<Result> DeleteAsync(string[] endpoints, CancellationToken ct)
        => EtcdEndpointFailover.CallAsync(endpoints, endpoint => etcd.DeleteAsync(endpoint, Key, prefix: false, ct));
}
