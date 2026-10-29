using Shared.Core;
using Shared.Etcd.Client;

namespace Shared.Etcd.Coordination;

// Лестница E9 самолечения portalloc (arch/17): нода без записи portalloc —
// ДО любых деструктивных действий — реконструкция из inspect живого контейнера
// (published-порт + host), put-if-absent под locks/portalloc; проигрыш txn →
// re-read (первый записавший — истина, S5). Контейнера нет / inspect
// недоступен → Failed (E9 не выдумывает порт, S7; новая аллокация — дело
// вызывающего процесса).
// Доменные воркеры используют общий класс через тонкий фасад: инспекция ноды,
// ключ portalloc, индекс чужих портов и доменный формат значения передаются
// делегатами (PortAllocHealerDeps) — поле порта канона своего домена
// (arch/15 §4 / arch/20 §3 «client», arch/23 §8 «s3») знает только фасад.
public sealed record PortAllocNodeAddress(string Host, int Port);

public sealed record PortAllocHealerDeps
{
    /// <summary>Полный ключ portalloc кластера: cluster → "/…/portalloc/&lt;C&gt;".</summary>
    public required Func<string, string> PortAllocKey { get; init; }

    /// <summary>
    /// Инспекция живого контейнера ноды: адрес (host, published-порт) или null —
    /// контейнера нет; failed — docker-слепота.
    /// </summary>
    public required Func<string, string, CancellationToken, Task<Result<PortAllocNodeAddress?>>> InspectNode { get; init; }

    /// <summary>Клиентские порты portalloc ВСЕХ чужих кластеров (диагностика коллизии).</summary>
    public required Func<string, CancellationToken, Task<Result<IReadOnlyCollection<int>>>> ForeignAllocatedPorts { get; init; }

    /// <summary>Разбор значения portalloc (формат канона домена).</summary>
    public required Func<string, IReadOnlyDictionary<string, PortAllocNodeAddress>> ParsePortAlloc { get; init; }

    /// <summary>Сериализация значения portalloc (формат канона домена).</summary>
    public required Func<IReadOnlyDictionary<string, PortAllocNodeAddress>, string> SerializePortAlloc { get; init; }

    public required int PortRangeFrom { get; init; }
    public required int PortRangeTo { get; init; }
}

public sealed class EtcdPortAllocHealer(
    IEtcdGateway etcd,
    string[] endpoints,
    ClaimStore claims,
    WorkJournal journal,
    PortAllocLock portLock,
    PortAllocHealerDeps deps)
{
    private const string Op = "healing-portalloc";

    // Восстановление адреса ОДНОЙ ноды: запись portalloc есть → её адрес
    // (реконструкция не нужна); нет — инспекция живого контейнера →
    // put-if-absent под глобальным клэймом. Успех: фактический адрес ноды.
    public async Task<Result<PortAllocNodeAddress>> HealNodeAsync(string cluster, string node, CancellationToken ct)
    {
        // Ветка 1: закрепление есть — advertise стабилен.
        var existing = await ReadPortAllocAsync(cluster, ct);
        if (!existing.IsSuccess)
            return Result<PortAllocNodeAddress>.Failed(existing.Error!);
        if (existing.Value.TryGetValue(node, out var pinned))
            return Result<PortAllocNodeAddress>.Success(pinned);

        // Контейнер — до клэйма: положительная инспекция решает ветку.
        var inspection = await deps.InspectNode(cluster, node, ct);
        if (!inspection.IsSuccess)
            return Result<PortAllocNodeAddress>.Failed(inspection.Error!); // слепота — не лечим
        if (inspection.Value is not { } found)
            return Result<PortAllocNodeAddress>.Failed(new ApplicationException(
                $"{Op} {cluster}/{node}: container missing — not inventing a port (S7)"));

        // Реконструированный порт обязан лежать в portalloc-диапазоне конфигурации:
        // вне его — мусорный inspect, запись сломала бы аллокатор.
        if (found.Port < deps.PortRangeFrom || found.Port >= deps.PortRangeTo)
            return Result<PortAllocNodeAddress>.Failed(new ApplicationException(
                $"{Op} {cluster}/{node}: published port {found.Port} outside range {deps.PortRangeFrom}–{deps.PortRangeTo}"));

        // journal-before-manipulations: фаза ДО первого txn.
        var started = await journal.WritePhaseAsync(cluster, Op, "started", claims.InstanceId, null, ct);
        if (!started.IsSuccess)
            return Result<PortAllocNodeAddress>.Failed(started.Error!);

        var acquired = await portLock.TryAcquireAsync(ct);
        if (!acquired.IsSuccess)
            return Result<PortAllocNodeAddress>.Failed(acquired.Error!);
        if (!acquired.Value)
            return Result<PortAllocNodeAddress>.Failed(new PortLockBusyException(portLock.Key));
        try
        {
            var key = deps.PortAllocKey(cluster);
            var merged = new Dictionary<string, PortAllocNodeAddress>(existing.Value)
            {
                [node] = found,
            };
            var txn = await TxnAsync(
                TxnRequest.Of([TxnCompare.NotExists(key)], [new TxnOp.Put(key, deps.SerializePortAlloc(merged), null)]), ct);
            if (!txn.IsSuccess)
                return Result<PortAllocNodeAddress>.Failed(txn.Error!);
            if (!txn.Value.Succeeded)
            {
                // уже записал сосед — читаем его истину (S5).
                var reread = await ReadPortAllocAsync(cluster, ct);
                if (!reread.IsSuccess)
                    return Result<PortAllocNodeAddress>.Failed(reread.Error!);
                if (!reread.Value.TryGetValue(node, out var foreign))
                    return Result<PortAllocNodeAddress>.Failed(new ApplicationException(
                        $"{Op} {cluster}/{node}: lost txn with no neighbor entry"));
                return Result<PortAllocNodeAddress>.Success(foreign);
            }
        }
        finally
        {
            await portLock.ReleaseAsync();
        }

        var done = await journal.WritePhaseAsync(cluster, Op, "reconstructed", claims.InstanceId, null, ct);
        if (!done.IsSuccess)
            return Result<PortAllocNodeAddress>.Failed(done.Error!);

        // Диагностика коллизии: реконструированный порт в portalloc чужого
        // кластера — warning в журнал (не ошибка: docker-публикация — факт).
        var foreignBusy = await deps.ForeignAllocatedPorts(cluster, ct);
        if (foreignBusy.IsSuccess && foreignBusy.Value.Contains(found.Port))
        {
            await journal.WritePhaseAsync(cluster, Op, "reconstructed-warning", claims.InstanceId,
                $"port {found.Port} already in portalloc of another cluster", ct);
        }

        return Result<PortAllocNodeAddress>.Success(found);
    }

    private async Task<Result<IReadOnlyDictionary<string, PortAllocNodeAddress>>> ReadPortAllocAsync(
        string cluster, CancellationToken ct)
    {
        var result = await EtcdEndpointFailover.CallAsync(
            endpoints, (endpoint, token) => etcd.GetAsync(endpoint, deps.PortAllocKey(cluster), token), ct);
        if (!result.IsSuccess)
            return Result<IReadOnlyDictionary<string, PortAllocNodeAddress>>.Failed(result.Error!);

        if (result.Value is not { } kv)
            return Result<IReadOnlyDictionary<string, PortAllocNodeAddress>>.Success(
                (IReadOnlyDictionary<string, PortAllocNodeAddress>)new Dictionary<string, PortAllocNodeAddress>());
        return Result<IReadOnlyDictionary<string, PortAllocNodeAddress>>.Success(deps.ParsePortAlloc(kv.Value));
    }

    private Task<Result<TxnResult>> TxnAsync(TxnRequest req, CancellationToken ct)
        => EtcdEndpointFailover.CallAsync(endpoints, (endpoint, token) => etcd.TxnAsync(endpoint, req, token), ct);
}
