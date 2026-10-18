using S3Worker.Core.Model;
using S3Worker.Docker.Drivers;
using Shared.Etcd.Client;

namespace S3Worker.Provisioning.Processes;

// Лестница E9 самолечения portalloc (arch/17; порт PortAllocHealer valkey):
// нода без записи portalloc — ДО любых деструктивных действий — реконструкция
// из inspect живого контейнера (published-порт + host), put-if-absent под
// locks/portalloc; проигрыш txn → re-read (первый записавший — истина, S5).
// Контейнера нет / inspect недоступен → Failed (E9 не выдумывает порт, S7;
// новая аллокация — дело вызывающего процесса P1).
public sealed class PortAllocHealer(
    IEtcdGateway etcd,
    string[] endpoints,
    IS3ClusterDriver driver,
    ClaimStore claims,
    WorkJournal journal,
    PortAllocLock portLock,
    PortAllocIndex index,
    S3ProvisioningOptions options)
{
    private const string Op = "healing-portalloc";

    // Восстановление адреса ОДНОЙ ноды: запись portalloc есть → её адрес
    // (реконструкция не нужна); нет — инспекция живого контейнера →
    // put-if-absent под глобальным клэймом. Успех: фактический адрес ноды.
    public async Task<Result<NodeAddress>> HealNodeAsync(string cluster, string node, CancellationToken ct)
    {
        // Ветка 1: закрепление есть — advertise стабилен.
        var existing = await ReadPortAllocAsync(cluster, ct);
        if (!existing.IsSuccess)
            return Result<NodeAddress>.Failed(existing.Error!);
        if (existing.Value.TryGetValue(node, out var pinned))
            return Result<NodeAddress>.Success(pinned);

        // Контейнер — до клэйма: положительная инспекция решает ветку.
        var inspection = await driver.InspectNodeAsync(cluster, node, ct);
        if (!inspection.IsSuccess)
            return Result<NodeAddress>.Failed(inspection.Error!); // слепота — не лечим
        if (inspection.Value is not { } found)
            return Result<NodeAddress>.Failed(new ApplicationException(
                $"healing-portalloc {cluster}/{node}: no container — not inventing a port (S7)"));

        // Реконструированный порт обязан лежать в portalloc-диапазоне конфигурации:
        // вне его — мусорный inspect, запись сломала бы аллокатор.
        if (found.S3HostPort < options.PortRangeFrom || found.S3HostPort >= options.PortRangeTo)
            return Result<NodeAddress>.Failed(new ApplicationException(
                $"healing-portalloc {cluster}/{node}: published port {found.S3HostPort} outside range {options.PortRangeFrom}–{options.PortRangeTo}"));

        // journal-before-manipulations: фаза ДО первого txn.
        var started = await journal.WritePhaseAsync(cluster, Op, "started", claims.InstanceId, null, ct);
        if (!started.IsSuccess)
            return Result<NodeAddress>.Failed(started.Error!);

        var acquired = await portLock.TryAcquireAsync(ct);
        if (!acquired.IsSuccess)
            return Result<NodeAddress>.Failed(acquired.Error!);
        if (!acquired.Value)
            return Result<NodeAddress>.Failed(new PortLockBusyException(portLock.Key));
        try
        {
            var key = S3ProcessCommon.PortAllocKey(cluster);
            var merged = new Dictionary<string, NodeAddress>(existing.Value)
            {
                [node] = new(found.Host, found.S3HostPort),
            };
            var txn = await TxnAsync(
                TxnRequest.Of([TxnCompare.NotExists(key)], [new TxnOp.Put(key, S3ProcessCommon.SerializePortAlloc(merged), null)]), ct);
            if (!txn.IsSuccess)
                return Result<NodeAddress>.Failed(txn.Error!);
            if (!txn.Value.Succeeded)
            {
                // уже записал сосед — читаем его истину (S5).
                var reread = await ReadPortAllocAsync(cluster, ct);
                if (!reread.IsSuccess)
                    return Result<NodeAddress>.Failed(reread.Error!);
                if (!reread.Value.TryGetValue(node, out var foreign))
                    return Result<NodeAddress>.Failed(new ApplicationException(
                        $"healing-portalloc {cluster}/{node}: lost txn without neighbor entry"));
                return Result<NodeAddress>.Success(foreign);
            }
        }
        finally
        {
            await portLock.ReleaseAsync();
        }

        var done = await journal.WritePhaseAsync(cluster, Op, "reconstructed", claims.InstanceId, null, ct);
        if (!done.IsSuccess)
            return Result<NodeAddress>.Failed(done.Error!);

        // Диагностика коллизии: реконструированный порт в portalloc чужого
        // кластера — warning в журнал (не ошибка: docker-публикация — факт).
        var foreignBusy = await index.ForeignAllocatedPortsAsync(cluster, ct);
        if (foreignBusy.IsSuccess && foreignBusy.Value.Contains(found.S3HostPort))
        {
            await journal.WritePhaseAsync(cluster, Op, "reconstructed-warning", claims.InstanceId,
                $"port {found.S3HostPort} already in portalloc of a foreign cluster", ct);
        }

        return Result<NodeAddress>.Success(new NodeAddress(found.Host, found.S3HostPort));
    }

    private async Task<Result<IReadOnlyDictionary<string, NodeAddress>>> ReadPortAllocAsync(
        string cluster, CancellationToken ct)
    {
        Result<Kv?> last = default;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.GetAsync(endpoint, S3ProcessCommon.PortAllocKey(cluster), ct);
            if (!result.IsSuccess)
            {
                last = result;
                continue;
            }

            if (result.Value is not { } kv)
                return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(
                    (IReadOnlyDictionary<string, NodeAddress>)new Dictionary<string, NodeAddress>());
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(S3ProcessCommon.ParsePortAlloc(kv.Value));
        }

        return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(last.Error ?? new EtcdUnreachableException("etcd endpoints are not set"));
    }

    private async Task<Result<TxnResult>> TxnAsync(TxnRequest req, CancellationToken ct)
    {
        Result<TxnResult> last = default;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.TxnAsync(endpoint, req, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last.Error ?? new EtcdUnreachableException("etcd endpoints are not set");
    }
}
