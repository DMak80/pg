using System.Text.Json;
using Shared.Core;
using Shared.Etcd.Client;
using ValkeyWorker.Core.Model;
using ValkeyWorker.Docker.Drivers;

namespace ValkeyWorker.Provisioning.Processes;

// Лестница E9 самолечения portalloc (arch/21 §5 C; arch/17): нода без записи
// portalloc — ДО любых деструктивных действий — реконструкция из inspect живого
// контейнера (published-порт + host), put-if-absent под locks/portalloc;
// проигрыш txn → re-read (первый записавший — истина, S5).
// Контейнера нет / inspect недоступен → Failed (E9 не выдумывает порт, S7).
public sealed class PortAllocHealer(
    IEtcdGateway etcd,
    string[] endpoints,
    IClusterDriver driver,
    ClaimStore claims,
    WorkJournal journal,
    PortAllocLock portLock,
    PortAllocIndex index,
    ValkeyProvisioningOptions options)
{
    private const string Op = "healing-portalloc";

    // Восстановление порта ОДНОЙ ноды (spec §4.5 C E9): запись portalloc есть →
    // её порт (реконструкция не нужна); нет — инспекция живого контейнера →
    // put-if-absent под глобальным клэймом. Успех: фактический порт ноды.
    public async Task<Result<int>> HealNodePortAsync(string cluster, string node, CancellationToken ct)
    {
        // Ветка 1: закрепление есть — advertise стабилен.
        var existing = await ReadPortAllocAsync(cluster, ct);
        if (!existing.IsSuccess)
            return Result<int>.Failed(existing.Error!);
        if (existing.Value.TryGetValue(node, out var pinned))
            return Result<int>.Success(pinned.ClientPort);

        // Контейнер — до клэйма: положительная инспекция решает ветку.
        var inspection = await driver.InspectNodeEndpointAsync(cluster, node, ct);
        if (!inspection.IsSuccess)
            return Result<int>.Failed(inspection.Error!); // слепота — не лечим
        if (inspection.Value is not { } found)
            return Result<int>.Failed(new ApplicationException(
                $"healing-portalloc {cluster}/{node}: контейнера нет — порт не выдумываем (S7)"));

        // Реконструированный порт обязан лежать в portalloc-диапазоне конфигурации:
        // вне его — мусорный inspect, запись сломала бы аллокатор.
        if (found.ClientHostPort < options.PortRangeFrom || found.ClientHostPort >= options.PortRangeTo)
            return Result<int>.Failed(new ApplicationException(
                $"healing-portalloc {cluster}/{node}: published-порт {found.ClientHostPort} вне диапазона {options.PortRangeFrom}–{options.PortRangeTo}"));

        // journal-before-manipulations: фаза ДО первого txn.
        var started = await journal.WritePhaseAsync(cluster, Op, "started", claims.InstanceId, null, ct);
        if (!started.IsSuccess)
            return Result<int>.Failed(started.Error!);

        var acquired = await portLock.TryAcquireAsync(ct);
        if (!acquired.IsSuccess)
            return Result<int>.Failed(acquired.Error!);
        if (!acquired.Value)
            return Result<int>.Failed(new PortLockBusyException(portLock.Key));
        try
        {
            var key = ProcessCommon.PortAllocKey(cluster);
            var merged = new Dictionary<string, NodeAddress>(existing.Value)
            {
                [node] = new(found.Host, found.ClientHostPort),
            };
            var txn = await TxnAsync(
                TxnRequest.Of([TxnCompare.NotExists(key)], [new TxnOp.Put(key, SerializePortAlloc(merged), null)]), ct);
            if (!txn.IsSuccess)
                return Result<int>.Failed(txn.Error!);
            if (!txn.Value.Succeeded)
            {
                // уже записал сосед — читаем его истину (S5).
                var reread = await ReadPortAllocAsync(cluster, ct);
                if (!reread.IsSuccess)
                    return Result<int>.Failed(reread.Error!);
                if (!reread.Value.TryGetValue(node, out var foreign))
                    return Result<int>.Failed(new ApplicationException(
                        $"healing-portalloc {cluster}/{node}: проигранный txn без записи соседа"));
                return Result<int>.Success(foreign.ClientPort);
            }
        }
        finally
        {
            await portLock.ReleaseAsync();
        }

        var done = await journal.WritePhaseAsync(cluster, Op, "reconstructed", claims.InstanceId, null, ct);
        if (!done.IsSuccess)
            return Result<int>.Failed(done.Error!);

        // Диагностика коллизии: реконструированный порт в portalloc чужого
        // кластера — warning в журнал (не ошибка: docker-публикация — факт).
        var foreignBusy = await index.ForeignAllocatedPortsAsync(cluster, ct);
        if (foreignBusy.IsSuccess && foreignBusy.Value.Contains(found.ClientHostPort))
        {
            await journal.WritePhaseAsync(cluster, Op, "reconstructed-warning", claims.InstanceId,
                $"порт {found.ClientHostPort} уже в portalloc чужого кластера", ct);
        }

        return Result<int>.Success(found.ClientHostPort);
    }

    private async Task<Result<IReadOnlyDictionary<string, NodeAddress>>> ReadPortAllocAsync(
        string cluster, CancellationToken ct)
    {
        Result<Kv?>? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.GetAsync(endpoint, ProcessCommon.PortAllocKey(cluster), ct);
            if (!result.IsSuccess)
            {
                last = result;
                continue;
            }

            if (result.Value is not { } kv)
                return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(
                    (IReadOnlyDictionary<string, NodeAddress>)new Dictionary<string, NodeAddress>());
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(ParsePortAlloc(kv.Value));
        }

        return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(last!.Error!);
    }

    // Формат arch/20 §3: {"node<k>":{"host":"h","client":17001}}.
    private static Dictionary<string, NodeAddress> ParsePortAlloc(string json)
    {
        var addresses = new Dictionary<string, NodeAddress>();
        using var doc = JsonDocument.Parse(json);
        foreach (var node in doc.RootElement.EnumerateObject())
            addresses[node.Name] = new NodeAddress(
                node.Value.GetProperty("host").GetString()!,
                node.Value.GetProperty("client").GetInt32());
        return addresses;
    }

    private static string SerializePortAlloc(IReadOnlyDictionary<string, NodeAddress> addresses)
    {
        using var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            foreach (var (node, addr) in addresses.OrderBy(p => p.Key, StringComparer.Ordinal))
            {
                writer.WritePropertyName(node);
                writer.WriteStartObject();
                writer.WriteString("host", addr.Host);
                writer.WriteNumber("client", addr.ClientPort);
                writer.WriteEndObject();
            }

            writer.WriteEndObject();
        }

        return System.Text.Encoding.UTF8.GetString(buffer.ToArray());
    }

    private async Task<Result<TxnResult>> TxnAsync(TxnRequest req, CancellationToken ct)
    {
        Result<TxnResult>? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.TxnAsync(endpoint, req, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }
}
