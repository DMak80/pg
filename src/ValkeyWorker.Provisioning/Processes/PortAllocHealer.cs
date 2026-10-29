using Shared.Core;
using Shared.Etcd.Client;
using Shared.Etcd.Coordination;
using ValkeyWorker.Core.Model;
using ValkeyWorker.Docker.Drivers;

namespace ValkeyWorker.Provisioning.Processes;

// Фасад общего лестничного хилера portalloc (Shared.Etcd, arch/17 E9) над
// доменным драйвером/индексом: успешный результат — клиентский порт ноды.
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
    private readonly EtcdPortAllocHealer inner = new(
        etcd, endpoints, claims, journal, portLock,
        new PortAllocHealerDeps
        {
            PortAllocKey = ProcessCommon.PortAllocKey,
            InspectNode = async (cluster, node, ct) =>
            {
                var inspection = await driver.InspectNodeEndpointAsync(cluster, node, ct);
                if (!inspection.IsSuccess)
                    return Result<PortAllocNodeAddress?>.Failed(inspection.Error!);
                return inspection.Value is { } found
                    ? new PortAllocNodeAddress(found.Host, found.ClientHostPort)
                    : null;
            },
            ForeignAllocatedPorts = async (cluster, ct) =>
            {
                var busy = await index.ForeignAllocatedPortsAsync(cluster, ct);
                return busy.IsSuccess
                    ? Result<IReadOnlyCollection<int>>.Success(busy.Value)
                    : Result<IReadOnlyCollection<int>>.Failed(busy.Error!);
            },
            PortRangeFrom = options.PortRangeFrom,
            PortRangeTo = options.PortRangeTo,
            ParsePortAlloc = json => ProcessCommon.ParsePortAlloc(json)
                .ToDictionary(p => p.Key, p => new PortAllocNodeAddress(p.Value.Host, p.Value.ClientPort)),
            SerializePortAlloc = dict => ProcessCommon.SerializePortAlloc(
                dict.ToDictionary(p => p.Key, p => new NodeAddress(p.Value.Host, p.Value.Port))),
        });

    // Восстановление порта ОДНОЙ ноды (arch/21 §5 C): запись portalloc есть →
    // её порт (реконструкция не нужна); нет — инспекция живого контейнера →
    // put-if-absent под глобальным клэймом. Успех: фактический порт ноды.
    public async Task<Result<int>> HealNodePortAsync(string cluster, string node, CancellationToken ct)
    {
        var healed = await inner.HealNodeAsync(cluster, node, ct);
        return healed.IsSuccess
            ? Result<int>.Success(healed.Value.Port)
            : Result<int>.Failed(healed.Error!);
    }
}
