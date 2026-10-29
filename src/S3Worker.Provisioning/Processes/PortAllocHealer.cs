using S3Worker.Core.Model;
using S3Worker.Docker.Drivers;
using Shared.Core;
using Shared.Etcd.Client;
using Shared.Etcd.Coordination;

namespace S3Worker.Provisioning.Processes;

// Фасад общего лестничного хилера portalloc (Shared.Etcd, arch/17 E9) над
// доменным драйвером/индексом: успешный результат — адрес ноды (host, порт).
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
    private readonly EtcdPortAllocHealer inner = new(
        etcd, endpoints, claims, journal, portLock,
        new PortAllocHealerDeps
        {
            PortAllocKey = S3ProcessCommon.PortAllocKey,
            InspectNode = async (cluster, node, ct) =>
            {
                var inspection = await driver.InspectNodeAsync(cluster, node, ct);
                if (!inspection.IsSuccess)
                    return Result<PortAllocNodeAddress?>.Failed(inspection.Error!);
                return inspection.Value is { } found
                    ? new PortAllocNodeAddress(found.Host, found.S3HostPort)
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
            ParsePortAlloc = json => S3ProcessCommon.ParsePortAlloc(json)
                .ToDictionary(p => p.Key, p => new PortAllocNodeAddress(p.Value.Host, p.Value.S3Port)),
            SerializePortAlloc = dict => S3ProcessCommon.SerializePortAlloc(
                dict.ToDictionary(p => p.Key, p => new NodeAddress(p.Value.Host, p.Value.Port))),
        });

    // Восстановление адреса ОДНОЙ ноды: запись portalloc есть → её адрес
    // (реконструкция не нужна); нет — инспекция живого контейнера →
    // put-if-absent под глобальным клэймом. Успех: фактический адрес ноды.
    public async Task<Result<NodeAddress>> HealNodeAsync(string cluster, string node, CancellationToken ct)
    {
        var healed = await inner.HealNodeAsync(cluster, node, ct);
        return healed.IsSuccess
            ? Result<NodeAddress>.Success(new NodeAddress(healed.Value.Host, healed.Value.Port))
            : Result<NodeAddress>.Failed(healed.Error!);
    }
}
