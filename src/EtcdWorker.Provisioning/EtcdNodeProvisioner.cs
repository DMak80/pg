using EtcdWorker.Docker.Drivers;

namespace EtcdWorker.Provisioning;

/// <summary>Провижининг managed-узла etcd (arch/25 §3.1): сеть контура →
/// контейнер/сервис драйвером (volume идемпотентно, host-публикация только
/// client-порта) → health-поллинг клиентской грани
/// (http://&lt;advertised|localhost&gt;:&lt;client-host-port&gt;) до бюджета NodeBootSec.
/// Идемпотентен: каждый шаг перепроверяет факт (Ensure* драйвера — no-op при
/// наличии).</summary>
public sealed class EtcdNodeProvisioner(
    IEtcdNodeDriver driver,
    Shared.Etcd.Client.IEtcdGateway etcd,
    int nodeBootSec)
{
    public async Task<Result> EnsureNodeAsync(EtcdNodeSpec spec, string? advertisedHost, CancellationToken ct)
    {
        var network = await driver.EnsureNetworkAsync(ct);
        if (!network.IsSuccess)
            return network;
        var node = await driver.EnsureNodeAsync(spec, ct);
        if (!node.IsSuccess)
            return node;

        // Health-поллинг клиентской грани: client-порт опубликован на
        // docker-хосте размещения (Plain — localhost воркера).
        var probeHost = advertisedHost is { Length: > 0 } host ? host : "localhost";
        var probeEndpoint = $"http://{probeHost}:{spec.ClientHostPort}";
        var deadline = DateTimeOffset.UtcNow.AddSeconds(Math.Max(1, nodeBootSec));
        Result last = Result.Success();
        while (DateTimeOffset.UtcNow < deadline)
        {
            var status = await etcd.StatusAsync(probeEndpoint, ct);
            if (status.IsSuccess)
                return Result.Success();
            last = Result.Failed(status.Error!);
            await Task.Delay(1000, ct);
        }

        return Result.Failed(new ApplicationException(
            $"etcdw-node-{spec.K}: client endpoint {probeEndpoint} did not come up within {nodeBootSec} s: {last.Error?.Message}"));
    }
}
