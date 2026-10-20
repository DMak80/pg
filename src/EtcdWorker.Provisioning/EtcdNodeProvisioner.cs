using EtcdWorker.Docker.Drivers;

namespace EtcdWorker.Provisioning;

/// <summary>Провижининг managed-узла etcd (arch/25 §3.1): сеть контура →
/// контейнер/сервис драйвером (volume идемпотентно, host-публикация только
/// client-порта) → health-поллинг клиентской грани (хост размещения
/// portalloc, см. ProbeHost) до бюджета NodeBootSec с прогресс-отметками
/// итераций (ILoopProgress — watchdog не сносит reconcile в законном
/// ожидании загрузки, arch/25 §6). Идемпотентен: каждый шаг перепроверяет
/// факт (Ensure* драйвера — no-op при наличии).</summary>
public sealed class EtcdNodeProvisioner(
    IEtcdNodeDriver driver,
    Shared.Etcd.Client.IEtcdGateway etcd,
    int nodeBootSec,
    Shared.Core.Hosting.ILoopProgress? progress = null)
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
        // docker-хосте размещения (spec.Host — host из portalloc; пустой и
        // стендовый "local" — localhost).
        var probeEndpoint = $"http://{ProbeHost(spec.Host)}:{spec.ClientHostPort}";
        var deadline = DateTimeOffset.UtcNow.AddSeconds(Math.Max(1, nodeBootSec));
        Result last = Result.Success();
        while (DateTimeOffset.UtcNow < deadline)
        {
            var status = await etcd.StatusAsync(probeEndpoint, ct);
            progress?.Mark(); // heartbeat: исполненная итерация health-ожидания (arch/25 §6)
            if (status.IsSuccess)
                return Result.Success();
            last = Result.Failed(status.Error!);
            await Task.Delay(1000, ct);
        }

        return Result.Failed(new ApplicationException(
            $"etcdw-node-{spec.K}: client endpoint {probeEndpoint} did not come up within {nodeBootSec} s: {last.Error?.Message}"));
    }

    // Хост пробы клиентской грани — host размещения узла из portalloc
    // (arch/25 §3.2; мультихост-канон: проба адресует хост размещения, а не
    // окружение самого воркера). Пустой хост и стендовое имя "local" —
    // локальный docker-демон → localhost.
    private static string ProbeHost(string? portAllocHost)
        => portAllocHost is { Length: > 0 } host && !string.Equals(host, "local", StringComparison.Ordinal)
            ? host
            : "localhost";
}
