using Microsoft.Extensions.Logging;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Etcd.Client;

namespace PgWorker.Provisioning.Endpoints;

/// <summary>
/// Индекс занятости портов из etcd (spec §3.3): busy = docker-публикации ∪ записи
/// portalloc ВСЕХ кластеров. Свои записи исключает вызывающий (exceptCluster) —
/// свой portalloc переиспользуется аллокатором как закрепление, а не занятость.
/// Битый JSON соседа — Warning-лог + skip ключа: чужой мусор не роняет наш provision.
/// </summary>
public sealed class PortAllocIndex(
    IEtcdGateway etcd, string[] endpoints, ILogger<PortAllocIndex> logger)
{
    private const string Prefix = "/pgworker/portalloc/";

    /// <summary>Все три порта каждой записи каждого ЧУЖОГО /pgworker/portalloc/&lt;C&gt;.</summary>
    public async Task<Result<IReadOnlySet<(string Host, int Port)>>> ReadBusyAsync(
        string exceptCluster, CancellationToken ct)
    {
        var range = await WithFailoverAsync(endpoint => etcd.RangeAsync(endpoint, Prefix, ct));
        if (!range.IsSuccess)
            return Result<IReadOnlySet<(string Host, int Port)>>.Failed(range.Error!);

        var busy = new HashSet<(string, int)>();
        foreach (var kv in range.Value)
        {
            var cluster = kv.Key.Split('/')[^1];
            if (cluster == exceptCluster)
                continue;

            var parsed = Portalloc.Parse(cluster, kv.Value);
            if (!parsed.IsSuccess)
            {
                // Не наш ключ — не наша ответственность: лог + skip (spec §2.3-принцип).
                logger.LogWarning("битый portalloc соседа {Cluster}: {Error}", cluster, parsed.Error!.Message);
                continue;
            }

            foreach (var addr in parsed.Value.Values)
            {
                busy.Add((addr.Host, addr.Ports.Pg));
                if (addr.Ports.Patroni > 0)
                    busy.Add((addr.Host, addr.Ports.Patroni));
                if (addr.Ports.Doorman > 0)
                    busy.Add((addr.Host, addr.Ports.Doorman));
            }
        }

        return Result<IReadOnlySet<(string Host, int Port)>>.Success(busy);
    }

    // Failover-обёртка: первый успешный endpoint выигрывает (паттерн процессов).
    private async Task<Result<T>> WithFailoverAsync<T>(Func<string, Task<Result<T>>> call)
    {
        Result<T>? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await call(endpoint);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }
}
