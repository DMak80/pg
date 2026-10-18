using System.Text.Json;
using Microsoft.Extensions.Logging;
using Shared.Core;
using Shared.Etcd.Client;

namespace S3Worker.Provisioning.Processes;

/// <summary>
/// Индекс занятости портов из etcd (arch/23 §8; порт PortAllocIndex valkey):
/// busy = docker-публикации (добавляет вызывающий) ∪ записи portalloc ВСЕХ
/// чужих кластеров (поле "s3"). Свои записи исключает вызывающий
/// (exceptCluster). Чужой мусор любой формы — Warning-лог + skip ключа:
/// чужой мусор не роняет наш provision.
/// </summary>
public sealed class PortAllocIndex(
    IEtcdGateway etcd, string[] endpoints, ILogger<PortAllocIndex> logger)
{
    private const string Prefix = "/s3worker/portalloc/";

    /// <summary>S3-порт каждой записи каждого ЧУЖОГО /s3worker/portalloc/&lt;C&gt;.</summary>
    public async Task<Result<IReadOnlySet<int>>> ForeignAllocatedPortsAsync(
        string exceptCluster, CancellationToken ct)
    {
        var range = await WithFailoverAsync(endpoint => etcd.RangeAsync(endpoint, Prefix, ct));
        if (!range.IsSuccess)
            return Result<IReadOnlySet<int>>.Failed(range.Error!);

        var busy = new HashSet<int>();
        foreach (var kv in range.Value)
        {
            var cluster = kv.Key.Split('/')[^1];
            if (cluster == exceptCluster)
                continue;

            // Формат arch/23 §8: {"<I>":{"host":"h","s3":18001}}.
            // Фильтр catch — все формы чужого мусора: JsonException (битый JSON),
            // KeyNotFoundException (нет обязательного поля), InvalidOperationException
            // (поле не того типа).
            try
            {
                using var doc = JsonDocument.Parse(kv.Value);
                foreach (var node in doc.RootElement.EnumerateObject())
                    busy.Add(node.Value.GetProperty("s3").GetInt32());
            }
            catch (Exception ex) when (ex is JsonException or KeyNotFoundException or InvalidOperationException)
            {
                // Не наш ключ — не наша ответственность: лог + skip.
                logger.LogWarning("corrupt portalloc of neighbor {Cluster}: {Error}", cluster, ex.Message);
            }
        }

        return Result<IReadOnlySet<int>>.Success(busy);
    }

    // Failover-обёртка: первый успешный endpoint выигрывает (паттерн процессов).
    private async Task<Result<T>> WithFailoverAsync<T>(Func<string, Task<Result<T>>> call)
    {
        Result<T> last = default;
        foreach (var endpoint in endpoints)
        {
            var result = await call(endpoint);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last.Error ?? new EtcdUnreachableException("etcd endpoints are not set");
    }
}
