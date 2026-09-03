using System.Text.Json;
using KafkaWorker.Core;
using KafkaWorker.Etcd.Client;
using Microsoft.Extensions.Logging;

namespace KafkaWorker.Provisioning.Processes;

/// <summary>
/// Индекс занятости портов из etcd (t91, arch/16 §2.1): busy = docker-публикации
/// (добавляет вызывающий) ∪ записи portalloc ВСЕХ чужих кластеров. Свои записи
/// исключает вызывающий (exceptCluster) — свой portalloc переиспользуется
/// аллокатором как закрепление, а не занятость. Чужой мусор любой формы — битый
/// JSON ИЛИ валидный JSON без обязательных полей host/client — Warning-лог + skip
/// ключа: чужой мусор не роняет наш provision (порт PortAllocIndex PgWorker,
/// spec §3.2/§6).
/// </summary>
public sealed class PortAllocIndex(
    IEtcdGateway etcd, string[] endpoints, ILogger<PortAllocIndex> logger)
{
    private const string Prefix = "/kafkaworker/portalloc/";

    /// <summary>Клиентский порт каждой записи каждого ЧУЖОГО /kafkaworker/portalloc/&lt;C&gt;.</summary>
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

            // Формат arch/15 §4: {"broker<k>":{"host":"h","client":16001}}.
            // Фильтр catch — все формы чужого мусора: JsonException (битый JSON),
            // KeyNotFoundException (нет обязательного поля), InvalidOperationException
            // (поле не того типа — GetString/GetInt32 на несоответствующем узле).
            try
            {
                using var doc = JsonDocument.Parse(kv.Value);
                foreach (var node in doc.RootElement.EnumerateObject())
                    busy.Add((node.Value.GetProperty("host").GetString()!,
                        node.Value.GetProperty("client").GetInt32()));
            }
            catch (Exception ex) when (ex is JsonException or KeyNotFoundException or InvalidOperationException)
            {
                // Не наш ключ — не наша ответственность: лог + skip.
                logger.LogWarning("битый portalloc соседа {Cluster}: {Error}", cluster, ex.Message);
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
