using System.Text.Json;
using Microsoft.Extensions.Logging;
using Shared.Core;
using Shared.Etcd.Client;

namespace PgWorker.Etcd.S3;

/// <summary>Чтение домена /s3/ : интерфейс для резолвера/фабрики —
/// тестовые фейки не тянут etcd-транспорт.</summary>
public interface IS3PoolStore
{
    Task<Result<S3Pool?>> ReadAsync(string pool, CancellationToken ct);

    Task<Result<IReadOnlyList<string>>> ListPoolNamesAsync(CancellationToken ct);
}

/// <summary>
/// Чтение домена /s3/ воркером (arch/23 §7): PgWorker — ЧИТАТЕЛЬ,
/// участником контракта не становится. ReadAsync собирает пул из config,
/// ключей заявки нод (nodes/&lt;I&gt;/state|placement — ИСТОЧНИК ИМЁН),
/// placed_on и portalloc (ИСТОЧНИК АДРЕСОВ); Failover — локальный цикл по
/// endpoints (паттерн WalStreamProcess/PortAllocIndex — библиотека без DI).
/// </summary>
public sealed class S3PoolStore(IEtcdGateway gateway, string[] endpoints, ILogger<S3PoolStore>? logger = null) : IS3PoolStore
{
    private const string ClustersPrefix = "/s3/clusters/";
    private const string PortAllocPrefix = "/s3worker/portalloc/";

    /// <summary>Пул по имени: null — config отсутствует (пула нет; Failed —
    /// недоступность etcd). Ноды — из ключей заявки; нода без portalloc-записи
    /// остаётся в Nodes с Host=null (заявлена, не провижинена — arch/19 §4).</summary>
    public async Task<Result<S3Pool?>> ReadAsync(string pool, CancellationToken ct)
    {
        var config = await RangeSingleAsync($"{ClustersPrefix}{pool}/config", ct);
        if (!config.IsSuccess)
            return Result<S3Pool?>.Failed(config.Error!);

        var nodes = await RangeAsync($"{ClustersPrefix}{pool}/nodes/", ct);
        if (!nodes.IsSuccess)
            return Result<S3Pool?>.Failed(nodes.Error!);

        var endpointsKv = await RangeSingleAsync($"{ClustersPrefix}{pool}/endpoints", ct);
        if (!endpointsKv.IsSuccess)
            return Result<S3Pool?>.Failed(endpointsKv.Error!);

        var portAlloc = await RangeSingleAsync($"{PortAllocPrefix}{pool}", ct);
        if (!portAlloc.IsSuccess)
            return Result<S3Pool?>.Failed(portAlloc.Error!);

        var secretsKv = await RangeSingleAsync($"{ClustersPrefix}{pool}/secrets", ct);
        if (!secretsKv.IsSuccess)
            return Result<S3Pool?>.Failed(secretsKv.Error!);

        // Битый config — parseError-путь: пул не читается (null) с диагностикой.
        var cfg = config.Value is null
            ? null
            : S3PoolModel.ParseConfig(config.Value.Value);
        if (config.Value is not null && cfg is null)
            logger?.LogWarning("broken config of pool {Pool} — pool skipped", pool);
        if (config.Value is null || cfg is null)
            return Result<S3Pool?>.Success((S3Pool?)null);

        var kvs = nodes.Value;
        var stateByNode = LeafValue(kvs, pool, "state");
        var placedOnByNode = LeafValue(kvs, pool, "placed_on");
        var names = S3PoolModel.InstanceNames(kvs);
        var alloc = portAlloc.Value is null
            ? new Dictionary<string, (string, int)>()
            : S3PoolModel.ParsePortAlloc(portAlloc.Value.Value);

        var poolNodes = new List<S3PoolNode>(names.Count);
        foreach (var name in names)
        {
            // Адрес — из portalloc-записи той же ноды; нет записи → null-адрес
            // (нода заявлена, но не провижинена — это НЕ инвалидность пула).
            // TryGetValue у словаря даёт default-кортеж — null-семантику
            // выставляем явно, чтобы не подставлять ""/0 как «адрес».
            (string? Host, int? S3Port) addr = alloc.TryGetValue(name, out var a)
                ? (a.Host, a.Port)
                : (null, null);
            stateByNode.TryGetValue(name, out var state);
            placedOnByNode.TryGetValue(name, out var placedOn);
            poolNodes.Add(new S3PoolNode(name, state, placedOn, addr.Host, addr.S3Port));
        }

        var endpointList = endpointsKv.Value is null
            ? []
            : S3PoolModel.ParseEndpoints(endpointsKv.Value.Value);
        return Result<S3Pool?>.Success(
            new S3Pool(pool, cfg.ToRemove, poolNodes, endpointList,
                secretsKv.Value is null ? null : S3PoolModel.ParseSecrets(secretsKv.Value.Value)));
    }

    /// <summary>Имена всех пулов — уникальные &lt;C&gt; из config-ключей
    /// /s3/clusters/&lt;C&gt;/config.</summary>
    public async Task<Result<IReadOnlyList<string>>> ListPoolNamesAsync(CancellationToken ct)
    {
        var range = await RangeAsync(ClustersPrefix, ct);
        if (!range.IsSuccess)
            return Result<IReadOnlyList<string>>.Failed(range.Error!);

        var names = new SortedSet<string>(StringComparer.Ordinal);
        foreach (var kv in range.Value)
        {
            // /s3/clusters/<C>/config (5 сегментов) — маркер существования пула.
            var parts = kv.Key.Split('/');
            if (parts.Length == 5 && parts[1] == "s3" && parts[2] == "clusters" && parts[4] == "config")
                names.Add(parts[3]);
        }

        return Result<IReadOnlyList<string>>.Success([.. names]);
    }

        // Значения листьев нод: nodes/<I>/<leaf> → value (для state/placed_on);
        // значения — JSON-строки etcd ("RUNNING") → распаковка к тексту.
    private static Dictionary<string, string> LeafValue(IReadOnlyList<Kv> kvs, string pool, string leaf)
    {
        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var kv in kvs)
        {
            var parts = kv.Key.Split('/');
            if (parts.Length == 7
                && parts[1] == "s3" && parts[2] == "clusters"
                && parts[3] == pool && parts[4] == "nodes" && parts[6] == leaf)
            {
                // JSON-строка → текст; не-строка/битое — как есть (толерантность).
                var raw = kv.Value;
                var text = raw.Length > 1 && raw[0] == '"'
                    ? TryUnquote(raw) ?? raw
                    : raw;
                map[parts[5]] = text;
            }
        }

        return map;
    }

    // Распаковка JSON-строки ("RUNNING" → RUNNING); битая — null.
    private static string? TryUnquote(string raw)
    {
        try
        {
            return JsonSerializer.Deserialize<string>(raw);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    // Префиксный range с failover (паттерн процессов; библиотека — без DI).
    private async Task<Result<IReadOnlyList<Kv>>> RangeAsync(string prefix, CancellationToken ct)
    {
        Result<IReadOnlyList<Kv>> last = default;
        foreach (var endpoint in endpoints)
        {
            var result = await gateway.RangeAsync(endpoint, prefix, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return Result<IReadOnlyList<Kv>>.Failed(
            last.Error ?? new EtcdUnreachableException("etcd endpoints not set"));
    }

    // Точечное чтение через RangeAsync (ключ известен по схеме; лишние значения
    // префикса отфильтрованы вызывающим — префикс уже точечный по построению).
    private async Task<Result<Kv?>> RangeSingleAsync(string key, CancellationToken ct)
    {
        var range = await RangeAsync(key, ct);
        return range.Bind(kvs => Result<Kv?>.Success(kvs.Count > 0 ? kvs[0] : null));
    }
}
