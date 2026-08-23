using PgWorker.Core;
using PgWorker.Etcd.Client;

namespace PgWorker.Moves;

/// <summary>
/// Заявки на переезды кластера — чтение/удаление ключей /pgworker/moves/&lt;C&gt;/bucket_&lt;i&gt;
/// (spec §4.1, arch/14 §3.3). Успех или перманентный отказ — заявку удаляет процесс;
/// одновременно обрабатывается старейшая заявка кластера (Д2).
/// Failover по endpoints — паттерн WorkJournal.WithFailoverAsync.
/// </summary>
public sealed class MoveRequestsStore(IEtcdGateway gateway, string[] endpoints)
{
    /// <summary>Все заявки кластера; битые ключи пропускаются (ошибки — в errors, их залогирует процесс).</summary>
    public async Task<Result<IReadOnlyList<(string Bucket, MoveRequest Request)>>> ListAsync(
        string cluster, CancellationToken ct)
    {
        var range = await WithFailoverAsync(endpoint => gateway.RangeAsync(endpoint, MoveNames.MovesPrefix(cluster), ct));
        if (!range.IsSuccess)
            return Result<IReadOnlyList<(string Bucket, MoveRequest Request)>>.Failed(range.Error!);

        return ParseRange(range.Value, out _);
    }

    /// <summary>Старейшая заявка кластера по requested_unix (tie-break — лексикографика ключа, детерминизм); null = заявок нет.</summary>
    public async Task<Result<(string Bucket, MoveRequest Request)?>> OldestAsync(
        string cluster, CancellationToken ct)
    {
        var list = await ListAsync(cluster, ct);
        if (!list.IsSuccess)
            return Result<(string Bucket, MoveRequest Request)?>.Failed(list.Error!);

        var oldest = list.Value
            .OrderBy(r => r.Request.RequestedUnix)
            .ThenBy(r => MoveNames.MoveKey(cluster, r.Bucket), StringComparer.Ordinal)
            .FirstOrDefault();

        return oldest == default
            ? Result<(string Bucket, MoveRequest Request)?>.Success(null)
            : Result<(string Bucket, MoveRequest Request)?>.Success((oldest.Bucket, oldest.Request));
    }

    /// <summary>Удаление заявки по завершении (успех/перманентный отказ, spec §4.1).</summary>
    public Task<Result> DeleteAsync(string cluster, string bucket, CancellationToken ct)
        => WithFailoverAsync(endpoint => gateway.DeleteAsync(endpoint, MoveNames.MoveKey(cluster, bucket), prefix: false, ct));

    // Range по префиксу кластера → (bucket, заявка); битый JSON — не ошибка тика (образец
    // ClusterSnapshotParser.ParseClusters: пропуск + запись в errors).
    internal static Result<IReadOnlyList<(string Bucket, MoveRequest Request)>> ParseRange(
        IReadOnlyList<Kv> kvs, out IReadOnlyList<string> errors)
    {
        var parseErrors = new List<string>();
        var requests = new List<(string Bucket, MoveRequest Request)>();

        foreach (var kv in kvs)
        {
            var bucket = kv.Key.Split('/')[^1]; // "/pgworker/moves/<C>/<bucket>" → leaf
            var parsed = MoveRequest.Parse(bucket, kv.Value);
            if (!parsed.IsSuccess)
            {
                parseErrors.Add($"заявка {kv.Key}: {parsed.Error!.Message}");
                continue;
            }

            requests.Add((bucket, parsed.Value));
        }

        errors = parseErrors;
        return requests;
    }

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

    private async Task<Result> WithFailoverAsync(Func<string, Task<Result>> call)
    {
        Result? last = null;
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
