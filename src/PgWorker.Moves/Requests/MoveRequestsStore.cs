using PgWorker.Core;
using PgWorker.Etcd.Client;

namespace PgWorker.Moves;

/// <summary>Листинг заявок кластера: распарсенные + ошибки битых ключей (их
/// залогирует процесс — «исправь или удали ключ», план Task 3 Step 3, ревью №2).</summary>
public sealed record MoveRequestsListing(
    IReadOnlyList<(string Bucket, MoveRequest Request)> Requests,
    IReadOnlyList<string> ParseErrors);

/// <summary>Старейшая заявка кластера (null Request = заявок нет) + ошибки битых ключей.</summary>
public sealed record OldestMoveRequest(
    (string Bucket, MoveRequest Request)? Request,
    IReadOnlyList<string> ParseErrors);

/// <summary>
/// Заявки на переезды кластера — чтение/удаление ключей /pgworker/moves/&lt;C&gt;/bucket_&lt;i&gt;
/// (spec §4.1, arch/14 §3.3). Успех или перманентный отказ — заявку удаляет процесс;
/// одновременно обрабатывается старейшая заявка кластера (Д2).
/// Failover по endpoints — паттерн WorkJournal.WithFailoverAsync.
/// </summary>
public sealed class MoveRequestsStore(IEtcdGateway gateway, string[] endpoints)
{
    /// <summary>Все заявки кластера; битые ключи пропускаются в Requests, их причины —
    /// в ParseErrors (процесс залогирует: «исправь или удали ключ», ревью №2).</summary>
    public async Task<Result<MoveRequestsListing>> ListAsync(
        string cluster, CancellationToken ct)
    {
        var range = await WithFailoverAsync(endpoint => gateway.RangeAsync(endpoint, MoveNames.MovesPrefix(cluster), ct));
        if (!range.IsSuccess)
            return Result<MoveRequestsListing>.Failed(range.Error!);

        var requests = ParseRange(range.Value, out var errors);
        return Result<MoveRequestsListing>.Success(new MoveRequestsListing(requests.Value, errors));
    }

    /// <summary>Старейшая заявка кластера по requested_unix (tie-break — лексикографика ключа, детерминизм);
    /// null = заявок нет. Ошибки битых ключей — рядом (ревью №2: логирует процесс).</summary>
    public async Task<Result<OldestMoveRequest>> OldestAsync(
        string cluster, CancellationToken ct)
    {
        var list = await ListAsync(cluster, ct);
        if (!list.IsSuccess)
            return Result<OldestMoveRequest>.Failed(list.Error!);

        var oldest = list.Value.Requests
            .OrderBy(r => r.Request.RequestedUnix)
            .ThenBy(r => MoveNames.MoveKey(cluster, r.Bucket), StringComparer.Ordinal)
            .Cast<(string Bucket, MoveRequest Request)?>()
            .FirstOrDefault();

        return Result<OldestMoveRequest>.Success(new OldestMoveRequest(oldest, list.Value.ParseErrors));
    }

    /// <summary>Удаление заявки по завершении (успех/перманентный отказ, spec §4.1).</summary>
    public Task<Result> DeleteAsync(string cluster, string bucket, CancellationToken ct)
        => WithFailoverAsync(endpoint => gateway.DeleteAsync(endpoint, MoveNames.MoveKey(cluster, bucket), prefix: false, ct));

    /// <summary>Запись/замена заявки (auto-finalize после успешного move).</summary>
    public Task<Result> PutAsync(string cluster, string bucket, MoveRequest request, CancellationToken ct)
        => WithFailoverAsync(endpoint => gateway.PutAsync(
            endpoint, MoveNames.MoveKey(cluster, bucket), request.Serialize(), null, ct));

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
