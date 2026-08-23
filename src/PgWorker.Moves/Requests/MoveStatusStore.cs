using PgWorker.Core;
using PgWorker.Etcd.Client;

namespace PgWorker.Moves;

/// <summary>
/// Статус-ключ переезда /clusters/&lt;C&gt;/buckets/status/bucket_&lt;i&gt; (формат скриптов,
/// spec §4.2, Д6) + атомарный flip-txn: compare routing=cur → put routing=new +
/// delete status (эквивалент etcd_flip скриптов, arch/11 §5). Failover по
/// endpoints — паттерн WorkJournal.WithFailoverAsync.
/// </summary>
public sealed class MoveStatusStore(IEtcdGateway gateway, string[] endpoints)
{
    /// <summary>Чтение статуса; null = ключа нет = бакет ACTIVE.</summary>
    public async Task<Result<MoveStatus?>> GetAsync(string cluster, string bucket, CancellationToken ct)
    {
        var result = await WithFailoverAsync(
            endpoint => gateway.GetAsync(endpoint, MoveNames.StatusKey(cluster, bucket), ct));
        if (!result.IsSuccess)
            return Result<MoveStatus?>.Failed(result.Error!);

        if (result.Value is not { } kv)
            return Result<MoveStatus?>.Success(null); // ACTIVE

        var parsed = MoveStatus.Parse(kv.Value);
        return parsed.IsSuccess
            ? Result<MoveStatus?>.Success(parsed.Value)
            : Result<MoveStatus?>.Failed(parsed.Error!);
    }

    /// <summary>Запись статуса (фаза переезда; updated_unix проставляет вызывающий).</summary>
    public Task<Result> PutAsync(string cluster, MoveStatus status, CancellationToken ct)
        => WithFailoverAsync(endpoint => gateway.PutAsync(
            endpoint, MoveNames.StatusKey(cluster, status.Bucket), status.Serialize(), lease: null, ct));

    /// <summary>
    /// Атомарный flip: txn [ValueEqual(routing, current)] → [Put(routing, next), Delete(status)].
    /// false = compare не сошёлся (routing изменился под руками — заморозка остаётся, разбор вручную).
    /// </summary>
    public async Task<Result<bool>> FlipAsync(
        string cluster, string bucket, string current, string next, CancellationToken ct)
    {
        var txn = await WithFailoverAsync(endpoint => gateway.TxnAsync(endpoint, TxnRequest.Of(
            [TxnCompare.ValueEqual(MoveNames.RoutingKey(cluster, bucket), current)],
            [
                new TxnOp.Put(MoveNames.RoutingKey(cluster, bucket), next, null),
                new TxnOp.Delete(MoveNames.StatusKey(cluster, bucket), Prefix: false),
            ]), ct));
        if (!txn.IsSuccess)
            return Result<bool>.Failed(txn.Error!);

        return Result<bool>.Success(txn.Value.Succeeded);
    }

    /// <summary>Удаление статус-ключа (rollback-семантика «нет ключа = ACTIVE»).</summary>
    public Task<Result> DeleteAsync(string cluster, string bucket, CancellationToken ct)
        => WithFailoverAsync(endpoint => gateway.DeleteAsync(
            endpoint, MoveNames.StatusKey(cluster, bucket), prefix: false, ct));

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
