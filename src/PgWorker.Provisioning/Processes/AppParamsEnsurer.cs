using PgWorker.Core;
using PgWorker.Etcd.Client;

namespace PgWorker.Provisioning.Processes;

/// <summary>
/// Ensure per-node app_params (spec §4.2, arch/14 §5 P2.5'): ключ
/// /clusters/&lt;C&gt;/shards/&lt;X&gt;/nodes/&lt;n&gt;/app_params — put-if-absent ОДНОЙ txn
/// [NotExists]+[put] с дефолтом PgWorker:AppParams:Default. Проигрыш compare —
/// законный исход (ключ есть: ручные правки оператора живы). Txn —
/// с failover по endpoints до первого живого (паттерн AppSecretEnsurer).
/// Вызывается только держателем клэйма &lt;C&gt; (инвариант мутаций /clusters/).
/// </summary>
public interface IAppParamsEnsurer
{
    /// Ensure per-node app_params (spec §4.2): put-if-absent значения по умолчанию
    /// для перечисленных нод; существующие ключи НЕ перезаписываются.
    Task<Result> EnsureShardAsync(string cluster, string shard, IEnumerable<string> nodes, CancellationToken ct);
}

public sealed class AppParamsEnsurer(IEtcdGateway etcd, string[] endpoints, string defaultValue)
    : IAppParamsEnsurer
{
    public async Task<Result> EnsureShardAsync(
        string cluster, string shard, IEnumerable<string> nodes, CancellationToken ct)
    {
        foreach (var node in nodes)
        {
            var done = await TxnAsync(
                TxnRequest.Of(
                    [TxnCompare.NotExists(Key(cluster, shard, node))],
                    [new TxnOp.Put(Key(cluster, shard, node), defaultValue, null)]),
                ct);
            if (!done.IsSuccess)
                return done; // транспортный сбой всех endpoints; проигрыш compare — не сбой
        }

        return Result.Success();
    }

    private static string Key(string cluster, string shard, string node)
        => $"/clusters/{cluster}/shards/{shard}/nodes/{node}/app_params";

    // Failover-обёртка: первый успешный endpoint выигрывает (образец AppSecretEnsurer).
    private async Task<Result<TxnResult>> TxnAsync(TxnRequest req, CancellationToken ct)
    {
        Result<TxnResult>? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.TxnAsync(endpoint, req, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }
}
