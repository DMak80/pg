using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Etcd.Client;
using PgWorker.Provisioning.Probes;

namespace PgWorker.Provisioning.Processes;

/// <summary>
/// P11-сверка мастер-ключей (задача 21; arch/14 §5 C, дока 12 P11): Patroni
/// callback — основной писатель /clusters/&lt;C&gt;/shards/&lt;X&gt;/master; reconciler —
/// «сверяющий демон»: GET /primary по нодам шарда → фактический primary;
/// расхождение (или ключа нет при живом primary) → lease-put TTL 5с значения
/// host:doormanPort. Ключ корректен → НИКАКИХ мутаций (не второй регулярный
/// писатель). Отказ callback (R3) закрыт этим двойным контуром.
/// </summary>
public sealed class MasterKeyReconciler(IEtcdGateway etcd, string[] endpoints, ShardProbe probe)
{
    private const int MasterLeaseTtlSec = 5; // P11: TTL 5с (ttl=5/loop_wait=2 Patroni)

    public async Task<Result> ReconcileAsync(
        ClusterSnapshot snap, IReadOnlyDictionary<string, NodeAddress> addresses, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;
        foreach (var shard in snap.Shards)
        {
            // Фактический primary: первая нода, ответившая 200 на /primary.
            var nodes = shard.Nodes
                .Where(n => addresses.ContainsKey($"{shard.Name}/{n.Name}"))
                .OrderBy(n => n.Name, StringComparer.Ordinal);
            NodeAddress? primary = null;
            foreach (var node in nodes)
            {
                if (await probe.IsPrimaryAsync(addresses[$"{shard.Name}/{node.Name}"], ct))
                {
                    primary = addresses[$"{shard.Name}/{node.Name}"];
                    break;
                }
            }

            if (primary is null)
                continue; // primary не отвечает (failover-окно/шард мёртв) — Patroni сам

            var key = $"/clusters/{cluster}/shards/{shard.Name}/master";
            var expected = $"{primary.Host}:{primary.Ports.Doorman}";
            if (shard.Master == expected)
                continue; // синхрон — мутаций нет (инвариант «только при рассинхроне»)

            // Коррекция: lease TTL 5 + put (ключ перепишет callback на on_role_change).
            var grant = await WithFailoverAsync(endpoint => etcd.LeaseGrantAsync(endpoint, MasterLeaseTtlSec, ct));
            if (!grant.IsSuccess)
                return grant;

            var put = await WithFailoverAsync(endpoint =>
                etcd.PutAsync(endpoint, key, expected, grant.Value, ct));
            if (!put.IsSuccess)
                return put;
        }

        return Result.Success();
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
