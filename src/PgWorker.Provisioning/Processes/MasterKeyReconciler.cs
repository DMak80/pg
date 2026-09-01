using System.Collections.Concurrent;
using System.Globalization;
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
/// писатель). Отказ callback (R3) закрыт этим двойным контуром. Lease,
/// выданный reconciler'ом (писатель-callback не работает), продлевается
/// отдельным циклом с периодом TTL/2.5 — ключ не мигает между тиками сверки;
/// продление снимается, когда primary перестал отвечать (ключ протухает ≤ TTL,
/// P11 и условие эвакуации мёртвого шарда).
/// </summary>
public sealed class MasterKeyReconciler(IEtcdGateway etcd, string[] endpoints, ShardProbe probe)
{
    private const int MasterLeaseTtlSec = 5; // P11: TTL 5с (ttl=5/loop_wait=2 Patroni)

    // Продление lease — в 2.5 раза чаще периода протухания (TTL/2.5 = 2с).
    internal static readonly TimeSpan KeepalivePeriod = TimeSpan.FromSeconds(MasterLeaseTtlSec / 2.5);

    // Выданные нами lease по мастер-ключам (шард без alive-primary удаляется).
    private readonly ConcurrentDictionary<string, long> _held = new();
    private readonly object _loopSync = new();
    private CancellationTokenSource? _renewalLoop;

    public async Task<Result> ReconcileAsync(
        ClusterSnapshot snap, IReadOnlyDictionary<string, NodeAddress> addresses, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;
        foreach (var shard in snap.Shards)
        {
            var key = $"/clusters/{cluster}/shards/{shard.Name}/master";

            // Усыновлённые шарды не сверяем (spec §3.4, arch/14 §5 C/R8): их master-ключ
            // пишет внешний HA-контур своим форматом node:port — коррекция порождает
            // войну писателей; резолв мастера понимает оба формата (§5 F).
            var adopted = shard.Nodes.Any(n =>
                addresses.TryGetValue($"{shard.Name}/{n.Name}", out var a) && a.Object is not null);
            if (adopted)
                continue;

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
            {
                // primary не отвечает (failover-окно/шард мёртв) — Patroni сам;
                // наш lease больше не продлеваем: ключ гаснет ≤ TTL (P11).
                _held.TryRemove(key, out _);
                continue;
            }

            var expected = $"{primary.Host}:{primary.Ports.Doorman}";
            if (IsCurrent(shard.Master, primary))
                continue; // синхрон — мутаций нет (инвариант «только при рассинхроне»)

            // Коррекция: lease TTL 5 + put (ключ перепишет callback on_role_change).
            var grant = await WithFailoverAsync(endpoint => etcd.LeaseGrantAsync(endpoint, MasterLeaseTtlSec, ct));
            if (!grant.IsSuccess)
                return grant;

            var put = await WithFailoverAsync(endpoint =>
                etcd.PutAsync(endpoint, key, expected, grant.Value, ct));
            if (!put.IsSuccess)
                return put;

            _held[key] = grant.Value;
            EnsureRenewalLoop();
        }

        return Result.Success();
    }

    /// <summary>
    /// Ключ соответствует факту primary. Хост-часть может расходиться с portalloc
    /// (advertised-режим, arch/16): ключ пишут lease-демоны нод с env-хостом
    /// КОНТЕЙНЕРА (PGW_NODE_HOST), а portalloc несёт advertised-имя — писатели
    /// согласны, когда совпадает doorman-порт (уникален per-node, arch/14 §2.4).
    /// Без doorman (EnableDoorman=false) — точное сравнение (прежняя семантика).
    /// </summary>
    private static bool IsCurrent(string? masterKey, NodeAddress primary)
    {
        if (masterKey == $"{primary.Host}:{primary.Ports.Doorman}")
            return true;
        if (primary.Ports.Doorman <= 0 || string.IsNullOrEmpty(masterKey))
            return false;
        var colon = masterKey.LastIndexOf(':');
        return colon > 0
               && int.TryParse(masterKey[(colon + 1)..], NumberStyles.Integer, CultureInfo.InvariantCulture, out var port)
               && port == primary.Ports.Doorman;
    }

    /// <summary>
    /// Продление удерживаемых lease (вызывается циклом с периодом KeepalivePeriod;
    /// PackageInternal для тестов). Ошибочный keepalive = lease протух —
    /// следующий reconcile перепишет ключ заново.
    /// </summary>
    internal async Task RenewHeldAsync(CancellationToken ct)
    {
        foreach (var (key, lease) in _held)
        {
            var renewed = await WithFailoverAsync(endpoint => etcd.LeaseKeepaliveAsync(endpoint, lease, ct));
            if (!renewed.IsSuccess)
                _held.TryRemove(key, out _);
        }
    }

    private void EnsureRenewalLoop()
    {
        lock (_loopSync)
        {
            if (_renewalLoop is not null)
                return;
            _renewalLoop = new CancellationTokenSource();
            _ = RenewalLoopAsync(_renewalLoop.Token);
        }
    }

    private async Task RenewalLoopAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(KeepalivePeriod);
        try
        {
            while (await timer.WaitForNextTickAsync(ct))
                await RenewHeldAsync(ct);
        }
        catch (OperationCanceledException)
        {
            // остановка приложения
        }
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
