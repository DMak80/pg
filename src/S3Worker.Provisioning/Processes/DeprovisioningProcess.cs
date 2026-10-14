using S3Worker.Core.Model;
using S3Worker.Docker.Drivers;
using Shared.Etcd.Client;

namespace S3Worker.Provisioning.Processes;

/// <summary>
/// Deprovisioning S3-кластера (arch/24 §5 B, фазы X0–X3): от заявки TO_REMOVE
/// до чистого etcd и снесённых docker-объектов. ПОРЯДОК «сначала docker,
/// потом etcd»: ошибка docker-хоста оставляет etcd-декларацию нетронутой —
/// следующий тик повторит демонтаж. X1 маркеры REMOVING всем нодам; X2 снос
/// контейнеров и volumes на всех хостах (404 = ок) + del portalloc; X3 del
/// префикса домена + координации &lt;C&gt;; journal done; клэйм снят явно.
/// Снос кластера уничтожает данные бэкапов его нод — осознанное действие
/// оператора (risk R2 arch/24 §9). Снапшоты P12 «до»/«после» — snapshot-делегат.
/// Вызывается только держателем клэйма.
/// </summary>
public sealed class DeprovisioningProcess(
    IEtcdGateway gateway,
    string[] endpoints,
    IS3ClusterDriver driver,
    ClaimStore claims,
    WorkJournal journal,
    BootWaitBudget bootWait,
    Func<CancellationToken, Task<Result>>? snapshot = null)
{
    private const string Op = "deprovision";

    public async Task<Result> TickAsync(S3ClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Cluster;

        // Мутации — только держателем живого клэйма (arch/17 S5).
        var claimed = S3ProcessCommon.EnsureClaimed(claims, cluster, Op);
        if (!claimed.IsSuccess)
            return claimed;

        // X0: journal-before-manipulations + снапшот «до».
        var started = await journal.WritePhaseAsync(cluster, Op, "started", claims.InstanceId, null, ct);
        if (!started.IsSuccess)
            return started;
        if (snapshot is not null)
        {
            var before = await snapshot(ct);
            if (!before.IsSuccess)
                return Fail(cluster, before.Error!, "snapshot-before");
        }

        // X1: маркеры REMOVING всем нодам (жизненный цикл демонтажа).
        foreach (var node in snap.Nodes.OrderBy(n => n.Node, StringComparer.Ordinal))
        {
            var marking = await S3ProcessCommon.WriteNodeStateAsync(
                gateway, endpoints, cluster, node.Node, S3NodeStates.Removing, ct);
            if (!marking.IsSuccess)
                return Fail(cluster, marking.Error!, "marking-removing");
        }

        // X2: docker сначала — снос контейнеров и volumes на всех хостах
        // (перечень объектов сироты+инстанса; 404 = ок на каждом движке). Имена
        // объектов сводятся к инстансам: контейнер s3w-<C>-<I> и том
        // s3w-<C>-<I>-data → инстанс <I> (RemoveNodeAsync снимает оба,
        // идемпотентно по 404 для сироты).
        var objects = await driver.ListNodeObjectsAsync(cluster, ct);
        if (!objects.IsSuccess)
            return Fail(cluster, objects.Error!, "list");
        var prefix = $"s3w-{cluster}-";
        var nodesToRemove = new SortedSet<string>(StringComparer.Ordinal);
        foreach (var name in objects.Value)
        {
            if (!name.StartsWith(prefix, StringComparison.Ordinal))
                continue;
            var tail = name[prefix.Length..];
            nodesToRemove.Add(tail.EndsWith("-data", StringComparison.Ordinal)
                ? tail[..^5]
                : tail);
        }

        foreach (var node in nodesToRemove)
        {
            var removed = await driver.RemoveNodeAsync(cluster, node, removeVolume: true, ct);
            if (!removed.IsSuccess)
                return Fail(cluster, removed.Error!, "remove-node");
        }

        // X2-конец: чистка portalloc — docker-публикации снесены, закрепление
        // больше не имеет смысла (до etcd-домена — порядок канона).
        var portAllocDel = await DeleteKeyAsync(S3ProcessCommon.PortAllocKey(cluster), ct);
        if (!portAllocDel.IsSuccess)
            return Fail(cluster, portAllocDel.Error!, "delete-portalloc");

        // X3: etcd после docker — домен + координация (work/rotations/probes).
        var domainDel = await DeletePrefixAsync(S3ProcessCommon.ClusterPrefix(cluster), ct);
        if (!domainDel.IsSuccess)
            return Fail(cluster, domainDel.Error!, "delete-domain");
        foreach (var key in new[]
                 {
                     S3ProcessCommon.WorkKey(cluster),
                     S3ProcessCommon.RotationKey(cluster),
                     S3ProcessCommon.ProbeRequestKey(cluster),
                 })
        {
            var del = await DeleteKeyAsync(key, ct);
            if (!del.IsSuccess)
                return Fail(cluster, del.Error!, $"delete {key}");
        }

        // Journal done (arch/24 §5 B: финальная фаза X3) + снапшот «после» +
        // ЯВНОЕ снятие клэйма (не ждём TTL). Демонтаж завершён — бюджет
        // boot-ожидания имени кластера начинается заново (повторный
        // provisioning того же имени — без старого since).
        bootWait.Reset(cluster);
        var done = await journal.WritePhaseAsync(cluster, Op, "done", claims.InstanceId, null, ct);
        if (!done.IsSuccess)
            return done;

        if (snapshot is not null)
        {
            var after = await snapshot(ct);
            if (!after.IsSuccess)
                return Fail(cluster, after.Error!, "snapshot-after");
        }

        // Явное снятие клэйма (не ждём TTL; ReleaseClusterAsync — void).
        await claims.ReleaseClusterAsync(cluster, ct);
        return Result.Success();
    }

    private Result Fail(string cluster, Exception error, string phase)
        => Result.Failed(new ApplicationException($"deprovision {cluster}: {phase}: {error.Message}", error));

    private async Task<Result> DeletePrefixAsync(string prefix, CancellationToken ct)
    {
        Result last = default;
        foreach (var endpoint in endpoints)
        {
            var del = await gateway.DeleteAsync(endpoint, prefix, prefix: true, ct);
            if (del.IsSuccess)
                return del;
            last = del;
        }

        return last.Error ?? new EtcdUnreachableException("etcd endpoints не заданы");
    }

    private async Task<Result> DeleteKeyAsync(string key, CancellationToken ct)
    {
        Result last = default;
        foreach (var endpoint in endpoints)
        {
            var del = await gateway.DeleteAsync(endpoint, key, prefix: false, ct);
            if (del.IsSuccess)
                return del;
            last = del;
        }

        return last.Error ?? new EtcdUnreachableException("etcd endpoints не заданы");
    }
}
