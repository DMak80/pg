using Shared.Core;
using Shared.Etcd.Client;
using ValkeyWorker.Docker.Drivers;

namespace ValkeyWorker.Provisioning.Processes;

/// <summary>
/// Deprovisioning valkey-кластера (arch/21 §5 B, фазы X0–X3): от заявки
/// TO_REMOVE до чистого etcd и удалённого контейнера. ПОРЯДОК «сначала docker,
/// потом etcd»: ошибка docker-хоста оставляет etcd-декларацию нетронутой —
/// следующий тик повторит демонтаж (томов у домена нет). Снапшоты P12
/// «до»/«после» — через snapshot-делегат. Вызывается только держателем клэйма.
/// </summary>
public sealed class DeprovisioningProcess(
    IEtcdGateway gateway,
    string[] endpoints,
    IClusterDriver driver,
    ClaimStore claims,
    WorkJournal journal,
    Func<CancellationToken, Task<Result>>? snapshot = null)
{
    private const string Op = "deprovision";

    public async Task<Result> TickAsync(ValkeyWorker.Core.Model.ValkeyClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Cluster;

        // Мутации — только держателем живого клэйма (arch/21 §6).
        var claimed = ProcessCommon.EnsureClaimed(claims, cluster, Op);
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

        // X1: docker сначала (rm -f vwk-<C>-*, 404 = ок; томов нет).
        var objects = await driver.ListNodeObjectsAsync(cluster, ct);
        if (!objects.IsSuccess)
            return Fail(cluster, objects.Error!, "list");
        foreach (var name in objects.Value)
        {
            // Имя объекта vwk-<C>-<node> → имя узла после префикса кластера.
            var node = name[$"vwk-{cluster}-".Length..];
            var removed = await driver.RemoveNodeAsync(cluster, node, ct);
            if (!removed.IsSuccess)
                return Fail(cluster, removed.Error!, "remove-node");
        }

        // X2: etcd после docker — домен + координация ВКЛЮЧАЯ заявки ротаций.
        var domainDel = await DeletePrefixAsync($"/valkey/clusters/{cluster}/", ct);
        if (!domainDel.IsSuccess)
            return Fail(cluster, domainDel.Error!, "delete-domain");
        foreach (var key in new[]
                 {
                     $"/valkeyworker/claims/{cluster}",
                     $"/valkeyworker/work/{cluster}",
                     $"/valkeyworker/portalloc/{cluster}",
                     $"/valkeyworker/rotations/{cluster}",
                 })
        {
            var del = await DeleteKeyAsync(key, ct);
            if (!del.IsSuccess)
                return Fail(cluster, del.Error!, $"delete {key}");
        }

        // X3: снапшот «после» + явное снятие клэйма (del + revoke lease).
        if (snapshot is not null)
        {
            var after = await snapshot(ct);
            if (!after.IsSuccess)
                return Fail(cluster, after.Error!, "snapshot-after");
        }

        var done = await journal.WritePhaseAsync(cluster, Op, "done", claims.InstanceId, null, ct);
        if (!done.IsSuccess)
            return done;
        await claims.ReleaseClusterAsync(cluster, ct);
        return Result.Success();
    }

    private Result Fail(string cluster, Exception error, string phase)
        => Result.Failed(new ApplicationException($"deprovision {cluster}: {phase}: {error.Message}", error));

    private async Task<Result> DeletePrefixAsync(string prefix, CancellationToken ct)
    {
        Result? last = null;
        foreach (var endpoint in endpoints)
        {
            var del = await gateway.DeleteAsync(endpoint, prefix, prefix: true, ct);
            if (del.IsSuccess)
                return del;
            last = del;
        }

        return last!;
    }

    private async Task<Result> DeleteKeyAsync(string key, CancellationToken ct)
    {
        Result? last = null;
        foreach (var endpoint in endpoints)
        {
            var del = await gateway.DeleteAsync(endpoint, key, prefix: false, ct);
            if (del.IsSuccess)
                return del;
            last = del;
        }

        return last!;
    }
}
