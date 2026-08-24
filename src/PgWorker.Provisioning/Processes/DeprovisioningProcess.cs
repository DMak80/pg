using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Docker.Drivers;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;

namespace PgWorker.Provisioning.Processes;

/// <summary>
/// Deprovisioning — безопасное удаление кластера (задача 20; spec §6.4 B,
/// arch/14 §5 B). D1 удаляет ноды (и сироты-контейнеры по имени pgw-<C>-*) ДО
/// чистки etcd — «мёртвые» ключи при сбое безвредны (кластер в TO_REMOVE,
/// повторный тик продолжает). Успех = пустой /clusters/&lt;C&gt; + ЯВНО снятый
/// клэйм (del + revoke lease, не ждём TTL — ревизия 2 плана, №5).
/// </summary>
public sealed class DeprovisioningProcess(
    IEtcdGateway etcd,
    string[] endpoints,
    IClusterDriver driver,
    ClaimStore claims,
    WorkJournal journal,
    Func<CancellationToken, Task<Result>>? snapshot = null) : IClusterProcess
{
    private const string Op = "deprovision";

    public async Task<Result<ProcessOutcome>> TickAsync(ClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;

        // Мутации — только держателем живого клэйма (инвариант spec §4.3).
        if (!claims.IsMine(cluster))
            return Result<ProcessOutcome>.Failed(new ApplicationException(
                $"deprovisioning {cluster}: клэйм не наш (или потерян) — мутации запрещены"));

        // D0: journal-before-manipulations (P7).
        var started = await journal.WritePhaseAsync(cluster, Op, "started", claims.InstanceId, null, ct);
        if (!started.IsSuccess)
            return Result<ProcessOutcome>.Failed(started.Error!);

        // D1: остановить и удалить все ноды + сироты; nodes/<n>/state=REMOVING.
        var removed = await RemoveNodesAsync(cluster, snap, ct);
        if (!removed.IsSuccess)
            return await FailAsync(cluster, removed.Error!, "removing-nodes", ct);

        // Guard D2: docker-объектов кластера не осталось — только теперь чистим etcd.
        var objects = await driver.ListNodeObjectsAsync(cluster, ct);
        if (!objects.IsSuccess)
            return await FailAsync(cluster, objects.Error!, "listing-objects", ct);
        if (objects.Value.Count > 0)
            return await FailAsync(
                cluster,
                new ApplicationException($"объекты {string.Join(",", objects.Value)} ещё живы"),
                "removing-nodes",
                ct);

        // D2: del prefix /clusters/<C>/ + заявки request_* + префиксы service-скопов
        // + /pgworker/portalloc, /pgworker/work и заявки переездов /pgworker/moves/<C>/
        // (spec §4.2, arch/14 §5 B; t01 spec §5.3 — заявки не переживают кластер).
        var cleaned = await CleanKeysAsync(cluster, snap, ct);
        if (!cleaned.IsSuccess)
            return await FailAsync(cluster, cleaned.Error!, "cleaning-keys", ct);

        // D3: снапшот P12 + успех = пустой /clusters/<C>/; клэйм снимаем ЯВНО.
        if (snapshot is not null)
        {
            var shot = await snapshot(ct);
            if (!shot.IsSuccess)
                return await FailAsync(cluster, shot.Error!, "snapshot", ct);
        }

        var config = await GetAsync($"/clusters/{cluster}/config", ct);
        if (!config.IsSuccess)
            return await FailAsync(cluster, config.Error!, "verifying", ct);
        if (config.Value is not null)
            return await FailAsync(
                cluster,
                new ApplicationException("config-ключ пережил очистку — повтор тиком"),
                "cleaning-keys",
                ct);

        await claims.ReleaseClusterAsync(cluster, ct); // del ключа + revoke lease — не ждём TTL
        return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
    }

    // D1: RemoveNode всех заявленных нод + сироты из ListNodeObjects.
    private async Task<Result> RemoveNodesAsync(string cluster, ClusterSnapshot snap, CancellationToken ct)
    {
        foreach (var shard in snap.Shards)
        foreach (var node in shard.Nodes)
        {
            if (node.State != NodeState.Removing)
            {
                var marked = await PutAsync(
                    $"/clusters/{cluster}/shards/{shard.Name}/nodes/{node.Name}/state", "REMOVING", ct);
                if (!marked.IsSuccess)
                    return marked;
            }

            var removed = await driver.RemoveNodeAsync(cluster, shard.Name, node.Name, ct);
            if (!removed.IsSuccess)
                return removed;
        }

        // Сироты: контейнер есть, nodes-ключа нет (сбое-хвост прошлых фаз).
        var objects = await driver.ListNodeObjectsAsync(cluster, ct);
        if (!objects.IsSuccess)
            return objects;

        var known = snap.Shards
            .SelectMany(s => s.Nodes.Select(n => PlainNodeName(cluster, s.Name, n.Name)))
            .ToHashSet();
        foreach (var orphan in objects.Value.Where(name => !known.Contains(name)).ToList())
        {
            // pgw-<C>-<X>-<n>: последний сегмент — нода, перед ним — шард
            // (имена нод = имя шарда + буква, дефисов внутри нет).
            var tail = orphan[($"pgw-{cluster}-").Length..].Split('-');
            if (tail.Length < 2)
                continue; // чужое имя с нашим префиксом — не трогаем вслепую

            var shardName = string.Join("-", tail[..^1]);
            var nodeName = tail[^1];
            var removed = await driver.RemoveNodeAsync(cluster, shardName, nodeName, ct);
            if (!removed.IsSuccess)
                return removed;
        }

        return Result.Success();
    }

    // D2: полная очистка ключей кластера и координации.
    private async Task<Result> CleanKeysAsync(string cluster, ClusterSnapshot snap, CancellationToken ct)
    {
        var delCluster = await DeleteAsync($"/clusters/{cluster}/", prefix: true, ct);
        if (!delCluster.IsSuccess)
            return delCluster;

        foreach (var shard in snap.Shards)
        {
            var scope = $"{cluster}-{shard.Name}";
            // Точечные заявки ресурсов (spec §4.2) — даже если scope ещё жив.
            foreach (var request in (string[])["request_cpu", "request_mem", "request_disk"])
            {
                var delRequest = await DeleteAsync($"/service/{scope}/{request}", prefix: false, ct);
                if (!delRequest.IsSuccess)
                    return delRequest;
            }

            // Префикс scope (guard пройден: docker-объектов нет).
            var delScope = await DeleteAsync($"/service/{scope}/", prefix: true, ct);
            if (!delScope.IsSuccess)
                return delScope;
        }

        var delPorts = await DeleteAsync($"/pgworker/portalloc/{cluster}", prefix: false, ct);
        if (!delPorts.IsSuccess)
            return delPorts;

        var delWork = await DeleteAsync($"/pgworker/work/{cluster}", prefix: false, ct);
        if (!delWork.IsSuccess)
            return delWork;

        // Журналы эвакуаций не переживают удаление кластера (t06 §5.6, симметрия с S3).
        var delEvacuations = await DeleteAsync($"/pgworker/evacuations/{cluster}/", prefix: true, ct);
        if (!delEvacuations.IsSuccess)
            return delEvacuations;

        // Заявки переездов (t01, spec §5.3 D2): префикс /pgworker/moves/<C>/ целиком.
        return await DeleteAsync($"/pgworker/moves/{cluster}/", prefix: true, ct);
    }

    private static string PlainNodeName(string cluster, string shard, string node)
        => $"pgw-{cluster}-{shard}-{node}";

    private async Task<Result<ProcessOutcome>> FailAsync(
        string cluster, Exception error, string phase, CancellationToken ct)
    {
        await journal.WritePhaseAsync(cluster, Op, phase, claims.InstanceId, error.Message, ct);
        // journal-after-manipulations-провал: тик завершился с ошибкой, но
        // следующий тик продолжит с той же фазы (ретрай цикла, §7).
        return Result<ProcessOutcome>.Failed(error);
    }

    // Failover-обёртки: первый успешный endpoint выигрывает.
    private async Task<Result<Kv?>> GetAsync(string key, CancellationToken ct)
    {
        Result<Kv?>? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.GetAsync(endpoint, key, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }

    private async Task<Result> PutAsync(string key, string value, CancellationToken ct)
    {
        Result? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.PutAsync(endpoint, key, value, null, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }

    private async Task<Result> DeleteAsync(string keyOrPrefix, bool prefix, CancellationToken ct)
    {
        Result? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.DeleteAsync(endpoint, keyOrPrefix, prefix, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }
}
