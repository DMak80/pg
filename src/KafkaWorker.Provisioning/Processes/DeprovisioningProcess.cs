using KafkaWorker.Core;
using KafkaWorker.Docker.Drivers;
using KafkaWorker.Etcd.Client;
using KafkaWorker.Etcd.Coordination;

namespace KafkaWorker.Provisioning.Processes;

/// <summary>
/// Deprovisioning — полный демонтаж Kafka-кластера (arch/16 §5 B, фазы X0–X3).
/// X1 удаляет контейнеры/сервисы и тома kfw-<C>-* (включая сирот; 404 = ок) ДО
/// чистки etcd — «мёртвые» ключи при сбое безвредны (кластер в TO_REMOVE,
/// повторный тик продолжает). X2: del --prefix /kafka/clusters/&lt;C&gt;/ +
/// координация /kafkaworker/{claims,work,portalloc}/&lt;C&gt; + ЗАЯВКА РОТАЦИИ
/// /kafkaworker/rotations/&lt;C&gt; (остаточная заявка не переживает удаление
/// кластера — иначе вечный алерт kafka-rotation-pending). Снапшоты P12 «до/после».
/// Успех = пустой префикс + ЯВНО снятый клэйм (не ждём TTL).
/// </summary>
public sealed class DeprovisioningProcess(
    IEtcdGateway etcd,
    string[] endpoints,
    IClusterDriver driver,
    ClaimStore claims,
    WorkJournal journal,
    Func<CancellationToken, Task<Result>>? snapshot = null)
{
    private const string Op = "deprovision";

    public async Task<Result> RunAsync(string cluster, IReadOnlyList<string> declaredBrokers, CancellationToken ct)
    {
        // Мутации — только держателем живого клэйма (arch/16 §5).
        if (!claims.IsMine(cluster))
            return Result.Failed(new ApplicationException(
                $"deprovisioning {cluster}: клэйм не наш (или потерян) — мутации запрещены"));

        // Снапшот P12 «до» (старт демонтажа — точка изменения).
        if (snapshot is not null)
        {
            var before = await snapshot(ct);
            if (!before.IsSuccess)
                return Fail(cluster, before.Error!, "snapshot-before");
        }

        // X0: journal-before-manipulations.
        var started = await journal.WriteAsync(cluster, Op, "started", claims.InstanceId, null, ct);
        if (!started.IsSuccess)
            return started;

        // X1: docker-удаление всех брокеров (тома тоже) + сироты kfw-<C>-*.
        var removed = await RemoveNodesAsync(cluster, declaredBrokers, ct);
        if (!removed.IsSuccess)
            return Fail(cluster, removed.Error!, "removing-nodes");

        // Guard: docker-объектов кластера не осталось — только теперь чистим etcd
        // (порядок «сначала docker, потом etcd» — arch/16 §5 B).
        var objects = await driver.ListNodeObjectsAsync(cluster, ct);
        if (!objects.IsSuccess)
            return Fail(cluster, objects.Error!, "listing-objects");
        if (objects.Value.Count > 0)
            return Fail(cluster,
                new ApplicationException($"объекты {string.Join(",", objects.Value)} ещё живы"),
                "removing-nodes");

        // X2: полная etcd-очистка: префикс кластера + координация + заявка ротации.
        var cleaned = await CleanKeysAsync(cluster, ct);
        if (!cleaned.IsSuccess)
            return Fail(cluster, cleaned.Error!, "cleaning-keys");

        // X3: снапшот P12 «после» + verify + явное снятие клэйма.
        if (snapshot is not null)
        {
            var after = await snapshot(ct);
            if (!after.IsSuccess)
                return Fail(cluster, after.Error!, "snapshot-after");
        }

        var config = await GetAsync($"/kafka/clusters/{cluster}/config", ct);
        if (!config.IsSuccess)
            return Fail(cluster, config.Error!, "verifying");
        if (config.Value is not null)
            return Fail(cluster,
                new ApplicationException("config-ключ пережил очистку — повтор тиком"),
                "cleaning-keys");

        await claims.ReleaseClusterAsync(cluster, ct); // del ключа + revoke lease — не ждём TTL
        return Result.Success();
    }

    // X1: RemoveNode(removeVolume) всех заявленных брокеров + сироты из ListNodeObjects.
    private async Task<Result> RemoveNodesAsync(string cluster, IReadOnlyList<string> brokers, CancellationToken ct)
    {
        foreach (var broker in brokers)
        {
            var removed = await driver.RemoveNodeAsync(cluster, broker, removeVolume: true, ct);
            if (!removed.IsSuccess)
                return removed; // 404 уже = успех на уровне драйвера/движка
        }

        // Сироты: контейнер есть, ключа brokers/<b> нет (сбое-хвост прошлых фаз).
        var objects = await driver.ListNodeObjectsAsync(cluster, ct);
        if (!objects.IsSuccess)
            return objects;

        var prefix = $"kfw-{cluster}-";
        foreach (var orphan in objects.Value.Where(name => name.StartsWith(prefix, StringComparison.Ordinal)))
        {
            // kfw-<C>-<b>: последний сегмент после префикса — имя брокера.
            var brokerName = orphan[prefix.Length..].Split('-')[0];
            var removed = await driver.RemoveNodeAsync(cluster, brokerName, removeVolume: true, ct);
            if (!removed.IsSuccess)
                return removed;
        }

        return Result.Success();
    }

    // X2: del --prefix /kafka/clusters/<C>/ + координационные ключи воркера.
    private async Task<Result> CleanKeysAsync(string cluster, CancellationToken ct)
    {
        var deletions = new[]
        {
            ($"/kafka/clusters/{cluster}/", true),
            ($"/kafkaworker/claims/{cluster}", false),
            ($"/kafkaworker/work/{cluster}", false),
            ($"/kafkaworker/portalloc/{cluster}", false),
            ($"/kafkaworker/rotations/{cluster}", false), // заявка ротации не переживает кластер
        };

        foreach (var (key, prefix) in deletions)
        {
            var deleted = await DeleteAsync(key, prefix, ct);
            if (!deleted.IsSuccess)
                return deleted;
        }

        return Result.Success();
    }

    private Result Fail(string cluster, Exception error, string phase)
    {
        journal.WriteAsync(cluster, Op, phase, claims.InstanceId, error.Message, CancellationToken.None)
            .GetAwaiter().GetResult();
        return Result.Failed(error);
    }

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
