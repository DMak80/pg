using System.Text.Json;
using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using KafkaWorker.Docker.Drivers;
using KafkaWorker.Etcd.Client;
using KafkaWorker.Etcd.Coordination;
using KafkaWorker.Core.Planning;
using KafkaWorker.Provisioning.Kafka;

namespace KafkaWorker.Provisioning.Processes;

/// <summary>
/// RemoveBrokerProcess (arch/16 §5 G): маркер brokers/&lt;b&gt;/state=TO_REMOVE →
/// guards (кластер Active; не controller; не последний; на брокере нет реплик
/// партиций — по DescribeTopics включая __-топики, иначе journal-ожидание —
/// drain идёт процессом I (reassign), демонтаж продолжится сам) →
/// удаление контейнера+тома → del префикса brokers/&lt;b&gt;/ → RMW endpoints
/// (убрать адрес) → portalloc-фильтрация → journal done. Идемпотентен.
/// Вызывается только держателем клэйма &lt;C&gt;.
/// </summary>
public sealed class RemoveBrokerProcess(
    IEtcdGateway etcd,
    string[] endpoints,
    IClusterDriver driver,
    ClaimStore claims,
    WorkJournal journal,
    IKafkaAdminClientFactory adminFactory,
    ProvisioningOptions options)
{
    private const string Op = "remove-broker";

    public async Task<Result> RunAsync(KafkaClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Cluster;
        if (!claims.IsMine(cluster))
            return Result.Failed(new ApplicationException(
                $"remove-broker {cluster}: клэйм не наш (или потерян) — мутации запрещены"));

        // Порядок: broker-only первыми — демонтаж controller-нод в принципе невозможен
        // (guard ниже), но и порядок обработки не должен прятать guard «последний».
        var candidates = snap.Brokers
            .Where(b => b.State == "TO_REMOVE")
            .OrderBy(b => b.Role == "controller" ? 1 : 0)
            .ToList();
        if (candidates.Count == 0)
            return Result.Success(); // маркеров нет — no-op

        foreach (var broker in candidates)
        {
            // Guard: контроллерные ноды не демонтируются (роль навсегда, arch/15 §2).
            if (broker.Role == "controller")
                return Fail(cluster,
                    new ApplicationException(
                        $"remove-broker {cluster}: {broker.Name} — controller-нода, демонтаж запрещён"),
                    "controller-guard");

            // Guard: не последний живой брокер кластера.
            var remaining = snap.Brokers.Count(b => b.State != "TO_REMOVE");
            if (remaining == 0)
                return Fail(cluster,
                    new ApplicationException(
                        $"remove-broker {cluster}: {broker.Name} — последний брокер, демонтаж опустошит кластер"),
                    "last-broker-guard");

            // Guard: на брокере нет реплик партиций (факт знает только Kafka).
            if (await HasPartitionsAsync(snap, broker.Name, ct))
            {
                var waiting = await journal.WriteAsync(
                    cluster, Op, "waiting-partitions", claims.InstanceId,
                    $"на {broker.Name} есть реплики партиций — drain идёт (процесс reassign), демонтаж продолжится сам", ct);
                return waiting; // не ошибка: следующий тик повторит проверку
            }

            var started = await journal.WriteAsync(
                cluster, Op, "removing", claims.InstanceId, null, ct);
            if (!started.IsSuccess)
                return started;

            // Демонтаж: контейнер + том данных; затем ключи brokers/<b>/, адрес,
            // порт (404 docker = успех на уровне драйвера).
            var removed = await driver.RemoveNodeAsync(cluster, broker.Name, removeVolume: true, ct);
            if (!removed.IsSuccess)
                return Fail(cluster, removed.Error!, "removing-node");

            var deleted = await DeleteAsync(BrokerPrefix(cluster, broker.Name), prefix: true, ct);
            if (!deleted.IsSuccess)
                return Fail(cluster, deleted.Error!, "deleting-keys");

            var endpointsUpdated = await RemoveFromEndpointsAsync(snap, broker.Name, ct);
            if (!endpointsUpdated.IsSuccess)
                return Fail(cluster, endpointsUpdated.Error!, "endpoints-rmw");

            var filtered = await FilterPortAllocAsync(cluster, broker.Name, ct);
            if (!filtered.IsSuccess)
                return Fail(cluster, filtered.Error!, "portalloc-filter");
        }

        return await journal.WriteAsync(cluster, Op, "done", claims.InstanceId, null, ct);
    }

    // «На брокере есть реплики» — по DescribeTopics.ReplicasPerPartition (A8).
    // Недоступность кластера — консервативно «есть» (демонтаж подождёт): без
    // факта рисковать нельзя (roadmap t02 разблокирует drain).
    private async Task<bool> HasPartitionsAsync(KafkaClusterSnapshot snap, string broker, CancellationToken ct)
    {
        if (snap.Endpoints is null || snap.AppUser is null || snap.AppPassword is null)
            return true; // кластер не поднят — факт неизвестен, ждём

        var brokerId = BrokerEnvBuilder.NodeId(broker);
        await using var admin = adminFactory.Create(snap.Endpoints, snap.AdminUser ?? "admin", snap.AdminPassword!, snap.CaPem);
        // Describe-all: guard видит и internal-реплики (__consumer_offsets) —
        // раньше фильтр __ прятал их и «пустой» брокер демонтировался с
        // потерей этих реплик (t02 §1, arch/16 §5 I/G).
        var topics = await admin.DescribeTopicsAsync(includeInternal: true, ct);
        if (!topics.IsSuccess)
            return true; // факт неизвестен — консервативно ждём

        return topics.Value.Any(t => t.ReplicasPerPartition.Any(p => p.Contains(brokerId)));
    }

    // RMW endpoints по mod_revision: убрать адрес демонтируемого брокера.
    private async Task<Result> RemoveFromEndpointsAsync(
        KafkaClusterSnapshot snap, string broker, CancellationToken ct)
    {
        var key = EndpointsKey(snap.Cluster);
        var current = await GetAsync(key, ct);
        if (!current.IsSuccess)
            return current;
        if (current.Value is null)
            return Result.Success(); // endpoints нет — нечего чистить

        var address = await AddressOfBrokerAsync(snap, broker, ct);
        var list = current.Value.Value
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(a => address is null || a != address)
            .ToList();

        var txn = await TxnAsync(
            TxnRequest.Of(
                [TxnCompare.ModRevisionEqual(key, (long)current.Value.ModRevision)],
                [new TxnOp.Put(key, string.Join(",", list), null)]),
            ct);
        if (!txn.IsSuccess)
            return txn;
        if (!txn.Value.Succeeded)
            return Result.Failed(new ApplicationException(
                $"endpoints {key} изменился с момента чтения — ретрай тиком"));

        return Result.Success();
    }

    // Адрес брокера из portalloc (advertised-правило arch/16 §2.1); null — не закреплён.
    private async Task<string?> AddressOfBrokerAsync(
        KafkaClusterSnapshot snap, string broker, CancellationToken ct)
    {
        var portAlloc = await GetAsync(PortAllocKey(snap.Cluster), ct);
        if (!portAlloc.IsSuccess || portAlloc.Value is not { } kv)
            return null;
        using var doc = JsonDocument.Parse(kv.Value);
        if (!doc.RootElement.TryGetProperty(broker, out var node))
            return null;
        var host = node.GetProperty("host").GetString()!;
        var port = node.GetProperty("client").GetInt32();
        return $"{options.AdvertisedClientHost ?? host}:{port}";
    }

    // portalloc-фильтрация: переписать закрепления без демонтируемого брокера.
    private async Task<Result> FilterPortAllocAsync(string cluster, string broker, CancellationToken ct)
    {
        var key = PortAllocKey(cluster);
        var current = await GetAsync(key, ct);
        if (!current.IsSuccess)
            return current;
        if (current.Value is null)
            return Result.Success();

        using var doc = JsonDocument.Parse(current.Value.Value);
        using var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            foreach (var node in doc.RootElement.EnumerateObject()
                         .Where(n => n.Name != broker)
                         .OrderBy(n => n.Name, StringComparer.Ordinal))
            {
                writer.WritePropertyName(node.Name);
                writer.WriteStartObject();
                writer.WriteString("host", node.Value.GetProperty("host").GetString());
                writer.WriteNumber("client", node.Value.GetProperty("client").GetInt32());
                writer.WriteEndObject();
            }

            writer.WriteEndObject();
        }

        var txn = await TxnAsync(
            TxnRequest.Of(
                [TxnCompare.ModRevisionEqual(key, (long)current.Value.ModRevision)],
                [new TxnOp.Put(key, System.Text.Encoding.UTF8.GetString(buffer.ToArray()), null)]),
            ct);
        if (!txn.IsSuccess)
            return txn;
        if (!txn.Value.Succeeded)
            return Result.Failed(new ApplicationException(
                $"portalloc {key} изменился с момента чтения — ретрай тиком"));

        return Result.Success();
    }

    private Result Fail(string cluster, Exception error, string phase)
    {
        journal.WriteAsync(cluster, Op, phase, claims.InstanceId, error.Message, CancellationToken.None)
            .GetAwaiter().GetResult();
        return Result.Failed(error);
    }

    private static string BrokerPrefix(string cluster, string broker)
        => $"/kafka/clusters/{cluster}/brokers/{broker}/";

    private static string EndpointsKey(string cluster) => $"/kafka/clusters/{cluster}/endpoints";

    private static string PortAllocKey(string cluster) => $"/kafkaworker/portalloc/{cluster}";

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
