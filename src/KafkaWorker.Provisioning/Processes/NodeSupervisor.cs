using System.Globalization;
using System.Text.Json;
using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using KafkaWorker.Core.Planning;
using KafkaWorker.Core.Templates;
using KafkaWorker.Docker.Drivers;
using KafkaWorker.Etcd.Client;
using KafkaWorker.Etcd.Coordination;
using KafkaWorker.Provisioning.Kafka;

namespace KafkaWorker.Provisioning.Processes;

/// <summary>
/// Надзор нод Active-кластера (arch/16 §5 C, порт NodeSupervisor PgWorker):
/// сверка декларации с фактом docker + AdminClient-проба. Снесённый контейнер
/// пересоздаётся (тот же volume/env — self-healing); брокер молчит дольше
/// NodeDeadSec → state=UNREACHABLE + пересоздание с ЧИСТЫМ томом (RF&gt;1 —
/// rejoin репликацией; RF=1 — journal-warning о потере данных, документированное
/// поведение). Ноды TO_REMOVE/REMOVING/PROVISIONING чужих процессов не трогаем.
/// Вызывается только держателем клэйма &lt;C&gt;.
/// </summary>
public sealed class NodeSupervisor(
    IEtcdGateway etcd,
    string[] endpoints,
    IClusterDriver driver,
    ClaimStore claims,
    WorkJournal journal,
    IKafkaAdminClientFactory adminFactory,
    ProvisioningOptions options)
{
    private const string Op = "supervise";

    public async Task<Result> RunAsync(KafkaClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Cluster;

        // Мутации — только держателем живого клэйма.
        if (!claims.IsMine(cluster))
            return Result.Failed(new ApplicationException(
                $"supervise {cluster}: клэйм не наш (или потерян) — мутации запрещены"));

        var addresses = await ReadPortAllocAsync(cluster, ct);
        if (!addresses.IsSuccess)
            return Result.Failed(addresses.Error!);

        // Факт docker: имена живых объектов kfw-<C>-*.
        var objects = await driver.ListNodeObjectsAsync(cluster, ct);
        if (!objects.IsSuccess)
            return Result.Failed(objects.Error!);
        var alive = objects.Value.ToHashSet();

        // AdminClient-проба: кто реально в кластере (по NodeId).
        var view = await DescribeAliveAsync(snap, ct);
        if (!view.IsSuccess)
            return Result.Failed(view.Error!);
        var inCluster = view.Value ?? new HashSet<int>();
        var unreachableNow = snap.Brokers
            .Where(b => b.State == "RUNNING")
            .Where(b => !inCluster.Contains(NodeId(b.Name)))
            .Select(b => b.Name)
            .ToList();

        // Трек молчания: journal.unreachable broker → first_seen (порт PgWorker).
        var rawTrack = await journal.ReadUnreachableAsync(cluster, ct);
        if (!rawTrack.IsSuccess)
            return Result.Failed(rawTrack.Error!);
        var track = new Dictionary<string, long>(rawTrack.Value);
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        foreach (var broker in unreachableNow)
            if (!track.ContainsKey(broker))
                track[broker] = now;
        foreach (var stale in track.Keys
                     .Where(k => snap.Brokers.All(b => b.Name != k) || !unreachableNow.Contains(k))
                     .ToList())
            track.Remove(stale); // брокер ожил или исчез из декларации

        // Warning-ы тика (RF=1-пересоздания) — в финальную supervision-запись.
        var warnings = new List<string>();

        // 1) Снесённые контейнеры → пересоздать (тот же volume/env).
        foreach (var broker in Supervisable(snap))
        {
            var containerName = $"kfw-{cluster}-{broker.Name}";
            if (alive.Contains(containerName))
                continue;

            var recreated = await RecreateAsync(snap, broker, addresses.Value, removeVolume: false, ct);
            if (!recreated.IsSuccess)
                return Fail(cluster, recreated.Error!, "recreate-container");
        }

        // 2) Молчание дольше NodeDeadSec → UNREACHABLE + пересоздание с чистым томом.
        foreach (var (brokerName, since) in track)
        {
            if (now - since <= options.NodeDeadSec)
                continue; // молчание ещё в пределах NodeDeadSec — терпим

            var broker = snap.Brokers.FirstOrDefault(b => b.Name == brokerName);
            if (broker is null || !Supervisable(snap).Contains(broker))
                continue;

            var marked = await PutAsync(BrokerStateKey(cluster, brokerName), "UNREACHABLE", ct);
            if (!marked.IsSuccess)
                return Fail(cluster, marked.Error!, "mark-unreachable");

            // RF=1: чистый том = потеря единственной копии — journal-warning
            // (документированное поведение, arch/16 §5 C).
            string? warning = snap.Config.ReplicationFactor <= 1
                ? $"брокер {brokerName}: RF=1, пересоздание с чистым томом — данные кластера потеряны"
                : null;

            var recreated = await RecreateAsync(snap, broker, addresses.Value, removeVolume: true, ct);
            if (!recreated.IsSuccess)
                return Fail(cluster, recreated.Error!, "recreate-unreachable");

            if (warning is not null)
                warnings.Add(warning);

            track.Remove(brokerName); // пересоздан — счётчик молчания заново
        }

        await journal.WriteSupervisionAsync(
            cluster, claims.InstanceId, track,
            warnings.Count == 0 ? null : string.Join("; ", warnings), ct);
        return Result.Success();
    }

    // Надзору подвластны только стабильные ноды (границы arch/16 §5 C).
    private static List<KafkaBrokerDecl> Supervisable(KafkaClusterSnapshot snap)
        => snap.Brokers
            .Where(b => b.State is null or "RUNNING" or "UNREACHABLE")
            .ToList();

    private async Task<Result> RecreateAsync(
        KafkaClusterSnapshot snap,
        KafkaBrokerDecl broker,
        IReadOnlyDictionary<string, NodeAddress> addresses,
        bool removeVolume,
        CancellationToken ct)
    {
        var cluster = snap.Cluster;

        // Пересоздание: снести (том — по флагу) и поднять заново с тем же
        // детерминированным env (том/адреса из portalloc — advertised стабилен).
        if (removeVolume)
        {
            var removed = await driver.RemoveNodeAsync(cluster, broker.Name, removeVolume: true, ct);
            if (!removed.IsSuccess)
                return removed;
        }

        var ensured = await EnsureNodeAsync(snap, broker, addresses, ct);
        if (!ensured.IsSuccess)
            return ensured;

        // Контейнер пересоздан: PROVISIONING; в RUNNING переведёт следующий
        // цикл по факту готовности (надзор не пишет RUNNING по факту контейнера).
        var marked = await PutAsync(BrokerStateKey(cluster, broker.Name), "PROVISIONING", ct);
        if (!marked.IsSuccess)
            return marked;

        return Result.Success();
    }

    // Пересборка KafkaNodeSpec (env из NodeEnvBuilder — тот же детерминизм, что
    // в provisioning; дублирование с K3 осознанное — supervisor самодостаточен).
    private async Task<Result> EnsureNodeAsync(
        KafkaClusterSnapshot snap, KafkaBrokerDecl broker,
        IReadOnlyDictionary<string, NodeAddress> addresses, CancellationToken ct)
    {
        var cluster = snap.Cluster;
        if (!addresses.TryGetValue(broker.Name, out var addr))
            return Result.Failed(new ApplicationException(
                $"supervise {cluster}: broker {broker.Name} не закреплён в portalloc"));

        if (snap.AppUser is null || snap.AppPassword is null)
            return Result.Failed(new ApplicationException(
                $"supervise {cluster}: нет app-кредов (ensure не выполнен)"));

        var controllers = snap.Brokers
            .Where(b => b.Role == "controller")
            .OrderBy(b => b.Name, StringComparer.Ordinal)
            .Select(b => $"{NodeId(b.Name)}@{b.Name}:9093")
            .ToList();
        var advertisedClient = $"{options.AdvertisedClientHost ?? addr.Host}:{addr.ClientPort}";

        var env = NodeEnvBuilder.Build(new NodeEnvSpec(
            cluster,
            NodeId(broker.Name),
            broker.Name,
            advertisedClient,
            broker.Role == "controller",
            controllers,
            snap.AppUser,
            [snap.AppPassword],
            snap.Config,
            snap.Config.Brokers,
            "/var/lib/kafka/data"));

        return await driver.EnsureNodeAsync(new KafkaNodeSpec(
            cluster, broker.Name, addr.Host, addr.ClientPort, options.NodeImage, env,
            broker.Resources?.Cpu,
            broker.Resources is null ? null : broker.Resources.MemGi * 1024L * 1024 * 1024), ct);
    }

    // Живые брокеры кластера по NodeId (AdminClient-проба).
    private async Task<Result<HashSet<int>?>> DescribeAliveAsync(KafkaClusterSnapshot snap, CancellationToken ct)
    {
        if (snap.Endpoints is null || snap.AppUser is null || snap.AppPassword is null)
            return Result<HashSet<int>?>.Success(null); // кластер ещё не поднят — проб невозможен

        await using var admin = adminFactory.Create(snap.Endpoints, snap.AppUser, snap.AppPassword);
        var view = await admin.DescribeClusterAsync(ct);
        if (!view.IsSuccess)
            return Result<HashSet<int>?>.Success(null); // кластер целиком недоступен — молчание трекается по всем

        return Result<HashSet<int>?>.Success(view.Value.Brokers.Select(b => b.Id).ToHashSet());
    }

    private static int NodeId(string nodeName)
        => int.TryParse(nodeName["broker".Length..], NumberStyles.Integer, CultureInfo.InvariantCulture, out var id)
            ? id
            : 0;

    private Result Fail(string cluster, Exception error, string phase)
    {
        journal.WriteAsync(cluster, Op, phase, claims.InstanceId, error.Message, CancellationToken.None)
            .GetAwaiter().GetResult();
        return Result.Failed(error);
    }

    private async Task<Result<IReadOnlyDictionary<string, NodeAddress>>> ReadPortAllocAsync(
        string cluster, CancellationToken ct)
    {
        var result = await GetAsync($"/kafkaworker/portalloc/{cluster}", ct);
        if (!result.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(result.Error!);
        if (result.Value is not { } kv)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(
                (IReadOnlyDictionary<string, NodeAddress>)new Dictionary<string, NodeAddress>());

        var addresses = new Dictionary<string, NodeAddress>();
        using var doc = JsonDocument.Parse(kv.Value);
        foreach (var node in doc.RootElement.EnumerateObject())
            addresses[node.Name] = new NodeAddress(
                node.Value.GetProperty("host").GetString()!,
                node.Value.GetProperty("client").GetInt32());
        return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(addresses);
    }

    private static string BrokerStateKey(string cluster, string broker)
        => $"/kafka/clusters/{cluster}/brokers/{broker}/state";

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
}
