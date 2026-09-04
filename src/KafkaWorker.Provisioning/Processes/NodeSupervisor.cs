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
/// Надзор нод Active-кластера (arch/16 §5 C, spec §4.2 C, порт NodeSupervisor
/// PgWorker): сверка декларации с фактом docker + AdminClient-проба. Снесённый
/// контейнер пересоздаётся (тот же volume/env — self-healing). Брокер молчит
/// дольше NodeDeadSec (по УСПЕШНОМУ ответу пробы) → state=UNREACHABLE +
/// пересоздание КОНТЕЙНЕРА с сохранением тома — данные неприкосновенны;
/// чистый том — только при доказанной физической утрате тома в docker
/// (RF=1 + утрата → journal-warning о потере единственной копии). Не более
/// ОДНОГО пересоздания по молчанию за тик (ждём возврата брокера в кластер).
/// Слепая проба (DescribeCluster недоступен / кластер не поднят) не стартует
/// и не исполняет бюджет молчания: пересоздания из-за собственной слепоты
/// воркера запрещены (потеря данных недопустима). Ноды TO_REMOVE/REMOVING/
/// PROVISIONING чужих процессов не трогаем. Вызывается только держателем
/// клэйма &lt;C&gt;.
/// </summary>
public sealed class NodeSupervisor(
    IEtcdGateway etcd,
    string[] endpoints,
    IClusterDriver driver,
    ClaimStore claims,
    WorkJournal journal,
    IKafkaAdminClientFactory adminFactory,
    ProvisioningOptions options,
    KafkaClusterBackoff? backoff = null,
    PortAllocHealer? healer = null)
{
    private const string Op = "supervise";

    private readonly KafkaClusterBackoff _backoff = backoff ?? new KafkaClusterBackoff(TimeProvider.System);
    private readonly PortAllocHealer? _healer = healer;

    public async Task<Result> RunAsync(KafkaClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Cluster;

        // Мутации — только держателем живого клэйма.
        if (!claims.IsMine(cluster))
            return Result.Failed(new ApplicationException(
                $"supervise {cluster}: клэйм не наш (или потерян) — мутации запрещены"));

        var pinned = await ReadPortAllocAsync(cluster, ct);
        if (!pinned.IsSuccess)
            return Result.Failed(pinned.Error!);

        // Лестница E9 (t05, spec §3.3): безадресные Supervisable-брокеры — до любых
        // деструктивных действий (RecreateAsync сносит контейнер ДО EnsureNode —
        // ветка «контейнер жив» из EnsureNode недостижима). Клэйм занят —
        // waiting-portalloc-lock (InProgress), следующий тик.
        var addresses = new Dictionary<string, NodeAddress>(pinned.Value);
        foreach (var broker in Supervisable(snap).Where(b => !addresses.ContainsKey(b.Name)).ToList())
        {
            if (_healer is null)
                return Fail(cluster, new ApplicationException(
                    $"supervise {cluster}: broker {broker.Name} не закреплён в portalloc (healer не сконфигурирован)"),
                    "healing-portalloc");

            var healed = await _healer.ResolveAsync(snap, broker.Name, addresses, ct);
            if (!healed.IsSuccess)
            {
                if (healed.Error is PortLockBusyException)
                    return await FinishWaitingPortLockAsync(cluster, ct);
                return Fail(cluster, healed.Error!, "healing-portalloc");
            }

            addresses[broker.Name] = healed.Value.Address;
            if (healed.Value.Recreated) // пересоздан в ветке 3 — state=PROVISIONING
            {
                var marked = await PutAsync(BrokerStateKey(cluster, broker.Name), "PROVISIONING", ct);
                if (!marked.IsSuccess)
                    return Fail(cluster, marked.Error!, "mark-provisioning");
            }
        }

        // Факт docker: имена живых объектов kfw-<C>-*.
        var objects = await driver.ListNodeObjectsAsync(cluster, ct);
        if (!objects.IsSuccess)
            return Result.Failed(objects.Error!);
        var alive = objects.Value.ToHashSet();

        // AdminClient-проба: кто реально в кластере (по NodeId). null = проба
        // недоступна (кластер целиком не отвечает или ещё не поднят) — слепота
        // пробы НЕ является молчанием брокеров: бюджет молчания не стартует и
        // не исполняется, прошлый трек сохраняется (данные неприкосновенны).
        var view = await DescribeAliveAsync(snap, ct);
        if (!view.IsSuccess)
            return Result.Failed(view.Error!);
        var probeBlind = view.Value is null;
        var inCluster = view.Value ?? [];
        var unreachableNow = probeBlind
            ? null
            : snap.Brokers
                .Where(b => b.State == "RUNNING")
                .Where(b => !inCluster.Contains(NodeId(b.Name)))
                .Select(b => b.Name)
                .ToList();

        // Трек молчания: journal.unreachable broker → first_seen (порт PgWorker).
        // Только УСПЕШНЫЙ ответ пробы «в кластере нет брокера X» начинает/держит
        // бюджет молчания X; при слепой пробе трек заморожен (чистка только по
        // исчезновению из декларации — «ожил» решит зрячая проба).
        var rawTrack = await journal.ReadUnreachableAsync(cluster, ct);
        if (!rawTrack.IsSuccess)
            return Result.Failed(rawTrack.Error!);
        var track = new Dictionary<string, long>(rawTrack.Value);
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        if (unreachableNow is not null)
        {
            foreach (var broker in unreachableNow)
                if (!track.ContainsKey(broker))
                    track[broker] = now;
            foreach (var stale in track.Keys
                         .Where(k => snap.Brokers.All(b => b.Name != k) || !unreachableNow.Contains(k))
                         .ToList())
                track.Remove(stale); // брокер ожил или исчез из декларации
        }
        else
        {
            foreach (var gone in track.Keys
                         .Where(k => snap.Brokers.All(b => b.Name != k))
                         .ToList())
                track.Remove(gone); // брокера нет в декларации — трек бессмысленен
        }

        // Warning-ы тика (RF=1-пересоздания) — в финальную supervision-запись.
        var warnings = new List<string>();

        // Перевод PROVISIONING → RUNNING по факту готовности (arch/16 §5 C: «в
        // RUNNING переводит следующий цикл по факту готовности»): контейнер жив
        // и зрячая проба видит брокера в кластере — PROVISIONING исчерпан.
        if (!probeBlind)
        {
            foreach (var broker in snap.Brokers.Where(b => b.State == "PROVISIONING"))
            {
                if (!alive.Contains($"kfw-{cluster}-{broker.Name}")
                    || !inCluster.Contains(NodeId(broker.Name)))
                    continue; // ещё грузится либо контейнера нет — не готов

                var running = await PutAsync(BrokerStateKey(cluster, broker.Name), "RUNNING", ct);
                if (!running.IsSuccess)
                    return Fail(cluster, running.Error!, "mark-running");
            }
        }

        // 1) Снесённые контейнеры → пересоздать (тот же volume/env).
        foreach (var broker in Supervisable(snap))
        {
            var containerName = $"kfw-{cluster}-{broker.Name}";
            if (alive.Contains(containerName))
                continue;

            var recreated = await RecreateAsync(snap, broker, addresses, removeVolume: false, ct);
            if (!recreated.IsSuccess)
                return Fail(cluster, recreated.Error!, "recreate-container");
        }

        // 2) Молчание дольше NodeDeadSec → UNREACHABLE + пересоздание
        // КОНТЕЙНЕРА. Том неприкосновенен: чистый том — только при доказанной
        // утрате тома в docker («не можем проверить» = том жив). Не более
        // ОДНОГО пересоздания за тик — ждём возврата брокера в кластер/ISR.
        // При слепой пробе секция не исполняется вовсе: собственная слепота
        // воркера — не повод пересоздавать брокеров.
        if (!probeBlind)
        {
            foreach (var (brokerName, since) in track
                         .OrderBy(t => t.Key, StringComparer.Ordinal)
                         .ToList()) // снимок: track.Remove внутри цикла
            {
                if (now - since <= options.NodeDeadSec)
                    continue; // молчание ещё в пределах NodeDeadSec — терпим

                var broker = snap.Brokers.FirstOrDefault(b => b.Name == brokerName);
                if (broker is null || !Supervisable(snap).Contains(broker))
                    continue;

                // Том жив, пока docker не докажет обратного (потеря данных
                // недопустима): проверка существования volume по имени.
                var volume = await driver.NodeVolumeExistsAsync(cluster, brokerName, ct);
                if (!volume.IsSuccess)
                    return Fail(cluster, volume.Error!, "check-volume");
                var removeVolume = !volume.Value; // чистый том — только утраченный

                var marked = await PutAsync(BrokerStateKey(cluster, brokerName), "UNREACHABLE", ct);
                if (!marked.IsSuccess)
                    return Fail(cluster, marked.Error!, "mark-unreachable");

                // RF=1 и том утрачен: чистый том = потеря единственной копии —
                // journal-warning (документированное поведение, arch/16 §5 C).
                string? warning = removeVolume && snap.Config.ReplicationFactor <= 1
                    ? $"брокер {brokerName}: том данных утрачен, RF=1 — единственная копия данных кластера потеряна"
                    : null;

                var recreated = await RecreateAsync(snap, broker, addresses, removeVolume, ct);
                if (!recreated.IsSuccess)
                    return Fail(cluster, recreated.Error!, "recreate-unreachable");

                if (warning is not null)
                    warnings.Add(warning);

                track.Remove(brokerName); // пересоздан — счётчик молчания заново
                break; // одно пересоздание по молчанию за тик
            }
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

        // Пересоздание: снести контейнер (том — по флагу; при молчании
        // контейнер ещё жив — его нужно снять перед пересозданием) и поднять
        // заново с тем же детерминированным env (адреса из portalloc —
        // advertised стабилен). 404 на удалении — успех (движок).
        var removed = await driver.RemoveNodeAsync(cluster, broker.Name, removeVolume, ct);
        if (!removed.IsSuccess)
            return removed;

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

        // Backoff недоступного кластера (t05, spec §3.2): окно активно — проба не
        // ходит в сеть (слепая проба без клиента; бюджет молчания не стартует,
        // unreachable-трек заморожен — флап ≠ смерть). Фейл — растит окно, успех —
        // сбрасывает (надзор — первый kafka-контакт конвейера).
        if (_backoff.IsBlocked(snap.Cluster))
            return Result<HashSet<int>?>.Success(null);

        await using var admin = adminFactory.Create(snap.Endpoints, snap.AppUser, snap.AppPassword);
        var view = await admin.DescribeClusterAsync(ct);
        if (!view.IsSuccess)
        {
            _backoff.RecordFailure(snap.Cluster, view.Error!.Message);
            return Result<HashSet<int>?>.Success(null); // кластер целиком недоступен — молчание трекается по всем
        }

        _backoff.RecordSuccess(snap.Cluster);
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

    // Клэйм portalloc занят другим — InProgress-семантика supervise: тик без
    // ошибки (фаза waiting-portalloc-lock в журнале), следующий тик повторит
    // (порт ProvisioningProcess K1 / AddBrokerProcess).
    private async Task<Result> FinishWaitingPortLockAsync(string cluster, CancellationToken ct)
        => await journal.WriteAsync(cluster, Op, "waiting-portalloc-lock", claims.InstanceId, null, ct);

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
