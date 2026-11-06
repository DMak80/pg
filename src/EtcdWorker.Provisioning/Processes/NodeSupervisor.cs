using EtcdWorker.Core;
using EtcdWorker.Docker.Drivers;
using EtcdWorker.Etcd;
using Shared.Etcd.Client;

namespace EtcdWorker.Provisioning.Processes;

/// <summary>Надзор managed-узлов (arch/etcdworker/03-node-supervision.md): health-пробы
/// клиентской грани (интервал HealthIntervalSec), трек ПЕРВОГО наблюдения
/// проблемы члена (unstarted ИЛИ health-молчание — метка для
/// DetectDeadReplacement; восстановление — сброс), state=UNREACHABLE при
/// молчании свыше NodeDeadSec; один repair-шаг за тик (контейнер отсутствует
/// → пересоздание с закреплённым портом; расхождение client-порта →
/// remove+recreate, volume жив); orphan-cleanup — объекты etcdw-node-* без
/// члена в составе. Слепая docker-проба — никаких действий.</summary>
public sealed class NodeSupervisor(
    IEtcdGateway gateway,
    string[] endpoints,
    EtcdWorkJournal journal,
    IEtcdNodeDriver driver,
    EtcdNodeProvisioner provisioner,
    EtcdPortAllocStore portAlloc,
    ClaimStore claims,
    EtcdProvisioningOptions options,
    TimeProvider clock)
{
    // Метки ПЕРВОГО наблюдения проблемы (k → unix) и последней пробы.
    private readonly Dictionary<int, long> _problemSince = [];
    private readonly Dictionary<int, long> _lastProbed = [];

    /// <summary>Наблюдение: факты проблем для MembershipProcess + актуализация
    /// state-листов (RUNNING/UNREACHABLE). Пробы — не чаще HealthIntervalSec.
    /// Проба — исполненная итерация наблюдения: progress?.Mark() между узлами
    /// (серия drop-таймаутящихся проб не должна выглядеть зависанием цикла —
    /// инвариант поллинга долгих фаз, arch/pgworker/08-operability.md §3).</summary>
    public async Task<Result<IReadOnlyList<ManagedDeadFact>>> ObserveAsync(
        IReadOnlyList<ManagedMember> managed,
        IReadOnlyList<EtcdNodesParser.ManagedNodeState> nodes,
        IReadOnlyDictionary<int, EtcdPortAllocStore.NodePort> ports,
        CancellationToken ct,
        Shared.Core.Hosting.ILoopProgress? progress = null)
    {
        var now = clock.GetUtcNow().ToUnixTimeSeconds();
        var nodeByK = nodes.ToDictionary(n => n.K);
        var facts = new List<ManagedDeadFact>();
        foreach (var m in managed)
        {
            var started = m.Member.Name is { Length: > 0 };
            var healthy = false;
            if (ports.TryGetValue(m.K, out var port))
            {
                var gateOk = !_lastProbed.TryGetValue(m.K, out var probed) || now - probed >= options.HealthIntervalSec;
                if (!gateOk)
                {
                    // Гейт пробы закрыт (проба была недавно): данных нет — нет
                    // факта, наблюдение узла пропускается (ProblemSince не
                    // фиксируется: «молчит дольше NodeDeadSec» честнее без
                    // фантомных проблем, arch/etcdworker/03-node-supervision.md).
                    continue;
                }

                _lastProbed[m.K] = now;
                var status = await gateway.StatusAsync($"http://{ProbeHost(port.Host)}:{port.Client}", ct);
                healthy = status.IsSuccess;
            }

            if (started && healthy)
            {
                _problemSince.Remove(m.K);
                if (nodeByK.TryGetValue(m.K, out var node) && node.State == ManagedNodeStates.Unreachable)
                    await PutStateAsync(m.K, ManagedNodeStates.Running, ct);
                continue;
            }

            // Проблема (unstarted ИЛИ молчание): фиксируем ПЕРВОЕ наблюдение.
            if (!_problemSince.TryGetValue(m.K, out var since))
            {
                since = now;
                _problemSince[m.K] = since;
            }

            // Окружение: контейнер узла отсутствует/не running?
            var inspect = await driver.InspectNodeAsync(m.K, ct);
            var environmentMissing = !inspect.IsSuccess || inspect.Value is not { Running: true };

            if (now - since > options.NodeDeadSec
                && nodeByK.TryGetValue(m.K, out var node2)
                && node2.State is ManagedNodeStates.Running or ManagedNodeStates.Unreachable)
                await PutStateAsync(m.K, ManagedNodeStates.Unreachable, ct);

            facts.Add(new ManagedDeadFact(m, since, environmentMissing));
            progress?.Mark();
        }

        return Result<IReadOnlyList<ManagedDeadFact>>.Success(facts);
    }

    /// <summary>Repair (один шаг за тик): заявленный узел без живого контейнера
    /// — пересоздание (volume жив, порт закреплён); расхождение client-порта
    /// контейнера — remove + recreate.</summary>
    public async Task<Result> RepairAsync(
        IReadOnlyList<EtcdNodesParser.ManagedNodeState> nodes,
        IReadOnlyDictionary<int, EtcdPortAllocStore.NodePort> ports,
        CancellationToken ct)
    {
        // Состав перечитывается НА МОМЕНТ решения (как OrphanCleanupAsync):
        // снимок начала тика устаревает, если доводка ЭТОГО же тика вывела
        // члена (перебор/замена) — repair по старому списку «воскрешал» узел
        // (state-ключ + контейнер выведенного члена). Узел без члена не
        // трогаем — мусорщик снесёт (идемпотентность по факту, arch/etcdworker/06-operability.md §3).
        var members = await MemberListFailoverAsync(ct);
        if (!members.IsSuccess)
            return Result.Failed(members.Error!);
        var (managed, _) = EtcdContourMembers.Build(members.Value);
        var managedKs = managed.Select(m => m.K).ToHashSet();

        foreach (var node in nodes)
        {
            if (node.State is not (ManagedNodeStates.Running or ManagedNodeStates.Unreachable))
                continue;
            if (!managedKs.Contains(node.K) || !ports.TryGetValue(node.K, out var port))
                continue;

            var inspect = await driver.InspectNodeAsync(node.K, ct);
            if (!inspect.IsSuccess)
                return Result.Failed(inspect.Error!);
            var expectedPort = port.Client;
            if (inspect.Value is { Running: true, ClientHostPort: { } actualPort } && actualPort == expectedPort)
                continue; // жив и совпадает — ничего не делаем

            if (inspect.Value is not null && inspect.Value.ClientHostPort != expectedPort)
            {
                // Порт разошёлся (чужая публикация) — пересоздать контейнер.
                var removed = await driver.RemoveNodeAsync(node.K, removeVolume: false, ct);
                if (!removed.IsSuccess)
                    return Result.Failed(removed.Error!);
            }

            await PutStateAsync(node.K, ManagedNodeStates.Provisioning, ct);
            var initialCluster = EtcdNodeSpecBuilder.InitialCluster(members.Value, node.K);
            var (cmd, env) = EtcdNodeSpecBuilder.Build(node.K, options, expectedPort, initialCluster);
            var spec = new EtcdNodeSpec(
                node.K, port.Host, expectedPort, options.Image, cmd, env, options.Network);
            var ensured = await provisioner.EnsureNodeAsync(spec, options.AdvertisedHost, ct);
            if (!ensured.IsSuccess)
                return Result.Failed(ensured.Error!);
            await PutStateAsync(node.K, ManagedNodeStates.Running, ct);
            return Result.Success(); // один repair-шаг за тик
        }

        return Result.Success();
    }

    /// <summary>Orphan-cleanup: объекты etcdw-node-* (label воркера), чей номер
    /// не имеет члена в составе — контейнер/сервис + том сносятся. Состав
    /// перечитывается НА МОМЕНТ решения: снимок начала тика устаревает, если
    /// доводка этого же тика добавила члена (свежеподнятый узел сносился как
    /// сирота — bootstrap-дедлок кворума); нечитаемый состав — мусорщик
    /// молчит (снос без факта небезопасен).</summary>
    public async Task<Result> OrphanCleanupAsync(CancellationToken ct)
    {
        var objects = await driver.ListNodeObjectsAsync(ct);
        if (!objects.IsSuccess)
            return Result.Failed(objects.Error!);
        var nodeObjects = objects.Value
            .Where(o => o.StartsWith("etcdw-node-", StringComparison.Ordinal))
            .ToList();
        if (nodeObjects.Count == 0)
            return Result.Success();

        var members = await MemberListFailoverAsync(ct);
        if (!members.IsSuccess)
            return Result.Failed(members.Error!);
        var (fresh, _) = EtcdContourMembers.Build(members.Value);
        var managedKs = fresh.Select(m => m.K).ToHashSet();
        foreach (var name in nodeObjects)
        {
            // etcdw-node-<k> | etcdw-node-<k>-data
            var suffix = name["etcdw-node-".Length..];
            var kPart = suffix.EndsWith("-data", StringComparison.Ordinal) ? suffix[..^5] : suffix;
            if (!int.TryParse(kPart, out var k) || k <= 0 || managedKs.Contains(k))
                continue;
            await journal.WriteAsync("membership", "orphan-cleanup", claims.InstanceId, null, ct);
            var removed = await driver.RemoveNodeAsync(k, removeVolume: true, ct);
            if (!removed.IsSuccess)
                return Result.Failed(removed.Error!);
            await portAlloc.ReleaseAsync(k, ct);
        }

        return Result.Success();
    }

    // Member list failover-перебором endpoints (состав на момент решения).
    private async Task<Result<IReadOnlyList<Shared.Etcd.Client.EtcdMember>>> MemberListFailoverAsync(
        CancellationToken ct)
    {
        Result<IReadOnlyList<Shared.Etcd.Client.EtcdMember>> last = default;
        foreach (var endpoint in endpoints)
        {
            var list = await gateway.MemberListAsync(endpoint, ct);
            if (list.IsSuccess)
                return list;
            last = list;
        }

        return Result<IReadOnlyList<Shared.Etcd.Client.EtcdMember>>.Failed(
            last.Error ?? new EtcdUnreachableException("etcd endpoints are not set"));
    }

    // Хост пробы клиентской грани — host размещения из portalloc (arch/etcdworker/03-node-supervision.md;
    // мультихост-канон: проба адресует хост размещения узла, а не окружение
    // самого воркера). NodePort.Host пишет AllocateAsync при закреплении порта
    // (Plain — имя хоста таблицы, Swarm — swarm-нода); пустой хост и
    // стендовое имя "local" — локальный docker-демон → localhost.
    private static string ProbeHost(string? portAllocHost)
        => portAllocHost is { Length: > 0 } host && !string.Equals(host, "local", StringComparison.Ordinal)
            ? host
            : "localhost";

    private async Task PutStateAsync(int k, string state, CancellationToken ct)
    {
        foreach (var endpoint in endpoints)
        {
            var put = await gateway.PutAsync(endpoint, EtcdDomainKeys.StateKey(k), $"\"{state}\"", lease: null, ct);
            if (put.IsSuccess)
                return;
        }
    }
}
