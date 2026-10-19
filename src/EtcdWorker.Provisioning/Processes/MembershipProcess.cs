using EtcdWorker.Core;
using EtcdWorker.Docker.Drivers;
using EtcdWorker.Etcd;
using Shared.Etcd.Client;

namespace EtcdWorker.Provisioning.Processes;

/// <summary>Доводка состава managed-узлов etcd (arch/25 §3.1, шаг 12 t32):
/// недобор декларации /etcd/config → add-learner → provision → promote;
/// перебор → вывод старейшего не-лидера (кворум-гвард ≥3 после вывода);
/// авто-замена мёртвого члена по фактам supervisor (порог NodeDeadSec, не
/// лидер). Каждый шаг перепроверяет факт (идемпотентность); one-shot за тик:
/// одна структурная операция — следующий тик продолжает. Журнал фаз —
/// EtcdWorkJournal (add-learner/promoting/removing/replacing).</summary>
public sealed class MembershipProcess(
    Shared.Etcd.Client.IEtcdGateway gateway,
    string[] endpoints,
    EtcdWorkJournal journal,
    IEtcdNodeDriver driver,
    EtcdPortAllocStore portAlloc,
    EtcdNodeProvisioner provisioner,
    ClaimStore claims,
    EtcdProvisioningOptions options)
{
    private const string Op = "membership";

    /// <summary>Тик доводки (только лидер ReconcileLoop): deadFacts —
    /// наблюдения supervisor (шаг 13). Возвращает transient-Failed —
    /// следующий тик продолжает.</summary>
    public async Task<Result> TickAsync(
        IReadOnlyList<ManagedDeadFact> deadFacts, CancellationToken ct)
    {
        // Кворум-гейт: без лидера состав не трогаем (наблюдение кластером).
        var status = await StatusFailoverAsync(ct);
        if (!status.IsSuccess)
            return Result.Failed(status.Error!);
        var leaderId = status.Value.LeaderMemberId;

        var members = await MemberListFailoverAsync(ct);
        if (!members.IsSuccess)
            return Result.Failed(members.Error!);
        var (managed, external) = EtcdContourMembers.Build(members.Value);

        // Декларация: нет/битая — только наблюдение (надзор без структурных).
        var configKv = await GetFailoverAsync(EtcdDomainKeys.ConfigKey, ct);
        if (!configKv.IsSuccess)
            return Result.Failed(configKv.Error!);
        var config = configKv.Value is { } kv ? EtcdConfigJson.Parse(kv.Value) : null;
        if (config is null)
            return Result.Success();

        var nodes = await ReadNodesAsync(ct);

        // In-progress resume: PROVISIONING-узел с членом — доводим promote
        // (member добавлен, контейнер поднимается/жив — promote ретраится
        // тиками до синхронизации).
        var inProgress = managed.FirstOrDefault(m =>
            nodes.FirstOrDefault(n => n.K == m.K) is { State: not null } node
            && node.State == ManagedNodeStates.Provisioning);
        if (inProgress is { } resuming)
            return await PromoteAsync(resuming, ct);

        // Авто-замена: мёртвый член (порог NodeDeadSec, окружение не
        // восстановлено, не лидер) → вывод; недобор доберёт следующим тиком.
        var dead = MembershipDecisions.DetectDeadReplacement(
            deadFacts, options.NodeDeadSec, leaderId,
            DateTimeOffset.UtcNow.ToUnixTimeSeconds());
        if (dead is { } replacing)
            return await RemoveAsync(replacing.K, replacing.Member.Id, "replacing", removeVolume: true, ct);

        // Перебор: вывод старейшего не-лидера под кворум-гвардом (итог =
        // managed + внешние − выводимый ≥ 3, arch/25 §1.1).
        if (managed.Count > config.Nodes)
        {
            var candidate = MembershipDecisions.PickRemoveCandidate(managed, leaderId);
            if (candidate is { } over
                && MembershipDecisions.QuorumRemoveGuard(managed.Count + external.Count - 1))
                return await RemoveAsync(over.K, over.Member.Id, "removing", removeVolume: true, ct);
        }

        // Недобор: доводим состав до декларации (add-learner → provision →
        // promote; promote-in-sync — уже этим же вызовом).
        if (managed.Count < config.Nodes)
            return await AddAsync(members.Value, managed, nodes, ct);

        return Result.Success();
    }

    private async Task<Result> AddAsync(
        IReadOnlyList<Shared.Etcd.Client.EtcdMember> allMembers,
        IReadOnlyList<ManagedMember> managed,
        IReadOnlyList<EtcdNodesParser.ManagedNodeState> nodes, CancellationToken ct)
    {
        var k = MembershipDecisions.NextNumber(
            managed.Select(m => m.K).Concat(nodes.Select(n => n.K)).ToList());

        // Placement: Plain — единственный хост таблицы; Swarm — анти-аффинити.
        string host;
        if (string.Equals(options.Mode, "Swarm", StringComparison.OrdinalIgnoreCase))
        {
            var swarmNodes = await driver.GetSwarmNodesAsync(ct);
            if (!swarmNodes.IsSuccess)
                return Result.Failed(swarmNodes.Error!);
            var placed = await PlacementFailoverAsync(swarmNodes.Value, nodes, ct);
            if (!placed.IsSuccess)
                return Result.Failed(placed.Error!);
            host = placed.Value;
        }
        else
        {
            host = options.PlainHost;
        }

        await journal.WriteAsync(Op, "add-learner", claims.InstanceId, null, ct);

        // Client-порт: portalloc RMW + docker-занятость.
        var busy = await driver.GetBusyPortsAsync(ct);
        if (!busy.IsSuccess)
            return Result.Failed(busy.Error!);
        var port = await portAlloc.AllocateAsync(
            k, host, options.PortFrom, options.PortTo, busy.Value, ct);
        if (!port.IsSuccess)
            return Result.Failed(port.Error!);

        // Листы узла (до member-add: узел виден в домене с самого начала).
        await PutFailoverAsync(EtcdDomainKeys.StateKey(k), $"\"{ManagedNodeStates.NotInitialized}\"", ct);
        await PutFailoverAsync(EtcdDomainKeys.PlacedOnKey(k), $"\"{host}\"", ct);

        // Член: learner по peer-URL имени сети (детект-паттерн).
        var added = await MemberFailoverAsync(
            (e, c) => gateway.MemberAddAsync(
                e, [$"http://etcdw-node-{k}:2380"], isLearner: true, c), ct);
        if (!added.IsSuccess)
        {
            await CleanupKeysAsync(k, ct);
            return Result.Failed(added.Error!);
        }

        await PutFailoverAsync(EtcdDomainKeys.StateKey(k), $"\"{ManagedNodeStates.Provisioning}\"", ct);

        // Контейнер (сеть контура, флаги EtcdNodeSpecBuilder, client-порт);
        // initial-cluster — полный состав для join (без него etcd не может
        // fetch cluster info при state=existing).
        var initialCluster = EtcdNodeSpecBuilder.InitialCluster(allMembers, k);
        var (cmd, env) = EtcdNodeSpecBuilder.Build(k, options.AdvertisedHost, port.Value.Client, initialCluster);
        var spec = new EtcdNodeSpec(
            k, host, port.Value.Client, options.Image, cmd, env, options.Network);
        var ensured = await provisioner.EnsureNodeAsync(spec, options.AdvertisedHost, ct);
        if (!ensured.IsSuccess)
        {
            // Бюджет исчерпан: откат члена, retry следующим тиком новым номером.
            await RemoveAsync(k, added.Value.Id, "removing", removeVolume: false, ct);
            return Result.Failed(ensured.Error!);
        }

        var promoted = await PromoteAsync(new ManagedMember(k, added.Value), ct);
        if (!promoted.IsSuccess)
            return promoted; // transient: promote-ретраи тиками (PROVISIONING)

        await journal.WriteAsync(Op, "done", claims.InstanceId, null, ct);
        return Result.Success();
    }

    // Promote learner → voting: журнал promoting; not-in-sync — transient
    // (узел остаётся PROVISIONING — следующий тик resume-веткой повторит).
    private async Task<Result> PromoteAsync(ManagedMember member, CancellationToken ct)
    {
        await journal.WriteAsync(Op, "promoting", claims.InstanceId, null, ct);
        var promoted = await MemberPromoteFailoverAsync(member.Member.Id, ct);
        if (!promoted.IsSuccess)
            return Result.Failed(promoted.Error!);
        await PutFailoverAsync(EtcdDomainKeys.StateKey(member.K), $"\"{ManagedNodeStates.Running}\"", ct);
        return Result.Success();
    }

    // Вывод члена + снос + чистка ключей/portalloc (идемпотентно по факту).
    private async Task<Result> RemoveAsync(int k, ulong memberId, string phase, bool removeVolume, CancellationToken ct)
    {
        await journal.WriteAsync(Op, phase, claims.InstanceId, null, ct);

        var removed = await MemberRemoveFailoverAsync(memberId, ct);
        if (!removed.IsSuccess)
            return Result.Failed(removed.Error!);

        var node = await driver.RemoveNodeAsync(k, removeVolume, ct);
        if (!node.IsSuccess)
            return Result.Failed(node.Error!);

        await CleanupKeysAsync(k, ct);
        var released = await portAlloc.ReleaseAsync(k, ct);
        if (!released.IsSuccess)
            return Result.Failed(released.Error!);

        await journal.WriteAsync(Op, "done", claims.InstanceId, null, ct);
        return Result.Success();
    }

    private async Task<Result<string>> PlacementFailoverAsync(
        IReadOnlyList<string> swarmNodes, IReadOnlyList<EtcdNodesParser.ManagedNodeState> nodes, CancellationToken ct)
    {
        var placedOn = nodes.Where(n => n.PlacedOn is { Length: > 0 }).Select(n => n.PlacedOn!).ToList();
        var portKvs = await portAlloc.ReadAllAsync(ct);
        if (portKvs.IsSuccess)
            placedOn.AddRange(portKvs.Value.Values.Select(v => v.Host));
        return PlacementPlanner.PickHost(swarmNodes, placedOn);
    }

    private async Task<IReadOnlyList<EtcdNodesParser.ManagedNodeState>> ReadNodesAsync(CancellationToken ct)
    {
        var range = await RangeFailoverAsync(EtcdDomainKeys.NodesPrefix, ct);
        return range.IsSuccess ? EtcdNodesParser.Parse(range.Value) : [];
    }

    private async Task CleanupKeysAsync(int k, CancellationToken ct)
    {
        await DeleteFailoverAsync(EtcdDomainKeys.StateKey(k), ct);
        await DeleteFailoverAsync(EtcdDomainKeys.PlacedOnKey(k), ct);
    }

    private async Task<Result<EtcdStatusPayload>> StatusFailoverAsync(CancellationToken ct)
    {
        Result<EtcdStatusPayload> last = default;
        foreach (var endpoint in endpoints)
        {
            var status = await gateway.StatusAsync(endpoint, ct);
            if (status.IsSuccess)
                return status;
            last = status;
        }

        return Result<EtcdStatusPayload>.Failed(
            last.Error ?? new EtcdUnreachableException("etcd endpoints are not set"));
    }

    private async Task<Result<IReadOnlyList<EtcdMember>>> MemberListFailoverAsync(CancellationToken ct)
    {
        Result<IReadOnlyList<EtcdMember>> last = default;
        foreach (var endpoint in endpoints)
        {
            var list = await gateway.MemberListAsync(endpoint, ct);
            if (list.IsSuccess)
                return list;
            last = list;
        }

        return Result<IReadOnlyList<EtcdMember>>.Failed(
            last.Error ?? new EtcdUnreachableException("etcd endpoints are not set"));
    }

    private async Task<Result<T>> MemberFailoverAsync<T>(
        Func<string, CancellationToken, Task<Result<T>>> call, CancellationToken ct)
    {
        Result<T> last = default;
        foreach (var endpoint in endpoints)
        {
            var result = await call(endpoint, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return Result<T>.Failed(
            last.Error ?? new EtcdUnreachableException("etcd endpoints are not set"));
    }

    private async Task<Result> MemberRemoveFailoverAsync(ulong memberId, CancellationToken ct)
    {
        Result last = default;
        foreach (var endpoint in endpoints)
        {
            var removed = await gateway.MemberRemoveAsync(endpoint, memberId, ct);
            if (removed.IsSuccess)
                return removed;
            last = removed;
        }

        return Result.Failed(
            last.Error ?? new EtcdUnreachableException("etcd endpoints are not set"));
    }

    private async Task<Result> MemberPromoteFailoverAsync(ulong memberId, CancellationToken ct)
    {
        Result last = default;
        foreach (var endpoint in endpoints)
        {
            var promoted = await gateway.MemberPromoteAsync(endpoint, memberId, ct);
            if (promoted.IsSuccess)
                return promoted;
            last = promoted;
        }

        return Result.Failed(
            last.Error ?? new EtcdUnreachableException("etcd endpoints are not set"));
    }

    private async Task<Result<Kv?>> GetFailoverAsync(string key, CancellationToken ct)
    {
        Result<Kv?> last = default;
        foreach (var endpoint in endpoints)
        {
            var got = await gateway.GetAsync(endpoint, key, ct);
            if (got.IsSuccess)
                return got;
            last = got;
        }

        return Result<Kv?>.Failed(
            last.Error ?? new EtcdUnreachableException("etcd endpoints are not set"));
    }

    private async Task<Result<IReadOnlyList<Kv>>> RangeFailoverAsync(string prefix, CancellationToken ct)
    {
        Result<IReadOnlyList<Kv>> last = default;
        foreach (var endpoint in endpoints)
        {
            var range = await gateway.RangeAsync(endpoint, prefix, ct);
            if (range.IsSuccess)
                return range;
            last = range;
        }

        return Result<IReadOnlyList<Kv>>.Failed(
            last.Error ?? new EtcdUnreachableException("etcd endpoints are not set"));
    }

    private async Task<Result> PutFailoverAsync(string key, string value, CancellationToken ct)
    {
        Result last = default;
        foreach (var endpoint in endpoints)
        {
            var put = await gateway.PutAsync(endpoint, key, value, lease: null, ct);
            if (put.IsSuccess)
                return put;
            last = put;
        }

        return Result.Failed(
            last.Error ?? new EtcdUnreachableException("etcd endpoints are not set"));
    }

    private async Task<Result> DeleteFailoverAsync(string key, CancellationToken ct)
    {
        Result last = default;
        foreach (var endpoint in endpoints)
        {
            var delete = await gateway.DeleteAsync(endpoint, key, prefix: false, ct);
            if (delete.IsSuccess)
                return delete;
            last = delete;
        }

        return Result.Failed(
            last.Error ?? new EtcdUnreachableException("etcd endpoints are not set"));
    }
}