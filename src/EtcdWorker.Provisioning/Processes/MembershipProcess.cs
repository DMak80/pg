using EtcdWorker.Core;
using EtcdWorker.Docker.Drivers;
using EtcdWorker.Etcd;
using Shared.Etcd.Client;
using Shared.Etcd.Coordination;

namespace EtcdWorker.Provisioning.Processes;

/// <summary>Доводка состава managed-узлов etcd (arch/etcdworker/02-membership.md): недобор
/// декларации /etcd/config → add-learner → provision → promote; перебор →
/// вывод старейшего не-лидера (кворум-гвард ≥3 после вывода); авто-замена
/// мёртвого члена по фактам supervisor (порог NodeDeadSec, не лидер). Каждый
/// шаг перепроверяет факт (идемпотентность); one-shot за тик: одна
/// структурная операция — следующий тик продолжает. Журнал фаз —
/// EtcdWorkJournal (add-learner/promoting/removing/replacing).</summary>
public sealed class MembershipProcess(
    Shared.Etcd.Client.IEtcdGateway gateway,
    string[] endpoints,
    EtcdWorkJournal journal,
    IEtcdNodeDriver driver,
    EtcdPortAllocStore portAlloc,
    EtcdNodeProvisioner provisioner,
    ClaimStore claims,
    EtcdProvisioningOptions options,
    PortAllocLock portLock)
{
    private const string Op = "membership";

    /// <summary>Тик доводки (только лидер ReconcileLoop): deadFacts —
    /// наблюдения supervisor. Возвращает transient-Failed —
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

        // In-progress resume: PROVISIONING-узел с членом — доводим контейнер
        // (краш-окно могло оставить узел без него), затем promote (member
        // добавлен — promote ретраится тиками до синхронизации).
        var inProgress = managed.FirstOrDefault(m =>
            nodes.FirstOrDefault(n => n.K == m.K) is { State: not null } node
            && node.State == ManagedNodeStates.Provisioning);
        if (inProgress is { } resuming)
            return await ResumeAsync(resuming, ct);

        // Авто-замена: мёртвый член (порог NodeDeadSec, окружение не
        // восстановлено, не лидер) → вывод; недобор доберёт следующим тиком.
        // Гейт здорового кворума цикла — лидер жив; ≥3-гвард перебора к
        // замене НЕ применяется: замена сохраняет итоговый состав, окно с
        // меньшим числом voting в 3-членном контуре — принятый риск R1
        // (мёртвый член не голосует, arch/etcdworker/02-membership.md).
        var dead = MembershipDecisions.DetectDeadReplacement(
            deadFacts, options.NodeDeadSec, leaderId,
            DateTimeOffset.UtcNow.ToUnixTimeSeconds());
        if (dead is { } replacing)
            return await RemoveAsync(replacing.K, replacing.Member.Id, "replacing", removeVolume: true, ct);

        // Перебор: вывод старейшего не-лидера под кворум-гвардом (итог =
        // managed + внешние − выводимый ≥ 3, arch/etcdworker/05-api.md §1).
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

        // Состав сошёлся — reconcile-чистка осиротевших ключей (takeover-
        // доводка): листы /etcd/nodes/<k>/* и записи portalloc без члена в
        // составе и без docker-объекта — краш-окно вывода не оставляет
        // вечных ключей.
        return await CleanupOrphanKeysAsync(nodes, ct);
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

        // Глобальный portalloc-клэйм (arch/etcdworker/02-membership.md/§5, паттерн t90): окно
        // аллокации client-порта + member-add; клэйм у соседнего инстанса —
        // PortLockBusyException → transient-отказ тика (ретрай следующим).
        var acquired = await portLock.TryAcquireAsync(ct);
        if (!acquired.IsSuccess)
            return Result.Failed(acquired.Error!);
        if (!acquired.Value)
            return Result.Failed(new PortLockBusyException(portLock.Key));
        EtcdPortAllocStore.NodePort port;
        Shared.Etcd.Client.EtcdMember addedMember;
        try
        {
            // Client-порт: portalloc RMW + docker-занятость.
            var busy = await driver.GetBusyPortsAsync(ct);
            if (!busy.IsSuccess)
                return Result.Failed(busy.Error!);
            var allocated = await portAlloc.AllocateAsync(
                k, host, options.PortFrom, options.PortTo, busy.Value, ct);
            if (!allocated.IsSuccess)
                return Result.Failed(allocated.Error!);
            port = allocated.Value;

            // Листы узла (до member-add: узел виден в домене с самого начала);
            // отказ записи — transient-отказ тика (иначе узел застрял бы в
            // NOT_INITIALIZED: resume матчит только PROVISIONING).
            var statePut = await PutFailoverAsync(EtcdDomainKeys.StateKey(k), $"\"{ManagedNodeStates.NotInitialized}\"", ct);
            if (!statePut.IsSuccess)
                return Result.Failed(statePut.Error!);
            var placedPut = await PutFailoverAsync(EtcdDomainKeys.PlacedOnKey(k), $"\"{host}\"", ct);
            if (!placedPut.IsSuccess)
                return Result.Failed(placedPut.Error!);

            // Член: learner по peer-URL имени сети (детект-паттерн).
            var added = await MemberFailoverAsync(
                (e, c) => gateway.MemberAddAsync(
                    e, [$"http://etcdw-node-{k}:2380"], isLearner: true, c), ct);
            if (!added.IsSuccess)
            {
                // Откат: члена нет — порт не закреплён за узлом (без снятия
                // утекал бы навсегда), листы чистим.
                await portAlloc.ReleaseAsync(k, ct);
                await CleanupKeysAsync(k, ct);
                return Result.Failed(added.Error!);
            }

            addedMember = added.Value;
        }
        finally
        {
            await portLock.ReleaseAsync();
        }

        var provisioningPut = await PutFailoverAsync(EtcdDomainKeys.StateKey(k), $"\"{ManagedNodeStates.Provisioning}\"", ct);
        if (!provisioningPut.IsSuccess)
            return Result.Failed(provisioningPut.Error!);

        // Контейнер (сеть контура, флаги EtcdNodeSpecBuilder, client-порт);
        // initial-cluster — полный состав для join (без него etcd не может
        // fetch cluster info при state=existing).
        var initialCluster = EtcdNodeSpecBuilder.InitialCluster(allMembers, k);
        var (cmd, env) = EtcdNodeSpecBuilder.Build(k, options.AdvertisedHost, port.Client, initialCluster);
        var spec = new EtcdNodeSpec(
            k, host, port.Client, options.Image, cmd, env, options.Network);
        var ensured = await provisioner.EnsureNodeAsync(spec, options.AdvertisedHost, ct);
        if (!ensured.IsSuccess)
        {
            // Бюджет исчерпан: откат члена, retry следующим тиком новым номером.
            await RemoveAsync(k, addedMember.Id, "removing", removeVolume: false, ct);
            return Result.Failed(ensured.Error!);
        }

        var promoted = await PromoteAsync(new ManagedMember(k, addedMember), ct);
        if (!promoted.IsSuccess)
            return promoted; // transient: promote-ретраи тиками (PROVISIONING)

        await journal.WriteAsync(Op, "done", claims.InstanceId, null, ct);
        return Result.Success();
    }

    // Takeover-доводка PROVISIONING-узла (arch/etcdworker/02-membership.md): контейнер мог не
    // доехать (краш-окно после member-add до ensure) — доводим его путём
    // добавления (порт — из portalloc, закреплён с member-add), затем promote.
    private async Task<Result> ResumeAsync(ManagedMember member, CancellationToken ct)
    {
        var inspect = await driver.InspectNodeAsync(member.K, ct);
        if (!inspect.IsSuccess)
            return Result.Failed(inspect.Error!);
        if (inspect.Value is not { Running: true })
        {
            var ports = await portAlloc.ReadAllAsync(ct);
            if (!ports.IsSuccess)
                return Result.Failed(ports.Error!);
            if (!ports.Value.TryGetValue(member.K, out var port))
                return Result.Failed(new ApplicationException(
                    $"etcdw-node-{member.K}: PROVISIONING member without portalloc entry"));
            var members = await MemberListFailoverAsync(ct);
            if (!members.IsSuccess)
                return Result.Failed(members.Error!);
            var host = port.Host is { Length: > 0 } h ? h : options.PlainHost;
            var initialCluster = EtcdNodeSpecBuilder.InitialCluster(members.Value, member.K);
            var (cmd, env) = EtcdNodeSpecBuilder.Build(member.K, options.AdvertisedHost, port.Client, initialCluster);
            var spec = new EtcdNodeSpec(
                member.K, host, port.Client, options.Image, cmd, env, options.Network);
            var ensured = await provisioner.EnsureNodeAsync(spec, options.AdvertisedHost, ct);
            if (!ensured.IsSuccess)
                return Result.Failed(ensured.Error!);
        }

        return await PromoteAsync(member, ct);
    }

    // Promote learner → voting: журнал promoting; already-voting (краш-окно
    // promote→RUNNING: член повышен, state-лист не дописан) — сразу RUNNING;
    // not-in-sync — transient (узел остаётся PROVISIONING — следующий тик
    // resume-веткой повторит).
    private async Task<Result> PromoteAsync(ManagedMember member, CancellationToken ct)
    {
        // Факт по свежему составу: promote уже-voting члена etcd 3.5 отвергает
        // перманентно («can only promote a learner») — сверка ДО вызова
        // закрывает краш-окно (идемпотентность по факту).
        var members = await MemberListFailoverAsync(ct);
        if (!members.IsSuccess)
            return Result.Failed(members.Error!);
        var fresh = members.Value.FirstOrDefault(m => m.Id == member.Member.Id);
        if (fresh is null)
            return Result.Failed(new ApplicationException(
                $"etcdw-node-{member.K}: PROVISIONING member {member.Member.Id} vanished from member list"));
        if (!fresh.IsLearner)
        {
            var caughtUp = await PutFailoverAsync(
                EtcdDomainKeys.StateKey(member.K), $"\"{ManagedNodeStates.Running}\"", ct);
            if (!caughtUp.IsSuccess)
                return Result.Failed(caughtUp.Error!);
            await journal.WriteAsync(Op, "done", claims.InstanceId, null, ct);
            return Result.Success();
        }

        await journal.WriteAsync(Op, "promoting", claims.InstanceId, null, ct);
        var promoted = await MemberPromoteFailoverAsync(member.Member.Id, ct);
        if (!promoted.IsSuccess)
            return Result.Failed(promoted.Error!);
        var put = await PutFailoverAsync(
            EtcdDomainKeys.StateKey(member.K), $"\"{ManagedNodeStates.Running}\"", ct);
        if (!put.IsSuccess)
            return Result.Failed(put.Error!); // transient: resume-ветка следующего тика допишет RUNNING
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

    // Reconcile-чистка (takeover-доводка): листы /etcd/nodes/<k>/* и записи
    // portalloc, у которых НЕТ члена в составе и НЕТ docker-объекта — чистятся
    // (краш-окно вывода — после member-remove до чистки ключей/portalloc —
    // больше не оставляет вечных ключей). Состав и docker-объекты — свежие,
    // НА МОМЕНТ решения (снимок начала тика мог устареть).
    private async Task<Result> CleanupOrphanKeysAsync(
        IReadOnlyList<EtcdNodesParser.ManagedNodeState> nodes, CancellationToken ct)
    {
        var ports = await portAlloc.ReadAllAsync(ct);
        if (!ports.IsSuccess)
            return Result.Failed(ports.Error!);
        if (nodes.Count == 0 && ports.Value.Count == 0)
            return Result.Success();

        var members = await MemberListFailoverAsync(ct);
        if (!members.IsSuccess)
            return Result.Failed(members.Error!);
        var (fresh, _) = EtcdContourMembers.Build(members.Value);
        var memberKs = fresh.Select(m => m.K).ToHashSet();

        var objects = await driver.ListNodeObjectsAsync(ct);
        if (!objects.IsSuccess)
            return Result.Failed(objects.Error!);
        var dockerKs = objects.Value
            .Select(ParseNodeK)
            .ToHashSet();

        var candidates = nodes.Select(n => n.K)
            .Concat(ports.Value.Keys)
            .Where(k => !memberKs.Contains(k) && !dockerKs.Contains(k))
            .Distinct()
            .OrderBy(k => k)
            .ToList();
        foreach (var k in candidates)
        {
            await journal.WriteAsync(Op, "cleanup-orphans", claims.InstanceId, null, ct);
            await CleanupKeysAsync(k, ct);
            var released = await portAlloc.ReleaseAsync(k, ct);
            if (!released.IsSuccess)
                return Result.Failed(released.Error!);
        }

        return Result.Success();
    }

    // etcdw-node-<k> | etcdw-node-<k>-data → k (0 — не имя объекта узла).
    private static int ParseNodeK(string name)
    {
        if (!name.StartsWith("etcdw-node-", StringComparison.Ordinal))
            return 0;
        var suffix = name["etcdw-node-".Length..];
        var kPart = suffix.EndsWith("-data", StringComparison.Ordinal) ? suffix[..^5] : suffix;
        return int.TryParse(kPart, out var k) && k > 0 ? k : 0;
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
