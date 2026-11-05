using System.Text;
using EtcdWorker.Core;
using EtcdWorker.Docker.Drivers;
using EtcdWorker.Etcd;
using EtcdWorker.IntegrationTests.Docker;
using EtcdWorker.Provisioning;
using EtcdWorker.Provisioning.Processes;
using Shared.Etcd.Client;
using Shared.Etcd.Coordination;
using Xunit;

namespace EtcdWorker.IntegrationTests.Membership;

// Ядро приёмки состава §7.2 (arch/etcdworker/02-membership.md): доводка nodes=N на живом контуре
// (seed внешним членом, managed-узлы контейнерами docker-хоста рига в сети
// сценария); rolling-вывод под кворум-гвардом; авто-замена мёртвого узла;
// orphan-cleanup. Каждый кейс полностью чистит свои контейнеры/тома/ключи.
[Collection(EtcdWorkerCollection.Name)]
public sealed class MembershipProcessTests(EtcdWorkerEnvironment env)
{
    private sealed record Rig(
        MembershipProcess Membership,
        NodeSupervisor Supervisor,
        EtcdPortAllocStore Ports,
        PlainEtcdNodeDriver Driver,
        EtcdProvisioningOptions Options,
        ClaimStore Claims);

    private static int[] FreePortWindow(int count)
    {
        var taken = new HashSet<int>();
        using var probe = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        var result = new List<int>();
        var candidate = 18100;
        while (result.Count < count && candidate < 19100)
        {
            var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, candidate);
            try
            {
                listener.Start();
                listener.Stop();
                result.Add(candidate);
            }
            catch (System.Net.Sockets.SocketException)
            {
                // порт занят (стенд/соседние серии) — следующая кандидатура
            }

            candidate++;
        }

        probe.Start();
        probe.Stop();
        return [.. result];
    }

    private Rig NewRig(int nodeDeadSec = 6)
    {
        var window = FreePortWindow(10);
        var options = new EtcdProvisioningOptions(
            "Plain", env.NetworkName, "quay.io/coreos/etcd:v3.5.21", null,
            window[0], window[^1], "local", NodeBootSec: 60, NodeDeadSec: nodeDeadSec, HealthIntervalSec: 1);
        var driver = new PlainEtcdNodeDriver(
            [new HostEndpoint("local", "unix:///var/run/docker.sock")],
            new Shared.Docker.DockerEngineFactory(), env.NetworkName);
        var journal = new EtcdWorkJournal(env.Gateway, [env.Endpoint]);
        var claims = new ClaimStore("/etcdworker", [env.Endpoint], env.Gateway, TimeProvider.System);
        var ports = new EtcdPortAllocStore(env.Gateway, [env.Endpoint]);
        var provisioner = new EtcdNodeProvisioner(driver, env.Gateway, 60);
        var portLock = new PortAllocLock(
            "/etcdworker", [env.Endpoint], env.Gateway, TimeProvider.System, claims.InstanceId);
        var membership = new MembershipProcess(
            env.Gateway, [env.Endpoint], journal, driver, ports, provisioner, claims, options, portLock);
        var supervisor = new NodeSupervisor(
            env.Gateway, [env.Endpoint], journal, driver, provisioner, ports, claims, options, TimeProvider.System);
        return new Rig(membership, supervisor, ports, driver, options, claims);
    }

    private static async Task DeclareAsync(EtcdWorkerEnvironment env, int nodes)
        => await env.Gateway.PutAsync(env.Endpoint, EtcdDomainKeys.ConfigKey,
            EtcdConfigJson.Serialize(nodes, DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
            lease: null, TestContext.Current.CancellationToken);

    private static async Task<List<ManagedMember>> ManagedAsync(EtcdWorkerEnvironment env)
    {
        var ct = TestContext.Current.CancellationToken;
        var list = await env.Gateway.MemberListAsync(env.Endpoint, ct);
        list.IsSuccess.Should().BeTrue(list.Error?.Message);
        var (managed, _) = EtcdContourMembers.Build(list.Value);
        return [.. managed];
    }

    // Цикл тиков состава до условия (бюджет секунд); каждый тик — как
    // ReconcileLoop лидера: надзор → доводка. Последние deadFacts/лидер +
    // история сэмплов тиков (managed/факты/state-листы/фаза журнала) —
    // в текст таймаута (телеметрия «почему доводка/замена не сошлись»).
    private static async Task RunUntilAsync(
        Rig rig, EtcdWorkerEnvironment env, Func<Task<bool>> done, int budgetSec, string because)
    {
        var ct = TestContext.Current.CancellationToken;
        var startedUtc = DateTimeOffset.UtcNow;
        var deadline = startedUtc.AddSeconds(budgetSec);
        string? lastError = null;
        Result<IReadOnlyList<ManagedDeadFact>>? lastFacts = null;
        var samples = new StringBuilder();
        var tick = 0;
        while (DateTimeOffset.UtcNow < deadline)
        {
            tick++;
            var managed = await ManagedAsync(env);
            var nodesRange = await env.Gateway.RangeAsync(env.Endpoint, EtcdDomainKeys.NodesPrefix, ct);
            if (!nodesRange.IsSuccess)
            {
                lastError = nodesRange.Error?.Message;
                await Task.Delay(1500, ct);
                continue;
            }

            var nodes = EtcdNodesParser.Parse(nodesRange.Value ?? []);
            var ports = await rig.Ports.ReadAllAsync(ct);
            var facts = await rig.Supervisor.ObserveAsync(managed, nodes, ports.Value, ct);
            lastFacts = facts;

            // История тиков: первые 2, затем каждый 8-й (≈12 c) — срез
            // состояния на моменты промахов предиката (mid-кейс телеметрия).
            if (tick <= 2 || tick % 8 == 0)
                AppendTickSample(samples, tick, startedUtc, env, managed, facts, nodes, ct);

            await rig.Membership.TickAsync(facts.Value, ct);
            if (await done())
                return;
            await Task.Delay(1500, ct);
        }

        // Хвост истории — срез на момент таймаута.
        AppendTickSample(samples, tick, startedUtc, env,
            await ManagedAsync(env), lastFacts, null, ct, final: true);

        var diagnosis = await DiagnoseAsync(env, startedUtc);
        IReadOnlyList<ManagedDeadFact> deadFacts = lastFacts is { IsSuccess: true } f ? f.Value : [];
        var factsView = lastFacts is { IsSuccess: true }
            ? string.Join(",", deadFacts.Select(f =>
                $"k{f.Member.K}:since={(f.ProblemSinceUnix is { } s ? DateTimeOffset.UtcNow.ToUnixTimeSeconds() - s + "s ago" : "null")},envMissing={f.EnvironmentMissing}"))
            : lastFacts?.Error?.Message ?? "<no facts>";
        var leader = await env.Gateway.StatusAsync(env.Endpoint, ct);
        var leaderView = leader.IsSuccess
            ? leader.Value?.LeaderMemberId?.ToString() ?? "none"
            : leader.Error!.Message;
        throw new TimeoutException(
            $"membership did not converge: {because}; last range error: {lastError}; deadFacts=[{factsView}]; leader={leaderView};{diagnosis}"
            + $"; ticks[{samples}]");
    }

    // Сэмпл истории тиков: managed (k:имя:V/L), факты надзора, state-листы
    // нод, фаза журнала — одна строка, ~всё нужное для разбора mid-кейса.
    private static void AppendTickSample(
        StringBuilder sb, int tick, DateTimeOffset startedUtc, EtcdWorkerEnvironment env,
        IReadOnlyList<ManagedMember> managed,
        Result<IReadOnlyList<ManagedDeadFact>>? facts,
        IReadOnlyList<EtcdNodesParser.ManagedNodeState>? nodes,
        CancellationToken ct,
        bool final = false)
    {
        try
        {
            var elapsed = (int)(DateTimeOffset.UtcNow - startedUtc).TotalSeconds;
            sb.Append($"\n  t{tick:+0000;-0000}").Append(final ? "+timeout " : " ").Append($"{elapsed}s managed=[");
            foreach (var m in managed)
                sb.Append($"k{m.K}:{m.Member.Name ?? "<unnamed>"}/{(m.Member.IsLearner ? 'L' : 'V')},");
            sb.Append(']').Append(" facts=[");
            if (facts is { IsSuccess: true } fr)
                foreach (var f in fr.Value)
                    sb.Append($"k{f.Member.K}:{(f.ProblemSinceUnix is { } s ? $"since{DateTimeOffset.UtcNow.ToUnixTimeSeconds() - s}s" : "no-since")}/{(f.EnvironmentMissing ? "envMissing" : "env-ok")},");
            else
                sb.Append(facts?.Error?.Message ?? "<no-facts-call>");
            sb.Append(']');
            if (nodes is { } ns && ns.Count > 0)
            {
                sb.Append(" states=[");
                foreach (var n in ns)
                    sb.Append($"k{n.K}={n.State ?? "?"},");
                sb.Append(']');
            }

            var work = env.Gateway.GetAsync(
                env.Endpoint, EtcdWorker.Provisioning.EtcdWorkJournal.Key, ct).ConfigureAwait(false).GetAwaiter().GetResult();
            if (work.IsSuccess && work.Value is { } w && w.Value is { Length: > 0 })
            {
                var phase = w.Value;
                var pIdx = phase.IndexOf("\"phase\"", StringComparison.Ordinal);
                if (pIdx >= 0 && phase.Length - pIdx > 90) phase = phase[..(pIdx + 90)];
                sb.Append(" journal=").Append(phase);
            }
            sb.Append('\n');
        }
        catch (Exception ex)
        {
            sb.Append("  sample-failed: ").Append(ex.Message).Append('\n');
        }
    }

    // Телеметрия таймаута (канон arch/testing/01-e2e-launch.md): журнал фаз, member list,
    // контейнеры узлов с последними лог-строками и хронология docker-событий
    // за время кейса — «почему не сошлось».
    private static async Task<string> DiagnoseAsync(EtcdWorkerEnvironment env, DateTimeOffset sinceUtc)
    {
        var sb = new StringBuilder();
        try
        {
            var work = await env.Gateway.GetAsync(
                env.Endpoint, EtcdWorker.Provisioning.EtcdWorkJournal.Key,
                TestContext.Current.CancellationToken);
            sb.Append(" work=").Append(work.IsSuccess && work.Value is { } w ? w.Value : "<none>");
            var members = await env.Gateway.MemberListAsync(
                env.Endpoint, TestContext.Current.CancellationToken);
            if (members.IsSuccess)
                foreach (var m in members.Value)
                    sb.Append($" member[{m.Name ?? "<unstarted>"}/{(m.IsLearner ? 'L' : 'V')}:{string.Join(",", m.PeerUrls)}]");
            var ps = RunDocker("ps -a");
            foreach (var line in ps.Split('\n', StringSplitOptions.RemoveEmptyEntries)
                         .Where(l => l.Contains("etcdw-node-")))
            {
                sb.Append(" [").Append(line.Trim()).Append(']');
                var name = line.Trim().Split()[^1];
                var logs = RunDocker($"logs --tail 3 {name}");
                foreach (var logLine in logs.Split('\n', StringSplitOptions.RemoveEmptyEntries).TakeLast(2))
                    sb.Append(" log: ").Append(logLine.Length <= 200 ? logLine : logLine[..200]);
            }

            if (ps.Length == 0)
                sb.Append(" <docker ps empty/failed>");

            // Хронология docker-событий кейса (die/kill/create/destroy узлов):
            // events-буфер демона жив недолго — снимается ДО teardown.
            var sinceUnix = sinceUtc.ToUnixTimeSeconds();
            var events = RunDocker($"events --since {sinceUnix} --until 1s --filter type=container");
            var etcdwEvents = events.Split('\n', StringSplitOptions.RemoveEmptyEntries)
                .Where(l => l.Contains("etcdw")).Take(40).ToList();
            if (etcdwEvents.Count > 0)
                sb.Append(" events=").Append(string.Join(" | ", etcdwEvents));
        }
        catch (Exception ex)
        {
            sb.Append(" diagnose-failed: ").Append(ex.Message);
        }

        return sb.ToString();
    }

    private static string RunDocker(string args)
    {
        var psi = new System.Diagnostics.ProcessStartInfo("docker", args)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        using var p = System.Diagnostics.Process.Start(psi)!;
        var output = p.StandardOutput.ReadToEnd();
        p.WaitForExit(5000);
        return output;
    }

    // Самоочистка кейса: ЧЛЕНЫ (строго, с ретраями — контейнеры ещё на месте,
    // кворум жив; иначе remove 503-ит вечно) → контейнеры/тома → листы/
    // portalloc/журнал/config → ассерт чистоты состава (канон e2e-isolation:
    // следующий кейс не должен наследовать мёртвые члены). Упорный (raft-
    // транзиент) отказ вывода → пересоздание seed-контура фикстурой: серия
    // получает чистый raft-состав, отравление не течёт в следующие кейсы.
    private static async Task CleanupAsync(Rig rig, EtcdWorkerEnvironment env)
    {
        var ct = TestContext.Current.CancellationToken;

        // 1) Члены — ДО контейнеров; ретраи: conf-change переходы дают
        // «unhealthy»/лаг member list на follower — по одному за проход.
        var removeErrors = new List<string>();
        var membersDeadline = DateTimeOffset.UtcNow.AddSeconds(60);
        while (DateTimeOffset.UtcNow < membersDeadline)
        {
            var remaining = await ManagedAsync(env);
            if (remaining.Count == 0)
                break;
            foreach (var m in remaining)
            {
                var removed = await env.Gateway.MemberRemoveAsync(env.Endpoint, m.Member.Id, ct);
                if (!removed.IsSuccess)
                    removeErrors.Add($"k{m.K}: {removed.Error?.Message}");
                await Task.Delay(2000, ct);
            }
        }

        // Анти-отравление: члены не вывелись за бюджет — seed-контур
        // пересоздаётся (новый raft-состав без managed-членов).
        var stuck = await ManagedAsync(env);
        if (stuck.Count > 0)
        {
            removeErrors.Add($"seed contour reset (stuck members: {string.Join(",", stuck.Select(m => m.K))})");
            await env.ResetSeedAsync(ct);
        }

        // 2) Контейнеры/тома узлов.
        var objects = await rig.Driver.ListNodeObjectsAsync(ct);
        if (objects.IsSuccess)
            foreach (var name in objects.Value)
            {
                if (!name.StartsWith("etcdw-node-", StringComparison.Ordinal))
                    continue;
                var suffix = name["etcdw-node-".Length..];
                var kPart = suffix.EndsWith("-data", StringComparison.Ordinal) ? suffix[..^5] : suffix;
                if (int.TryParse(kPart, out var k))
                    await rig.Driver.RemoveNodeAsync(k, removeVolume: true, ct);
            }

        // 3) Ключи домена (config — тоже: декларация не должна протекать).
        await env.Gateway.DeleteAsync(env.Endpoint, "/etcd/nodes/", prefix: true, ct);
        await env.Gateway.DeleteAsync(env.Endpoint, EtcdDomainKeys.ConfigKey, prefix: false, ct);
        await env.Gateway.DeleteAsync(env.Endpoint, EtcdPortAllocStore.Key, prefix: false, ct);
        await env.Gateway.DeleteAsync(env.Endpoint, EtcdWorker.Provisioning.EtcdWorkJournal.Key, prefix: false, ct);

        // 4) Ассерт чистоты: managed-членов не осталось (после возможного
        // reset — состав нового seed; текст — с телеметрией remove-попыток).
        var final = await ManagedAsync(env);
        final.Should().BeEmpty(
            $"managed members must be removed by cleanup (left: {string.Join(",", final.Select(m => m.K))}) — иначе кворум контура отравлен для следующих кейсов; remove-попытки: {string.Join(" | ", removeErrors)}");
    }

    // AAA: клэйм /etcdworker/locks/portalloc у соседнего инстанса — окно
    // аллокации порта не входит: тик завершается transient-отказом
    // (PortLockBusyException), состав не меняется; снятие клэйма — следующий
    // тик доводит состав (arch/etcdworker/02-membership.md/§5, t90-паттерн)
    [Fact]
    public async Task AddAsync_PortAllocLockHeld_TransientRefusal()
    {
        var rig = NewRig();
        try
        {
            await DeclareAsync(env, 1);
            var neighbor = new PortAllocLock(
                "/etcdworker", [env.Endpoint], env.Gateway, TimeProvider.System, "neighbor-inst");
            var held = await neighbor.TryAcquireAsync(TestContext.Current.CancellationToken);
            held.IsSuccess.Should().BeTrue(held.Error?.Message);
            held.Value.Should().BeTrue("the portalloc lock key is free before the tick");

            // Act: тик с недобором при занятом соседом клэйме.
            var ports = await rig.Ports.ReadAllAsync(TestContext.Current.CancellationToken);
            var facts = await rig.Supervisor.ObserveAsync([], [], ports.Value, TestContext.Current.CancellationToken);
            var tick = await rig.Membership.TickAsync(facts.Value, TestContext.Current.CancellationToken);

            // Assert: transient-отказ, члены/ключи не менялись.
            tick.IsSuccess.Should().BeFalse();
            tick.Error.Should().BeOfType<PortLockBusyException>(
                "a tick without the global portalloc claim must refuse transiently and retry next tick");
            (await ManagedAsync(env)).Should().BeEmpty("no member is added while the neighbor holds the claim");
            (await rig.Ports.ReadAllAsync(TestContext.Current.CancellationToken)).Value.Should().BeEmpty(
                "no port is allocated while the neighbor holds the claim");

            // Снятие клэйма — доводка проходит (клэйм свободно берётся тиком).
            await neighbor.ReleaseAsync();
            await RunUntilAsync(rig, env, async () =>
            {
                var managed = await ManagedAsync(env);
                return managed.Count == 1 && managed.All(m => m.Member.Name is { Length: > 0 });
            }, budgetSec: 360, "1 managed started member after the lock is released");
        }
        finally
        {
            await CleanupAsync(rig, env);
        }
    }

    // AAA: nodes=2 → seed + 2 managed, все started (learner → promote),
    // peer-URL managed — имена сети
    [Fact]
    public async Task ScaleUp_TwoManagedNodes_AllStarted()
    {
        var rig = NewRig();
        try
        {
            await DeclareAsync(env, 2);

            // Act: тики доводки до 3 started-членов.
            await RunUntilAsync(rig, env, async () =>
            {
                var managed = await ManagedAsync(env);
                return managed.Count == 2
                       && managed.All(m => m.Member.Name is { Length: > 0 })
                       && managed.All(m => !m.Member.IsLearner);
            }, budgetSec: 360, "2 managed started members");

            // Assert: peer-URL всех managed — имена сети (детект-паттерн).
            var managed = await ManagedAsync(env);
            managed.Should().HaveCount(2);
            managed.Select(m => m.K).Should().BeEquivalentTo([1, 2]);
            foreach (var m in managed)
                m.Member.PeerUrls.Should().Contain(u => u == $"http://etcdw-node-{m.K}:2380");
        }
        finally
        {
            await CleanupAsync(rig, env);
        }
    }

    // AAA: nodes=3 → nodes=2: rolling-вывод старейшего не-лидера под
    // кворум-гвардом (итог ≥ 3 с внешним seed)
    [Fact]
    public async Task ScaleDown_RollingRemoval_KeepsQuorum()
    {
        var rig = NewRig();
        try
        {
            await DeclareAsync(env, 3);
            await RunUntilAsync(rig, env, async () => (await ManagedAsync(env)).Count == 3,
                budgetSec: 200, "3 managed started members");

            // Act: снижаем декларацию — вывод одного (не лидера).
            await DeclareAsync(env, 2);
            await RunUntilAsync(rig, env, async () => (await ManagedAsync(env)).Count == 2,
                budgetSec: 60, "rolling removal to 2 managed");

            // Assert: состав 2 managed + seed, кворум жив (member list отвечает).
            var list = await env.Gateway.MemberListAsync(env.Endpoint, TestContext.Current.CancellationToken);
            list.IsSuccess.Should().BeTrue();
            list.Value.Should().HaveCount(3);
        }
        finally
        {
            await CleanupAsync(rig, env);
        }
    }

    // AAA: снос контейнера+тома ОДНОГО из двух managed (кворум выживает:
    // seed + второй узел) → авто-замена: вывод мёртвого ЧЛЕНА и доводка нового
    // (порог NodeDeadSec, EnvironmentMissing)
    [Fact]
    public async Task AutoReplace_DeadNode_NewMember()
    {
        var rig = NewRig();
        try
        {
            // Arrange: два started-узла (замена одного не роняет кворум).
            await DeclareAsync(env, 2);
            await RunUntilAsync(rig, env, async () =>
            {
                var managed = await ManagedAsync(env);
                return managed.Count == 2 && managed.All(m => m.Member.Name is { Length: > 0 });
            }, budgetSec: 480, "2 managed started members");

            // Act: снос контейнера И тома НЕ лидера. Лидер должен быть
            // СТАБИЛЕН (две пробы подряд) — иначе выборы уведут его на жертву
            // и гвард замены (не-лидер) корректно заблокирует вывод.
            ulong? leaderId = null;
            var ct2 = TestContext.Current.CancellationToken;
            for (var i = 0; i < 20; i++)
            {
                var s1 = (await env.Gateway.StatusAsync(env.Endpoint, ct2)).Value?.LeaderMemberId;
                await Task.Delay(1000, ct2);
                var s2 = (await env.Gateway.StatusAsync(env.Endpoint, ct2)).Value?.LeaderMemberId;
                if (s1 is not null && s1 == s2)
                {
                    leaderId = s1;
                    break;
                }
            }

            leaderId.Should().NotBeNull("stable leader expected before killing a non-leader");
            // Жертва — ДЕТЕРМИНИРОВАННА: старейший не-лидер (паттерн
            // PickRemoveCandidate). Порядок member list недетерминирован
            // (шаг 1 ruling) — First по списку ловил то k1, то k2.
            var dead = (await ManagedAsync(env))
                .Where(m => m.Member.Id != leaderId)
                .OrderBy(m => m.K)
                .First();
            var killed = await rig.Driver.RemoveNodeAsync(dead.K, removeVolume: true, TestContext.Current.CancellationToken);
            killed.IsSuccess.Should().BeTrue(killed.Error?.Message);
            // Проблема наблюдается дольше порога (NodeDeadSec=6).
            await Task.Delay(TimeSpan.FromSeconds(8), TestContext.Current.CancellationToken);
            await RunUntilAsync(rig, env, async () =>
            {
                var managed = await ManagedAsync(env);
                return managed.Count == 2
                       && managed.All(m => m.Member.Name is { Length: > 0 })
                       && managed.All(m => !m.Member.IsLearner)
                       && managed.All(m => m.Member.Id != dead.Member.Id);
            }, budgetSec: 480, "dead member replaced by a new started member");

            // Assert: состава 2 managed, мёртвого ЧЛЕНА нет (заменил новый).
            // НОМЕР может быть переиспользован — канон NextNumber = max+1 по
            // чистому составу (листы/ключи выведенного сняты RemoveAsync,
            // portalloc released): «новость» замены — НОВЫЙ member-ID, не
            // номер (переиспользование номера после полного вывода — штатно).
            var managed2 = await ManagedAsync(env);
            managed2.Should().HaveCount(2);
            managed2.Select(m => m.Member.Id).Should().NotContain(dead.Member.Id);
            managed2.Should().OnlyContain(m => !m.Member.IsLearner);
        }
        finally
        {
            await CleanupAsync(rig, env);
        }
    }

    // AAA: orphan — контейнер узла без члена в составе сносится надзором
    // (от 2 managed: вывод одного члена member-API не роняет кворум 3 voting)
    [Fact]
    public async Task Orphan_ContainerWithoutMember_Removed()
    {
        var rig = NewRig();
        try
        {
            await DeclareAsync(env, 2);
            await RunUntilAsync(rig, env, async () => (await ManagedAsync(env)).Count == 2,
                budgetSec: 480, "2 managed started members");

            // Act: вывод члена member-API (контейнер остаётся — orphan).
            var victim = (await ManagedAsync(env))
                .OrderByDescending(m => m.K).First();
            // «unhealthy cluster» на member-remove — транзиент raft-лага:
            // ретраи до успеха (бюджет 30 с).
            Result removed = Result.Failed(new ApplicationException("not attempted"));
            for (var i = 0; i < 15; i++)
            {
                removed = await env.Gateway.MemberRemoveAsync(
                    env.Endpoint, victim.Member.Id, TestContext.Current.CancellationToken);
                if (removed.IsSuccess)
                    break;
                await Task.Delay(2000, TestContext.Current.CancellationToken);
            }

            removed.IsSuccess.Should().BeTrue(removed.Error?.Message);
            var cleanup = await rig.Supervisor.OrphanCleanupAsync(
                TestContext.Current.CancellationToken);

            // Assert: объекты выведенного члена снесены; выживший член жив
            // (orphan-cleanup сверяется с ФАКТИЧЕСКИМ составом на момент
            // решения, а не со снимком вызывающего тика).
            cleanup.IsSuccess.Should().BeTrue(cleanup.Error?.Message);
            var objects = await rig.Driver.ListNodeObjectsAsync(TestContext.Current.CancellationToken);
            var nodeObjects = objects.Value
                .Where(o => o.StartsWith("etcdw-node-", StringComparison.Ordinal)).ToList();
            nodeObjects.Should().NotContain(o => o.StartsWith("etcdw-node-2", StringComparison.Ordinal),
                "orphan container and volume of the removed member are removed");
            nodeObjects.Should().OnlyContain(o => o.StartsWith("etcdw-node-1", StringComparison.Ordinal),
                "the container of the live member is kept by the fact-based orphan cleanup");
        }
        finally
        {
            await CleanupAsync(rig, env);
        }
    }

    // AAA: краш-окно promote→RUNNING (член уже voting, state-лист не дописан:
    // процесс умер между MemberPromote и put) — resume-ветка сверяет факт по
    // свежему составу (promote voting-члена etcd 3.5 отвергает перманентно),
    // дописывает state=RUNNING; узел не застревает в PROVISIONING навсегда
    [Fact]
    public async Task Resume_PromotedMemberWithoutRunningState_Converges()
    {
        var rig = NewRig();
        try
        {
            // Arrange: один started managed-узел (state=RUNNING пройден тиком).
            await DeclareAsync(env, 1);
            await RunUntilAsync(rig, env, async () =>
            {
                var managed = await ManagedAsync(env);
                return managed.Count == 1
                       && managed.All(m => m.Member.Name is { Length: > 0 })
                       && managed.All(m => !m.Member.IsLearner);
            }, budgetSec: 360, "1 managed started voting member");

            // Симуляция краш-окна: state-лист откат в PROVISIONING (член
            // реально voting — put между promote и state не состоялся).
            var member = (await ManagedAsync(env)).Single();
            await env.Gateway.PutAsync(env.Endpoint, EtcdDomainKeys.StateKey(member.K),
                $"\"{ManagedNodeStates.Provisioning}\"", lease: null, TestContext.Current.CancellationToken);

            // Act: тики доводки — already-voting ветка дописывает RUNNING.
            await RunUntilAsync(rig, env, async () =>
            {
                var range = await env.Gateway.RangeAsync(
                    env.Endpoint, EtcdDomainKeys.StateKey(member.K), TestContext.Current.CancellationToken);
                return range.IsSuccess && range.Value is [{ Value: var v }] && v.Trim('"') == ManagedNodeStates.Running;
            }, budgetSec: 60, "state leaf converges back to RUNNING for an already-voting member");

            // Assert: член по-прежнему voting, состав не изменился (никаких
            // лишних member-add/remove в окне resume).
            var after = await ManagedAsync(env);
            after.Should().HaveCount(1);
            after.Single().Member.Id.Should().Be(member.Member.Id);
            after.Single().Member.IsLearner.Should().BeFalse("the member was never demoted back to learner");
        }
        finally
        {
            await CleanupAsync(rig, env);
        }
    }
}
