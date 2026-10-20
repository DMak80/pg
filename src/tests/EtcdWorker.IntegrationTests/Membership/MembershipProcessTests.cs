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

// Ядро приёмки состава §7.2 (arch/25 §3): доводка nodes=N на живом контуре
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
        var membership = new MembershipProcess(
            env.Gateway, [env.Endpoint], journal, driver, ports, provisioner, claims, options);
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
    // ReconcileLoop лидера: надзор → доводка. Последние deadFacts/лидер —
    // в текст таймаута (телеметрия «почему замена не выбирается»).
    private static async Task RunUntilAsync(
        Rig rig, EtcdWorkerEnvironment env, Func<Task<bool>> done, int budgetSec, string because)
    {
        var ct = TestContext.Current.CancellationToken;
        var deadline = DateTimeOffset.UtcNow.AddSeconds(budgetSec);
        string? lastError = null;
        Result<IReadOnlyList<ManagedDeadFact>>? lastFacts = null;
        while (DateTimeOffset.UtcNow < deadline)
        {
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
            await rig.Membership.TickAsync(facts.Value, ct);
            if (await done())
                return;
            await Task.Delay(1500, ct);
        }

        var diagnosis = await DiagnoseAsync(env);
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
            $"membership did not converge: {because}; last range error: {lastError}; deadFacts=[{factsView}]; leader={leaderView};{diagnosis}");
    }

    // Телеметрия таймаута (канон docs/e2e-launch.md): контейнеры узлов,
    // их последние лог-строки, журнал фаз — «почему не сошлось».
    private static async Task<string> DiagnoseAsync(EtcdWorkerEnvironment env)
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

    // AAA: nodes=2 → seed + 2 managed, все started (learner → promote),
    // peer-URL managed — имена сети
    [Fact]
    public async Task ScaleUp_TwoManagedNodes_AllStarted()
    {
        DockerTrait.SkipIfUnavailable();
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
        DockerTrait.SkipIfUnavailable();
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
    // seed + второй узел) → авто-замена: вывод мёртвого члена и новый номер
    // (порог NodeDeadSec, EnvironmentMissing)
    [Fact]
    public async Task AutoReplace_DeadNode_NewNumber()
    {
        DockerTrait.SkipIfUnavailable();
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
            var dead = (await ManagedAsync(env)).First(m => m.Member.Id != leaderId);
            var killed = await rig.Driver.RemoveNodeAsync(dead.K, removeVolume: true, TestContext.Current.CancellationToken);
            killed.IsSuccess.Should().BeTrue(killed.Error?.Message);
            // Проблема наблюдается дольше порога (NodeDeadSec=6).
            await Task.Delay(TimeSpan.FromSeconds(8), TestContext.Current.CancellationToken);
            await RunUntilAsync(rig, env, async () =>
            {
                var managed = await ManagedAsync(env);
                return managed.Count == 2
                       && managed.All(m => m.K != dead.K)
                       && managed.All(m => m.Member.Name is { Length: > 0 });
            }, budgetSec: 480, "replacement node with new number");

            // Assert: старого номера нет, новый номер (сквозная нумерация).
            var managed2 = await ManagedAsync(env);
            managed2.Should().HaveCount(2);
            managed2.Select(m => m.K).Should().NotContain(dead.K);
            managed2.Max(m => m.K).Should().BeGreaterThan(dead.K);
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
        DockerTrait.SkipIfUnavailable();
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
}
