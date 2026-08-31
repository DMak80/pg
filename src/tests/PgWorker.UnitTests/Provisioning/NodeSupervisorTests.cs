using System.Net;
using System.Text;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Processes;
using PgWorker.Provisioning.Probes;

namespace PgWorker.UnitTests.Provisioning;

// NodeSupervisor + MasterKeyReconciler (задача 21; arch/14 §5 C, P11):
// самовосстановление декларации, rebuild мёртвой не-лидерской ноды при
// кворуме, лидер не трогается (failover — Patroni), детект мёртвого шарда,
// сверка мастер-ключа только при рассинхроне.
public class NodeSupervisorTests
{
    private const string Ep = "http://etcd:2379";
    private static readonly InstallSecrets Secrets = new("su-pw", "sb-pw", "adm-pw", "mov-pw");
    private static readonly ThresholdsOptions Thresholds = new(NodeDeadSec: 90, ShardDeadSec: 300);

    // Patroni-проба: 200/500 по порту ноды (два Patroni-порта на хостах h1/h2).
    private static ShardProbe Probe(Func<int, HttpResponseMessage> respondByPort)
        => new(new HttpClient(new FakeHandler(r => respondByPort(r.RequestUri!.Port))));

    private sealed class FakeHandler(Func<HttpRequestMessage, HttpResponseMessage> responder) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
            => Task.FromResult(responder(request));
    }

    // Полноценные ответы: /cluster парсит тело (members), /primary — только статус.
    private static HttpResponseMessage Ok() => new(HttpStatusCode.OK)
    {
        Content = new StringContent("""{"members":[]}""", Encoding.UTF8, "application/json"),
    };

    private static HttpResponseMessage Down() => new(HttpStatusCode.ServiceUnavailable);

    private static void SeedCluster(Fakes.FakeEtcd etcd, int nodes = 3)
    {
        etcd.Seed("/clusters/shop/config", """{"buckets":2,"dbname":"shop","created_unix":1755900000}""");
        etcd.Seed("/clusters/shop/shards/shard1/replicas", nodes.ToString());
        for (var i = 0; i < nodes; i++)
            etcd.Seed($"/clusters/shop/shards/shard1/nodes/shard1{(char)('a' + i)}/state", "RUNNING");
        etcd.Seed("/clusters/shop/shards/shard1/dsn", "host=h1,h2 port=15000,15000 dbname=shop user=bucket_admin");
        etcd.Seed("/clusters/shop/buckets/routing/bucket_0", "shard1");
        etcd.Seed("/clusters/shop/buckets/routing/bucket_1", "shard1");
        // portalloc: ноды h1/h2 чередуются, порты уникальны per-нода (18000/18001/18002)
        var alloc = new Dictionary<string, NodeAddress>();
        for (var i = 0; i < nodes; i++)
            alloc[$"shard1/shard1{(char)('a' + i)}"] = new NodeAddress(
                i % 2 == 0 ? "h1" : "h2",
                new NodePorts(15000 + i, 18000 + i, 16500 + i));
        etcd.Seed("/pgworker/portalloc/shop", PgWorker.Core.Model.Portalloc.Serialize(alloc));
    }

    private static async Task<ClusterSnapshot> Snapshot(Fakes.FakeEtcd etcd, string cluster = "shop")
    {
        var range = await etcd.RangeAsync(Ep, "/clusters/", CancellationToken.None);
        var parsed = ClusterSnapshotParser.ParseClusters(range.Value, out _);
        return parsed.Value.Single(c => c.Config.Cluster == cluster);
    }

    private sealed record Rig(Fakes.FakeEtcd Etcd, Fakes.FakeDriver Driver, ClaimStore Claims,
        WorkJournal Journal, NodeSupervisor Supervisor);

    private static async Task<Rig> NewRig(
        Func<int, HttpResponseMessage> respond,
        IReadOnlyList<string>? nodeObjects = null,
        long? staleUnreachableForShard1A = null,
        long? staleUnreachableAll = null,
        Func<HttpRequestMessage, HttpResponseMessage>? respondRaw = null,
        IReadOnlyDictionary<string, NodeAddress>? addresses = null,
        Fakes.FakeSql? sql = null)
    {
        var etcd = new Fakes.FakeEtcd();
        SeedCluster(etcd);
        if (addresses is not null)
            etcd.Seed("/pgworker/portalloc/shop", Portalloc.Serialize(addresses));
        var claims = new ClaimStore([Ep], etcd, TimeProvider.System);
        await claims.TryClaimClusterAsync("shop", CancellationToken.None);
        var journal = new WorkJournal(etcd, [Ep]);
        if (staleUnreachableForShard1A.HasValue || staleUnreachableAll.HasValue)
        {
            var track = new Dictionary<string, long>();
            if (staleUnreachableForShard1A is { } staleA)
                track["shard1/shard1a"] = staleA;
            if (staleUnreachableAll is { } staleAll)
                for (var i = 0; i < 3; i++)
                    track[$"shard1/shard1{(char)('a' + i)}"] = staleAll;
            await journal.WriteSupervisionAsync("shop", "seed", track, CancellationToken.None);
        }

        var driver = new Fakes.FakeDriver
        {
            NodeObjects = (nodeObjects ?? new List<string>
            {
                "pgw-shop-shard1-shard1a", "pgw-shop-shard1-shard1b", "pgw-shop-shard1-shard1c",
            }).ToList(),
        };
        var probe = new ShardProbe(new HttpClient(
            new FakeHandler(respondRaw ?? (r => respond(r.RequestUri!.Port)))));
        var supervisor = new NodeSupervisor(
            etcd, [Ep], driver, probe, sql ?? new Fakes.FakeSql(), claims, journal,
            Thresholds, TimeProvider.System, Secrets,
            new AppParamsEnsurer(etcd, [Ep], "sslmode=require"),
            new MasterKeyReconciler(etcd, [Ep], probe));
        return new Rig(etcd, driver, claims, journal, supervisor);
    }

    [Fact]
    public async Task Tick_ManuallyRemovedContainer_EnsureNodeRestores()
    {
        // Arrange — контейнер shard1a снесён руками (docker его не видит), Patroni жив
        var rig = await NewRig(_ => Ok(), nodeObjects:
        [
            "pgw-shop-shard1-shard1b", "pgw-shop-shard1-shard1c",
        ]);

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: декларативное самовосстановление — нода пересоздана, state PROVISIONING
        outcome.Value.Outcome.Should().Be(ProcessOutcome.Done);
        rig.Driver.EnsuredNodes.Should().ContainSingle().Which.Should().Be("shard1/shard1a");
        rig.Etcd.Store["/clusters/shop/shards/shard1/nodes/shard1a/state"].Value.Should().Be("PROVISIONING");
    }

    [Fact]
    public async Task Tick_DeadNonLeaderNodeWithQuorum_Rebuild()
    {
        // Arrange — shard1a мертва дольше NodeDeadSec (трек устарел), лидер shard1b,
        // кворум жив (b, c отвечают); /primary: b — primary
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var rig = await NewRig(
            port => port == 18000 ? Down() : Ok(), // shard1a (18000) мертва, b/c живы
            staleUnreachableForShard1A: now - 200);
        rig.Etcd.Seed("/service/shop-shard1/leader", """{"name":"shard1b"}""");
        rig.Etcd.Seed("/clusters/shop/shards/shard1/master", "h1:16500"); // устаревший (shard1a была)

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: rebuild — RemoveNode + EnsureNode того же addr, state REBUILDING
        outcome.Value.Outcome.Should().Be(ProcessOutcome.Done);
        rig.Driver.RemovedNodes.Should().ContainSingle().Which.Should().Be("shard1/shard1a");
        rig.Driver.EnsuredNodes.Should().Contain("shard1/shard1a");
        rig.Etcd.Store["/clusters/shop/shards/shard1/nodes/shard1a/state"].Value.Should().Be("REBUILDING");
    }

    [Fact]
    public async Task Tick_StaleFailoverKeyForOtherLeader_Cleaned()
    {
        // Arrange — хвост ускоренного failover: ключ остался (Patroni не всегда
        // потребляет), но лидер уже ДРУГОЙ — висящий ключ заставил бы бывшего
        // лидера при возврате уступить лидерство
        var rig = await NewRig(_ => Ok());
        rig.Etcd.Seed("/service/shop-shard1/leader", "shard1b");
        rig.Etcd.Seed("/service/shop-shard1/failover",
            """{"leader":"shard1a","member":"shard1b","scheduled_at":"2026-08-26T19:00:00Z"}""");

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert — устаревший ключ удалён надзором
        outcome.Value.Outcome.Should().Be(ProcessOutcome.Done);
        rig.Etcd.Store.Should().NotContainKey("/service/shop-shard1/failover",
            "ключ про другого лидера — исполнен или устарел, не должен висеть");
    }

    [Fact]
    public async Task Tick_FailoverKeyFromCurrentLeader_Kept()
    {
        // Arrange — оператор запланировал switchover patronictl от ЖИВОГО лидера:
        // ключ про текущего лидера — не наш объект, не трогаем
        var rig = await NewRig(_ => Ok());
        rig.Etcd.Seed("/service/shop-shard1/leader", "shard1a");
        rig.Etcd.Seed("/service/shop-shard1/failover",
            """{"leader":"shard1a","member":"shard1b","scheduled_at":"2030-01-01T00:00:00Z"}""");

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert — заявка оператора сохранена
        outcome.Value.Outcome.Should().Be(ProcessOutcome.Done);
        rig.Etcd.Store.Should().ContainKey("/service/shop-shard1/failover");
    }

    [Fact]
    public async Task Tick_DeletedLeaderContainer_FailoverAccelerated_NodeRecreated()
    {
        // Arrange — контейнер ЛИДЕРА shard1a снесён руками, лидер-ключ в DCS ещё
        // висит (lease протух бы через ttl), реплики живы: надзор должен УСКОРИТЬ
        // failover (ключ + удаление мёртвого лидер-ключа), а не ждать возврата
        // того же лидера рестартом; контейнер поднимается параллельно
        var rig = await NewRig(_ => Ok(), nodeObjects:
        [
            "pgw-shop-shard1-shard1b", "pgw-shop-shard1-shard1c",
        ]);
        rig.Etcd.Seed("/service/shop-shard1/leader", "shard1a");

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert — failover-first: ключ написан (кандидат — живая нода), мёртвый
        // лидер-ключ удалён, контейнер лидера пересоздаётся параллельно
        outcome.Value.Outcome.Should().Be(ProcessOutcome.Done);
        var failover = rig.Etcd.Store.Should().ContainKey("/service/shop-shard1/failover").WhoseValue.Value;
        failover.Should().Contain("\"leader\":\"shard1a\"").And.Contain("\"member\":");
        rig.Etcd.Store.Should().NotContainKey("/service/shop-shard1/leader",
            "мёртвый лидер-лок снят — кандидат промоутится в пределах loop_wait, не ttl");
        rig.Driver.EnsuredNodes.Should().Contain("shard1/shard1a");
        rig.Etcd.Store["/clusters/shop/shards/shard1/nodes/shard1a/state"].Value.Should().Be("PROVISIONING");
    }

    [Fact]
    public async Task Tick_DeletedReplicaContainer_NoFailoverAcceleration()
    {
        // Arrange — снесён контейнер РЕПЛИКИ (лидер жив на месте): ускорение
        // failover не нужно — обычное самовосстановление декларации
        var rig = await NewRig(_ => Ok(), nodeObjects:
        [
            "pgw-shop-shard1-shard1a", "pgw-shop-shard1-shard1c",
        ]);
        rig.Etcd.Seed("/service/shop-shard1/leader", "shard1a");

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert — никаких ключей failover/удалений лидера, реплика пересоздана
        outcome.Value.Outcome.Should().Be(ProcessOutcome.Done);
        rig.Etcd.Store.Should().NotContainKey("/service/shop-shard1/failover");
        rig.Etcd.Store.Should().ContainKey("/service/shop-shard1/leader");
        rig.Driver.EnsuredNodes.Should().ContainSingle().Which.Should().Be("shard1/shard1b");
    }

    [Fact]
    public async Task Tick_DeletedLeaderContainer_NoAliveCandidate_NoAcceleration()
    {
        // Arrange — контейнер лидера снесён, но НЕТ живых кандидатов (пробы глухие):
        // промоутить некого — лидер возвращается рестартом контейнера
        var rig = await NewRig(_ => Down(), nodeObjects:
        [
            "pgw-shop-shard1-shard1b", "pgw-shop-shard1-shard1c",
        ]);
        rig.Etcd.Seed("/service/shop-shard1/leader", "shard1a");

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert — ключей не писали, лидер-ключ не трогали, контейнер поднят
        outcome.Value.Outcome.Should().Be(ProcessOutcome.Done);
        rig.Etcd.Store.Should().NotContainKey("/service/shop-shard1/failover");
        rig.Etcd.Store.Should().ContainKey("/service/shop-shard1/leader");
        rig.Driver.EnsuredNodes.Should().Contain("shard1/shard1a");
    }

    [Fact]
    public async Task Tick_MarkedNode_Recreated_StateRebuildingNotOverwrittenByUnreachable()
    {
        // Arrange — оператор пометил shard1b (TO_RECREATE), лидер shard1a жив:
        // recreate выполняется этим же тиком (Remove+Ensure+REBUILDING), проба
        // новой ноды ещё глухая (Patroni грузится) — тот же тик не должен
        // затирать REBUILDING на UNREACHABLE по устаревшему снапшоту
        var rig = await NewRig(port => port == 18001 ? Down() : Ok());
        rig.Etcd.Seed("/clusters/shop/shards/shard1/nodes/shard1b/state", "TO_RECREATE");
        rig.Etcd.Seed("/service/shop-shard1/leader", """{"name":"shard1a"}""");

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert — контейнер+volume снесены и пересозданы, финальный state REBUILDING
        outcome.Value.Outcome.Should().Be(ProcessOutcome.Done);
        rig.Driver.RemovedNodes.Should().ContainSingle().Which.Should().Be("shard1/shard1b");
        rig.Driver.EnsuredNodes.Should().Contain("shard1/shard1b");
        rig.Etcd.Store["/clusters/shop/shards/shard1/nodes/shard1b/state"].Value
            .Should().Be("REBUILDING", "UNREACHABLE того же тика затирал бы REBUILDING (гонка снапшота)");
    }

    [Fact]
    public async Task Tick_MarkedLeaderSoft_SwitchoverTriggered_NodeKept()
    {
        // Arrange — оператор пометил ЛИДЕРА shard1a (TO_RECREATE, режим soft по
        // умолчанию): нода жива — сначала graceful-switchover через Patroni REST,
        // снос в этом тике НЕ происходит (лидерство должно уехать первым)
        var switchover = new List<string>();
        var rig = await NewRig(_ => Ok(), respondRaw: r =>
        {
            switchover.Add($"{r.Method} {r.RequestUri!.AbsolutePath}");
            return Ok();
        });
        rig.Etcd.Seed("/clusters/shop/shards/shard1/nodes/shard1a/state", "TO_RECREATE");
        rig.Etcd.Seed("/service/shop-shard1/leader", """{"name":"shard1a"}""");

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert — switchover отправлен лидеру, docker не тронут, маркер ждёт
        outcome.Value.Outcome.Should().Be(ProcessOutcome.Done);
        switchover.Should().Contain("POST /switchover");
        rig.Driver.RemovedNodes.Should().BeEmpty("мягко: сначала переезд лидерства, снос — следующим тиком");
        rig.Driver.EnsuredNodes.Should().BeEmpty();
        rig.Etcd.Store["/clusters/shop/shards/shard1/nodes/shard1a/state"].Value
            .Should().Be("TO_RECREATE", "ожившая нода не отменяет заявку оператора на пересоздание");
    }

    [Fact]
    public async Task Tick_MarkedLeaderHard_RemovedImmediately()
    {
        // Arrange — оператор пометил ЛИДЕРА shard1a в режиме hard (маркер
        // nodes/shard1a/recreate=hard): снос сразу, failover делает Patroni;
        // живой свидетель-реплика (b, c) для приёма лидерства есть
        var switchover = new List<string>();
        var rig = await NewRig(_ => Ok(), respondRaw: r =>
        {
            switchover.Add($"{r.Method} {r.RequestUri!.AbsolutePath}");
            return Ok();
        });
        rig.Etcd.Seed("/clusters/shop/shards/shard1/nodes/shard1a/state", "TO_RECREATE");
        rig.Etcd.Seed("/clusters/shop/shards/shard1/nodes/shard1a/recreate", "hard");
        rig.Etcd.Seed("/service/shop-shard1/leader", """{"name":"shard1a"}""");

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert — грубо: немедленное удаление+пересоздание, switchover не звался
        outcome.Value.Outcome.Should().Be(ProcessOutcome.Done);
        switchover.Should().NotContain("POST /switchover");
        rig.Driver.RemovedNodes.Should().ContainSingle().Which.Should().Be("shard1/shard1a");
        rig.Driver.EnsuredNodes.Should().Contain("shard1/shard1a");
        rig.Etcd.Store["/clusters/shop/shards/shard1/nodes/shard1a/state"].Value.Should().Be("REBUILDING");
        rig.Etcd.Store.Should().NotContainKey("/clusters/shop/shards/shard1/nodes/shard1a/recreate",
            "маркер режима исполнен и убран");
    }

    [Fact]
    public async Task Tick_MarkedDeadLeader_HardAutomatically()
    {
        // Arrange — помеченный ЛИДЕР shard1a УМЕР (проба глухая, режим даже soft):
        // мягкий switchover бессмыслен — грубо срабатывает автоматически;
        // свидетели b/c живы, примут лидерство через failover Patroni
        var rig = await NewRig(port => port == 18000 ? Down() : Ok());
        rig.Etcd.Seed("/clusters/shop/shards/shard1/nodes/shard1a/state", "TO_RECREATE");
        rig.Etcd.Seed("/service/shop-shard1/leader", """{"name":"shard1a"}""");

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert — авто-грубо: мёртвый лидер удалён и пересоздан несмотря на soft
        outcome.Value.Outcome.Should().Be(ProcessOutcome.Done);
        rig.Driver.RemovedNodes.Should().ContainSingle().Which.Should().Be("shard1/shard1a");
        rig.Driver.EnsuredNodes.Should().Contain("shard1/shard1a");
        rig.Etcd.Store["/clusters/shop/shards/shard1/nodes/shard1a/state"].Value.Should().Be("REBUILDING");
    }

    [Fact]
    public async Task Tick_DeadLeaderNode_NoRebuild()
    {
        // Arrange — мертва ЛИДЕР-нода shard1a: failover делает Patroni (P11), не мы
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var rig = await NewRig(
            port => port == 18000 ? Down() : Ok(),
            staleUnreachableForShard1A: now - 200);
        rig.Etcd.Seed("/service/shop-shard1/leader", """{"name":"shard1a"}""");

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: никаких docker-мутаций, нода отмечена UNREACHABLE
        outcome.Value.Outcome.Should().Be(ProcessOutcome.Done);
        rig.Driver.RemovedNodes.Should().BeEmpty();
        rig.Driver.EnsuredNodes.Should().BeEmpty();
        rig.Etcd.Store["/clusters/shop/shards/shard1/nodes/shard1a/state"].Value.Should().Be("UNREACHABLE");
    }

    [Fact]
    public async Task Tick_WholeShardDead_MasterExpired_DeadShards()
    {
        // Arrange — весь шард молчит дольше ShardDeadSec, master-ключа нет
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var rig = await NewRig(_ => Down(), staleUnreachableAll: now - 400);

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: шард попал в DeadShards (триггер эвакуации для цикла, задачи 22/23)
        outcome.Value.Outcome.Should().Be(ProcessOutcome.Done);
        outcome.Value.DeadShards.Should().BeEquivalentTo(["shard1"]);
    }

    [Fact]
    public async Task Tick_WholeShardDeadButMasterAlive_NotDead()
    {
        // Arrange — ноды молчат, но master-ключ жив (Patroni lease) — надежда есть
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var rig = await NewRig(_ => Down(), staleUnreachableAll: now - 400);
        rig.Etcd.Seed("/clusters/shop/shards/shard1/master", "h1:16500");

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: эвакуация не запускается (arch/14 §5 C: master протух — обязательное условие)
        outcome.Value.Outcome.Should().Be(ProcessOutcome.Done);
        outcome.Value.DeadShards.Should().BeEmpty();
    }

    // ---------- Границы надзора (t06 spec §5.4) ----------

    // Дозасев второго шарда в существующий сид (порты не пересекаются с shard1).
    private static void SeedShard2(Fakes.FakeEtcd etcd, bool withDsn, bool markedToRemove)
    {
        etcd.Seed("/clusters/shop/shards/shard2/replicas", "3");
        for (var i = 0; i < 3; i++)
            etcd.Seed($"/clusters/shop/shards/shard2/nodes/shard2{(char)('a' + i)}/state", "RUNNING");
        if (withDsn)
            etcd.Seed("/clusters/shop/shards/shard2/dsn", "host=h1,h2 port=15010,15011 dbname=shop user=bucket_admin");
        if (markedToRemove)
            etcd.Seed("/clusters/shop/shards/shard2/state", "TO_REMOVE");
        // portalloc: объединённый (shard1 из SeedCluster + новый shard2)
        var alloc = new Dictionary<string, NodeAddress>();
        for (var i = 0; i < 3; i++)
        {
            alloc[$"shard1/shard1{(char)('a' + i)}"] = new(
                i % 2 == 0 ? "h1" : "h2", new NodePorts(15000 + i, 18000 + i, 16500 + i));
            alloc[$"shard2/shard2{(char)('a' + i)}"] = new(
                i % 2 == 0 ? "h2" : "h1", new NodePorts(15010 + i, 18010 + i, 16510 + i));
        }

        etcd.Store["/pgworker/portalloc/shop"] = new(
            Portalloc.Serialize(alloc), 99, 1);
    }

    // Трек «мёртв давно» для всех нод shard2 (порог ShardDeadSec истёк).
    private static async Task SeedShard2DeadTrackAsync(WorkJournal journal)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var track = new Dictionary<string, long>();
        for (var i = 0; i < 3; i++)
            track[$"shard2/shard2{(char)('a' + i)}"] = now - 400;
        await journal.WriteSupervisionAsync("shop", "seed", track, CancellationToken.None);
    }

    [Fact]
    public async Task Tick_ShardWithoutDsn_DockerRm_NotRecreated()
    {
        // Arrange — declared-шард без dsn (add идёт): контейнер снесён руками;
        // восстановление — домен AddShardProcess, не надзора (t06 §5.4)
        var rig = await NewRig(_ => Ok(), nodeObjects: ["pgw-shop-shard1-shard1a",
            "pgw-shop-shard1-shard1b", "pgw-shop-shard1-shard1c"]);
        SeedShard2(rig.Etcd, withDsn: false, markedToRemove: false);

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert — надзор не пересоздаёт ноды недоднятого шарда
        outcome.Value.Outcome.Should().Be(ProcessOutcome.Done);
        rig.Driver.EnsuredNodes.Should().NotContain(n => n.StartsWith("shard2/", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Tick_ShardWithoutDsn_DeadProbes_NodeStateNotTouched()
    {
        // Arrange — declared-шард без dsn с НЕ_INITIALIZED-нодами, пробы глухие
        // (Patroni поднимается): state нод — вход A1-гварда AddShardProcess,
        // UNREACHABLE-перезапись ломала бы декларацию (waiting-keys, t06 §5.4)
        var rig = await NewRig(port => port >= 18010 ? Down() : Ok(), nodeObjects:
        [
            "pgw-shop-shard1-shard1a", "pgw-shop-shard1-shard1b", "pgw-shop-shard1-shard1c",
            "pgw-shop-shard2-shard2a", "pgw-shop-shard2-shard2b", "pgw-shop-shard2-shard2c",
        ]);
        SeedShard2(rig.Etcd, withDsn: false, markedToRemove: false);
        foreach (var node in new[] { "shard2a", "shard2b", "shard2c" })
            rig.Etcd.Seed($"/clusters/shop/shards/shard2/nodes/{node}/state", "NOT_INITIALIZED");

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert — ноды недоднятого шарда надзору не принадлежат: state не тронут
        outcome.Value.Outcome.Should().Be(ProcessOutcome.Done);
        foreach (var node in new[] { "shard2a", "shard2b", "shard2c" })
            rig.Etcd.Store[$"/clusters/shop/shards/shard2/nodes/{node}/state"].Value
                .Should().Be("NOT_INITIALIZED", $"{node} — домен AddShardProcess");
    }

    [Fact]
    public async Task Tick_QuarantinedNodes_StateKept_NoUnreachableOverwrite()
    {
        // Arrange — эвакуированный шард: все ноды QUARANTINED (E3), REST глухой
        // (остановлены намеренно), журнал эвакуации DONE; docker-объекты на месте
        // (stop без удаления). Пробы надзора не должны затирать QUARANTINED на
        // UNREACHABLE — на инварианте строятся guard'ы G6/Д6 (t06 §5.4).
        var rig = await NewRig(_ => Down());
        foreach (var node in new[] { "shard1a", "shard1b", "shard1c" })
            rig.Etcd.Seed($"/clusters/shop/shards/shard1/nodes/{node}/state", "QUARANTINED");
        rig.Etcd.Seed("/pgworker/evacuations/shop/shard1",
            """{"evacuated_unix":1755900000,"reason":"shard-dead","buckets":{},"state":"DONE"}""");

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert — тик надзора прошёл, state карантинных нод не тронут
        outcome.Value.Outcome.Should().Be(ProcessOutcome.Done);
        foreach (var node in new[] { "shard1a", "shard1b", "shard1c" })
            rig.Etcd.Store[$"/clusters/shop/shards/shard1/nodes/{node}/state"].Value
                .Should().Be("QUARANTINED", $"{node} — карантин держится до разбора (E3)");
        rig.Driver.EnsuredNodes.Should().BeEmpty("карантинные ноды не пересоздаются");
    }

    [Fact]
    public async Task Tick_MarkedShard_DockerRm_NotRecreated()
    {
        // Arrange — шард с dsn помечен TO_REMOVE; контейнер снесён руками —
        // пересоздавать демонтируемое нельзя (домен RemoveShardProcess)
        var rig = await NewRig(_ => Ok(), nodeObjects: ["pgw-shop-shard1-shard1a",
            "pgw-shop-shard1-shard1b", "pgw-shop-shard1-shard1c"]);
        SeedShard2(rig.Etcd, withDsn: true, markedToRemove: true);

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert — самовосстановление отключено для помеченного шарда
        outcome.Value.Outcome.Should().Be(ProcessOutcome.Done);
        rig.Driver.EnsuredNodes.Should().NotContain(n => n.StartsWith("shard2/", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Tick_DeadShardWithoutBuckets_NotEvacuationCandidate()
    {
        // Arrange — мёртвый шарда2 без бакетов по routing (routing → shard1):
        // эвакуация пустого шарда бессмысленна и карантинила бы ноды (G6)
        var rig = await NewRig(port => port >= 18010 ? Down() : Ok(), nodeObjects:
        [
            "pgw-shop-shard1-shard1a", "pgw-shop-shard1-shard1b", "pgw-shop-shard1-shard1c",
            "pgw-shop-shard2-shard2a", "pgw-shop-shard2-shard2b", "pgw-shop-shard2-shard2c",
        ]);
        SeedShard2(rig.Etcd, withDsn: true, markedToRemove: false);
        await SeedShard2DeadTrackAsync(rig.Journal);

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert — пустой мёртвый шард не попадает в DeadShards (t06 §5.4)
        outcome.Value.Outcome.Should().Be(ProcessOutcome.Done);
        outcome.Value.DeadShards.Should().NotContain("shard2");
    }

    [Fact]
    public async Task Tick_DeadShardWithoutDsn_NotEvacuationCandidate()
    {
        // Arrange — мёртвый declared-шард БЕЗ dsn, routing аномально указывает на
        // него: add ещё идёт — эвакуировать нечего (t06 §5.4)
        var rig = await NewRig(port => port >= 18010 ? Down() : Ok(), nodeObjects:
        [
            "pgw-shop-shard1-shard1a", "pgw-shop-shard1-shard1b", "pgw-shop-shard1-shard1c",
            "pgw-shop-shard2-shard2a", "pgw-shop-shard2-shard2b", "pgw-shop-shard2-shard2c",
        ]);
        SeedShard2(rig.Etcd, withDsn: false, markedToRemove: false);
        rig.Etcd.Seed("/clusters/shop/buckets/routing/bucket_1", "shard2"); // аномалия сида
        await SeedShard2DeadTrackAsync(rig.Journal);

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert — незарегистрированный шард не эвакуируется
        outcome.Value.Outcome.Should().Be(ProcessOutcome.Done);
        outcome.Value.DeadShards.Should().BeEmpty();
    }

    [Fact]
    public async Task Tick_DeadMarkedShardWithBuckets_IsEvacuationCandidate()
    {
        // Arrange — мёртвый помеченный шард С бакетами: эвакуация — способ
        // освободить бакеты умирающего шарда, после чего G3 пропустит демонтаж (Д6)
        var rig = await NewRig(port => port >= 18010 ? Down() : Ok(), nodeObjects:
        [
            "pgw-shop-shard1-shard1a", "pgw-shop-shard1-shard1b", "pgw-shop-shard1-shard1c",
            "pgw-shop-shard2-shard2a", "pgw-shop-shard2-shard2b", "pgw-shop-shard2-shard2c",
        ]);
        SeedShard2(rig.Etcd, withDsn: true, markedToRemove: true);
        rig.Etcd.Seed("/clusters/shop/buckets/routing/bucket_1", "shard2");
        await SeedShard2DeadTrackAsync(rig.Journal);

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert — TO_REMOVE-маркер НЕ отключает аварийную эвакуацию (Д6)
        outcome.Value.Outcome.Should().Be(ProcessOutcome.Done);
        outcome.Value.DeadShards.Should().Contain("shard2");
    }

    // ---------- Границы надзора для усыновлённых нод (adopt-repair spec §3.4) ----------

    // Portalloc усыновлённого шарда: object-контейнеры вместо pgw-имён, patroni=0
    // (Patroni-REST у внешних нод нет — живость по SQL-пробе).
    private static Dictionary<string, NodeAddress> AdoptedAddresses()
    {
        var alloc = new Dictionary<string, NodeAddress>();
        for (var i = 0; i < 3; i++)
        {
            var name = $"shard1{(char)('a' + i)}";
            alloc[$"shard1/{name}"] =
                new NodeAddress(i % 2 == 0 ? "h1" : "h2", new NodePorts(15000 + i, 0, 0), $"as-{name}");
        }

        return alloc;
    }

    // AAA (spec §3.4, arch/14 §5 C/R9): усыновлённая (object) нода ВНЕ домена
    // EnsureNode — self-healing off. Реальный ListNodeObjectsAsync отдаёт только
    // имена pgw-<C>- (фейк синхронизирован с этим контрактом и выкидывает as-*),
    // поэтому не-пересоздание обеспечивает GUARD по object-полю, а не матчинг
    // docker-списка: ревью Фазы 7 — старая версия теста сеяла as-* в фейк и
    // маскировала дефект (guard отсутствовал → в рантайме плодились pgw-дубли).
    [Fact]
    public async Task EnsureDeclared_ExternalObjectContainerAlive_NodeNotRecreated()
    {
        // Arrange — у всех нод object-адреса; docker-список НЕ содержит pgw-*
        // (реальный драйвер as-* не отдаёт — фейк отфильтрует их так же);
        // Patroni жив (не влияет — сверка декларации раньше проб).
        var rig = await NewRig(_ => Ok(),
            nodeObjects: ["as-shard1a", "as-shard1b", "as-shard1c"],
            addresses: AdoptedAddresses());

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: дубль не создан (guard self-healing off при пустом docker-списке),
        // состояние нод не PROVISIONING, failover-ключи HA-контура не тронуты.
        outcome.Value.Outcome.Should().Be(ProcessOutcome.Done);
        rig.Driver.EnsuredNodes.Should().BeEmpty("guard: object-нода вне домена EnsureNode");
        foreach (var node in new[] { "shard1a", "shard1b", "shard1c" })
            rig.Etcd.Store[$"/clusters/shop/shards/shard1/nodes/{node}/state"].Value
                .Should().NotBe("PROVISIONING");
    }

    // AAA (spec §3.4, R9): мёртвый object-контейнер тоже НЕ пересоздаётся —
    // rebuild поднял бы второй Patroni на тот же scope; сверка декларации не
    // применяется к усыновлённым ни в состоянии «жив», ни в состоянии «мёртв»
    // (честный UNREACHABLE — домен SuperviseShardAsync, не EnsureNode).
    [Fact]
    public async Task EnsureDeclared_AdoptedNodeObjectMissing_StillNoEnsureNode()
    {
        // Arrange — docker-список пуст: object-контейнеров нет (как будто
        // внешний контейнер умер); без guard это дало бы EnsureNode на каждый тик.
        var rig = await NewRig(_ => Ok(),
            nodeObjects: [],
            addresses: AdoptedAddresses());

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: ни EnsureNode, ни PROVISIONING — self-healing off (R9).
        outcome.Value.Outcome.Should().Be(ProcessOutcome.Done);
        rig.Driver.EnsuredNodes.Should().BeEmpty("мёртвый object не rebuild'ится (R9)");
        foreach (var node in new[] { "shard1a", "shard1b", "shard1c" })
            rig.Etcd.Store[$"/clusters/shop/shards/shard1/nodes/{node}/state"].Value
                .Should().NotBe("PROVISIONING");
    }

    // AAA (spec §3.4): усыновлённая нода без Patroni-REST (patroni=0) — живость
    // SQL-пробой SELECT 1 по admin-DSN: PG жив, сайдкар мёртв ≠ нода мертва.
    [Fact]
    public async Task Supervise_AdoptedNodeWithoutPatroni_SqlProbeKeepsRunning()
    {
        // Arrange — Patroni-пробы глухие (эмуляторы остановлены), но SQL жив;
        // у всех нод patroni=0 (object) — живость определяется SQL-пробой.
        var sql = new Fakes.FakeSql();
        var rig = await NewRig(_ => Down(),
            nodeObjects: ["as-shard1a", "as-shard1b", "as-shard1c"],
            addresses: AdoptedAddresses(), sql: sql);

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: state остаётся RUNNING (не UNREACHABLE), трек недоступности пуст.
        outcome.Value.Outcome.Should().Be(ProcessOutcome.Done);
        sql.Scalars.Should().Contain(s => s.Sql == "SELECT 1", "живость object-ноды — SQL-проба");
        foreach (var node in new[] { "shard1a", "shard1b", "shard1c" })
            rig.Etcd.Store[$"/clusters/shop/shards/shard1/nodes/{node}/state"].Value
                .Should().Be("RUNNING", "SQL жив — PG жив");
        var track = await rig.Journal.ReadUnreachableAsync("shop", CancellationToken.None);
        track.Value.Should().BeEmpty("ни одна нода не ушла в трек недоступности");
    }

    // AAA (spec §3.4): мёртвая усыновлённая нода (SQL тоже падает) дольше
    // NodeDeadSec при живом кворуме — НЕ rebuild (self-healing off, R9),
    // только честный UNREACHABLE для оператора.
    [Fact]
    public async Task Supervise_DeadAdoptedNode_NoRebuildOnlyUnreachable()
    {
        // Arrange — shard1a мертва (SQL-проба падает по её DSN), кворум жив:
        // b/c — канонические ноды с живым Patroni; лидер shard1b.
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var sql = new Fakes.FakeSql
        {
            ScalarResultByDsn = dsn => dsn.Contains("Port=15000;")
                ? Result<object?>.Failed(new ApplicationException("PG недоступен"))
                : Result<object?>.Success(1L),
        };
        var rig = await NewRig(
            port => port == 18000 ? Down() : Ok(),
            nodeObjects: ["as-shard1a", "pgw-shop-shard1-shard1b", "pgw-shop-shard1-shard1c"],
            staleUnreachableForShard1A: now - 200,
            addresses: AdoptedAddresses().ToDictionary(p => p.Key, p => p.Key is "shard1/shard1b" or "shard1/shard1c"
                ? new NodeAddress(p.Value.Host, new NodePorts(p.Value.Ports.Pg, 18000 + (p.Value.Ports.Pg - 15000), 16500 + (p.Value.Ports.Pg - 15000)))
                : p.Value),
            sql: sql);
        rig.Etcd.Seed("/service/shop-shard1/leader", """{"name":"shard1b"}""");

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: state=UNREACHABLE, docker не тронут (rebuild поднял бы второй Patroni).
        outcome.Value.Outcome.Should().Be(ProcessOutcome.Done);
        rig.Etcd.Store["/clusters/shop/shards/shard1/nodes/shard1a/state"].Value.Should().Be("UNREACHABLE");
        rig.Driver.RemovedNodes.Should().BeEmpty("усыновлённые не пересоздаются (R9)");
        rig.Driver.EnsuredNodes.Should().BeEmpty();
    }

    // AAA (spec §3.4): маркер recreate у object-ноды — self-healing off:
    // контейнер не пересоздаётся, только журнальная запись оператору.
    [Fact]
    public async Task Recreate_AdoptedNodeMarker_IgnoredWithJournal()
    {
        // Arrange — панель пометила shard1b (TO_RECREATE, soft); нода — object.
        var journalPhases = new List<string>();
        var rig = await NewRig(_ => Ok(), addresses: AdoptedAddresses());
        rig.Etcd.OnPut = key =>
        {
            if (key == "/pgworker/work/shop")
                journalPhases.Add(rig.Etcd.Store[key].Value);
        };
        rig.Etcd.Seed("/clusters/shop/shards/shard1/nodes/shard1b/state", "TO_RECREATE");
        rig.Etcd.Seed("/clusters/shop/shards/shard1/nodes/shard1b/recreate", "soft");
        rig.Etcd.Seed("/service/shop-shard1/leader", """{"name":"shard1a"}""");

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: контейнер не пересоздан; журнал содержит recreate-external (оператору).
        outcome.Value.Outcome.Should().Be(ProcessOutcome.Done);
        rig.Driver.RemovedNodes.Should().BeEmpty("self-healing off для object-нод (R9)");
        rig.Driver.EnsuredNodes.Should().BeEmpty();
        journalPhases.Should().Contain(p => p.Contains("recreate-external"),
            "журнал сообщает: пересоздание усыновлённой ноды — вручную");
    }

    // AAA (spec §3.4, R8): шарды с object-нодами не сверяются reconciler'ом —
    // их master-ключ пишет внешний HA-контур своим форматом node:port.
    [Fact]
    public async Task Reconcile_ShardWithObjectNodes_SkippedNoWrite()
    {
        // Arrange — у шарда адрес с Object; фактический primary жив (probe 200);
        // ключ указывает «не туда» — но писатель внешний, reconciler не воюет.
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/clusters/shop/shards/shard1/master", "s1a:5433");
        var addresses = new Dictionary<string, NodeAddress>
        {
            ["shard1/shard1a"] = new("local", new NodePorts(5433, 0, 0), "as-shard1a"),
        };
        var snap = new ClusterSnapshot(
            new ClusterConfig("shop", 2, "shop", null, ClusterState.Active),
            [new ShardSpec("shard1", 1, null, "s1a:5433",
            [
                new NodeSpec("shard1", "shard1a", NodeState.Running),
            ])],
            []);
        var probe = Probe(_ => Ok());
        var reconciler = new MasterKeyReconciler(etcd, [Ep], probe);
        var before = etcd.Store["/clusters/shop/shards/shard1/master"].ModRevision;

        // Act
        var result = await reconciler.ReconcileAsync(snap, addresses, CancellationToken.None);

        // Assert: etcd-put мастера НЕ было (внешний писатель не воюет, R8).
        result.IsSuccess.Should().BeTrue();
        etcd.Store["/clusters/shop/shards/shard1/master"].Value.Should().Be("s1a:5433");
        etcd.Store["/clusters/shop/shards/shard1/master"].ModRevision.Should().Be(before);
        etcd.Keepalives.Should().BeEmpty();
    }

    // Сид кластера с параметризованными именами/портами (параллельный тест).
    private static void SeedNamedCluster(Fakes.FakeEtcd etcd, string cluster, int portOffset)
    {
        etcd.Seed($"/clusters/{cluster}/config",
            $$"""{"buckets":2,"dbname":"{{cluster}}","created_unix":1755900000}""");
        etcd.Seed($"/clusters/{cluster}/shards/shard1/replicas", "3");
        for (var i = 0; i < 3; i++)
            etcd.Seed($"/clusters/{cluster}/shards/shard1/nodes/shard1{(char)('a' + i)}/state", "RUNNING");
        etcd.Seed($"/clusters/{cluster}/shards/shard1/dsn", "host=h1,h2 port=15000,15001 dbname=x user=bucket_admin");
        etcd.Seed($"/clusters/{cluster}/buckets/routing/bucket_0", "shard1");
        etcd.Seed($"/clusters/{cluster}/buckets/routing/bucket_1", "shard1");
        var alloc = new Dictionary<string, NodeAddress>();
        for (var i = 0; i < 3; i++)
            alloc[$"shard1/shard1{(char)('a' + i)}"] = new NodeAddress(
                i % 2 == 0 ? "h1" : "h2",
                new NodePorts(15000 + portOffset + i, 18000 + portOffset + i, 16500 + portOffset + i));
        etcd.Seed($"/pgworker/portalloc/{cluster}", PgWorker.Core.Model.Portalloc.Serialize(alloc));
    }

    [Fact]
    public async Task Tick_TwoClustersParallel_OneSupervisorSingleton_DeadShardsDoNotCross()
    {
        // Arrange — ОДИН синглтон-надзор (как в DI) и два кластера с шаблонно
        // совпадающими именами шардов «shard1» (rework №1): у shopA шард
        // полностью мёртв дольше ShardDeadSec, у shopB — жив. Тики идут
        // параллельно, как в ReconcileLoop при MaxClusters > 1.
        var etcd = new Fakes.FakeEtcd();
        SeedNamedCluster(etcd, "shopA", portOffset: 0);
        SeedNamedCluster(etcd, "shopB", portOffset: 100);
        var claims = new ClaimStore([Ep], etcd, TimeProvider.System);
        await claims.TryClaimClusterAsync("shopA", CancellationToken.None);
        await claims.TryClaimClusterAsync("shopB", CancellationToken.None);
        var journal = new WorkJournal(etcd, [Ep]);
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        await journal.WriteSupervisionAsync("shopA", "seed", new Dictionary<string, long>
        {
            ["shard1/shard1a"] = now - 400,
            ["shard1/shard1b"] = now - 400,
            ["shard1/shard1c"] = now - 400,
        }, CancellationToken.None);

        var driver = new Fakes.FakeDriver
        {
            NodeObjects =
            [
                "pgw-shopA-shard1-shard1a", "pgw-shopA-shard1-shard1b", "pgw-shopA-shard1-shard1c",
                "pgw-shopB-shard1-shard1a", "pgw-shopB-shard1-shard1b", "pgw-shopB-shard1-shard1c",
            ],
        };
        // Пробы по Patroni-порту: shopA (18000–18002) — глухо, shopB (18100–18102) — жив.
        var supervisor = new NodeSupervisor(
            etcd, [Ep], driver, Probe(port => port >= 18100 ? Ok() : Down()), new Fakes.FakeSql(),
            claims, journal, Thresholds, TimeProvider.System, Secrets,
            new AppParamsEnsurer(etcd, [Ep], "sslmode=require"));

        // Act — параллельные тики двух кластеров одним синглтоном
        var results = await Task.WhenAll(
            supervisor.TickAsync(await Snapshot(etcd, "shopA"), CancellationToken.None),
            supervisor.TickAsync(await Snapshot(etcd, "shopB"), CancellationToken.None));

        // Assert — мёртвые шарды изолированы ЗНАЧЕНИЕМ тика: событие эвакуации
        // получил только свой кластер, живой shopB не «унаследовал» shopA.
        results.Should().OnlyContain(r => r.IsSuccess);
        results[0].Value.DeadShards.Should().Equal("shard1");
        results[1].Value.DeadShards.Should().BeEmpty();
        driver.EnsuredNodes.Should().BeEmpty(); // контейнеры на месте — пересозданий нет
    }

    [Fact]
    public async Task MasterKeyReconciler_KeyPointsToReplica_RewrittenToPrimary()
    {
        // Arrange — ключ указывает на реплику (h2); фактический primary — h1
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/clusters/shop/shards/shard1/master", "h2:16500");
        var addresses = new Dictionary<string, NodeAddress>
        {
            ["shard1/shard1a"] = new("h1", new NodePorts(15000, 18000, 16500)),
            ["shard1/shard1b"] = new("h2", new NodePorts(15000, 18001, 16501)),
        };
        var snap = new ClusterSnapshot(
            new ClusterConfig("shop", 2, "shop", null, ClusterState.Active),
            [new ShardSpec("shard1", 2, null, "h2:16500",
            [
                new NodeSpec("shard1", "shard1a", NodeState.Running),
                new NodeSpec("shard1", "shard1b", NodeState.Running),
            ])],
            []);
        // /primary: только shard1a (порт 18000) отвечает 200
        var probe = Probe(port => port == 18000 ? Ok() : Down());
        var reconciler = new MasterKeyReconciler(etcd, [Ep], probe);

        // Act
        var result = await reconciler.ReconcileAsync(snap, addresses, CancellationToken.None);

        // Assert: ключ переписан по факту primary (h1:doorman-порт) под lease TTL 5
        result.IsSuccess.Should().BeTrue();
        etcd.Store["/clusters/shop/shards/shard1/master"].Value.Should().Be("h1:16500");
        etcd.Txns.Should().BeEmpty(); // коррекция — прямой put (не txn)
    }

    [Fact]
    public async Task MasterKeyReconciler_KeyCorrect_NoMutation()
    {
        // Arrange — ключ уже указывает на фактический primary (h1)
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/clusters/shop/shards/shard1/master", "h1:16500");
        var addresses = new Dictionary<string, NodeAddress>
        {
            ["shard1/shard1a"] = new("h1", new NodePorts(15000, 18000, 16500)),
            ["shard1/shard1b"] = new("h2", new NodePorts(15000, 18001, 16501)),
        };
        var snap = new ClusterSnapshot(
            new ClusterConfig("shop", 2, "shop", null, ClusterState.Active),
            [new ShardSpec("shard1", 2, null, "h1:16500",
            [
                new NodeSpec("shard1", "shard1a", NodeState.Running),
                new NodeSpec("shard1", "shard1b", NodeState.Running),
            ])],
            []);
        var probe = Probe(port => port == 18000 ? Ok() : Down());
        var reconciler = new MasterKeyReconciler(etcd, [Ep], probe);
        var before = etcd.Store["/clusters/shop/shards/shard1/master"].ModRevision;

        // Act
        var result = await reconciler.ReconcileAsync(snap, addresses, CancellationToken.None);

        // Assert: синхрон — ноль мутаций (не второй регулярный писатель, P11)
        result.IsSuccess.Should().BeTrue();
        etcd.Store["/clusters/shop/shards/shard1/master"].ModRevision.Should().Be(before);
    }

    [Fact]
    public async Task MasterKeyReconciler_HeldLease_RenewedEveryPass()
    {
        // Arrange — ключа нет, primary жив: reconcile выдаст lease и запомнит его
        var etcd = new Fakes.FakeEtcd();
        var addresses = new Dictionary<string, NodeAddress>
        {
            ["shard1/shard1a"] = new("h1", new NodePorts(15000, 18000, 16500)),
        };
        var snap = new ClusterSnapshot(
            new ClusterConfig("shop", 1, "shop", null, ClusterState.Active),
            [new ShardSpec("shard1", 1, null, null,
            [
                new NodeSpec("shard1", "shard1a", NodeState.Running),
            ])],
            []);
        var reconciler = new MasterKeyReconciler(etcd, [Ep], Probe(port => port == 18000 ? Ok() : Down()));

        // Act — сверка (put под lease), затем два прохода продления
        var result = await reconciler.ReconcileAsync(snap, addresses, CancellationToken.None);
        await reconciler.RenewHeldAsync(CancellationToken.None);
        await reconciler.RenewHeldAsync(CancellationToken.None);

        // Assert — выданный lease продлевается каждым проходом (период TTL/2.5),
        // ключ не мигает между тиками сверки
        result.IsSuccess.Should().BeTrue();
        etcd.Keepalives.Should().HaveCount(2);
    }

    [Fact]
    public async Task MasterKeyReconciler_PrimaryDead_HeldLeaseDropped()
    {
        // Arrange — ключ записан reconciler'ом (primary был жив), потом primary замолчал
        var etcd = new Fakes.FakeEtcd();
        var addresses = new Dictionary<string, NodeAddress>
        {
            ["shard1/shard1a"] = new("h1", new NodePorts(15000, 18000, 16500)),
        };
        var snap = new ClusterSnapshot(
            new ClusterConfig("shop", 1, "shop", null, ClusterState.Active),
            [new ShardSpec("shard1", 1, null, null,
            [
                new NodeSpec("shard1", "shard1a", NodeState.Running),
            ])],
            []);
        var alive = true;
        var reconciler = new MasterKeyReconciler(
            etcd, [Ep], Probe(port => port == 18000 && alive ? Ok() : Down()));
        await reconciler.ReconcileAsync(snap, addresses, CancellationToken.None);

        // Act — сверка с замолчавшим primary + проход продления тем же инстансом
        alive = false;
        await reconciler.ReconcileAsync(snap, addresses, CancellationToken.None);
        await reconciler.RenewHeldAsync(CancellationToken.None);

        // Assert — lease снят с продления: ключ гаснет ≤ TTL (P11, эвакуация)
        etcd.Keepalives.Should().BeEmpty();
    }

    [Fact]
    public void MasterKeyReconciler_KeepalivePeriod_TwoAndHalfTimesFasterThanTtl()
    {
        // Arrange/Act/Assert — продление в 2.5 раза чаще периода протухания:
        // TTL 5с → период keepalive 2с
        MasterKeyReconciler.KeepalivePeriod.Should().Be(TimeSpan.FromSeconds(2));
    }

    // AAA: миграция app_params (arch/14 §5 C) — ноды шарда с dsn без ключа получают
    // дефолт; ручное значение существующего ключа не перезаписывается
    [Fact]
    public async Task Tick_NodeWithoutAppParams_MigrationPutsDefault()
    {
        // Arrange — кластер «до app_params»: у shard1b ключ есть (ручной), у остальных нет
        var rig = await NewRig(_ => Ok());
        rig.Etcd.Seed("/clusters/shop/shards/shard1/nodes/shard1b/app_params", "sslmode=verify-full");

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert — отсутствующие дописаны дефолтом, ручное значение живо (put-if-absent)
        outcome.IsSuccess.Should().BeTrue();
        rig.Etcd.Store["/clusters/shop/shards/shard1/nodes/shard1a/app_params"].Value
            .Should().Be("sslmode=require");
        rig.Etcd.Store["/clusters/shop/shards/shard1/nodes/shard1b/app_params"].Value
            .Should().Be("sslmode=verify-full");
    }
}
