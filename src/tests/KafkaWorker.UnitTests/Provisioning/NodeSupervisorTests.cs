using System.Text.Json;
using FluentAssertions;
using KafkaWorker.Core.Model;
using KafkaWorker.Etcd.Coordination;
using KafkaWorker.Etcd.Parsing;
using KafkaWorker.Provisioning.Kafka;
using KafkaWorker.Provisioning.Processes;
using Microsoft.Extensions.Logging.Abstractions;

namespace KafkaWorker.UnitTests.Provisioning;

// NodeSupervisor (arch/16 §5 C, spec §4.2 C): снесённый контейнер
// пересоздаётся с тем же томом; молчание дольше NodeDeadSec → UNREACHABLE +
// пересоздание КОНТЕЙНЕРА без удаления тома (чистый том — только при
// доказанной физической утрате тома в docker; RF=1 + утрата → warning);
// слепая проба (DescribeCluster недоступен) не стартует и не исполняет бюджет
// молчания; не более одного пересоздания по молчанию за тик; чужие процессы
// (TO_REMOVE/REMOVING/PROVISIONING) не трогаются. Потеря данных недопустима.

public class NodeSupervisorTests
{
    private const string Ep = "http://etcd:2379";

    private sealed record Rig(
        Fakes.FakeEtcd Etcd,
        Fakes.FakeKafkaDriver Driver,
        FakeKafkaAdminClient Admin,
        FakeAdminFactory AdminFactory,
        ClaimStore Claims,
        WorkJournal Journal,
        NodeSupervisor Supervisor);

    private static async Task<Rig> NewRig(
        int nodeDeadSec = 90,
        int rf = 3,
        Action<Fakes.FakeEtcd, Fakes.FakeKafkaDriver, FakeKafkaAdminClient>? setup = null,
        KafkaClusterBackoff? backoff = null,
        bool healer = false)
    {
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/kafka/clusters/events/config",
            $$"""{"brokers":3,"replication_factor":{{rf}},"min_insync_replicas":1,"default_partitions":12,"default_retention_ms":604800000,"created_unix":1756500000}""");
        for (var k = 1; k <= 3; k++)
        {
            etcd.Seed($"/kafka/clusters/events/brokers/broker{k}/state", "RUNNING");
            etcd.Seed($"/kafka/clusters/events/brokers/broker{k}/role", k <= 3 ? "controller" : "broker");
        }
        etcd.Seed("/kafka/clusters/events/endpoints", "h1:16000,h1:16001,h1:16002");
        etcd.Seed("/kafka/clusters/events/app_user", "app");
        etcd.Seed("/kafka/clusters/events/app_password", "AbCdEf0123456789AbCdEf0123456789");
        etcd.Seed("/kafkaworker/portalloc/events",
            """{"broker1":{"host":"h1","client":16000},"broker2":{"host":"h1","client":16001},"broker3":{"host":"h1","client":16002}}""");

        var claims = new ClaimStore([Ep], etcd, TimeProvider.System);
        await claims.TryClaimClusterAsync("events", CancellationToken.None);
        var journal = new WorkJournal(etcd, [Ep]);
        var driver = new Fakes.FakeKafkaDriver
        {
            NodeObjects = ["kfw-events-broker1", "kfw-events-broker2", "kfw-events-broker3"],
        };
        var admin = new FakeKafkaAdminClient
        {
            ClusterView = new KafkaClusterView(
                [new KafkaBrokerView(1, "b1"), new KafkaBrokerView(2, "b2"), new KafkaBrokerView(3, "b3")],
                ControllerId: 1),
        };
        var adminFactory = new FakeAdminFactory(admin);
        var options = new ProvisioningOptions(16000, 16999, 600, nodeDeadSec, null, "apache/kafka:4.0.0");
        var supervisor = new NodeSupervisor(
            etcd, [Ep], driver, claims, journal, adminFactory, options,
            backoff: backoff,
            healer: healer
                ? new PortAllocHealer(
                    etcd, [Ep], driver, claims, journal,
                    new PortAllocLock([Ep], etcd, TimeProvider.System, claims.InstanceId),
                    new PortAllocIndex(etcd, [Ep], NullLogger<PortAllocIndex>.Instance), options)
                : null);
        setup?.Invoke(etcd, driver, admin);
        return new Rig(etcd, driver, admin, adminFactory, claims, journal, supervisor);
    }

    private static async Task<KafkaClusterSnapshot> Snapshot(Fakes.FakeEtcd etcd)
    {
        var range = await etcd.RangeAsync(Ep, "/kafka/clusters/", CancellationToken.None);
        return KafkaSnapshotParser.Parse(range.Value).Value.Single(c => c.Cluster == "events");
    }

    private sealed class FakeAdminFactory(FakeKafkaAdminClient client) : IKafkaAdminClientFactory
    {
        public int CreateCalls { get; private set; }

        public IKafkaAdminClient Create(string bootstrap, string user, string password)
        {
            CreateCalls++;
            return client;
        }
    }

    [Fact]
    public async Task Run_MissingContainer_RecreatedWithSameEnv()
    {
        // Arrange: broker2 снесён руками (docker-объекта нет), кластер жив.
        var rig = await NewRig();
        rig.Driver.NodeObjects.Remove("kfw-events-broker2");

        // Act: тик надзора.
        var result = await rig.Supervisor.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: EnsureNode вызван для broker2 с тем же env/портом из portalloc
        // (advertised стабилен); контейнер снова в списке объектов.
        result.IsSuccess.Should().BeTrue();
        var spec = rig.Driver.Ensured.Single(s => s.NodeName == "broker2");
        spec.ClientHostPort.Should().Be(16001);
        spec.Env["KAFKA_ADVERTISED_LISTENERS"].Should().Contain("CLIENT://h1:16001");
        rig.Driver.NodeObjects.Should().Contain("kfw-events-broker2");
        // Успешные ноды не тронуты; пересозданный — PROVISIONING (в RUNNING
        // переведёт следующий цикл по факту готовности).
        rig.Driver.Ensured.Should().HaveCount(1);
        rig.Etcd.Store["/kafka/clusters/events/brokers/broker2/state"].Value.Should().Be("PROVISIONING");
    }

    // Сид journal-трека молчания: unreachable {broker: first_seen_unix}.
    private static void SeedSilent(Fakes.FakeEtcd etcd, params (string Broker, long SinceUnix)[] entries)
        => etcd.Seed("/kafkaworker/work/events",
            "{\"op\":\"supervise\",\"phase\":\"supervising\",\"instance\":\"x\",\"updated_unix\":1," +
            "\"unreachable\":{" + string.Join(",", entries.Select(e => $"\"{e.Broker}\":{e.SinceUnix}")) + "}}");

    [Fact]
    public async Task Run_BrokerSilentBeyondBudget_UnreachableAndRecreateKeepsVolume()
    {
        // Arrange: broker3 не отвечает в DescribeCluster (нет id=3) и молчит
        // уже дольше NodeDeadSec (трек в journal.unreachable засеян в прошлом);
        // том данных физически существует.
        var rig = await NewRig(nodeDeadSec: 90);
        rig.Admin.ClusterView = new KafkaClusterView(
            [new KafkaBrokerView(1, "b1"), new KafkaBrokerView(2, "b2")], ControllerId: 1);
        var since = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 200;
        SeedSilent(rig.Etcd, ("broker3", since));

        // Act: тик надзора.
        var result = await rig.Supervisor.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: broker3 → UNREACHABLE, контейнер пересоздан БЕЗ удаления
        // тома (removeVolume=false — данные неприкосновенны, брокер вернётся
        // со своим томом); RF=3 — без warning.
        result.IsSuccess.Should().BeTrue();
        rig.Etcd.Store["/kafka/clusters/events/brokers/broker3/state"].Value.Should().Be("PROVISIONING");
        rig.Driver.Removed.Should().Contain(("broker3", false));
        rig.Driver.Removed.Should().NotContain(r => r.Node == "broker3" && r.RemoveVolume);
        rig.Driver.Ensured.Should().Contain(s => s.NodeName == "broker3");
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.LastError.Should().BeNull("том сохранён — потери данных нет");
    }

    [Fact]
    public async Task Run_BrokerSilentVolumeLost_RecreatedWithFreshVolume()
    {
        // Arrange: broker3 молчит дольше бюджета И том данных физически
        // утрачен (объекта volume в docker нет — терять нечего).
        var rig = await NewRig(nodeDeadSec: 90);
        rig.Admin.ClusterView = new KafkaClusterView(
            [new KafkaBrokerView(1, "b1"), new KafkaBrokerView(2, "b2")], ControllerId: 1);
        rig.Driver.MissingVolumes.Add("kfw-events-broker3-data");
        SeedSilent(rig.Etcd, ("broker3", DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 200));

        // Act: тик надзора.
        var result = await rig.Supervisor.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: пересоздание с чистым томом (removeVolume=true — воркер
        // пересоздаёт утраченный том заново); RF=3 — warning не нужен.
        result.IsSuccess.Should().BeTrue();
        rig.Driver.Removed.Should().Contain(("broker3", true));
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.LastError.Should().BeNull("RF>1 — rejoin репликацией");
    }

    [Fact]
    public async Task Run_SilentBrokerRfOneVolumeLost_WarningJournaled()
    {
        // Arrange: RF=1-кластер, broker1 молчит дольше бюджета, том утрачен.
        var rig = await NewRig(nodeDeadSec: 90, rf: 1);
        rig.Admin.ClusterView = new KafkaClusterView([], ControllerId: null);
        rig.Driver.MissingVolumes.Add("kfw-events-broker1-data");
        SeedSilent(rig.Etcd, ("broker1", DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 200));

        // Act: тик надзора.
        var result = await rig.Supervisor.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: пересоздание с чистым томом + journal-warning о потере
        // единственной копии данных (документированное поведение).
        result.IsSuccess.Should().BeTrue();
        rig.Driver.Removed.Should().Contain(("broker1", true));
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.LastError.Should().Contain("RF=1");
    }

    [Fact]
    public async Task Run_SilentBrokerRfOneVolumeKept_NoWarning()
    {
        // Arrange: RF=1-кластер, broker1 молчит дольше бюджета, но том жив.
        var rig = await NewRig(nodeDeadSec: 90, rf: 1);
        rig.Admin.ClusterView = new KafkaClusterView([], ControllerId: null);
        SeedSilent(rig.Etcd, ("broker1", DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 200));

        // Act: тик надзора.
        var result = await rig.Supervisor.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: том не удаляется (removeVolume=false), warning не пишется —
        // данные в томе, брокер вернётся с ними.
        result.IsSuccess.Should().BeTrue();
        rig.Driver.Removed.Should().Contain(("broker1", false));
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.LastError.Should().BeNull("том жив — данные на месте");
    }

    [Fact]
    public async Task Run_SilentBrokerWithinBudget_Waited()
    {
        // Arrange: broker3 молчит, но меньше NodeDeadSec (трек свежий).
        var rig = await NewRig(nodeDeadSec: 90);
        rig.Admin.ClusterView = new KafkaClusterView(
            [new KafkaBrokerView(1, "b1"), new KafkaBrokerView(2, "b2")], ControllerId: 1);
        SeedSilent(rig.Etcd, ("broker3", DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 10));

        // Act: тик надзора.
        var result = await rig.Supervisor.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: ждём — контейнер жив, state не менялся, пересоздания нет.
        result.IsSuccess.Should().BeTrue();
        rig.Etcd.Store["/kafka/clusters/events/brokers/broker3/state"].Value.Should().Be("RUNNING");
        rig.Driver.Removed.Should().BeEmpty();
    }

    [Fact]
    public async Task Run_TwoSilentBrokers_OnlyOneRecreatedPerTick()
    {
        // Arrange: broker2 и broker3 молчат дольше бюджета (оба вне
        // DescribeCluster, трек давно у обоих).
        var rig = await NewRig(nodeDeadSec: 90);
        rig.Admin.ClusterView = new KafkaClusterView(
            [new KafkaBrokerView(1, "b1")], ControllerId: 1);
        var since = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 200;
        SeedSilent(rig.Etcd, ("broker2", since), ("broker3", since));

        // Act: один тик надзора.
        var result = await rig.Supervisor.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: пересоздан ТОЛЬКО broker2 (первый по имени) — одно
        // пересоздание по молчанию за тик (ждём возврата в кластер/ISR);
        // broker3 остаётся в треке на следующий тик, контейнер/state не тронуты.
        result.IsSuccess.Should().BeTrue();
        rig.Driver.Removed.Should().HaveCount(1).And.Contain(("broker2", false));
        rig.Driver.Ensured.Should().Contain(s => s.NodeName == "broker2");
        rig.Etcd.Store["/kafka/clusters/events/brokers/broker2/state"].Value.Should().Be("PROVISIONING");
        rig.Etcd.Store["/kafka/clusters/events/brokers/broker3/state"].Value.Should().Be("RUNNING");
        var track = await rig.Journal.ReadUnreachableAsync("events", CancellationToken.None);
        track.Value.Should().NotContainKey("broker2").And.ContainKey("broker3");
    }

    [Fact]
    public async Task Run_ProbeUnavailable_NoRecreationAndTrackPreserved()
    {
        // Arrange: DescribeCluster-проба недоступна (сеть/слепота воркера);
        // у broker3 давний трек молчания; все контейнеры живы, тома живы.
        var rig = await NewRig(nodeDeadSec: 90);
        rig.Admin.ClusterError = new ApplicationException("probe timeout");
        var since = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 500;
        SeedSilent(rig.Etcd, ("broker3", since));

        // Act: тик надзора.
        var result = await rig.Supervisor.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: слепота пробы ≠ молчание брокеров — ничего не пересоздано,
        // тома не тронуты, state не менялся; прошлый трек сохранён как есть
        // (budget не сброшен и не расширен на остальных).
        result.IsSuccess.Should().BeTrue();
        rig.Driver.Removed.Should().BeEmpty();
        rig.Driver.Ensured.Should().BeEmpty();
        rig.Etcd.Store["/kafka/clusters/events/brokers/broker3/state"].Value.Should().Be("RUNNING");
        var track = await rig.Journal.ReadUnreachableAsync("events", CancellationToken.None);
        track.Value.Should().HaveCount(1).And.ContainKey("broker3");
        track.Value["broker3"].Should().Be(since);
    }

    [Fact]
    public async Task Run_ProbeUnavailable_SilenceBudgetNotStarted()
    {
        // Arrange: проба недоступна, journal-трека молчания нет.
        var rig = await NewRig(nodeDeadSec: 90);
        rig.Admin.ClusterError = new ApplicationException("probe timeout");

        // Act: тик надзора.
        var result = await rig.Supervisor.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: бюджет молчания НЕ стартует по слепой пробе (только
        // успешный ответ «в кластере нет брокера X» начинает счётчик X);
        // трек остался пуст.
        result.IsSuccess.Should().BeTrue();
        rig.Driver.Removed.Should().BeEmpty();
        var track = await rig.Journal.ReadUnreachableAsync("events", CancellationToken.None);
        track.Value.Should().BeEmpty();
    }

    [Fact]
    public async Task Run_ForeignStates_Untouched()
    {
        // Arrange: broker2 — TO_REMOVE (панель), broker3 — PROVISIONING
        // (provisioning-процесс), оба контейнера отсутствуют.
        var rig = await NewRig();
        rig.Etcd.Seed("/kafka/clusters/events/brokers/broker2/state", "TO_REMOVE");
        rig.Etcd.Seed("/kafka/clusters/events/brokers/broker3/state", "PROVISIONING");
        rig.Driver.NodeObjects.Remove("kfw-events-broker2");
        rig.Driver.NodeObjects.Remove("kfw-events-broker3");
        rig.Admin.ClusterView = new KafkaClusterView([new KafkaBrokerView(1, "b1")], ControllerId: 1);

        // Act: тик надзора.
        var result = await rig.Supervisor.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: только broker1 в зоне надзора (жив) — никаких пересозданий
        // и перезаписей state у чужих процессов.
        result.IsSuccess.Should().BeTrue();
        rig.Driver.Ensured.Should().BeEmpty();
        rig.Etcd.Store["/kafka/clusters/events/brokers/broker2/state"].Value.Should().Be("TO_REMOVE");
        rig.Etcd.Store["/kafka/clusters/events/brokers/broker3/state"].Value.Should().Be("PROVISIONING");
    }

    [Fact]
    public async Task Run_NoEndpoints_NoProbeNoCrash()
    {
        // Arrange: Active-кластер без endpoints/app-кредов (ещё поднимается).
        var rig = await NewRig();
        rig.Etcd.Store.Remove("/kafka/clusters/events/endpoints");
        rig.Etcd.Store.Remove("/kafka/clusters/events/app_password");

        // Act: тик надзора.
        var result = await rig.Supervisor.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: проба невозможна — не ошибка; пересоздание снесённых живо.
        result.IsSuccess.Should().BeTrue();
    }

    // AAA: backoff-окно активно → проба не ходит в сеть (0 Create), кластер
    // трактуется как слепая проба (probeBlind) — надзор не падает, docker-часть
    // работает (spec §3.2).
    [Fact]
    public async Task Run_BackoffWindow_ProbeSkippedWithoutClient()
    {
        // Arrange: окно блокировки events активно (неудача записана «сейчас»).
        var backoff = new KafkaClusterBackoff(new FixedTimeProvider());
        backoff.RecordFailure("events", "connection refused");
        var rig = await NewRig(backoff: backoff);

        // Act: тик надзора в окне.
        var result = await rig.Supervisor.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: тик успешен, клиент не создавался.
        result.IsSuccess.Should().BeTrue();
        rig.AdminFactory.CreateCalls.Should().Be(0);
    }

    // Фаза последней записи журнала работ (waiting-portalloc-lock и др.).
    private static async Task<string?> JournalPhaseAsync(Rig rig)
    {
        var kv = (await rig.Etcd.GetAsync(Ep, "/kafkaworker/work/events", CancellationToken.None)).Value;
        if (kv is null)
            return null;
        using var doc = JsonDocument.Parse(kv.Value);
        return doc.RootElement.GetProperty("phase").GetString();
    }

    // Декларация до одного брокера: ladder-сценарии гоняют broker1 одного.
    private static void KeepSingleBroker(Rig rig)
    {
        foreach (var k in new[] { "broker2", "broker3" })
        {
            rig.Etcd.Store.Remove($"/kafka/clusters/events/brokers/{k}/state");
            rig.Etcd.Store.Remove($"/kafka/clusters/events/brokers/{k}/role");
        }
    }

    // AAA: клэйм занят — тик надзора УСПЕШЕН (InProgress), journal-фаза
    // waiting-portalloc-lock, никаких мутаций — следующий тик повторит.
    [Fact]
    public async Task Run_PortLockBusy_WaitsWithoutError()
    {
        // Arrange: один брокер, portalloc утерян, контейнер снесён, клэйм у соседа.
        var rig = await NewRig(healer: true);
        KeepSingleBroker(rig);
        rig.Etcd.Store.Remove("/kafkaworker/portalloc/events");
        rig.Driver.NodeObjects.Remove("kfw-events-broker1");
        var foreign = new PortAllocLock([Ep], rig.Etcd, TimeProvider.System, "other-instance");
        (await foreign.TryAcquireAsync(CancellationToken.None)).Value.Should().BeTrue();

        // Act: тик надзора.
        var result = await rig.Supervisor.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: waiting-portalloc-lock — не ошибка тика; действий не было.
        result.IsSuccess.Should().BeTrue("waiting-portalloc-lock — InProgress, не ошибка тика");
        (await JournalPhaseAsync(rig)).Should().Be("waiting-portalloc-lock");
        rig.Driver.Ensured.Should().BeEmpty("под чужим клэймом — никаких действий");
    }

    // AAA: portalloc утерян при объявленных брокерах (инцидент 2026-09-04) —
    // надзор самолечится (E9): контейнер снесен + portalloc пуст → новая
    // аллокация + пересоздание + PROVISIONING (вечного «не закреплён» нет).
    [Fact]
    public async Task Run_LostPortAlloc_HealsInsteadOfDeadlock()
    {
        // Arrange: один брокер, portalloc утерян, контейнер снесён, инспекции
        // нет (S7-ветка).
        var rig = await NewRig(healer: true);
        KeepSingleBroker(rig);
        rig.Etcd.Store.Remove("/kafkaworker/portalloc/events");
        rig.Driver.NodeObjects.Remove("kfw-events-broker1");
        rig.Driver.Endpoints.Clear();

        // Act: тик надзора.
        var result = await rig.Supervisor.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: брокер аллоцирован заново и пересоздан, state=PROVISIONING.
        result.IsSuccess.Should().BeTrue();
        rig.Driver.Ensured.Should().ContainSingle(s => s.NodeName == "broker1");
        (await rig.Etcd.GetAsync(Ep, "/kafka/clusters/events/brokers/broker1/state", CancellationToken.None))
            .Value!.Value.Should().Be("PROVISIONING");
    }

    // AAA (Ф7-1): PROVISIONING-брокер add-broker'а — контейнер жив, проба
    // видит, но endpoints ещё БЕЗ его адреса → остаётся PROVISIONING; после
    // endpoints-RMW владельца → следующий тик переводит RUNNING (литералы
    // 16000–16002 — константы сида рига, FakeEtcd — чистая память).
    [Fact]
    public async Task Run_ProvisioningWithoutEndpoints_KeepsProvisioning()
    {
        // Arrange: дефолтный риг (3 брокера, portalloc/контейнеры на месте);
        // зрячая проба видит broker1+broker2 (broker3 «молчит» — трек стартует,
        // NodeDeadSec=90 — пересозданий в тесте нет).
        var rig = await NewRig();
        rig.Admin.ClusterView = new KafkaClusterView(
            [new KafkaBrokerView(1, "b1"), new KafkaBrokerView(2, "b2")], ControllerId: 1);

        // Брокер add-broker'а: state=PROVISIONING; portalloc рига НЕ трогаем
        // (закрепления всех трёх на месте — healer-гейт не срабатывает).
        await rig.Etcd.PutAsync(Ep, "/kafka/clusters/events/brokers/broker2/state",
            "PROVISIONING", null, CancellationToken.None);

        // Фаза 1: endpoints БЕЗ адреса broker2 (RMW add-broker'а «ещё не дошёл»;
        // дефолтные endpoints содержат 16001 — перезаписываем).
        await rig.Etcd.PutAsync(Ep, "/kafka/clusters/events/endpoints",
            "h1:16000", null, CancellationToken.None);

        // Act: тик надзора.
        var result = await rig.Supervisor.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: перевода нет — advertised-адреса broker2 нет в endpoints,
        // значит чужой процесс (add-broker) не закончен, RUNNING не наш.
        result.IsSuccess.Should().BeTrue();
        (await rig.Etcd.GetAsync(Ep, "/kafka/clusters/events/brokers/broker2/state",
            CancellationToken.None)).Value!.Value.Should().Be("PROVISIONING",
            "адреса broker2 (h1:16001) нет в endpoints — RUNNING не наш");

        // Act 2: владелец доделал endpoints-RMW (адрес broker2 появился) →
        // следующий тик переводит (перевод читает снапшот начала тика).
        await rig.Etcd.PutAsync(Ep, "/kafka/clusters/events/endpoints",
            "h1:16000,h1:16001,h1:16002", null, CancellationToken.None);

        (await rig.Supervisor.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None))
            .IsSuccess.Should().BeTrue();

        // Assert 2: третий факт сошёлся — broker2 RUNNING.
        (await rig.Etcd.GetAsync(Ep, "/kafka/clusters/events/brokers/broker2/state",
            CancellationToken.None)).Value!.Value.Should().Be("RUNNING");
    }

    // AAA (Ф7-4): portalloc закреплён, endpoints отстал (сбой RMW ветки 3) —
    // тик надзора сходит endpoints к канону; повторный тик — значение стабильно.
    [Fact]
    public async Task Run_EndpointsLagPortalloc_Converges()
    {
        // Arrange: дефолтный риг (portalloc: broker1 h1:16000, broker2 h1:16001,
        // broker3 h1:16002 — дефолт сида), endpoints «недоехавший» — старое значение.
        var rig = await NewRig();
        await rig.Etcd.PutAsync(Ep, "/kafka/clusters/events/endpoints",
            "h1:21099", null, CancellationToken.None);

        // Act: тик надзора — сходимость.
        var result = await rig.Supervisor.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: endpoints = advertise-адреса всех закреплённых брокеров
        // (AdvertisedClientHost=null в риге → host:port из portalloc; ожидание —
        // парс сида portalloc, порядок по имени брокера — канон).
        result.IsSuccess.Should().BeTrue();
        var portAlloc = (await rig.Etcd.GetAsync(Ep, "/kafkaworker/portalloc/events",
            CancellationToken.None)).Value!.Value;
        using var doc = JsonDocument.Parse(portAlloc);
        var expected = string.Join(",", doc.RootElement.EnumerateObject()
            .OrderBy(p => p.Name, StringComparer.Ordinal)
            .Select(p => $"h1:{p.Value.GetProperty("client").GetInt32()}"));
        (await rig.Etcd.GetAsync(Ep, "/kafka/clusters/events/endpoints", CancellationToken.None))
            .Value!.Value.Should().Be(expected,
                "канон = advertise-адреса всех закреплённых брокеров (сходится за один тик)");

        // Повторный тик: значение уже канонично — стабильно (ноль записей).
        (await rig.Supervisor.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None))
            .IsSuccess.Should().BeTrue();
        (await rig.Etcd.GetAsync(Ep, "/kafka/clusters/events/endpoints", CancellationToken.None))
            .Value!.Value.Should().Be(expected, "повторный тик ничего не меняет");
    }
}
