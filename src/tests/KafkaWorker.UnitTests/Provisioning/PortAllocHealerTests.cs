using System.Text.Json;
using FluentAssertions;
using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using KafkaWorker.Docker.Drivers;
using Shared.Etcd.Client;
using KafkaWorker.Etcd.Parsing;
using KafkaWorker.Core.Templates;
using KafkaWorker.Provisioning.Processes;
using Microsoft.Extensions.Logging.Abstractions;

namespace KafkaWorker.UnitTests.Provisioning;

// Лестница E9 (spec §3.3): portalloc пуст при объявленных брокерах —
// тупик «не закреплён в portalloc» (инцидент as-kafkaworker 2026-09-04)
// заменён самолечением: инспекция живого контейнера либо новая аллокация
// под клэймом locks/portalloc + RMW endpoints.
public class PortAllocHealerTests
{
    private const string Ep = "http://etcd:2379";

    private sealed record Rig(
        Fakes.FakeEtcd Etcd,
        Fakes.FakeKafkaDriver Driver,
        ClaimStore Claims,
        WorkJournal Journal,
        ProvisioningOptions Options,
        KafkaClusterSnapshot Snapshot,
        IReadOnlyDictionary<string, NodeAddress> Addresses,
        PortAllocHealer Healer);

    // Риг: Active-кластер events (broker1 controller+RUNNING), portalloc ключ
    // ОТСУТСТВУЕТ (утерян), кроме варианта pinned — тогда сидируется.
    private static async Task<Rig> NewRig(NodeAddress? pinned = null)
    {
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/kafka/clusters/events/config",
            """{"brokers":1,"replication_factor":1,"min_insync_replicas":1,"default_partitions":1,"default_retention_ms":3600000}""");
        etcd.Seed("/kafka/clusters/events/brokers/broker1/state", "RUNNING");
        etcd.Seed("/kafka/clusters/events/brokers/broker1/role", "controller");
        etcd.Seed("/kafka/clusters/events/app_user", "app");
        etcd.Seed("/kafka/clusters/events/app_password", "AbCdEf0123456789AbCdEf0123456789");
        etcd.SeedSecurity("events"); // канонический кластер (ветка 3 строит env с PKI)
        if (pinned is { } addr)
            etcd.Seed("/kafkaworker/portalloc/events",
                $"{{\"broker1\":{{\"host\":\"{addr.Host}\",\"client\":{addr.ClientPort}}}}}");

        var claims = new ClaimStore("/kafkaworker", [Ep], etcd, TimeProvider.System);
        await claims.TryClaimClusterAsync("events", CancellationToken.None);
        var journal = new WorkJournal("/kafkaworker", etcd, [Ep]);
        var driver = new Fakes.FakeKafkaDriver(); // контейнеров нет: NodeObjects пуст

        var range = await etcd.RangeAsync(Ep, "/kafka/clusters/", CancellationToken.None);
        var snapshot = KafkaSnapshotParser.Parse(range.Value).Value.Single(c => c.Cluster == "events");

        var portAlloc = new PortAllocIndex(etcd, [Ep], NullLogger<PortAllocIndex>.Instance);
        var addresses = await ReadPortAllocAsync(etcd);
        var options = new ProvisioningOptions(21000, 21100, 100, 90, "host.docker.internal", "apache/kafka:4.0.0");
        var healer = new PortAllocHealer(
            etcd, [Ep], driver, claims, journal,
            new PortAllocLock("/kafkaworker", [Ep], etcd, TimeProvider.System, claims.InstanceId),
            portAlloc, options, new BrokerCertificateCache());
        return new Rig(etcd, driver, claims, journal, options, snapshot, addresses, healer);
    }

    // Чтение portalloc рига (форма записи — arch/15 §4).
    private static async Task<IReadOnlyDictionary<string, NodeAddress>> ReadPortAllocAsync(Fakes.FakeEtcd etcd)
    {
        var kv = (await etcd.GetAsync(Ep, "/kafkaworker/portalloc/events", CancellationToken.None)).Value;
        if (kv is null)
            return new Dictionary<string, NodeAddress>();
        var addresses = new Dictionary<string, NodeAddress>();
        using var doc = JsonDocument.Parse(kv.Value);
        foreach (var node in doc.RootElement.EnumerateObject())
            addresses[node.Name] = new NodeAddress(
                node.Value.GetProperty("host").GetString()!,
                node.Value.GetProperty("client").GetInt32());
        return addresses;
    }

    private static async Task<string?> PortAllocJson(Rig rig)
        => (await rig.Etcd.GetAsync(Ep, "/kafkaworker/portalloc/events", CancellationToken.None)).Value?.Value;

    // Чужой держатель клэйма locks/portalloc.
    private static async Task<PortAllocLock> HoldPortLockAsync(Rig rig)
    {
        var foreign = new PortAllocLock("/kafkaworker", [Ep], rig.Etcd, TimeProvider.System, "other-instance");
        (await foreign.TryAcquireAsync(CancellationToken.None)).Value.Should().BeTrue();
        return foreign;
    }

    // AAA: ветка 2 — контейнер жив: portalloc восстановлен инспекцией
    // (put-if-absent version==0), контейнер НЕ трогаем, адрес = inspected.
    [Fact]
    public async Task Resolve_ContainerAlive_ReconstructsPortAlloc()
    {
        // Arrange: portalloc ключа нет; инспекция засеяна.
        var rig = await NewRig();
        rig.Driver.Endpoints["broker1"] = new NodeEndpointInspection(
            "h1", 21037, "CLIENT://host.docker.internal:21037");

        // Act: лестница для broker1.
        var resolved = await rig.Healer.ResolveAsync(rig.Snapshot, "broker1", rig.Addresses, CancellationToken.None);

        // Assert: адрес из инспекции, запись восстановлена, контейнер не тронут.
        resolved.IsSuccess.Should().BeTrue();
        resolved.Value.Address.Should().Be(new NodeAddress("h1", 21037));
        resolved.Value.Recreated.Should().BeFalse("a live container is left untouched");
        (await PortAllocJson(rig)).Should().Contain("21037");
        rig.Driver.Removed.Should().BeEmpty("a live container is left untouched");
        rig.Driver.Ensured.Should().BeEmpty("no recreation happened");
    }

    // AAA: ветка 3 — контейнера нет (S7): новая аллокация под клэймом,
    // контейнер пересоздан по новому адресу, endpoints RMW-обновлён.
    [Fact]
    public async Task Resolve_ContainerGone_ReallocatesAndRecreates()
    {
        // Arrange: инспекция null (нет записи), контейнеров нет.
        var rig = await NewRig();

        // Act: лестница для broker1.
        var resolved = await rig.Healer.ResolveAsync(rig.Snapshot, "broker1", rig.Addresses, CancellationToken.None);

        // Assert: аллокация в диапазоне опций + пересоздание + endpoints обновлён.
        resolved.IsSuccess.Should().BeTrue();
        resolved.Value.Recreated.Should().BeTrue("S7: container recreated");
        var allocatedPort = resolved.Value.Address.ClientPort;
        allocatedPort.Should().BeInRange(rig.Options.PortFrom, rig.Options.PortTo);
        (await PortAllocJson(rig)).Should().Contain(allocatedPort.ToString());
        rig.Driver.Ensured.Should().ContainSingle(s => s.NodeName == "broker1" && s.ClientHostPort == allocatedPort);
        var endpoints = (await rig.Etcd.GetAsync(Ep, "/kafka/clusters/events/endpoints", CancellationToken.None)).Value;
        endpoints.Should().NotBeNull();
        endpoints!.Value.Should().Contain(allocatedPort.ToString(), "endpoints RMW-updated (clients re-read on the next tick)");
    }

    // AAA: ветка 3 — EnsureNode (docker-фаза) исполняется ПОСЛЕ освобождения
    // portalloc-клэйма: секция лока — только «чтение занятости → запись
    // portalloc» (arch/15 §4, без keepalive: docker-операции не укладываются
    // в TTL 15 с, чужой takeover не должен застать секцию открытой).
    [Fact]
    public async Task Reallocate_EnsureNode_RunsOutsidePortLock()
    {
        // Arrange: во время docker-фазы сосед пробует взять клэйм.
        var rig = await NewRig();
        var foreign = new PortAllocLock("/kafkaworker", [Ep], rig.Etcd, TimeProvider.System, "other-instance");
        var lockFreeDuringEnsure = false;
        rig.Driver.EnsureResultByNode = _ =>
        {
            lockFreeDuringEnsure = foreign.TryAcquireAsync(CancellationToken.None).GetAwaiter().GetResult().Value;
            return Result.Success();
        };

        // Act: лестница (ветка 3 — контейнера нет).
        var resolved = await rig.Healer.ResolveAsync(rig.Snapshot, "broker1", rig.Addresses, CancellationToken.None);

        // Assert: аллокация удалась, к моменту EnsureNode клэйм уже отпущен.
        resolved.IsSuccess.Should().BeTrue();
        lockFreeDuringEnsure.Should().BeTrue(
            "EnsureNode runs after the portalloc lock is released");
    }

    // AAA: ветка 3 — endpoints строятся из СВЕЖЕГО portalloc (после
    // освобождения лока): сосед, довыделивший broker2 за время нашей
    // docker-фазы, остаётся в bootstrap-списке (снимок секции устарел бы).
    [Fact]
    public async Task Reallocate_EndpointsBuiltFromFreshPortAlloc()
    {
        // Arrange: во время docker-фазы сосед закрепляет broker2 в portalloc.
        var rig = await NewRig();
        rig.Driver.EnsureResultByNode = _ =>
        {
            var current = rig.Etcd
                .GetAsync(Ep, "/kafkaworker/portalloc/events", CancellationToken.None)
                .GetAwaiter().GetResult().Value!.Value; // наш broker1 уже записан
            rig.Etcd.PutAsync(Ep, "/kafkaworker/portalloc/events",
                current[..^1] + @",""broker2"":{""host"":""h1"",""client"":21090}}",
                null, CancellationToken.None).GetAwaiter().GetResult();
            return Result.Success();
        };

        // Act: лестница (ветка 3 — контейнера нет).
        var resolved = await rig.Healer.ResolveAsync(rig.Snapshot, "broker1", rig.Addresses, CancellationToken.None);

        // Assert: endpoints содержат и наш, и соседский адрес.
        resolved.IsSuccess.Should().BeTrue();
        var allocatedPort = resolved.Value.Address.ClientPort.ToString();
        var endpoints = (await rig.Etcd.GetAsync(Ep, "/kafka/clusters/events/endpoints", CancellationToken.None)).Value;
        endpoints.Should().NotBeNull();
        endpoints!.Value.Should().Contain(allocatedPort, "our healed broker is in the bootstrap list");
        endpoints!.Value.Should().Contain("21090",
            "the neighbor's broker allocated during our docker phase stays in the bootstrap list");
    }

    // AAA: ветка 1 — адрес уже в portalloc → без записей (ранний выход
    // гарантирует вызывающий; healer это тоже уважает).
    [Fact]
    public async Task Resolve_PinnedAddress_ReturnsWithoutWrites()
    {
        // Arrange: portalloc сидирован закреплением.
        var rig = await NewRig(pinned: new NodeAddress("h1", 21010));

        // Act: лестница.
        var resolved = await rig.Healer.ResolveAsync(rig.Snapshot, "broker1", rig.Addresses, CancellationToken.None);

        // Assert: закрепление отдано, мутаций нет.
        resolved.Value.Address.Should().Be(new NodeAddress("h1", 21010));
        resolved.Value.Recreated.Should().BeFalse();
        rig.Driver.Ensured.Should().BeEmpty();
    }

    // AAA: ветка 2, проигрыш version==0 — сосед записал portalloc между
    // чтением и txn (гонка S5): OnTxnBeforeCompare сеет чужой ключ ДО
    // compare → txn NotExists проигрывает → re-read, адрес соседа = истина.
    [Fact]
    public async Task Resolve_ContainerAlive_TxnLostTakesForeignTruth()
    {
        // Arrange: живой контейнер + гонка (сосед пишет до compare).
        var rig = await NewRig();
        rig.Driver.Endpoints["broker1"] = new NodeEndpointInspection(
            "h1", 21037, "CLIENT://host.docker.internal:21037");
        rig.Etcd.OnTxnBeforeCompare = _ =>
        {
            // Сеём «соседа» до compare: ключ появился после нашего чтения.
            rig.Etcd.PutAsync(Ep, "/kafkaworker/portalloc/events",
                """{"broker1":{"host":"h1","client":21050}}""", null, CancellationToken.None).GetAwaiter().GetResult();
        };

        // Act: лестница.
        var resolved = await rig.Healer.ResolveAsync(rig.Snapshot, "broker1", rig.Addresses, CancellationToken.None);

        // Assert: первый записавший — истина (S5): re-read перекрывает инспекцию.
        resolved.IsSuccess.Should().BeTrue();
        resolved.Value.Address.Should().Be(new NodeAddress("h1", 21050),
            "first writer wins (S5): re-read overrides our inspection 21037");
        rig.Driver.Ensured.Should().BeEmpty();
    }

    // AAA: ветка 2, частичная утеря — ключ portalloc жив с записью соседа
    // broker2, записи нашего broker1 нет: RMW-merge дописывает broker1
    // адресом инспекции, запись соседа не затёрта, контейнер не тронут.
    [Fact]
    public async Task Resolve_ContainerAlive_PartialLossMergesIntoLiveKey()
    {
        // Arrange: ключ жив с чужой записью broker2 (нашей broker1 нет);
        // инспекция broker1 засеяна (живой контейнер).
        var rig = await NewRig();
        rig.Etcd.Seed("/kafkaworker/portalloc/events",
            """{"broker2":{"host":"h2","client":21020}}""");
        rig.Driver.Endpoints["broker1"] = new NodeEndpointInspection(
            "h1", 21037, "CLIENT://host.docker.internal:21037");

        // Act: лестница для broker1.
        var resolved = await rig.Healer.ResolveAsync(rig.Snapshot, "broker1", rig.Addresses, CancellationToken.None);

        // Assert: merge — ключ содержит и broker1 (адрес инспекции), и
        // соседа broker2 (запись не затёрта); контейнер не тронут.
        resolved.IsSuccess.Should().BeTrue();
        resolved.Value.Address.Should().Be(new NodeAddress("h1", 21037));
        resolved.Value.Recreated.Should().BeFalse("a live container is left untouched");
        var pinned = await ReadPortAllocAsync(rig.Etcd);
        pinned["broker1"].Should().Be(new NodeAddress("h1", 21037),
            "reconstructed address is pinned in etcd");
        pinned["broker2"].Should().Be(new NodeAddress("h2", 21020),
            "neighbor records survive the merge");
        rig.Driver.Ensured.Should().BeEmpty("a live container is left untouched");
        rig.Driver.Removed.Should().BeEmpty("a live container is left untouched");
    }

    // AAA: клэйм занят — без мутаций, PortLockBusyException наружу
    // (supervise → waiting-portalloc-lock, следующий тик).
    [Fact]
    public async Task Resolve_PortLockBusy_NoMutations()
    {
        // Arrange: клэйм держит сосед.
        var rig = await NewRig();
        await HoldPortLockAsync(rig);

        // Act: лестница (ветка 3 — контейнера нет).
        var resolved = await rig.Healer.ResolveAsync(rig.Snapshot, "broker1", rig.Addresses, CancellationToken.None);

        // Assert: PortLockBusyException, никаких записей под чужим клэймом.
        resolved.IsSuccess.Should().BeFalse();
        resolved.Error.Should().BeOfType<PortLockBusyException>();
        (await PortAllocJson(rig)).Should().BeNull("no writes under a foreign claim");
        rig.Driver.Ensured.Should().BeEmpty();
    }

    // AAA: ошибка инспекции (docker молчит) — никаких действий (S7:
    // «не можем проверить» ≠ «мёртв»), фейл тика.
    [Fact]
    public async Task Resolve_InspectionFails_NoActions()
    {
        // Arrange: инспекция падает (docker-хост недоступен).
        var rig = await NewRig();
        rig.Driver.EndpointFaultByNode = _ => Result<NodeEndpointInspection?>.Failed(
            new ApplicationException("docker host unreachable"));

        // Act: лестница.
        var resolved = await rig.Healer.ResolveAsync(rig.Snapshot, "broker1", rig.Addresses, CancellationToken.None);

        // Assert: слепота не лечится — фейл без мутаций.
        resolved.IsSuccess.Should().BeFalse();
        (await PortAllocJson(rig)).Should().BeNull();
        rig.Driver.Ensured.Should().BeEmpty();
    }
}
