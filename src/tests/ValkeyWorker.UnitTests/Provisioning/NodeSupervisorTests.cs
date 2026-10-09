using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Shared.Etcd.Client;
using ValkeyWorker.Core.Model;
using ValkeyWorker.Etcd.Parsing;
using ValkeyWorker.UnitTests.Provisioning;
using Xunit;

namespace ValkeyWorker.UnitTests.Provisioning;

// NodeSupervisor (arch/21 §5 C): снос/пересоздание, автоконверге лимитов,
// слепой docker-инспект (S7), UNREACHABLE по NodeDeadSec (таймаут и refused —
// молчание при живом docker-факте), E9, endpoints-RMW.
public class NodeSupervisorTests
{
    private static readonly FixedTimeProvider Clock = new();

    private sealed class Rig
    {
        public Fakes.FakeEtcd Etcd = new();
        public Fakes.FakeDriver Driver = new();
        public Fakes.FakeValkeyConnection Valkey = new() { TrustAnyPassword = true };
        public ClaimStore Claims = null!;
        public ValkeyWorker.Provisioning.Processes.NodeSupervisor Supervisor = null!;

        public static Rig Create()
        {
            var rig = new Rig();
            rig.Claims = new ClaimStore("/valkeyworker", ["http://etcd:2379"], rig.Etcd, TimeProvider.System);
            var healer = new ValkeyWorker.Provisioning.Processes.PortAllocHealer(
                rig.Etcd, ["http://etcd:2379"], rig.Driver, rig.Claims,
                new WorkJournal("/valkeyworker", rig.Etcd, ["http://etcd:2379"]),
                new PortAllocLock("/valkeyworker", ["http://etcd:2379"], rig.Etcd, TimeProvider.System, "inst-1"),
                new ValkeyWorker.Provisioning.Processes.PortAllocIndex(
                    rig.Etcd, ["http://etcd:2379"],
                    NullLogger<ValkeyWorker.Provisioning.Processes.PortAllocIndex>.Instance),
                new ValkeyWorker.Provisioning.Processes.ValkeyProvisioningOptions(
                    17000, 17999, 100, 90, "localhost", "valkey/valkey:9.1.2"));
            rig.Supervisor = new ValkeyWorker.Provisioning.Processes.NodeSupervisor(
                rig.Etcd, ["http://etcd:2379"], rig.Driver, rig.Claims,
                new WorkJournal("/valkeyworker", rig.Etcd, ["http://etcd:2379"]),
                rig.Valkey,
                new ValkeyWorker.Provisioning.Processes.ValkeyProvisioningOptions(
                    17000, 17999, 100, 90, "localhost", "valkey/valkey:9.1.2"),
                healer, Clock);
            return rig;
        }

        // Active-кластер на env-модели: config без state, креды, portalloc,
        // живой контейнер с валидным env VALKEY_TLS_* (серт текущего CA)
        // + клэйм прогона (надзор мутирует только под своим клэймом).
        public string SeedActive(string cluster, int port = 17001, string resources = """{"cpu":"2","mem":"1Gi","disk":"10Gi"}""")
        {
            Claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken).GetAwaiter().GetResult();
            Etcd.Seed($"/valkey/clusters/{cluster}/config",
                """{"nodes":1,"maxmemory_bytes":536870912,"maxmemory_policy":"allkeys-lru","created_unix":1756500000}""");
            Etcd.Seed($"/valkey/clusters/{cluster}/nodes/node1/state", "RUNNING");
            Etcd.Seed($"/valkey/clusters/{cluster}/nodes/node1/resources", resources);
            Etcd.Seed($"/valkey/clusters/{cluster}/endpoints", $"localhost:{port}");
            Etcd.Seed($"/valkey/clusters/{cluster}/app_user", "app");
            Etcd.Seed($"/valkey/clusters/{cluster}/app_password", "AppPassword0123456789abcdef12345");
            Etcd.Seed($"/valkey/clusters/{cluster}/admin_user", "admin");
            Etcd.Seed($"/valkey/clusters/{cluster}/admin_password", "AdminPassword0123456789abcdef12345");
            var (caPem, caKeyPem) = ValkeyWorker.Core.Valkey.ValkeyPki.GenerateCa(cluster);
            Etcd.Seed($"/valkey/clusters/{cluster}/ca_pem", caPem);
            Etcd.Seed($"/valkey/clusters/{cluster}/ca_key", caKeyPem);
            Etcd.Seed($"/valkeyworker/portalloc/{cluster}", "{\"node1\":{\"host\":\"h1\",\"client\":" + port + "}}");
            Driver.Containers[$"vwk-{cluster}-node1"] =
                new Fakes.FakeDriver.ContainerFact("h1", port, 2m, 1024L * 1024 * 1024,
                    ["valkey-server", "--appendonly", "no"],
                    ValkeyWorker.Provisioning.Processes.NodeTlsProvisioner.BuildNodeTlsEnv(
                        caPem, caKeyPem, "node1", "localhost"),
                    "valkey/valkey:9.1.2", "id1");
            return caPem;
        }

        public ValkeyClusterSnapshot Snapshot(string cluster)
        {
            var range = Etcd.RangeAsync("", "/valkey/clusters/", TestContext.Current.CancellationToken)
                .GetAwaiter().GetResult();
            return ValkeySnapshotParser.Parse(range.Value).Value.Clusters.First(c => c.Cluster == cluster);
        }
    }

    [Fact]
    public async Task СносКонтейнера_ПересозданиеТемиЖеКредамиИПортом()
    {
        // Arrange: контейнер снесён (объекта нет), portalloc на 17001.
        const string cluster = "gone";
        var rig = Rig.Create();
        rig.SeedActive(cluster);
        rig.Driver.Containers.Clear();

        // Act: тик надзора.
        var result = await rig.Supervisor.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: пересоздан (Ensure с cmd-обёрткой из etcd-кредов и env
        // VALKEY_TLS_*), state=PROVISIONING; следующий тик (PING ok) → RUNNING.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        var ensured = rig.Driver.Ensured.Should().ContainSingle().Subject;
        ensured.NodeName.Should().Be("node1");
        ensured.ClientHostPort.Should().Be(17001);
        ensured.Args.Should().HaveCount(3);
        ensured.Args[0].Should().Be("sh");
        ensured.Args[1].Should().Be("-c");
        ensured.Args[2].Should().Contain("'>AdminPassword0123456789abcdef12345'");
        ensured.Env.Should().NotBeNull().And.ContainKeys("VALKEY_TLS_CERT", "VALKEY_TLS_KEY", "VALKEY_TLS_CA");
        rig.Etcd.Store["/valkey/clusters/gone/nodes/node1/state"].Value.Should().Be("PROVISIONING");

        // следующий тик: PING → RUNNING
        var second = await rig.Supervisor.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);
        second.IsSuccess.Should().BeTrue(second.Error?.Message);
        rig.Etcd.Store["/valkey/clusters/gone/nodes/node1/state"].Value.Should().Be("RUNNING");
    }

    [Fact]
    public async Task АвтоконвергеЛимитов_ПересозданиеОдноЗаТик()
    {
        // Arrange: декларация cpu 2, контейнер cpu 1 (дрейф); env валиден.
        const string cluster = "drift";
        var rig = Rig.Create();
        rig.SeedActive(cluster);
        rig.Driver.Containers["vwk-drift-node1"] =
            new Fakes.FakeDriver.ContainerFact("h1", 17001, 1m, 1024L * 1024 * 1024,
                ["valkey-server", "--appendonly", "no"],
                rig.Driver.Containers["vwk-drift-node1"].Env, "valkey/valkey:9.1.2", "id1");

        // Act
        var result = await rig.Supervisor.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: пересоздание с лимитами декларации (cpu=2), ОДНО за тик.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Driver.Ensured.Should().ContainSingle();
        rig.Driver.Ensured[0].CpuCores.Should().Be(2m);
        rig.Etcd.Store["/valkey/clusters/drift/nodes/node1/state"].Value.Should().Be("PROVISIONING");

        // Совпадающие лимиты → пусто (второй прогон после ensure — уже 2 ядра).
        var second = await rig.Supervisor.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);
        second.IsSuccess.Should().BeTrue(second.Error?.Message);
        rig.Driver.Ensured.Should().HaveCount(1);
    }

    [Fact]
    public async Task СлепойInspect_FailedКонтейнерЖивStateНеМеняется()
    {
        // Arrange: docker-хост молчит на инспектах.
        const string cluster = "blind";
        var rig = Rig.Create();
        rig.SeedActive(cluster);
        rig.Driver.ResourcesFault = true;
        rig.Driver.EndpointFault = true;

        // Act
        var result = await rig.Supervisor.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: ошибка тика, пересозданий нет, state без изменений.
        result.IsSuccess.Should().BeFalse();
        rig.Driver.Ensured.Should().BeEmpty();
        rig.Etcd.Store["/valkey/clusters/blind/nodes/node1/state"].Value.Should().Be("RUNNING");
    }

    [Fact]
    public async Task Unreachable_ПоПорогуNodeDead_ПересозданиеЗатемRunning()
    {
        // Arrange: нода молчит; трек first_seen уже старше NodeDeadSec+1
        // (молчание копилось прошлыми тиками).
        const string cluster = "dead";
        var rig = Rig.Create();
        rig.SeedActive(cluster);
        rig.Valkey.Silent = true;
        var journal = new WorkJournal("/valkeyworker", rig.Etcd, ["http://etcd:2379"]);
        var stale = Clock.GetUtcNow().AddSeconds(-91).ToUnixTimeSeconds();
        await journal.WriteSupervisionAsync(
            "dead", "inst-1", new Dictionary<string, long> { ["node1"] = stale }, null,
            TestContext.Current.CancellationToken);

        // Act: тик с молчащей нодой (порог исчерпан).
        var result = await rig.Supervisor.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: UNREACHABLE + пересоздание; трек ноды сброшен (счётчик заново).
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Etcd.Store["/valkey/clusters/dead/nodes/node1/state"].Value.Should().Be("PROVISIONING");
        rig.Driver.Ensured.Should().ContainSingle();
        var track = await journal.ReadUnreachableAsync("dead", TestContext.Current.CancellationToken);
        track.Value.Should().NotContainKey("node1");

        // Затем: зрячая проба отвечает → RUNNING (PROVISIONING переводится надзором).
        rig.Valkey.Silent = false;
        var second = await rig.Supervisor.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);
        second.IsSuccess.Should().BeTrue(second.Error?.Message);
        rig.Etcd.Store["/valkey/clusters/dead/nodes/node1/state"].Value.Should().Be("RUNNING");
    }

    [Fact]
    public async Task СлепойDocker_ТикFailedТрекЗамороженБезДействий()
    {
        // Arrange: docker-хост молчит на инспектах (собственная слепота воркера,
        // S7); трек first_seen уже есть — слепой тик не смеет его двигать.
        const string cluster = "s7";
        var rig = Rig.Create();
        rig.SeedActive(cluster);
        var journal = new WorkJournal("/valkeyworker", rig.Etcd, ["http://etcd:2379"]);
        await journal.WriteSupervisionAsync(
            "s7", "inst-1", new Dictionary<string, long> { ["node1"] = 1000 }, null,
            TestContext.Current.CancellationToken);
        rig.Driver.EndpointFault = true;

        // Act
        var result = await rig.Supervisor.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: ошибка тика (слепой inspect — не проба), пересозданий нет,
        // state не менялся, трек не перезаписан.
        result.IsSuccess.Should().BeFalse();
        rig.Driver.Ensured.Should().BeEmpty();
        rig.Etcd.Store["/valkey/clusters/s7/nodes/node1/state"].Value.Should().Be("RUNNING");
        var track = await journal.ReadUnreachableAsync("s7", TestContext.Current.CancellationToken);
        track.Value!["node1"].Should().Be(1000, "слепой тик не двигает трек");
    }

    [Fact]
    public async Task ОстановленныйКонтейнер_RefusedМолчание_ПорогПересоздание()
    {
        // Arrange: docker stop (объект жив, Running=false), PING — connection
        // refused (не таймаут); трек молчания исчерпан прошлыми тиками.
        const string cluster = "stopped";
        var rig = Rig.Create();
        rig.SeedActive(cluster);
        rig.Driver.Stopped.Add("vwk-stopped-node1");
        rig.Valkey.ConnectionFault = true;
        var journal = new WorkJournal("/valkeyworker", rig.Etcd, ["http://etcd:2379"]);
        var stale = Clock.GetUtcNow().AddSeconds(-91).ToUnixTimeSeconds();
        await journal.WriteSupervisionAsync(
            "stopped", "inst-1", new Dictionary<string, long> { ["node1"] = stale }, null,
            TestContext.Current.CancellationToken);

        // Act: тик надзора.
        var result = await rig.Supervisor.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: refused при живом docker-факте — молчание (не слепая проба):
        // UNREACHABLE-путь → пересоздание, state=PROVISIONING.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Etcd.Store["/valkey/clusters/stopped/nodes/node1/state"].Value.Should().Be("PROVISIONING");
        rig.Driver.Ensured.Should().ContainSingle();

        // Затем: контейнер поднят (Running), проба отвечает → RUNNING.
        rig.Valkey.ConnectionFault = false;
        var second = await rig.Supervisor.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);
        second.IsSuccess.Should().BeTrue(second.Error?.Message);
        rig.Etcd.Store["/valkey/clusters/stopped/nodes/node1/state"].Value.Should().Be("RUNNING");
    }

    [Fact]
    public async Task E9_PortallocУтерян_РеконструкцияБезДеструктива()
    {
        // Arrange: portalloc-ключ удалён, контейнер жив на 17001.
        const string cluster = "e9";
        var rig = Rig.Create();
        rig.SeedActive(cluster);
        rig.Etcd.Store.Remove("/valkeyworker/portalloc/e9");

        // Act
        var result = await rig.Supervisor.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: реконструкция из inspect (ключ вернулся), пересоздания не было.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Etcd.Store.Should().ContainKey("/valkeyworker/portalloc/e9");
        rig.Etcd.Store["/valkeyworker/portalloc/e9"].Value.Should().Contain("17001");
        rig.Driver.Ensured.Should().BeEmpty();
        rig.Driver.Removed.Should().BeEmpty();
    }

    [Fact]
    public async Task EndpointsРасхождение_RmwККанону()
    {
        // Arrange: endpoints указывают не на portalloc-порт.
        const string cluster = "ep";
        var rig = Rig.Create();
        rig.SeedActive(cluster, port: 17001);
        rig.Etcd.Store["/valkey/clusters/ep/endpoints"] =
            new Fakes.FakeEtcd.Entry("localhost:19999", rig.Etcd.Store["/valkey/clusters/ep/endpoints"].ModRevision, 2);

        // Act
        var result = await rig.Supervisor.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: endpoints сошлись к portalloc-канону.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Etcd.Store["/valkey/clusters/ep/endpoints"].Value.Should().Be("localhost:17001");
    }

    // env-TLS: пересоздание (снос контейнера) поднимает ноду с env
    // VALKEY_TLS_* + cmd-обёрткой канона (арх/21 §5 C).
    [Fact]
    public async Task Supervise_Recreate_UsesEnvAndCmdWrapper()
    {
        // Arrange: Active-кластер на env-модели; контейнер снесён.
        const string cluster = "tlsrec";
        var rig = Rig.Create();
        rig.SeedActive(cluster);
        rig.Driver.Containers.Clear(); // снос контейнера — пересоздание

        // Act: тик надзора.
        var result = await rig.Supervisor.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: контейнер пересоздан с cmd-обёрткой (TLS-args канона —
        // вхождения экранированных литералов в строке обёртки) и env серта.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        var ensured = rig.Driver.Ensured.Should().ContainSingle().Subject;
        ensured.Args.Should().HaveCount(3);
        ensured.Args[0].Should().Be("sh");
        ensured.Args[1].Should().Be("-c");
        ensured.Args[2].Should().Contain("'--tls-port' '6379'")
            .And.Contain("'--tls-cert-file' '/tls/node.crt'");
        ensured.Env.Should().NotBeNull().And.ContainKeys("VALKEY_TLS_CERT", "VALKEY_TLS_KEY", "VALKEY_TLS_CA");
    }

    // env-TLS (Р9): валидный env живой ноды — пересоздания НЕТ (свежий серт
    // случаен, сверка по валидности — идемпотентность надзора).
    [Fact]
    public async Task Supervise_ValidEnv_NoRecreate()
    {
        // Arrange: Active-кластер, контейнер жив с валидным env (SeedActive).
        const string cluster = "envok";
        var rig = Rig.Create();
        rig.SeedActive(cluster);

        // Act: тик надзора.
        var result = await rig.Supervisor.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: пересозданий нет (лимиты совпадают, env валиден).
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Driver.Ensured.Should().BeEmpty();
        rig.Driver.Removed.Should().BeEmpty();
    }

    // env-TLS (AC2, миграция живого кластера): нода БЕЗ env (старая volume-
    // модель) → пересоздание с env ≤2 тиков; легаси-том vwk-<C>-tls удаляется
    // легаси-чисткой после перевода всех нод на env-модель.
    [Fact]
    public async Task Supervise_NoEnvNode_RecreatesWithEnvAndCleansLegacyVolume()
    {
        // Arrange: живой контейнер старой модели (env нет), осиротевший
        // легаси-том на месте.
        const string cluster = "migr";
        var rig = Rig.Create();
        rig.SeedActive(cluster);
        rig.Driver.Containers["vwk-migr-node1"] =
            rig.Driver.Containers["vwk-migr-node1"] with { Env = null };
        rig.Driver.LegacyVolumes.Add($"vwk-{cluster}-tls");

        // Act: первый тик — env-сверка невалидна → пересоздание с env.
        var first = await rig.Supervisor.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: пересоздан с env (все ноды кластера теперь на env-модели) —
        // легаси-том удалён легаси-чисткой этого же тика.
        first.IsSuccess.Should().BeTrue(first.Error?.Message);
        rig.Driver.Ensured.Should().ContainSingle();
        rig.Driver.Ensured[0].Env.Should().NotBeNull();
        rig.Driver.LegacyVolumes.Should().NotContain($"vwk-{cluster}-tls");
        rig.Etcd.Store["/valkey/clusters/migr/nodes/node1/state"].Value.Should().Be("PROVISIONING");

        // Второй тик: нода отвечает → RUNNING; пересозданий больше нет.
        var second = await rig.Supervisor.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);
        second.IsSuccess.Should().BeTrue(second.Error?.Message);
        rig.Etcd.Store["/valkey/clusters/migr/nodes/node1/state"].Value.Should().Be("RUNNING");
        rig.Driver.Ensured.Should().HaveCount(1);
    }

    // env-TLS (409 легаси-чистки): том занят — Failed утилиты НЕ фейлит тик
    // надзора и НЕ пишется в warnings (безусловный ретрай следующим тиком).
    [Fact]
    public async Task Supervise_LegacyVolumeInUse_TickGreenNoWarning()
    {
        // Arrange: кластер на env-модели (env валиден), легаси-том занят (409).
        const string cluster = "busy";
        var rig = Rig.Create();
        rig.SeedActive(cluster);
        rig.Driver.LegacyVolumes.Add($"vwk-{cluster}-tls");
        rig.Driver.LegacyVolumeDeleteFault = _ => true; // 409 volume-in-use

        // Act: тик надзора.
        var result = await rig.Supervisor.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: тик зелёный, пересозданий нет, warning про том нет (том
        // остался — ретрай следующим тиком), стационарная запись без warning.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Driver.Ensured.Should().BeEmpty();
        rig.Driver.LegacyVolumes.Should().Contain($"vwk-{cluster}-tls");
        using var doc = System.Text.Json.JsonDocument.Parse(rig.Etcd.Store[$"/valkeyworker/work/{cluster}"].Value);
        doc.RootElement.TryGetProperty("last_error", out _).Should().BeFalse();
    }

    // t06: CA в etcd отсутствует — пересоздание отложено (миграция T доиграет),
    // warning надзора, без docker-мутаций.
    [Fact]
    public async Task Supervise_NoCaInEtcd_RecreationDeferred()
    {
        // Arrange: Active-кластер без ca_pem/ca_key (премиграционный).
        const string cluster = "noca";
        var rig = Rig.Create();
        rig.SeedActive(cluster);
        rig.Etcd.Store.Remove("/valkey/clusters/noca/ca_pem");
        rig.Etcd.Store.Remove("/valkey/clusters/noca/ca_key");
        rig.Driver.Containers.Clear();

        // Act: тик надзора.
        var result = await rig.Supervisor.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: тик зелёный (отложено, не провал), пересозданий нет,
        // в work/<C> — supervision-warning.
        result.IsSuccess.Should().BeTrue();
        rig.Driver.Ensured.Should().BeEmpty();
        rig.Driver.Removed.Should().BeEmpty();
        // last_error в journal — JSON-экранированный: декодируем.
        using var doc = System.Text.Json.JsonDocument.Parse(rig.Etcd.Store["/valkeyworker/work/noca"].Value);
        doc.RootElement.GetProperty("last_error").GetString()
            .Should().Contain("пересоздание отложено");
    }
}
