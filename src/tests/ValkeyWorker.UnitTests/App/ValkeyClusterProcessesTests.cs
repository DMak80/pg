using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Shared.Etcd.Client;
using ValkeyWorker.App;
using ValkeyWorker.App.Loops;
using ValkeyWorker.UnitTests.App;
using ValkeyWorker.UnitTests.Provisioning;
using Xunit;

namespace ValkeyWorker.UnitTests.App;

// Цикл процессов (t06): Active-ветка — миграция TLS ПЕРВЫМ шагом, InProgress
// ⇒ надзор/converge/ротация в этом тике не идут (журнал work/<C> остаётся
// op=migrate-tls done — не перезаписан надзором).
public class ValkeyClusterProcessesTests
{
    private static readonly FixedTimeProvider Clock = new();

    private const string Image = "valkey/valkey:9.1.2";

    private sealed class Rig
    {
        public Fakes.FakeEtcd Etcd = new();
        public Fakes.FakeDriver Driver = new();
        public Fakes.FakeValkeyConnection Valkey = new() { TrustAnyPassword = true };
        public ClaimStore Claims = null!;
        public ValkeyClusterProcesses Processes = null!;

        public Rig()
        {
            Claims = new ClaimStore("/valkeyworker", ["http://etcd:2379"], Etcd, TimeProvider.System);
            var journal = new WorkJournal("/valkeyworker", Etcd, ["http://etcd:2379"]);
            var options = new ValkeyWorker.Provisioning.Processes.ValkeyProvisioningOptions(
                17000, 17999, 100, 90, "localhost", "valkey/valkey:9.1.2");
            var tlsProvisioner = new ValkeyWorker.Provisioning.Processes.NodeTlsProvisioner(Driver, Image, Clock);
            Processes = new ValkeyClusterProcesses(
                Etcd, new FixedOptionsMonitor(new ValkeyWorkerOptions
                {
                    Etcd = new EtcdOptions { Endpoints = ["http://etcd:2379"] },
                    Parallelism = new ParallelismOptions { MaxClusters = 1 },
                }),
                Claims, journal,
                new ValkeyWorker.Provisioning.Processes.ProvisioningProcess(
                    Etcd, ["http://etcd:2379"], Driver, Claims, journal,
                    new PortAllocLock("/valkeyworker", ["http://etcd:2379"], Etcd, TimeProvider.System, "inst-1"),
                    new ValkeyWorker.Provisioning.Processes.PortAllocIndex(
                        Etcd, ["http://etcd:2379"],
                        NullLogger<ValkeyWorker.Provisioning.Processes.PortAllocIndex>.Instance),
                    new ValkeyWorker.Provisioning.Processes.ClusterSecretEnsurer(Etcd, ["http://etcd:2379"]),
                    tlsProvisioner, Valkey, options),
                new ValkeyWorker.Provisioning.Processes.DeprovisioningProcess(
                    Etcd, ["http://etcd:2379"], Driver, Claims, journal),
                new ValkeyWorker.Provisioning.Processes.NodeSupervisor(
                    Etcd, ["http://etcd:2379"], Driver, Claims, journal, Valkey, options,
                    new ValkeyWorker.Provisioning.Processes.PortAllocHealer(
                        Etcd, ["http://etcd:2379"], Driver, Claims, journal,
                        new PortAllocLock("/valkeyworker", ["http://etcd:2379"], Etcd, TimeProvider.System, "inst-1"),
                        new ValkeyWorker.Provisioning.Processes.PortAllocIndex(
                            Etcd, ["http://etcd:2379"],
                            NullLogger<ValkeyWorker.Provisioning.Processes.PortAllocIndex>.Instance),
                        options),
                    tlsProvisioner, Clock),
                new ValkeyWorker.Provisioning.Processes.ConfigConverger(
                    Valkey, Etcd, ["http://etcd:2379"], journal),
                new ValkeyWorker.Provisioning.Processes.PasswordRotator(
                    Etcd, ["http://etcd:2379"], Claims, journal, Valkey,
                    ValkeyWorker.Core.Model.ValkeyPasswordGenerator.Generate),
                new ValkeyWorker.Provisioning.Processes.TlsMigrator(
                    Etcd, ["http://etcd:2379"], Driver, Claims, journal,
                    new ValkeyWorker.Provisioning.Processes.ClusterSecretEnsurer(Etcd, ["http://etcd:2379"]),
                    tlsProvisioner, Valkey, options, snapshot: null, Clock),
                new ValkeyWorker.Provisioning.Processes.CaRotator(
                    Etcd, ["http://etcd:2379"], Driver, Claims, journal,
                    tlsProvisioner, Valkey, options, snapshot: null, Clock),
                NullLogger<ValkeyClusterProcesses>.Instance);
        }

        // Канонический TLS-кластер (миграция t06 уже отработала): креды, CA-ключи,
        // endpoints, portalloc, state RUNNING, контейнер с TLS-args (порт CaRotator).
        public void SeedTlsCanonical(string cluster, int port = 17001)
        {
            Claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken).GetAwaiter().GetResult();
            Etcd.Seed($"/valkey/clusters/{cluster}/config",
                """{"nodes":1,"maxmemory_bytes":536870912,"maxmemory_policy":"allkeys-lru","created_unix":1756500000}""");
            Etcd.Seed($"/valkey/clusters/{cluster}/nodes/node1/state", "RUNNING");
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
                    ["valkey-server", "--tls-port", "6379", "--port", "0"], Image, "id-tls");
        }

        // Чтение журнала работы кластера (ассерты вентиля).
        public string? Work(string cluster)
            => Etcd.Store.TryGetValue($"/valkeyworker/work/{cluster}", out var kv) ? kv.Value : null;

        // Премиграционный Active-кластер: без ca-ключей, plain-контейнер.
        public void SeedPlain(string cluster, int port = 17001)
        {
            Claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken).GetAwaiter().GetResult();
            Etcd.Seed($"/valkey/clusters/{cluster}/config",
                """{"nodes":1,"maxmemory_bytes":536870912,"maxmemory_policy":"allkeys-lru","created_unix":1756500000}""");
            Etcd.Seed($"/valkey/clusters/{cluster}/nodes/node1/state", "RUNNING");
            Etcd.Seed($"/valkey/clusters/{cluster}/endpoints", $"localhost:{port}");
            Etcd.Seed($"/valkey/clusters/{cluster}/app_user", "app");
            Etcd.Seed($"/valkey/clusters/{cluster}/app_password", "AppPassword0123456789abcdef12345");
            Etcd.Seed($"/valkey/clusters/{cluster}/admin_user", "admin");
            Etcd.Seed($"/valkey/clusters/{cluster}/admin_password", "AdminPassword0123456789abcdef12345");
            Etcd.Seed($"/valkeyworker/portalloc/{cluster}", "{\"node1\":{\"host\":\"h1\",\"client\":" + port + "}}");
            Driver.Containers[$"vwk-{cluster}-node1"] =
                new Fakes.FakeDriver.ContainerFact("h1", port, 2m, 1024L * 1024 * 1024,
                    ["valkey-server", "--appendonly", "no"], "valkey/valkey:9.1.2", "id-plain");
        }
    }

    [Fact]
    public async Task ActiveTick_МиграцияПервой_НадзорОтложен()
    {
        // Arrange: премиграционный Active-кластер (plain-контейнер, без CA).
        var rig = new Rig();
        rig.SeedPlain("tlsfirst");

        // Act: один тик цикла.
        var claims = await rig.Processes.TickAsync(TestContext.Current.CancellationToken);

        // Assert: миграция доведена до done в ЭТОМ тике; журнал НЕ перезаписан
        // надзором (supervising) — надзор в этом тике не шёл (InProgress-выход).
        claims.Should().BeGreaterThanOrEqualTo(0);
        var journal = rig.Etcd.Store["/valkeyworker/work/tlsfirst"].Value;
        journal.Should().Contain("\"migrate-tls\"").And.Contain("\"done\"");
        journal.Should().NotContain("supervis");
        // Контейнер пересоздан с TLS-args (миграция T2).
        rig.Driver.Ensured.Should().ContainSingle();
        rig.Driver.Ensured[0].Args.Should().Contain("--tls-port");
    }

    [Fact]
    public async Task Active_CaRotationInProgress_SupervisorConvergerSkipped()
    {
        // Arrange — канонический TLS-кластер + заявка CA-ротации: тик
        // исполнит ротацию (окно было открыто в момент вызова K) и вернёт
        // InProgress — надзор/конвергер/ротатор в этом тике НЕ вызываются
        var rig = new Rig();
        rig.SeedTlsCanonical("gate1");
        rig.Etcd.Seed("/valkeyworker/ca_rotations/gate1",
            """{"requested_unix":1756500000,"requested_by":"it"}""");

        // Act
        await rig.Processes.TickAsync(TestContext.Current.CancellationToken);

        // Assert — журнал работы держит op=rotate-ca done: надзор НЕ
        // перезаписал его своим op («supervise» появился бы, если бы ветка
        // дошла до C) — эксклюзивность окна доказана journal'ом
        var work = rig.Work("gate1");
        work.Should().Contain("rotate-ca", "K исполнился и держит ветку");
        work.Should().Contain("done", "цикл завершён одним тиком");
        work.Should().NotContain("supervise", "надзор в окне не идёт");
        // Коммит прошёл: заявка снята (staging после C отсутствует —
        // ассертить его наличие НЕЛЬЗЯ)
        rig.Etcd.Store.TryGetValue("/valkeyworker/ca_rotations/gate1", out _)
            .Should().BeFalse("заявка снята атомарно коммиту фазы C");
    }

    [Fact]
    public async Task Active_CaRotationWaiting_PasswordRotationPlays()
    {
        // Arrange — канонический кластер + заявки: CA (K вернёт Waiting —
        // окно не открывается) И пароль (E доиграет тем же тиком ниже по
        // ветке). Журнал: K запишет waiting-password-rotation, но затем
        // надзор C и ротатор E перезапишут work/<C> своими op — финальное
        // состояние журнала «rotate done», а НЕ waiting-фаза K.
        var rig = new Rig();
        rig.SeedTlsCanonical("gate2");
        rig.Etcd.Seed("/valkeyworker/ca_rotations/gate2",
            """{"requested_unix":1756500000,"requested_by":"it"}""");
        rig.Etcd.Seed("/valkeyworker/rotations/gate2",
            """{"role":"app","requested_unix":1756500000,"requested_by":"it"}""");

        // Act
        await rig.Processes.TickAsync(TestContext.Current.CancellationToken);

        // Assert — ветка НЕ заблокирована Waiting: ротатор E доиграл свою
        // заявку этим же тиком (заявка rotations снята, журнал — op rotate
        // done); CA-заявка жива, окно НЕ открывалось
        rig.Etcd.Store.TryGetValue("/valkeyworker/rotations/gate2", out _)
            .Should().BeFalse("E доиграл заявку пароля этим же тиком");
        var work = rig.Work("gate2");
        work.Should().Contain("\"rotate\"", "журнал завершён ротатором E (K не заблокировал ветку)");
        work.Should().Contain("done", "E доведён до конца");
        rig.Etcd.Store.TryGetValue("/valkeyworker/ca_rotations/gate2", out _)
            .Should().BeTrue("CA-заявка ждёт (окно не открывалось)");
        rig.Etcd.Store.TryGetValue("/valkey/clusters/gate2/ca_next_key", out _)
            .Should().BeFalse("окно не открывалось");
    }
}
